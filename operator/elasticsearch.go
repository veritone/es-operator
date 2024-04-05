package operator

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"net/url"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	kube_record "k8s.io/client-go/tools/record"
	metrics "k8s.io/metrics/pkg/client/clientset/versioned"
)

const (
	esDataSetLabelKey                       = "es-operator-dataset"
	esOperatorAnnotationKey                 = "es-operator.zalando.org/operator"
	esScalingOperationKey                   = "es-operator.zalando.org/current-scaling-operation"
	defaultElasticsearchDataSetEndpointPort = 9200
)

type ElasticsearchOperator struct {
	logger                *log.Entry
	kube                  *kubernetes.Clientset
	metrics               *metrics.Clientset
	interval              time.Duration
	autoscalerInterval    time.Duration
	metricsInterval       time.Duration
	priorityNodeSelectors labels.Set
	operatorID            string
	namespace             string
	clusterDNSZone        string
	elasticsearchEndpoint *url.URL
	operating             map[types.UID]operatingEntry
	sync.Mutex
	recorder    kube_record.EventRecorder
	stsSelector string
}

type operatingEntry struct {
	cancel context.CancelFunc
	doneCh <-chan struct{}
	logger *log.Entry
}

// NewElasticsearchOperator initializes a new ElasticsearchDataSet operator instance.
func NewElasticsearchOperator(
	kube *kubernetes.Clientset,
	metrics *metrics.Clientset,
	priorityNodeSelectors map[string]string,
	interval,
	autoscalerInterval time.Duration,
	operatorID,
	namespace,
	clusterDNSZone string,
	elasticsearchEndpoint *url.URL,
) *ElasticsearchOperator {

	stsSelector := flag.String("sts-selector", "es.operator/enabled", "Find participating ES StatefulSets")

	return &ElasticsearchOperator{
		logger: log.WithFields(
			log.Fields{
				"operator": "elasticsearch",
			},
		),
		kube:                  kube,
		metrics:               metrics,
		interval:              interval,
		autoscalerInterval:    autoscalerInterval,
		metricsInterval:       60 * time.Second,
		priorityNodeSelectors: labels.Set(priorityNodeSelectors),
		operatorID:            operatorID,
		namespace:             namespace,
		clusterDNSZone:        clusterDNSZone,
		elasticsearchEndpoint: elasticsearchEndpoint,
		operating:             make(map[types.UID]operatingEntry),
		recorder:              createEventRecorder(kube),
		stsSelector:           *stsSelector,
	}
}

// Run runs the main loop of the operator.
func (o *ElasticsearchOperator) Run(ctx context.Context) {

	o.logger.Info("Run Loop...")

	o.logger.Info("Run Loop, run auto scalar")
	go o.runAutoscaler(ctx)

	o.runWatch(ctx)
	<-ctx.Done()

	o.logger.Info("Terminating main operator loop.")
}

// Run setups up a shared informer for listing and watching changes to pods and
// starts listening for events.
func (o *ElasticsearchOperator) runWatch(ctx context.Context) {
	informer := cache.NewSharedIndexInformer(
		cache.NewListWatchFromClient(
			o.kube.AppsV1().RESTClient(),
			"es-operator",
			o.namespace, fields.Everything(),
		),
		&appsv1.StatefulSet{},
		0, // skip resync
		cache.Indexers{},
	)

	informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			o.add(ctx, obj)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			o.update(ctx, oldObj, newObj)
		},
		DeleteFunc: func(obj interface{}) {
			o.del(ctx, obj)
		},
	})

	go informer.Run(ctx.Done())

	if !cache.WaitForCacheSync(ctx.Done(), informer.HasSynced) {
		log.Errorf("Timed out waiting for caches to sync")
		return
	}

	log.Info("Synced es-operator watcher")
}

func (o *ElasticsearchOperator) resolveAndOperateSTS(ctx context.Context, source string, obj interface{}) {
	sts, ok := obj.(*appsv1.StatefulSet)
	if !ok {
		log.Errorf("%s handler failed to get StatefulSet object", source)
		return
	}

	log.Infof("%s handler will search for STS %s.%s and check for qualification", source, sts.Namespace, sts.Name)

	resources, err := o.collectResources(ctx)
	if err != nil {
		for _, esr := range resources {
			log.Infof("%s handler is qualifying STS %s.%s against known STS es resource %s.%s [%t]", source, sts.Namespace, sts.Name, esr.sts.Namespace, esr.sts.Name, esr.enabled)
			if esr.enabled {
				if esr.sts.UID == sts.UID {
					err := o.operateES(esr, false)
					if err != nil {
						log.Errorf("Add %s is unable to operate %s.%s: %v", source, esr.sts.Namespace, esr.sts.Name, err)
					}
				}
			}
		}
	} else {
		log.Errorf("Add %s is unable to collect STS resources: %v", source, err)
	}
}

func (o *ElasticsearchOperator) add(ctx context.Context, obj interface{}) {
	o.resolveAndOperateSTS(ctx, "Add", obj)
}

func (o *ElasticsearchOperator) update(ctx context.Context, oldObj, newObj interface{}) {
	o.resolveAndOperateSTS(ctx, "Update", newObj)
}

func (o *ElasticsearchOperator) del(ctx context.Context, obj interface{}) {
	o.resolveAndOperateSTS(ctx, "Delete", obj)
}

// runAutoscaler runs the EDS autoscaler which checks at an interval if any
// EDS resources needs to be autoscaled. If autoscaling is needed this will be
// indicated on the EDS with the
// 'es-operator.zalando.org/current-scaling-operation' annotation. The
// annotation indicates the desired scaling which will be reconciled by the
// operator.
func (o *ElasticsearchOperator) runAutoscaler(ctx context.Context) {
	nextCheck := time.Now().Add(-o.autoscalerInterval)

	for {
		o.logger.Infof("Checking autoscaling at interval %d", o.autoscalerInterval)

		select {
		case <-time.After(time.Until(nextCheck)):

			o.logger.Info("Autoscaling interval met")

			nextCheck = time.Now().Add(o.autoscalerInterval)

			o.logger.Info("Collect resources now...")
			resources, err := o.collectResources(ctx)
			if err != nil {
				o.logger.Errorf("Error collecting resources %v", err)
				continue
			}

			o.logger.Info("Try scaling...")
			for _, esr := range resources {
				o.logger.Infof("Is this esr %s.%s enabled? %t", esr.sts.Namespace, esr.sts.Name, esr.enabled)
				if esr.enabled {
					o.logger.Infof("Scale ESR now... %s.%s", esr.sts.Namespace, esr.sts.Name)
					err := o.scaleESR(ctx, esr)
					if err != nil {
						o.logger.Errorf("Error while in scaleESR: %v", err)
						continue
					}
				}
			}

			o.logger.Info("Done for now...\n\n\n")

		case <-ctx.Done():
			o.logger.Info("Terminating autoscaler loop.")
			return
		}
	}
}

// Drain drains a pod for Elasticsearch data.
func (esr *ESResource) Drain(ctx context.Context, pod *v1.Pod) error {
	if esr.skipDraining {
		return nil
	}
	return esr.esClient.Drain(ctx, pod)
}

// PreScaleDownHook ensures that the IndexReplicas is set as defined in the EDS
// 'scaling-operation' annotation prior to scaling down the internal
// StatefulSet.
func (esr *ESResource) PreScaleDownHook(ctx context.Context) error {
	return esr.applyScalingOperation(ctx)
}

// OnStableReplicasHook ensures that the indexReplicas is set as defined in the
// EDS scaling-operation annotation.
func (esr *ESResource) OnStableReplicasHook(ctx context.Context) error {
	err := esr.applyScalingOperation(ctx)
	if err != nil {
		return err
	}

	// cleanup state in ES
	return esr.esClient.Cleanup(ctx)
}

// UpdateStatus updates the status of the EDS to set the current replicas from
// StatefulSet and updating the observedGeneration.
func (o *ElasticsearchOperator) UpdateStatus(ctx context.Context, esr *ESResource) error {
	observedGeneration := esr.sts.Status.ObservedGeneration

	replicas := int32(0)
	if esr.sts.Spec.Replicas != nil {
		replicas = *esr.sts.Spec.Replicas
	}

	if esr.sts.Generation != observedGeneration ||
		esr.sts.Status.Replicas != replicas {
		esr.sts.Status.Replicas = replicas
		esr.sts.Status.ObservedGeneration = esr.sts.Generation
		sts, err := o.kube.AppsV1().StatefulSets(esr.sts.Namespace).UpdateStatus(ctx, esr.sts, metav1.UpdateOptions{})
		o.logger.Logf(log.DebugLevel, "sts status updated %s.%s", sts.Namespace, sts.Name)
		if err != nil {
			return err
		}
	}

	return nil
}

func (esr *ESResource) applyScalingOperation(ctx context.Context) error {
	operation, err := esrScalingOperation(esr)
	if err != nil {
		return err
	}

	if operation != nil && operation.ScalingDirection != NONE {
		err = esr.esClient.UpdateIndexSettings(operation.IndexReplicas)
		if err != nil {
			return err
		}
		err = esr.removeScalingOperationAnnotation(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

// removeScalingOperationAnnotation removes the 'scaling-operation' annotation
// from the EDS.
// If the annotation is already gone, this is a no-op.
func (esr *ESResource) removeScalingOperationAnnotation(ctx context.Context) error {
	if _, ok := esr.sts.Annotations[esScalingOperationKey]; ok {
		delete(esr.sts.Annotations, esScalingOperationKey)
		_, err := esr.kube.AppsV1().StatefulSets(esr.sts.Namespace).
			Update(ctx, esr.sts, metav1.UpdateOptions{})
		if err != nil {
			return fmt.Errorf("failed to remove 'scaling-operating' annotation from ES STS: %v", err)
		}
	}
	return nil
}

func (o *ElasticsearchOperator) operateES(esr *ESResource, deleted bool) error {

	// insert into operating
	o.Lock()
	defer o.Unlock()

	// restart if already being operated on.
	if entry, ok := o.operating[esr.sts.UID]; ok {
		entry.cancel()
		// wait for previous operation to terminate
		entry.logger.Infof("Waiting for operation to stop")
		<-entry.doneCh
	}

	if deleted {
		return nil
	}

	doneCh := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())

	logger := log.WithFields(log.Fields{
		"name":      esr.sts.Name,
		"namespace": esr.sts.Namespace,
	})
	// add to operating table
	o.operating[esr.sts.UID] = operatingEntry{
		cancel: cancel,
		doneCh: doneCh,
		logger: logger,
	}

	operator := &Operator{
		kube:                  o.kube,
		priorityNodeSelectors: o.priorityNodeSelectors,
		interval:              o.interval,
		logger:                logger,
		recorder:              o.recorder,
	}

	go operator.Run(ctx, doneCh, esr)

	return nil
}

func (o *ElasticsearchOperator) getElasticsearchEndpoint(eds *appsv1.StatefulSet) *url.URL {
	if o.elasticsearchEndpoint != nil {
		return o.elasticsearchEndpoint
	}

	// TODO: discover port from EDS
	return &url.URL{
		Scheme: "http",
		Host: fmt.Sprintf(
			"%s.%s.svc.%s:%d",
			eds.Name,
			eds.Namespace,
			o.clusterDNSZone,
			defaultElasticsearchDataSetEndpointPort,
		),
	}
}

// getReplicas returns the desired node replicas.
// In case it was not specified, it will return '1'.
func getReplicas(esr *ESResource) int {
	replicas := esr.replicas

	if esr.enabled {
		replicas = int(math.Max(float64(replicas), float64(esr.minReplicas)))
	}
	return replicas
}

// collectResources collects all the ElasticsearchDataSet resources and there
// corresponding StatefulSets if they exist.
func (o *ElasticsearchOperator) collectResources(ctx context.Context) (map[types.UID]*ESResource, error) {

	o.logger.Info("Collect Resources")

	resources := make(map[types.UID]*ESResource)

	// Find all participating ES StatefulSets by label
	statefulSets, err := o.kube.AppsV1().StatefulSets(o.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: o.stsSelector,
	})
	if err == nil {
		o.logger.Infof("Retrieved a list of statefulsets based on selector %s, size %d", o.stsSelector, statefulSets.Size())

		for _, sts := range statefulSets.Items {

			o.logger.Infof("Add %s.%s to list of resources", sts.Namespace, sts.Name)

			endpoint := o.getElasticsearchEndpoint(&sts)

			o.logger.Infof("ES endpoint for %s.%s is %s", sts.Namespace, sts.Name, endpoint)

			// TODO: abstract this
			client := &ESClient{
				Endpoint: endpoint,
			}

			esr := &ESResource{
				kube:     o.kube,
				esClient: client,
				sts:      &sts,
				logger: log.WithFields(
					log.Fields{
						"es-resource": sts.Namespace + "." + sts.Name,
					},
				),
			}
			esr.getScalingConfiguration(&sts)

			resources[sts.UID] = esr

			selector := sts.Spec.Selector.MatchLabels

			labelSelector := ""
			for key, value := range selector {
				labelSelector += fmt.Sprintf("%s=%s,", key, value)
			}
			labelSelector = labelSelector[:len(labelSelector)-1]

			pods, err := o.kube.CoreV1().Pods(sts.Namespace).List(ctx, metav1.ListOptions{LabelSelector: labelSelector})
			if err != nil {
				o.logger.Errorf("Error gethering pods for %s.%s: %v", sts.Namespace, sts.Name, err)
			}

			for _, pod := range pods.Items {
				o.logger.Infof("Pod %s in sts %s.%s", pod.Name, sts.Namespace, sts.Name)

				podMetrics, err := o.metrics.MetricsV1beta1().PodMetricses(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
				if err != nil {
					o.logger.Errorf("Error gethering metrics for pod %s for %s.%s: %v", pod.Name, sts.Namespace, sts.Name, err)
				}

				for _, container := range podMetrics.Containers {
					cpuUsageNanoCores := container.Usage.Cpu().MilliValue()
					cpuUsageMilliCores := cpuUsageNanoCores / 1000000
					o.logger.Infof("Container %s in sts %s.%s has cpu amount of %dnc %dmc", container.Name, sts.Namespace, sts.Name, cpuUsageNanoCores, cpuUsageMilliCores)
				}
			}
		}
	} else {
		o.logger.Errorf("Error retreiving the list of elastic search clusters: %v", err)
	}

	return resources, nil
}

func (o *ElasticsearchOperator) scaleESR(ctx context.Context, esr *ESResource) error {
	err := validateScalingSettings(esr)
	if err != nil {
		o.recorder.Event(esr.sts, v1.EventTypeWarning, "ScalingInvalid", fmt.Sprintf(
			"Scaling settings are invalid: %v", err),
		)
		return err
	}

	log.Infof("scaleESR => esrScalingOperation %s.%s", esr.sts.Namespace, esr.sts.Name)

	// first, try to find an existing annotation and return it
	scalingOperation, err := esrScalingOperation(esr)
	if err != nil {
		log.Errorf("esrScalingOperation %s.%s failed: %v", esr.sts.Namespace, esr.sts.Name, err)
		return err
	}

	// exit early if the scaling operation is already defined
	if scalingOperation != nil && scalingOperation.ScalingDirection != NONE {
		return nil
	}

	currentReplicas := getReplicas(esr)
	as := NewAutoScaler(esr, o.metricsInterval)

	if esr.enabled {
		scalingOperation, err := as.GetScalingOperation()
		if err != nil {
			return err
		}

		// update EDS definition.
		if scalingOperation.NodeReplicas != nil && *scalingOperation.NodeReplicas != currentReplicas {
			now := metav1.Now()
			if *scalingOperation.NodeReplicas > currentReplicas {
				esr.lastScaleUpStarted = &now
			} else {
				esr.lastScaleDownStarted = &now
			}
			log.Infof("Updating last scaling event in EDS '%s/%s'", esr.sts.Namespace, esr.sts.Name)

			// update status
			sts, err := o.kube.AppsV1().StatefulSets(esr.sts.Namespace).UpdateStatus(ctx, esr.sts, metav1.UpdateOptions{})
			if err != nil {
				return err
			}
			log.Infof("%s.%s Updated", sts.Namespace, sts.Name)
		}

		// TODO: move to a function
		jsonBytes, err := json.Marshal(scalingOperation)
		if err != nil {
			return err
		}

		if scalingOperation.ScalingDirection != NONE {
			esr.sts.Annotations[esScalingOperationKey] = string(jsonBytes)

			log.Infof("Updating desired scaling for ESR '%s/%s'. New desired replicas: %d. %s", esr.sts.Namespace, esr.sts.Name, esr.replicas, scalingOperation.Description)
			_, err = o.kube.AppsV1().StatefulSets(esr.sts.Namespace).Update(ctx, esr.sts, metav1.UpdateOptions{})
			if err != nil {
				return err
			}
		}

	}

	return nil
}

func getOwnerUID(objectMeta metav1.ObjectMeta) (types.UID, bool) {
	if len(objectMeta.OwnerReferences) == 1 {
		return objectMeta.OwnerReferences[0].UID, true
	}
	return "", false
}

// templateInjectLabels injects labels into a pod template spec.
func templateInjectLabels(template v1.PodTemplateSpec, labels map[string]string) v1.PodTemplateSpec {
	if template.ObjectMeta.Labels == nil {
		template.ObjectMeta.Labels = map[string]string{}
	}

	for key, value := range labels {
		if _, ok := template.ObjectMeta.Labels[key]; !ok {
			template.ObjectMeta.Labels[key] = value
		}
	}
	return template
}

// edsScalingOperation returns the scaling operation read from the
// scaling-operation annotation on the EDS. If no operation is defined it will
// return nil.
func esrScalingOperation(esr *ESResource) (*ScalingOperation, error) {
	log.Infof("esrScalingOperation => get annotations %s.%s: %s:%v", esr.sts.Namespace, esr.sts.Name, esScalingOperationKey, esr.sts.Annotations[esScalingOperationKey])
	if op, ok := esr.sts.Annotations[esScalingOperationKey]; ok {
		scalingOperation := &ScalingOperation{}
		err := json.Unmarshal([]byte(op), scalingOperation)
		if err != nil {
			return nil, err
		}
		return scalingOperation, nil
	}
	return nil, nil
}

// validateScalingSettings checks that the scaling settings are valid.
//
//   - min values can not be > than the corresponding max values.
//   - min can not be less than 0.
//   - 'replicas', 'indexReplicas' and 'shardsPerNode' settings should not
//     conflict.
func validateScalingSettings(esr *ESResource) error {
	esr.logger.Log(log.DebugLevel, "validate scaling")
	// don't validate if scaling is not enabled
	if esr.enabled {

		// check that min is not greater than max values
		if esr.minReplicas > esr.maxReplicas {
			return fmt.Errorf(
				"minReplicas(%d) can't be greater than maxReplicas(%d)",
				esr.minReplicas,
				esr.maxReplicas,
			)
		}

		if esr.minIndexReplicas > esr.maxIndexReplicas {
			return fmt.Errorf(
				"minIndexReplicas(%d) can't be greater than maxIndexReplicas(%d)",
				esr.minIndexReplicas,
				esr.maxIndexReplicas,
			)
		}

		if esr.minShardsPerNode > esr.maxShardsPerNode {
			return fmt.Errorf(
				"minShardsPerNode(%d) can't be greater than maxShardsPerNode(%d)",
				esr.minShardsPerNode,
				esr.maxShardsPerNode,
			)
		}

		// ensure that relation between replicas and indexReplicas is valid
		if esr.minReplicas < (esr.minIndexReplicas + 1) {
			return fmt.Errorf(
				"minReplicas(%d) can not be less than minIndexReplicas(%d)+1",
				esr.minReplicas,
				esr.minIndexReplicas,
			)
		}

		if esr.maxReplicas < (esr.maxIndexReplicas + 1) {
			return fmt.Errorf(
				"maxReplicas(%d) can not be less than maxIndexReplicas(%d)+1",
				esr.maxReplicas,
				esr.maxIndexReplicas,
			)
		}
	}
	return nil
}
