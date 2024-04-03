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
	pv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
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
	go o.collectMetrics(ctx)
	go o.runAutoscaler(ctx)

	o.logger.Info("Terminating main operator loop.")
}

// collectMetrics collects metrics for all the managed EDS resources.
// The metrics are stored in the coresponding ElasticsearchMetricSet and used
// by the autoscaler for scaling EDS.
func (o *ElasticsearchOperator) collectMetrics(ctx context.Context) {
	nextCheck := time.Now().Add(-o.metricsInterval)

	for {
		o.logger.Debug("Collecting metrics")
		select {
		case <-time.After(time.Until(nextCheck)):
			nextCheck = time.Now().Add(o.metricsInterval)

			resources, err := o.collectResources(ctx)
			if err != nil {
				o.logger.Error(err)
				continue
			}

			for _, esr := range resources {
				if esr.enabled {
					metrics := &ElasticsearchMetricsCollector{
						kube:   o.kube,
						logger: log.WithFields(log.Fields{"collector": "metrics"}),
						esr:    *esr,
					}
					err := metrics.collectMetrics(ctx)
					if err != nil {
						o.logger.Error(err)
						continue
					}
				}
			}
		case <-ctx.Done():
			o.logger.Info("Terminating metrics collector loop.")
			return
		}
	}
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
		o.logger.Debug("Checking autoscaling")
		select {
		case <-time.After(time.Until(nextCheck)):
			nextCheck = time.Now().Add(o.autoscalerInterval)

			resources, err := o.collectResources(ctx)
			if err != nil {
				o.logger.Error(err)
				continue
			}

			for _, esr := range resources {
				if esr.enabled {
					endpoint := o.getElasticsearchEndpoint(esr.sts)

					esr.esClient = &ESClient{
						Endpoint:             endpoint,
						excludeSystemIndices: true,
					}

					err := o.scaleESR(ctx, esr)
					if err != nil {
						o.logger.Error(err)
						continue
					}
				}
			}
		case <-ctx.Done():
			o.logger.Info("Terminating autoscaler loop.")
			return
		}
	}
}

// ensurePodDisruptionBudget creates a PodDisruptionBudget for the
// ElasticsearchDataSet if it doesn't already exist.
func (o *ElasticsearchOperator) ensurePodDisruptionBudget(ctx context.Context, esr *ESResource) error {
	var pdb *pv1.PodDisruptionBudget
	var err error

	pdb, err = o.kube.PolicyV1().PodDisruptionBudgets(esr.sts.Namespace).Get(ctx, esr.sts.Name, metav1.GetOptions{})
	if err != nil {
		if !errors.IsNotFound(err) {
			return fmt.Errorf(
				"failed to get PodDisruptionBudget for %s %s/%s: %v",
				esr.sts.Kind,
				esr.sts.Namespace, esr.sts.Name,
				err,
			)
		}
		pdb = nil
	}

	// Removed owner check that was here...

	// Removed selector

	createPDB := false

	if pdb == nil {
		createPDB = true
		maxUnavailable := intstr.FromInt(0)
		pdb = &pv1.PodDisruptionBudget{
			ObjectMeta: metav1.ObjectMeta{
				Name:      esr.sts.Name,
				Namespace: esr.sts.Namespace,
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion: esr.sts.APIVersion,
						Kind:       esr.sts.Kind,
						Name:       esr.sts.Name,
						UID:        esr.sts.UID,
					},
				},
			},
			Spec: pv1.PodDisruptionBudgetSpec{
				MaxUnavailable: &maxUnavailable,
			},
		}
	}

	if createPDB {
		var err error
		_, err = o.kube.PolicyV1().PodDisruptionBudgets(pdb.Namespace).Create(ctx, pdb, metav1.CreateOptions{})
		if err != nil {
			return fmt.Errorf(
				"failed to create PodDisruptionBudget for %s %s/%s: %v",
				esr.sts.Kind,
				esr.sts.Namespace, esr.sts.Name,
				err,
			)
		}
		r.recorder.Event(esr.sts, v1.EventTypeNormal, "CreatedPDB", fmt.Sprintf(
			"Created PodDisruptionBudget '%s/%s' for %s",
			pdb.Namespace, pdb.Name, esr.sts.Kind,
		))
	}

	return nil
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
		"es":        esr.sts.Name,
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
	resources := make(map[types.UID]*ESResource)

	// Find all participating ES StatefulSets by label
	statefulSets, err := o.kube.AppsV1().StatefulSets(o.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: o.stsSelector,
	})

	for _, sts := range statefulSets.Items {
		endpoint := o.getElasticsearchEndpoint(&sts)

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

		resources[sts.UID] = esr

		metricSets, err := o.metrics.MetricsV1beta1().PodMetricses(sts.Namespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			return nil, err
		}

		pods, err := o.kube.CoreV1().Pods(o.namespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			return nil, err
		}

		cpuUsage := getCPUUsagePercent(metricSets.Items, pods.Items)
		o.logger.Debugf("The cpu utlization accross all pods is %f", cpuUsage)

		// Let's revisit this, not sure what is happening here, why it is needed
		for _, pod := range pods.Items {
			for _, esr := range resources {
				if v, ok := pod.Labels[esDataSetLabelKey]; ok && v == esr.sts.Name {
					esr.pods = append(esr.pods, pod)
					break
				}
			}
		}
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

	// first, try to find an existing annotation and return it
	scalingOperation, err := esrScalingOperation(esr)
	if err != nil {
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
