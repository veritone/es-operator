package operator

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/runtime"

	"github.com/cenk/backoff"
	log "github.com/sirupsen/logrus"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	kube_record "k8s.io/client-go/tools/record"
)

const (
	operatorPodDrainingAnnotationKey      = "operator.zalando.org/draining"
	operatorParentGenerationAnnotationKey = "operator.zalando.org/parent-generation"
	controllerRevisionHashLabelKey        = "controller-revision-hash"
	// podEvictionHeadroom is the extra time we wait to catch situations when the Pod is ignoring SIGTERM and
	// is killed with SIGKILL after TerminationGracePeriodSeconds
	// Same headroom as the cluster-autoscaler:
	// https://github.com/kubernetes/autoscaler/blob/cluster-autoscaler-1.2.2/cluster-autoscaler/core/scale_down.go#L77
	podEvictionHeadroom = 30 * time.Second
	// stabilizationTimeout defines the max timeout for a StatefulSet to
	// stabilize.
	stabilizationTimeout = 10 * time.Minute
)

type StatefulResourceGetter interface {
	Get(ctx context.Context) (StatefulResource, error)
}

type StatefulResource interface {
	// Name returns the name of the resource.
	Name() string
	// Namespace returns the namespace where the resource is located.
	Namespace() string
	// APIVersion returns the APIVersion of the resource.
	APIVersion() string
	// Kind returns the kind of the resource.
	Kind() string
	// Generation returns the generation of the resource.
	Generation() int64
	// UID returns the uid of the resource.
	UID() types.UID
	// Labels returns the labels of the resource.
	Labels() map[string]string
	// LabelSelector returns a set of labels to be used for label selecting.
	LabelSelector() map[string]string

	// Replicas returns the desired replicas of the resource.
	Replicas() int32
	// PodTemplateSpec returns the pod template spec of the resource. This
	// is added to the underlying StatefulSet.
	PodTemplateSpec() *v1.PodTemplateSpec
	// VolumeClaimTemplates returns the volume claim templates of the
	// resource. This is added to the underlying StatefulSet.
	VolumeClaimTemplates() []v1.PersistentVolumeClaim
	Self() runtime.Object

	// EnsureResources
	EnsureResources(ctx context.Context) error

	// UpdateStatus updates the status of the StatefulResource. The
	// statefulset is parsed to provide additional information like
	// replicas to the status.
	UpdateStatus(ctx context.Context, sts *appsv1.StatefulSet) error

	// PreScaleDownHook is triggered when a scaledown is to be performed.
	// It's ensured that the hook will be triggered at least once, but it
	// may trigger multiple times e.g. if the scaledown fails at a later
	// stage and has to be retried.
	PreScaleDownHook(ctx context.Context) error

	// OnStableReplicasHook is triggered when the statefulSet is observed
	// to be stable meaning readyReplicas == desiredReplicas.
	// This hook can for instance be used to perform cleanup tasks.
	OnStableReplicasHook(ctx context.Context) error

	// Drain drains a pod for data. It's expected that the method only
	// returns after the pod has been drained.
	Drain(ctx context.Context, pod *v1.Pod) error
}

// Operator is a generic operator that can manage Pods filtered by a selector.
type Operator struct {
	kube                  *kubernetes.Clientset
	priorityNodeSelectors labels.Set
	interval              time.Duration
	logger                *log.Entry
	recorder              kube_record.EventRecorder
}

func (o *Operator) Run(ctx context.Context, done chan<- struct{}, esr *ESResource) {
	nextCheck := time.Now().Add(-o.interval)

	for {
		o.logger.Debug("Operator loop")
		select {
		case <-time.After(time.Until(nextCheck)):
			nextCheck = time.Now().Add(o.interval)

			err := o.operate(ctx, esr)
			if err != nil {
				log.Errorf("Failed to operate resource: %v", err)
				continue
			}
		case <-ctx.Done():
			done <- struct{}{}
			o.logger.Info("Terminating operator loop.")
			return
		}
	}
}

func (o *Operator) operate(ctx context.Context, esr *ESResource) error {
	err := o.operatePods(ctx, esr)
	return err
}

func getSTSParentGeneration(sts *appsv1.StatefulSet) int64 {
	if g, ok := sts.Annotations[operatorParentGenerationAnnotationKey]; ok {
		generation, err := strconv.ParseInt(g, 10, 64)
		if err != nil {
			return 0
		}
		return generation
	}
	return 0
}

// operatePods operates on Pods by picking all Pods one by one to update,
// ensuring the Pod gets updated.
// In case the statefulset replicas does not match the desired replicas,
// autoscaling is performed.
// Scale-up is always prefered over any other action like draining old pods.
// Updating a Pod means:
// 1. scale out StatefulSet (if needed).
// 2. mark Pod draining.
// 3. drain Pod.
// 4. delete Pod.
func (o *Operator) operatePods(ctx context.Context, esr *ESResource) error {

	replicas := esr.replicas
	desiredReplicas := getReplicas(esr)

	// prefer scale up over draining nodes.
	if replicas < desiredReplicas {
		err := o.rescaleStatefulSet(ctx, esr)
		if err != nil {
			return fmt.Errorf("failed to rescale StatefulSet: %v", err)
		}

		return esr.OnStableReplicasHook(ctx)
	}

	pods, err := o.kube.CoreV1().Pods(esr.sts.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list pods of StatefulSet: %v", err)
	}

	pod, err := o.getPodToUpdate(ctx, pods.Items, esr)
	if err != nil {
		return fmt.Errorf("failed to get Pod to update: %v", err)
	}

	// return if there are no Pods to be updated.
	if pod == nil {
		err := o.rescaleStatefulSet(ctx, esr)
		if err != nil {
			return fmt.Errorf("failed to rescale StatefulSet: %v", err)
		}

		err = waitForStableStatefulSet(ctx, o.kube, esr.sts, stabilizationTimeout)
		if err != nil {
			return fmt.Errorf("StatefulSet %s/%s is not stable: %v", esr.sts.Namespace, esr.sts.Name, err)
		}
		return esr.OnStableReplicasHook(ctx)
	}

	// scale out by one to perform the update
	if int32(desiredReplicas) == int32(replicas) {
		replicas++
		replicas32 := int32(replicas)
		esr.sts.Spec.Replicas = &replicas32

		_, err = o.kube.AppsV1().StatefulSets(esr.sts.Namespace).Update(ctx, esr.sts, metav1.UpdateOptions{})
		if err != nil {
			return fmt.Errorf("failed to scale StatefulSet %s/%s to %d: %v", esr.sts.Namespace, esr.sts.Name, replicas, err)
		}
		o.recorder.Event(esr.sts, v1.EventTypeNormal, "ScaledStatefulSet",
			fmt.Sprintf("Scaled out StatefulSet '%s/%s' to %d Replicas to perform rolling update",
				esr.sts.Namespace, esr.sts.Name, replicas))
	}

	// wait for StatefulSet to be stable before continuing
	err = waitForStableStatefulSet(ctx, o.kube, esr.sts, stabilizationTimeout)
	if err != nil {
		return fmt.Errorf("StatefulSet %s/%s is not stable: %v", esr.sts.Namespace, esr.sts.Name, err)
	}

	// TODO: make sure operation is being performed on the
	// right Pod (StatefulSet Pods have the same name through time but may
	// have different UUIDs).

	// mark Pod draining
	err = o.annotatePod(ctx, pod, operatorPodDrainingAnnotationKey, "true")
	if err != nil {
		return fmt.Errorf("failed to mark Pod %s/%s draining: %v", pod.Namespace, pod.Name, err)
	}

	// drain Pod
	o.recorder.Event(esr.sts, v1.EventTypeNormal, "DrainingPod", fmt.Sprintf("Draining Pod '%s/%s'", pod.Namespace,
		pod.Name))
	err = esr.Drain(ctx, pod)
	if err != nil {
		return fmt.Errorf("failed to drain Pod %s/%s: %v", pod.Namespace, pod.Name, err)
	}
	o.recorder.Event(esr.sts, v1.EventTypeNormal, "DrainedPod", fmt.Sprintf("Successfully drained Pod '%s/%s'",
		pod.Namespace,
		pod.Name))

	// delete Pod
	o.recorder.Event(esr.sts, v1.EventTypeNormal, "DeletingPod", fmt.Sprintf("Deleting Pod '%s/%s'", pod.Namespace,
		pod.Name))
	err = o.kube.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{
		GracePeriodSeconds: pod.Spec.TerminationGracePeriodSeconds,
	})
	if err != nil {
		return fmt.Errorf("failed to delete Pod %s/%s: %v", pod.Namespace, pod.Name, err)
	}

	// wait for Pod to be terminated and gone from the node.
	err = waitForPodTermination(ctx, o.kube, pod)
	if err != nil {
		log.Warnf("Pod %s/%s not terminated within grace period: %v", pod.Namespace, pod.Name, err)
	}

	o.recorder.Event(esr.sts, v1.EventTypeNormal, "DeletedPod", fmt.Sprintf("Successfully deleted Pod '%s/%s'",
		pod.Namespace,
		pod.Name))

	// we don't know if we're done, ie. if there are more pods to be operated - returning false here.
	return esr.OnStableReplicasHook(ctx)
}

// rescaleStatefulSet rescales the StatefulSet
func (o *Operator) rescaleStatefulSet(ctx context.Context, esr *ESResource) error {
	replicaDiff := 0
	currentReplicas := esr.replicas
	desiredReplicas := getReplicas(esr)

	replicaDiff = desiredReplicas - currentReplicas

	if replicaDiff > 0 {
		// scale up or scale down StatefulSet
		replicas := currentReplicas
		if replicaDiff > 0 {
			replicas += replicaDiff
		} else {
			// TODO: optimize by scaling down all pending pods
			replicas--
		}

		// get all Pods of the StatefulSet
		pods, err := o.kube.CoreV1().Pods(esr.sts.Namespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			return err
		}

		// Pods are named with an increasing number when part of a StatefulSet.
		// We use this property to sort Pods by the lowest ordinal number and
		// drain those that would be scaled down by Kubernetes when reducing
		// the replica count on the StatefulSet.
		pods.Items, err = sortStatefulSetPods(pods.Items)
		if err != nil {
			return err
		}

		if len(pods.Items) > replicas {
			log.Infof("Starting pod draining from %d to %d pods", len(pods.Items), replicas)
			for _, pod := range pods.Items[replicas:] {
				newSTS, err := esr.kube.AppsV1().StatefulSets(esr.sts.Namespace).Get(ctx, esr.sts.Name, metav1.GetOptions{})

				if err != nil {
					newDesiredReplicas := newSTS.Spec.Replicas
					if *newDesiredReplicas > int32(desiredReplicas) {
						log.Infof("EDS %s/%s target scaling definition changed from %d to %d, aborting scale-down", newSTS.Namespace, newSTS.Name, desiredReplicas, newDesiredReplicas)
						return nil
					}

					// if pod is Pending we don't need to safely drain it.
					if pod.Status.Phase == v1.PodPending {
						continue
					}

					// wait for StatefulSet to be stable before continuing
					// always ensure a stable StatefulSet before draining
					err = waitForStableStatefulSet(ctx, o.kube, esr.sts, stabilizationTimeout)
					if err != nil {
						return fmt.Errorf("StatefulSet %s/%s is not stable: %v", esr.sts.Namespace, esr.sts.Name, err)
					}

					log.Infof("Draining Pod %s/%s for scaledown", pod.Namespace, pod.Name)
					err = esr.Drain(ctx, &pod)
					if err != nil {
						return fmt.Errorf("failed to drain pod %s/%s: %v", pod.Namespace, pod.Name, err)
					}
					log.Infof("Pod %s/%s drained", pod.Namespace, pod.Name)
				}
			}
		}

		// always scale down by one
		replicasInt32 := int32(replicas)
		esr.sts.Spec.Replicas = &replicasInt32

		if replicas != currentReplicas {
			o.recorder.Event(esr.sts, v1.EventTypeNormal, "ChangingReplicas",
				fmt.Sprintf("Changing replicas %d -> %d for StatefulSet '%s/%s'", currentReplicas, replicas, esr.sts.Namespace,
					esr.sts.Name))
		}

		// TODO: only update if something changed
		_, err = o.kube.AppsV1().StatefulSets(esr.sts.Namespace).Update(ctx, esr.sts, metav1.UpdateOptions{})
		if err != nil {
			return fmt.Errorf("failed to update StatefulSet %s/%s: %v", esr.sts.Namespace, esr.sts.Name, err)
		}

		err = waitForStableStatefulSet(ctx, o.kube, esr.sts, stabilizationTimeout)
		if err != nil {
			return fmt.Errorf("StatefulSet %s/%s is not stable: %v", esr.sts.Namespace, esr.sts.Name, err)
		}

		log.Infof("Updated StatefulSet %s/%s and marked it as 'not updating'", esr.sts.Namespace, esr.sts.Name)
	}
	return nil
}

// sortStatefulSetPods sorts pods based on their ordinal numbers which is the
// last part of the pod name.
func sortStatefulSetPods(pods []v1.Pod) ([]v1.Pod, error) {
	type ordinalPod struct {
		Number int
		Pod    v1.Pod
	}

	ordinalNumbers := make([]ordinalPod, len(pods))
	for i, pod := range pods {
		ordinal := strings.TrimPrefix(pod.Name, pod.GenerateName)
		number, err := strconv.Atoi(ordinal)
		if err != nil {
			return nil, err
		}
		ordinalNumbers[i] = ordinalPod{Number: number, Pod: pod}
	}

	sort.Slice(ordinalNumbers, func(i, j int) bool {
		return ordinalNumbers[i].Number < ordinalNumbers[j].Number
	})

	sortedPods := make([]v1.Pod, len(pods))
	for i, ordinal := range ordinalNumbers {
		sortedPods[i] = ordinal.Pod
	}

	return sortedPods, nil
}

// getPodToUpdate gets a single Pod to update based on priority.
// if no update is needed it returns nil.
func (o *Operator) getPodToUpdate(ctx context.Context, pods []v1.Pod, esr *ESResource) (*v1.Pod, error) {
	// return early if there are no Pods to manage
	if len(pods) == 0 {
		return nil, nil
	}

	prioritizedNodes, unschedulableNodes, err := o.getNodes(ctx)
	if err != nil {
		return nil, err
	}

	prioritizedPods, err := prioritizePodsForUpdate(pods, esr.sts, prioritizedNodes, unschedulableNodes)
	if err != nil {
		return nil, err
	}

	if len(prioritizedPods) == 0 {
		return nil, nil
	}

	log.Infof("Found %d Pods on StatefulSet %s/%s to update", len(prioritizedPods), esr.sts.Namespace, esr.sts.Name)

	return &prioritizedPods[0], nil
}

// getNodes gets all nodes matching the priority node selector and all nodes
// that are marked unschedulable.
func (o *Operator) getNodes(ctx context.Context) (map[string]v1.Node, map[string]v1.Node, error) {
	opts := metav1.ListOptions{}
	nodes, err := o.kube.CoreV1().Nodes().List(ctx, opts)
	if err != nil {
		return nil, nil, err
	}

	priorityNodesMap := make(map[string]v1.Node, len(nodes.Items))
	unschedulableNodesMap := make(map[string]v1.Node, len(nodes.Items))
	for _, node := range nodes.Items {
		if len(node.Labels) > 0 && isSubset(o.priorityNodeSelectors, labels.Set(node.Labels)) {
			priorityNodesMap[node.Name] = node
		}

		if node.Spec.Unschedulable {
			unschedulableNodesMap[node.Name] = node
		}
	}
	return priorityNodesMap, unschedulableNodesMap, nil
}

// annotatePod annotates the Pod with the specified annotation key and value.
// If the key/value is already present on the Pod, this is a no-op.
func (o *Operator) annotatePod(ctx context.Context, pod *v1.Pod, annotationKey, annotationValue string) error {
	if value, ok := pod.Annotations[annotationKey]; !ok || value != annotationValue {
		annotation := []byte(fmt.Sprintf(`{"metadata": {"annotations": {"%s": "%s"}}}`, annotationKey, annotationValue))
		_, err := o.kube.CoreV1().Pods(pod.Namespace).Patch(ctx, pod.Name, types.StrategicMergePatchType, annotation, metav1.PatchOptions{})
		if err != nil {
			return err
		}
	}
	return nil
}

// waitForPodTermination waits for a Pod to be terminated by looking up the Pod
// in the API server.
// It waits for up to TerminationGracePeriodSeconds as specified on the Pod +
// an additional eviction head room.
// This is to fully respect the termination expectations as described in:
// https://kubernetes.io/docs/concepts/workloads/pods/pod/#termination-of-pods
func waitForPodTermination(ctx context.Context, client kubernetes.Interface, pod *v1.Pod) error {
	if pod.Spec.TerminationGracePeriodSeconds == nil {
		// if no grace period is defined, we don't wait.
		return nil
	}

	waitForTermination := func() error {
		newpod, err := client.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
		if err != nil {
			if errors.IsNotFound(err) {
				return nil
			}
			return err
		}

		// StatefulSet pods have the same name after restart, check the uid as well
		if newpod.GetObjectMeta().GetUID() == pod.GetObjectMeta().GetUID() {
			return fmt.Errorf("the Pod has not terminated")
		}

		return nil
	}

	gracePeriod := time.Duration(*pod.Spec.TerminationGracePeriodSeconds)*time.Second + podEvictionHeadroom

	backoffCfg := backoff.NewExponentialBackOff()
	backoffCfg.MaxElapsedTime = gracePeriod
	return backoff.Retry(waitForTermination, backoffCfg)
}

// waitForStableStatefulSet waits for a StatefulSet to stabilize. Stabilization
// means that the number of replicas and number of ready replicas match.
func waitForStableStatefulSet(ctx context.Context, kube *kubernetes.Clientset, sts *appsv1.StatefulSet, timeout time.Duration) error {
	checkStsReplicas := func() error {
		newSts, err := kube.AppsV1().StatefulSets(sts.Namespace).Get(ctx, sts.Name, metav1.GetOptions{})
		if err != nil {
			return backoff.Permanent(err)
		}
		if newSts.Spec.Replicas == nil {
			return backoff.Permanent(fmt.Errorf("cannot determine desired replicas from spec"))
		}
		if *newSts.Spec.Replicas != newSts.Status.ReadyReplicas {
			log.Infof("Waiting for stabilization: StatefulSet %s/%s has %d/%d ready replicas", newSts.Namespace, newSts.Name, newSts.Status.ReadyReplicas, *newSts.Spec.Replicas)
			return fmt.Errorf("%d/%d replicas ready", newSts.Status.ReadyReplicas, *newSts.Spec.Replicas)
		}

		return nil
	}

	backoffCfg := backoff.NewExponentialBackOff()
	backoffCfg.MaxElapsedTime = timeout
	backoffCtxCfg := backoff.WithContext(backoffCfg, ctx)
	return backoff.Retry(checkStsReplicas, backoffCtxCfg)
}

type updatePriority struct {
	Pod      v1.Pod
	Priority int
	Number   int
}

const (
	podDrainingPriority       = 16
	unschedulableNodePriority = 8
	nodeSelectorPriority      = 4
	podOldRevisionPriority    = 2
	stsReplicaDiffPriority    = 1
	// priorityNames
	podDrainingPriorityName       = "PodDraining"
	unschedulableNodePriorityName = "UnschedulableNode"
	nodeSelectorPriorityName      = "NodeSelector"
	podOldRevisionPriorityName    = "PodOldRevision"
	stsReplicaDiffPriorityName    = "STSReplicaDiff"
)

func prioToName(priority int) string {
	switch priority {
	case podDrainingPriority:
		return podDrainingPriorityName
	case unschedulableNodePriority:
		return unschedulableNodePriorityName
	case nodeSelectorPriority:
		return nodeSelectorPriorityName
	case podOldRevisionPriority:
		return podOldRevisionPriorityName
	case stsReplicaDiffPriority:
		return stsReplicaDiffPriorityName
	default:
		return ""
	}
}

func priorityNames(priority int) []string {
	priorities := make([]string, 0)
	for _, prio := range []int{podDrainingPriority, unschedulableNodePriority, nodeSelectorPriority, podOldRevisionPriority, stsReplicaDiffPriority} {
		if priority >= prio {
			priorities = append(priorities, prioToName(prio))
		}
	}
	return priorities
}

// prioritizePodsForUpdate prioritizes Pods to update next. The Pods are
// prioritized based on the following rules:
//
// 1. Pods already marked draining get highest priority.
// 2. Pods NOT on a priority node get high priority.
// 3. Pods not up to date with StatefulSet revision get high priority.
// 4. Pods part of a StatefulSet where desired replicas != actual replicas get medium priority.
func prioritizePodsForUpdate(pods []v1.Pod, sts *appsv1.StatefulSet, priorityNodes, unschedulableNodes map[string]v1.Node) ([]v1.Pod, error) {
	priorities := make([]*updatePriority, 0, len(pods))
	for _, pod := range pods {
		ordinal := strings.TrimPrefix(pod.Name, pod.GenerateName)
		number, err := strconv.Atoi(ordinal)
		if err != nil {
			return nil, err
		}

		prio := &updatePriority{
			Pod:    pod,
			Number: number,
		}

		// if Pod is marked draining it gets the highest priority.
		if _, ok := pod.Annotations[operatorPodDrainingAnnotationKey]; ok {
			prio.Priority += podDrainingPriority
		}

		// check if Pod has assigned node
		if pod.Spec.NodeName == "" {
			log.Debugf("Skipping Pod %s/%s. No assigned node found.", prio.Pod.Namespace, prio.Pod.Name)
			continue
		}

		// if Pod is on an unschedulable node it gets high priority.
		// An unschedulable node indicates that it is about to be
		// drained, so we should priorities moving pods away from the
		// node.
		if _, ok := unschedulableNodes[pod.Spec.NodeName]; ok {
			prio.Priority += unschedulableNodePriority
		}

		// if Pod is NOT on a priority selected node it gets high priority.
		if _, ok := priorityNodes[pod.Spec.NodeName]; !ok {
			prio.Priority += nodeSelectorPriority
		}

		// if Pod has a different revision than the updated revision on
		// the StatefulSet then it gets high priority.
		// TODO: check if UpdateRevision is always set.
		if hash, ok := pod.Labels[controllerRevisionHashLabelKey]; ok && sts.Status.UpdateRevision != hash {
			prio.Priority += podOldRevisionPriority
		}

		// if Pod is part of a StatefulSet where desired and actual
		// replicas doesn't match then it gets medium priority.
		desiredReplicas := sts.Spec.Replicas

		replicas := int32(0)
		if sts.Spec.Replicas != nil {
			replicas = *sts.Spec.Replicas
		}

		// scale out by one to perform the update
		if *desiredReplicas != int32(replicas) {
			prio.Priority += stsReplicaDiffPriority
		}

		priorities = append(priorities, prio)
	}

	// sort by priority, ordinal number
	sort.Slice(priorities, func(i, j int) bool {
		if priorities[i].Priority == priorities[j].Priority {
			return priorities[i].Number < priorities[j].Number
		}
		return priorities[i].Priority > priorities[j].Priority
	})

	sortedPods := make([]v1.Pod, 0, len(pods))
	for _, prio := range priorities {
		// only consider Pods with a priority > 1.
		// Priority 1 just indicate that the Pod is part of a
		// StatefulSet which is currently updating, but it does not
		// mean the Pod itself needs to be updated.
		if prio.Priority > 1 {
			log.Infof(
				"Pod %s/%s should be updated. Priority: %d (%s)",
				prio.Pod.Namespace,
				prio.Pod.Name,
				prio.Priority,
				strings.Join(priorityNames(prio.Priority), ","),
			)
			sortedPods = append(sortedPods, prio.Pod)
		}
	}
	return sortedPods, nil
}

// isOwnedReference returns true if the dependent object is owned by the owner
// object.
func isOwnedReference(owner StatefulResource, dependent metav1.ObjectMeta) bool {
	for _, ref := range dependent.OwnerReferences {
		if ref.APIVersion == owner.APIVersion() &&
			ref.Kind == owner.Kind() &&
			ref.UID == owner.UID() &&
			ref.Name == owner.Name() {
			return true
		}
	}
	return false
}

// https://github.com/kubernetes/kubernetes/pull/95179
func isSubset(subSet, superSet labels.Set) bool {
	if len(superSet) == 0 {
		return true
	}

	for k, v := range subSet {
		value, ok := superSet[k]
		if !ok {
			return false
		}
		if value != v {
			return false
		}
	}
	return true
}
