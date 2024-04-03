package operator

import (
	"fmt"

	"math"

	"time"

	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
)

// 1. check if we have enough data
// 2. decide if we need to scale up or down
// 3. retrieve elasticsearch data (indices, shards, replicas)
// 4. decide if we only need to increase/decrease nodes
//    -> if not, don't do anything for now...

type ScalingDirection int

func (d ScalingDirection) String() string {
	switch d {
	case DOWN:
		return "DOWN"
	case UP:
		return "UP"
	case NONE:
		return "NONE"
	}
	return ""
}

const (
	DOWN ScalingDirection = iota
	NONE
	UP
)

type ScalingOperation struct {
	ScalingDirection ScalingDirection
	NodeReplicas     *int
	IndexReplicas    []ESIndex
	Description      string
}

func noopScalingOperation(description string) *ScalingOperation {
	return &ScalingOperation{
		ScalingDirection: NONE,
		Description:      description,
	}
}

type AutoScaler struct {
	logger          *log.Entry
	esr             *ESResource
	metricsInterval time.Duration
	pods            []v1.Pod
	esClient        *ESClient
}

func NewAutoScaler(esr *ESResource, metricsInterval time.Duration) *AutoScaler {
	return &AutoScaler{
		logger: log.WithFields(log.Fields{
			"sts":       esr.sts.Name,
			"namespace": esr.sts.Namespace,
		}),
		esr:             esr,
		metricsInterval: metricsInterval,
		pods:            esr.pods,
		esClient:        esr.esClient,
	}
}

func (as *AutoScaler) scalingHint() ScalingDirection {

	esr := as.esr
	cpuMetrics := as.esr.metrics["cpu"]

	// no metrics yet
	if esr.metrics == nil {
		return NONE
	}

	// TODO: only consider metric samples that are not too old.
	cpuSampleSize := len(cpuMetrics)

	// check for enough data points
	requiredScaledownSamples := int(math.Ceil(float64(as.esr.scaleDownThresholdDurationSeconds) / as.metricsInterval.Seconds()))
	if cpuSampleSize >= requiredScaledownSamples {
		// check if CPU is below threshold for the last n samples
		scaleDownRequired := true
		for _, currentItem := range cpuMetrics[cpuSampleSize-requiredScaledownSamples:] {
			if currentItem >= float64(as.esr.scaleDownCPUBoundary) {
				scaleDownRequired = false
				break
			}
		}
		if scaleDownRequired {
			if esr.lastScaleDownStarted.Time.Before(time.Now().Add(-time.Duration(esr.scaleDownCooldownSeconds) * time.Second)) {
				as.logger.Infof("Scaling hint: %s", DOWN)
				return DOWN
			}
			as.logger.Info("Not scaling down, currently in cool-down period.")
		}
	}

	requiredScaleUpSamples := int(math.Ceil(float64(esr.scaleUpThresholdDurationSeconds) / as.metricsInterval.Seconds()))
	if cpuSampleSize >= requiredScaleUpSamples {
		// check if CPU is above threshold for the last n samples
		scaleUpRequired := true
		for _, currentItem := range cpuMetrics[cpuSampleSize-requiredScaleUpSamples:] {
			if currentItem <= float64(esr.scaleUpCPUBoundary) {
				scaleUpRequired = false
				break
			}
		}
		if scaleUpRequired {
			if esr.lastScaleUpStarted.Time.Before(time.Now().Add(-time.Duration(esr.scaleUpCooldownSeconds) * time.Second)) {
				as.logger.Infof("Scaling hint: %s", UP)
				return UP
			}
			as.logger.Info("Not scaling up, currently in cool-down period.")
		}
	}
	return NONE
}

// TODO: check alternative approach by configuring the tags used for `index.routing.allocation`
// and deriving the indices from there.
func (as *AutoScaler) GetScalingOperation() (*ScalingOperation, error) {
	direction := as.scalingHint()
	esIndices, err := as.esClient.GetIndices()
	if err != nil {
		return nil, err
	}

	esShards, err := as.esClient.GetShards()
	if err != nil {
		return nil, err
	}

	esNodes, err := as.esClient.GetNodes()
	if err != nil {
		return nil, err
	}

	managedIndices := as.getManagedIndices(esIndices, esShards)
	managedNodes := as.getManagedNodes(as.pods, esNodes)
	return as.calculateScalingOperation(managedIndices, managedNodes, direction), nil
}

func (as *AutoScaler) getManagedNodes(pods []v1.Pod, esNodes []ESNode) []ESNode {
	podIPs := make(map[string]struct{})
	for _, pod := range pods {
		if pod.Status.PodIP != "" {
			podIPs[pod.Status.PodIP] = struct{}{}
		}
	}
	managedNodes := make([]ESNode, 0, len(pods))
	for _, node := range esNodes {
		if _, ok := podIPs[node.IP]; ok {
			managedNodes = append(managedNodes, node)
		}
	}
	return managedNodes
}

func (as *AutoScaler) getManagedIndices(esIndices []ESIndex, esShards []ESShard) map[string]ESIndex {
	podIPs := make(map[string]struct{})
	for _, pod := range as.pods {
		if pod.Status.PodIP != "" {
			podIPs[pod.Status.PodIP] = struct{}{}
		}
	}
	managedIndices := make(map[string]ESIndex)
	for _, shard := range esShards {
		if _, ok := podIPs[shard.IP]; ok {
			for _, index := range esIndices {
				if shard.Index == index.Index {
					managedIndices[shard.Index] = index
					break
				}
			}
		}
	}
	return managedIndices
}

func (as *AutoScaler) calculateScalingOperation(managedIndices map[string]ESIndex, managedNodes []ESNode, scalingHint ScalingDirection) *ScalingOperation {
	esr := as.esr
	// cpuMetrics := as.esr.metrics["cpu"]

	currentDesiredNodeReplicas := esr.replicas
	if currentDesiredNodeReplicas == 0 {
		return noopScalingOperation("DesiredReplicas is not set yet.")
	}

	if len(managedIndices) == 0 {
		return noopScalingOperation("No indices allocated yet.")
	}

	scalingOperation := as.scaleUpOrDown(managedIndices, scalingHint, currentDesiredNodeReplicas)

	// safety check: ensure we don't scale below minIndexReplicas+1
	if scalingOperation.NodeReplicas != nil && *scalingOperation.NodeReplicas < esr.minIndexReplicas+1 {
		return noopScalingOperation(fmt.Sprintf("Scaling would violate the minimum required nodes to hold %d index replicas.", esr.minIndexReplicas))
	}

	// safety check: ensure we don't scale-down if disk usage is already above threshold
	if scalingOperation.ScalingDirection == DOWN && esr.diskUsagePercentScaledownWatermark > 0 && as.getMaxDiskUsage(managedNodes) > float64(esr.diskUsagePercentScaledownWatermark) {
		return noopScalingOperation(fmt.Sprintf("Scaling would violate the minimum required disk free percent: %.2f", 75.0))
	}

	return scalingOperation
}

func (as *AutoScaler) getMaxDiskUsage(managedNodes []ESNode) float64 {
	maxDisk := 0.0
	for _, node := range managedNodes {
		maxDisk = math.Max(maxDisk, node.DiskUsedPercent)
	}
	return maxDisk
}

func (as *AutoScaler) ensureBoundsNodeReplicas(newDesiredNodeReplicas int) int {
	if as.esr.maxReplicas > 0 && as.esr.maxReplicas < newDesiredNodeReplicas {
		as.logger.Warnf("Requested to scale up to %d, which is beyond the defined maxReplicas of %d.", newDesiredNodeReplicas, as.esr.maxReplicas)
		return as.esr.maxReplicas
	}
	if as.esr.minReplicas > 0 && as.esr.minReplicas > newDesiredNodeReplicas {
		return as.esr.minReplicas
	}
	return newDesiredNodeReplicas
}

func (as *AutoScaler) scaleUpOrDown(esIndices map[string]ESIndex, scalingHint ScalingDirection, currentDesiredNodeReplicas int) *ScalingOperation {
	esr := as.esr

	newDesiredIndexReplicas := make([]ESIndex, 0, len(esIndices))
	indexScalingDirection := NONE

	currentTotalShards := int32(0)
	for _, index := range esIndices {
		as.logger.Debugf("Index: %s, primaries: %d, replicas: %d", index.Index, index.Primaries, index.Replicas)
		currentTotalShards += index.Primaries * (index.Replicas + 1)

		// ensure to meet min index replicas requirements
		if index.Replicas < int32(esr.minIndexReplicas) {
			indexScalingDirection = UP
			newDesiredIndexReplicas = append(newDesiredIndexReplicas, ESIndex{
				Index:     index.Index,
				Primaries: index.Primaries,
				Replicas:  int32(esr.minIndexReplicas),
			})
		}

		// ensure to meet max index replicas requirements
		if index.Replicas > int32(esr.maxIndexReplicas) {
			indexScalingDirection = DOWN
			newDesiredIndexReplicas = append(newDesiredIndexReplicas, ESIndex{
				Index:     index.Index,
				Primaries: index.Primaries,
				Replicas:  int32(esr.maxIndexReplicas),
			})
		}
	}

	currentShardToNodeRatio := shardToNodeRatio(int(currentTotalShards), currentDesiredNodeReplicas)

	// independent of the scaling direction: in case the scaling setting MaxShardsPerNode has changed, we might need to scale up.
	if currentShardToNodeRatio > float64(esr.maxShardsPerNode) {
		newDesiredNodeReplicas := as.ensureBoundsNodeReplicas(int(math.Ceil(shardToNodeRatio(int(currentTotalShards), esr.maxShardsPerNode))))
		return &ScalingOperation{
			ScalingDirection: as.calculateScalingDirection(currentDesiredNodeReplicas, newDesiredNodeReplicas),
			NodeReplicas:     &newDesiredNodeReplicas,
			Description:      fmt.Sprintf("Current shard-to-node ratio (%.2f) exceeding the desired limit of (%d).", currentShardToNodeRatio, esr.maxShardsPerNode),
		}
	}

	// independent of the scaling direction: in case there are indices with < MinIndexReplicas or > MaxIndexReplicas,
	// we try to scale these indices.
	if len(newDesiredIndexReplicas) > 0 {
		return &ScalingOperation{
			ScalingDirection: indexScalingDirection,
			IndexReplicas:    newDesiredIndexReplicas,
			Description:      "Scaling indices replicas to fit MinIndexReplicas/MaxIndexReplicas requirement",
		}
	}

	switch scalingHint {
	case UP:
		if currentShardToNodeRatio <= float64(esr.minShardsPerNode) {
			newTotalShards := currentTotalShards
			for _, index := range esIndices {
				if index.Replicas >= int32(esr.maxIndexReplicas) {
					return noopScalingOperation(fmt.Sprintf("Not allowed to scale up due to maxIndexReplicas (%d) reached for index %s.",
						esr.maxIndexReplicas, index.Index))
				}
				newTotalShards += index.Primaries
				newDesiredIndexReplicas = append(newDesiredIndexReplicas, ESIndex{
					Index:     index.Index,
					Primaries: index.Primaries,
					Replicas:  index.Replicas + 1,
				})
			}
			if newTotalShards > currentTotalShards {
				newDesiredNodeReplicas := currentDesiredNodeReplicas

				scalingMsg := "Increasing index replicas."

				// Evaluate new number of nodes only if we above MinShardsPerNode parameter
				if shardToNodeRatio(int(newTotalShards), currentDesiredNodeReplicas) >= float64(esr.minShardsPerNode) {
					newDesiredNodeReplicas = as.ensureBoundsNodeReplicas(
						calculateNodesWithSameShardToNodeRatio(currentDesiredNodeReplicas, int(currentTotalShards), int(newTotalShards)))
					if newDesiredNodeReplicas != currentDesiredNodeReplicas {
						scalingMsg = fmt.Sprintf("Trying to keep shard-to-node ratio (%.2f), and increasing index replicas.", shardToNodeRatio(int(newTotalShards), newDesiredNodeReplicas))
					}
				}

				return &ScalingOperation{
					Description:   scalingMsg,
					NodeReplicas:  &newDesiredNodeReplicas,
					IndexReplicas: newDesiredIndexReplicas,
					// we don't use "as.calculateScalingDirection" because the func "calculateNodesWithSameShardToNodeRatio" can produce the same number of nodes
					// but we still need to scale up shards
					ScalingDirection: UP,
				}
			}
		}

		// round down to the next non-fractioned shard-to-node ratio
		newDesiredNodeReplicas := as.ensureBoundsNodeReplicas(calculateIncreasedNodes(currentDesiredNodeReplicas, int(currentTotalShards)))

		return &ScalingOperation{
			ScalingDirection: as.calculateScalingDirection(currentDesiredNodeReplicas, newDesiredNodeReplicas),
			NodeReplicas:     &newDesiredNodeReplicas,
			Description:      fmt.Sprintf("Increasing node replicas to %d.", newDesiredNodeReplicas),
		}
	case DOWN:
		newTotalShards := currentTotalShards
		for _, index := range esIndices {
			if index.Replicas > int32(esr.minIndexReplicas) {
				newTotalShards -= index.Primaries
				newDesiredIndexReplicas = append(newDesiredIndexReplicas, ESIndex{
					Index:     index.Index,
					Primaries: index.Primaries,
					Replicas:  index.Replicas - 1,
				})
			}
		}
		if newTotalShards != currentTotalShards {
			newDesiredNodeReplicas := as.ensureBoundsNodeReplicas(calculateNodesWithSameShardToNodeRatio(currentDesiredNodeReplicas, int(currentTotalShards), int(newTotalShards)))
			return &ScalingOperation{
				ScalingDirection: as.calculateScalingDirection(currentDesiredNodeReplicas, newDesiredNodeReplicas),
				NodeReplicas:     &newDesiredNodeReplicas,
				IndexReplicas:    newDesiredIndexReplicas,
				Description:      fmt.Sprintf("Keeping shard-to-node ratio (%.2f), and decreasing index replicas.", currentShardToNodeRatio),
			}
		}
		// increase shard-to-node ratio, and scale down by at least one
		newDesiredNodeReplicas := as.ensureBoundsNodeReplicas(calculateDecreasedNodes(currentDesiredNodeReplicas, int(currentTotalShards)))
		ratio := shardToNodeRatio(int(newTotalShards), newDesiredNodeReplicas)
		if ratio > float64(esr.maxShardsPerNode) {
			return noopScalingOperation(fmt.Sprintf("Scaling would violate the shard-to-node maximum (%.2f/%d).", ratio, esr.maxShardsPerNode))
		}

		return &ScalingOperation{
			ScalingDirection: as.calculateScalingDirection(currentDesiredNodeReplicas, newDesiredNodeReplicas),
			NodeReplicas:     &newDesiredNodeReplicas,
			Description:      fmt.Sprintf("Decreasing node replicas to %d.", newDesiredNodeReplicas),
		}
	}
	return noopScalingOperation("Nothing to do")
}

func shardToNodeRatio(shards, nodes int) float64 {
	return float64(shards) / float64(nodes)
}

func calculateNodesWithSameShardToNodeRatio(currentDesiredNodeReplicas, currentTotalShards, newTotalShards int) int {
	currentShardToNodeRatio := shardToNodeRatio(currentTotalShards, currentDesiredNodeReplicas)
	if currentShardToNodeRatio <= 1 {
		return currentDesiredNodeReplicas
	}
	return int(math.Ceil(float64(newTotalShards) / float64(currentShardToNodeRatio)))
}

func calculateDecreasedNodes(currentDesiredNodeReplicas, currentTotalShards int) int {
	currentShardToNodeRatio := shardToNodeRatio(currentTotalShards, currentDesiredNodeReplicas)
	newDesiredNodes := int(math.Min(float64(currentDesiredNodeReplicas)-float64(1), math.Ceil(float64(currentTotalShards)/math.Ceil(currentShardToNodeRatio+0.00001))))
	if newDesiredNodes <= 1 {
		return 1
	}
	return newDesiredNodes
}

func calculateIncreasedNodes(currentDesiredNodeReplicas, currentTotalShards int) int {
	currentShardToNodeRatio := shardToNodeRatio(currentTotalShards, currentDesiredNodeReplicas)
	if currentShardToNodeRatio <= 1 {
		return currentTotalShards
	}
	return int(math.Ceil(float64(currentTotalShards) / math.Floor(currentShardToNodeRatio-0.00001)))
}

func (as *AutoScaler) calculateScalingDirection(oldNodeReplicas, newNodeReplicas int) ScalingDirection {
	if newNodeReplicas > oldNodeReplicas {
		return UP
	}
	if newNodeReplicas < oldNodeReplicas {
		return DOWN
	}
	return NONE
}
