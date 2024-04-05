package operator

import (
	"strconv"

	log "github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
)

type ESResource struct {

	// Kube Client
	kube *kubernetes.Clientset

	// Logger
	logger *log.Entry

	// ES client specific to the ES Resource
	esClient *ESClient

	// The ElasticSearch StatefulSet
	sts *appsv1.StatefulSet

	// Metrics keyed by name and series of float64 samples
	metrics map[string][]float64

	// List of pods in the StatefulSet
	pods []v1.Pod

	// Configuration members
	enabled                            bool
	minReplicas                        int
	maxReplicas                        int
	minShards                          int
	maxShards                          int
	minIndexReplicas                   int
	maxIndexReplicas                   int
	minShardsPerNode                   int
	maxShardsPerNode                   int
	scaleUpCPUBoundary                 int
	scaleDownCPUBoundary               int
	scaleDownThresholdDurationSeconds  int
	scaleUpCooldownSeconds             int
	scaleDownCooldownSeconds           int
	scaleUpThresholdDurationSeconds    int
	diskUsagePercentScaledownWatermark int
	skipDraining                       bool

	// Status related
	lastScaleDownEnded   *metav1.Time
	lastScaleDownStarted *metav1.Time
	lastScaleUpEnded     *metav1.Time
	lastScaleUpStarted   *metav1.Time
	observedGeneration   int
	replicas             int
}

func (esr *ESResource) getLabelValue(key string, sts *appsv1.StatefulSet, defaultValue interface{}) interface{} {
	var val interface{} = defaultValue

	if str, exists := sts.ObjectMeta.Labels[key]; exists {
		switch defaultValue.(type) {
		case int:
			intVal, err := strconv.Atoi(str)
			esr.logger.Infof("getLabelValue %s = %d", key, intVal)
			if err == nil {
				val = intVal
			}
		case bool:
			boolVal, err := strconv.ParseBool(str)
			esr.logger.Infof("getLabelValue %s = %t", key, boolVal)
			if err == nil {
				val = boolVal
			}
		}
	}

	esr.logger.Infof("getLabelValue %s = %v", key, val)
	return val
}

func (esr *ESResource) getScalingConfiguration(sts *appsv1.StatefulSet) {

	esr.enabled = esr.getLabelValue("es.operator/enabled", sts, false).(bool)
	esr.logger.Infof("The label for es.operator/enabled is %t", esr.enabled)

	esr.skipDraining = esr.getLabelValue("es.operator/skipDraining", sts, false).(bool)
	esr.minReplicas = esr.getLabelValue("es.operator/minReplicas", sts, 1).(int)
	esr.maxReplicas = esr.getLabelValue("es.operator/maxReplicas", sts, 8).(int)
	esr.minShards = esr.getLabelValue("es.operator/minShards", sts, 1).(int)
	esr.maxShards = esr.getLabelValue("es.operator/maxShards", sts, 3).(int)
	esr.minIndexReplicas = esr.getLabelValue("es.operator/minIndexReplicas", sts, 1).(int)
	esr.maxIndexReplicas = esr.getLabelValue("es.operator/maxIndexReplicas", sts, 2).(int)
	esr.minShardsPerNode = esr.getLabelValue("es.operator/minShardsPerNode", sts, 2).(int)
	esr.maxShardsPerNode = esr.getLabelValue("es.operator/maxShardsPerNode", sts, 4).(int)
	esr.scaleUpCPUBoundary = esr.getLabelValue("es.operator/scaleUpCPUBoundary", sts, 50).(int)
	esr.scaleDownCPUBoundary = esr.getLabelValue("es.operator/scaleDownCPUBoundary", sts, 20).(int)
	esr.scaleDownThresholdDurationSeconds = esr.getLabelValue("es.operator/scaleDownThresholdDurationSeconds", sts, 120).(int)
	esr.scaleUpCooldownSeconds = esr.getLabelValue("es.operator/scaleUpCooldownSeconds", sts, 120).(int)
	esr.scaleDownCooldownSeconds = esr.getLabelValue("es.operator/scaleDownCooldownSeconds", sts, 120).(int)
	esr.scaleUpThresholdDurationSeconds = esr.getLabelValue("es.operator/scaleUpThresholdDurationSeconds", sts, 120).(int)
	esr.diskUsagePercentScaledownWatermark = esr.getLabelValue("es.operator/diskUsagePercentScaledownWatermark", sts, 120).(int)
}
