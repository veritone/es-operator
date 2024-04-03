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

func getLabelValue(key string, sts *appsv1.StatefulSet, defaultValue interface{}) interface{} {
	var val interface{} = defaultValue

	if str, exists := sts.ObjectMeta.Labels[key]; exists {
		switch defaultValue.(type) {
		case int:
			intVal, err := strconv.Atoi(str)
			if err != nil {
				val = intVal
			}
		case bool:
			boolVal, err := strconv.ParseBool(str)
			if err != nil {
				val = boolVal
			}
		}
	}

	return val
}

func (esr ESResource) getScalingConfiguration(sts *appsv1.StatefulSet) {
	esr.enabled = getLabelValue("es.operator/enabbled", sts, false).(bool)
	esr.skipDraining = getLabelValue("es.operator/skipDraining", sts, false).(bool)
	esr.minReplicas = getLabelValue("es.operator/minReplicas", sts, 1).(int)
	esr.maxReplicas = getLabelValue("es.operator/maxReplicas", sts, 8).(int)
	esr.minShards = getLabelValue("es.operator/minShards", sts, 1).(int)
	esr.maxShards = getLabelValue("es.operator/maxShards", sts, 3).(int)
	esr.minIndexReplicas = getLabelValue("es.operator/minIndexReplicas", sts, 1).(int)
	esr.maxIndexReplicas = getLabelValue("es.operator/maxIndexReplicas", sts, 2).(int)
	esr.minShardsPerNode = getLabelValue("es.operator/minShardsPerNode", sts, 2).(int)
	esr.maxShardsPerNode = getLabelValue("es.operator/maxShardsPerNode", sts, 4).(int)
	esr.scaleUpCPUBoundary = getLabelValue("es.operator/scaleUpCPUBoundary", sts, 50).(int)
	esr.scaleDownCPUBoundary = getLabelValue("es.operator/scaleDownCPUBoundary", sts, 20).(int)
	esr.scaleDownThresholdDurationSeconds = getLabelValue("es.operator/scaleDownThresholdDurationSeconds", sts, 120).(int)
	esr.scaleUpCooldownSeconds = getLabelValue("es.operator/scaleUpCooldownSeconds", sts, 120).(int)
	esr.scaleDownCooldownSeconds = getLabelValue("es.operator/scaleDownCooldownSeconds", sts, 120).(int)
	esr.scaleUpThresholdDurationSeconds = getLabelValue("es.operator/scaleUpThresholdDurationSeconds", sts, 120).(int)
	esr.diskUsagePercentScaledownWatermark = getLabelValue("es.operator/diskUsagePercentScaledownWatermark", sts, 120).(int)
}
