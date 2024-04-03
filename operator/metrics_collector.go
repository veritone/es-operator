package operator

import (
	"context"
	"sort"

	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/metrics/pkg/apis/metrics/v1beta1"
	metrics "k8s.io/metrics/pkg/client/clientset/versioned"
)

type ElasticsearchMetricsCollector struct {
	logger  *log.Entry
	metrics *metrics.Clientset
	kube    *kubernetes.Clientset
	esr     ESResource
}

func (c *ElasticsearchMetricsCollector) collectMetrics(ctx context.Context) error {
	// first, collect metrics for all pods....
	metrics, err := c.metrics.MetricsV1beta1().PodMetricses(c.esr.sts.Namespace).List(ctx, metav1.ListOptions{})
	if err == nil {
		cpuUsagePercent := getCPUUsagePercent(metrics.Items, c.esr.pods)

		c.logger.Debug("The current cpuUsage is %f", cpuUsagePercent)

		if len(cpuUsagePercent) == 0 {
			c.logger.Debug("Didn't have any metrics to collect.")
		}
	}

	return err
}

func getCPUUsagePercent(metrics []v1beta1.PodMetrics, pods []v1.Pod) []int32 {
	podResources := make(map[string]map[string]v1.ResourceRequirements, len(pods))
	for _, pod := range pods {
		containerMap := make(map[string]v1.ResourceRequirements, len(pod.Spec.Containers))
		for _, container := range pod.Spec.Containers {
			containerMap[container.Name] = container.Resources
		}
		podResources[pod.Name] = containerMap
	}

	// calculate the max usage/request value for each pod by looking at all
	// containers in every pod.
	cpuUsagePercent := []int32{}
	for _, podMetrics := range metrics {
		if containerResources, ok := podResources[podMetrics.Name]; ok {
			podMax := int32(0)
			for _, container := range podMetrics.Containers {
				if resources, ok := containerResources[container.Name]; ok {
					requestedCPU := resources.Requests.Cpu().MilliValue()
					usageCPU := container.Usage.Cpu().MilliValue()
					if requestedCPU > 0 {
						usagePcnt := int32(100 * usageCPU / requestedCPU)
						if usagePcnt > podMax {
							podMax = usagePcnt
						}
					}
				}
			}
			cpuUsagePercent = append(cpuUsagePercent, podMax)
		}
	}
	return cpuUsagePercent
}

func calculateMedian(cpuMetrics []int32) int32 {
	sort.Slice(cpuMetrics, func(i, j int) bool { return cpuMetrics[i] < cpuMetrics[j] })
	// in case of even number of samples we now get the lower one.
	return cpuMetrics[(len(cpuMetrics)-1)/2]
}
