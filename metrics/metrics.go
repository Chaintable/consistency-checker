package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	NodeInfo = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "pipeline",
		Subsystem: "consistency",
		Name:      "node_info",
	}, []string{"chain_id", "role"})
)

func init() {
	prometheus.MustRegister(NodeInfo)
}
