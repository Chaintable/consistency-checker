package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	NodeInfo = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "pipeline",
		Name:      "node_info",
	}, []string{"chain_id", "role"})

	// pipeline_latest_pushed_block_number
	LatestPushedBlockNumber = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "pipeline",
		Name:      "block_num",
	})

	LatestPushedBlockTime = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "pipeline",
		Name:      "block_time",
	})
)

func init() {
	prometheus.MustRegister(NodeInfo, LatestPushedBlockNumber, LatestPushedBlockTime)
}
