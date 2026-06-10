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

	// fork巡检改写的对象数：非0说明标记曾被外部覆盖或此前标记失败
	ForkScanRewrites = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "pipeline",
		Name:      "fork_scan_rewrites_total",
	})

	// fork巡检因db中无canonical记录而跳过的高度数（冷启动/丢盘时增长）
	ForkScanSkips = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "pipeline",
		Name:      "fork_scan_skipped_total",
	})

	ForkScanErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "pipeline",
		Name:      "fork_scan_errors_total",
	})

	// drop block的fork标记改写最终失败次数（重试后仍失败）
	DropBlockRewriteFailures = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "pipeline",
		Name:      "drop_block_rewrite_failures_total",
	})
)

func init() {
	prometheus.MustRegister(NodeInfo, LatestPushedBlockNumber, LatestPushedBlockTime,
		ForkScanRewrites, ForkScanSkips, ForkScanErrors, DropBlockRewriteFailures)
}
