package config

import (
	"log"
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Listen                    string   `yaml:"listen"`
	ReadyRatio                float64  `yaml:"ready_ratio"`                   // 副本节点准备好的比例，达到后推送kafka
	CheckNum                  int      `yaml:"check_num"`                     // 每次得知block更新后，check副本节点的次数
	CheckInterval             int      `yaml:"check_interval_ms"`             // 每次check副本节点的间隔
	RpcNodeTimeout            int      `yaml:"rpc_node_timeout_ms"`           // 每次check单副本节点RPC的超时时间
	MsgWaitTimeout            int      `yaml:"msg_wait_timeout"`              // 每次check单副本节点RPC的间隔
	ChainID                   int64    `yaml:"chain_id"`                      // 链ID
	Version                   string   `yaml:"version"`                       // 版本号
	ConsistencyDBPath         string   `yaml:"consistency_db_path"`           // 一致性检查的数据库路径
	OuterS3Bucket             string   `yaml:"outer_s3_bucket"`               // 业务S3的bucket
	OuterS3Region             string   `yaml:"outer_s3_region"`               // 业务S3的region
	InnerBrokers              []string `yaml:"inner_brokers"`                 // 内部kafka的brokers
	InnerNewBlockTopic        string   `yaml:"inner_new_block_topic"`         // 内部kafka的topic
	InnerNewBlockGroupID      string   `yaml:"inner_new_block_group_id"`      // 内部kafka的group id
	OuterBrokers              []string `yaml:"outer_brokers"`                 // 业务kafka的brokers
	OuterNewBlockTopic        string   `yaml:"outer_new_block_topic"`         // 业务kafka的chain topic
	OuterVersionNewBlockTopic string   `yaml:"outer_version_new_block_topic"` // 业务kafka的version topic
	EtcdEndpoints             []string `yaml:"etcd_endpoints"`                // etcd的endpoints
	CommitInterval            int      `yaml:"commit_interval"`               // 提交到kafka的间隔
	EtcdWriteTimeout          int      `yaml:"etcd_write_timeout_ms"`         // etcd写入超时时间(毫秒)
	EtcdLockTTL               int64    `yaml:"etcd_lock_ttl"`                 // etcd锁的ttl(秒)
	VersionCheckInterval      int      `yaml:"version_check_interval"`        // 版本检查间隔(秒)
	ForkScanInterval          int      `yaml:"fork_scan_interval_sec"`        // fork标记巡检间隔(秒)，<=0禁用
	ForkScanLookback          uint64   `yaml:"fork_scan_lookback"`            // fork标记巡检回看的高度数
}

var defaultConfig = Config{
	Listen:               ":8663",
	ReadyRatio:           0.8,
	CheckInterval:        20,
	RpcNodeTimeout:       50,
	MsgWaitTimeout:       5000,
	EtcdWriteTimeout:     5000, // 5 seconds default
	EtcdLockTTL:          20,
	VersionCheckInterval: 5, // 5 seconds default
	ForkScanInterval:     60,
	ForkScanLookback:     64,
}

// IsVersionMode 判断是否启用版本模式
// 当 Version 和 OuterVersionNewBlockTopic 都配置时启用版本模式
func (c *Config) IsVersionMode() bool {
	return c.Version != "" && c.OuterVersionNewBlockTopic != ""
}

func LoadConfig(configPath string) Config {
	configFile, err := os.Open(configPath)
	if err != nil {
		log.Fatalf("open config file error: %v\n", err)
	}
	defer configFile.Close()

	var config Config = defaultConfig
	parser := yaml.NewDecoder(configFile)
	err = parser.Decode(&config)
	if err != nil {
		log.Fatalf("parse config file error: %v\n", err)
	}
	return config
}
