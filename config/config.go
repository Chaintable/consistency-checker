package config

import (
	"log"
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Listen                       string   `yaml:"listen"`
	ReadyRatio                   float64  `yaml:"ready_ratio"`               // 副本节点准备好的比例，达到后推送kafka
	CheckNum                     int      `yaml:"check_num"`                 // 每次得知block更新后，check副本节点的次数
	CheckInterval                int      `yaml:"check_interval_ms"`         // 每次checkk副本节点的间隔
	ChainID                      int64    `yaml:"chain_id"`                  // 链ID
	ConsistencyDBPath            string   `yaml:"consistency_db_path"`       // 一致性检查的数据库路径
	OuterS3Bucket                string   `yaml:"outer_s3_bucket"`           // 业务S3的bucket
	OuterS3Region                string   `yaml:"outer_s3_region"`           // 业务S3的region
	InnerBrokers                 []string `yaml:"inner_brokers"`             // 内部kafka的brokers
	InnerReplicaStateChangeTopic string   `yaml:"inner_replica_state_topic"` // 内部kafka的topic
	InnerNewBlockTopic           string   `yaml:"inner_new_block_topic"`     // 内部kafka的topic
	InnerNewBlockGroupID         string   `yaml:"inner_new_block_group_id"`  // 内部kafka的group id
	OuterBrokers                 []string `yaml:"outer_brokers"`             // 业务kafka的brokers
	OuterNewBlockTopic           string   `yaml:"outer_new_block_topic"`     // 业务kafka的topic
}

var defaultConfig = Config{
	Listen:        ":8663",
	ReadyRatio:    0.8,
	CheckInterval: 20,
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
