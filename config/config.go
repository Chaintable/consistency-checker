package config

import (
	"log"
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Listen                       string   `yaml:"listen"`
	ReadyRatio                   float64  `yaml:"ready_ratio"`
	CheckInterval                int      `yaml:"check_interval_ms"`
	ChainID                      int      `yaml:"chain_id"`
	ConsistencyDBPath            string   `yaml:"consistency_db_path"`
	InnerS3Bucket                string   `yaml:"inner_s3_bucket"`
	InnerS3Region                string   `yaml:"inner_s3_region"`
	InnerBrokers                 []string `yaml:"inner_brokers"`
	InnerReplicaStateChangeTopic string   `yaml:"inner_replica_state_topic"`
	InnerNewBlockGroupID         string   `yaml:"inner_new_block_group_id"`
	OuterBrokers                 []string `yaml:"outer_brokers"`
	OuterNewBlockTopic           string   `yaml:"outer_new_block_topic"`
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
