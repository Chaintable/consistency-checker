package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Chaintable/consistency-checker/metrics"

	"github.com/Chaintable/consistency-checker/check"
	"github.com/Chaintable/consistency-checker/config"
)

func parseCmdlineAndLoadConfig() config.Config {
	cmdlineConfig := config.Config{}

	flag.StringVar(&cmdlineConfig.Listen, "listen", "", "listen")

	configFilePath := flag.String("config", "", "config file")

	flag.Parse()

	// load file config
	fileConfig := config.LoadConfig(*configFilePath)

	// override file config with cmdline config
	if cmdlineConfig.Listen != "" {
		fileConfig.Listen = cmdlineConfig.Listen
	}

	if fileConfig.ChainID == 0 {
		log.Fatalf("chain_id must be set in config file")
	}

	if (fileConfig.Version != "" && fileConfig.OuterVersionNewBlockTopic == "") || (fileConfig.Version == "" && fileConfig.OuterVersionNewBlockTopic != "") {
		log.Fatalf("both version and outer_version_new_block_topic must be set or both must be empty")
	}

	return fileConfig
}

func main() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	config := parseCmdlineAndLoadConfig()
	log.Printf("[main] config: %+v", config)

	checker, err := check.NewChecker(&config)
	if err != nil {
		log.Fatalf("[main] NewChecker error %+v", err)
	}

	go func() {
		checker.Run()
	}()

	metrics.NodeInfo.WithLabelValues(fmt.Sprint(config.ChainID), "consistency-checker").Set(1)

	startHTTPServer(config.Listen)

	sig := <-sigChan

	log.Printf("[main] sig %v received, shutting down...", sig)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := checker.Close(shutdownCtx); err != nil {
		log.Printf("[main] graceful shutdown timed out: %v", err)
	}

}
