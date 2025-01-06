package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

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

	checker.Close()

	log.Printf("[main] sig %v received, shutting down...", sig)
}
