package main

import (
	"github.com/sshaman1101/sonm-net-monitor/discovery"
	"github.com/sshaman1101/sonm-net-monitor/storage"
)

func main() {
	discoCfg := discovery.DefaultDiscoveryConfig()
	storCfg := storage.DefaultStorageConfig()

	stor := storage.NewNodeStorage(storCfg)
	discovery.Start(discoCfg, stor)
}
