package discovery

import (
	"log"
	"time"

	"context"

	"github.com/ethereum/go-ethereum/whisper/whisperv2"
	cli "github.com/sonm-io/core/cmd/cli/commands"
	"github.com/sonm-io/core/common"
	frd "github.com/sonm-io/core/fusrodah/miner"
	"github.com/sshaman1101/sonm-net-monitor/storage"
)

const (
	defaultGrpcTimeout       = 10 * time.Second
	defaultDiscoveryPeriod   = 30 * time.Second
	defaultStorageDumpPeriod = 1 * time.Minute
)

func startP2PServer() *frd.Server {
	p2p, err := frd.NewServer(nil)
	if err != nil {
		panic("Cannot init p2p instance: " + err.Error())
	}

	err = p2p.Start()
	if err != nil {
		panic("Cannot start p2p instance: " + err.Error())
	}

	p2p.Serve()
	return p2p
}

type DiscoveryConfig struct {
	p2p *frd.Server

	GrpcRequestTimeout     time.Duration
	DiscoveryPeriod        time.Duration
	DebugStorageDumpPeriod time.Duration
}

func DefaultDiscoveryConfig() *DiscoveryConfig {
	c := &DiscoveryConfig{
		GrpcRequestTimeout:     defaultGrpcTimeout,
		DiscoveryPeriod:        defaultDiscoveryPeriod,
		DebugStorageDumpPeriod: defaultStorageDumpPeriod,
	}
	c.p2p = startP2PServer()
	return c
}

func Start(config *DiscoveryConfig, stor storage.NodeStorage) {
	hubChan := make(chan *storage.Hub)
	config.p2p.Frd.AddHandling(nil, nil, func(msg *whisperv2.Message) {
		hubKey := msg.Recover()
		h := &storage.Hub{
			Addr:   string(msg.Payload),
			PubKey: hubKey,
		}
		hubChan <- h
	}, common.TopicMinerDiscover)

	t := time.NewTicker(config.DiscoveryPeriod)
	defer t.Stop()

	show := time.NewTicker(config.DebugStorageDumpPeriod)
	defer show.Stop()

	for {
		select {
		case h := <-hubChan:
			go func(hb *storage.Hub) {
				checkAndStore(hb.ToPtr(), stor)
			}(h)
		case <-t.C:
			config.p2p.Frd.Send(config.p2p.GetPubKeyString(), true, common.TopicHubDiscover)
		case <-show.C:
			hubCount, _ := stor.GetHubsCount()
			workerCount, _ := stor.GetWorkersCount()
			log.Printf("[!!!] Hubs: %d  Wrk: %d\r\n", hubCount, workerCount)
			log.Println(string(stor.Dump()))
		}
	}
}

func checkAndStore(h *storage.Hub, storage storage.NodeStorage) {
	isAvailable := checkHubAvailability(h)
	h.Available = isAvailable
	if isAvailable {
		h.Workers = queryWorkers(h)
	}

	storage.StoreHub(h)
}

func checkHubAvailability(h *storage.Hub) bool {
	addr := h.GetClientAddr()
	if addr == "" {
		return false
	}

	itr, err := cli.NewGrpcInteractor(addr, defaultGrpcTimeout)
	if err != nil {
		log.Printf("Cannot build cli interactor: %v\r\n", err)
		return false
	}

	_, err = itr.HubPing(context.Background())
	if err != nil {
		return false
	} else {
		return true
	}
}

func queryWorkers(h *storage.Hub) []*storage.Worker {
	addr := h.GetClientAddr()
	if addr == "" {
		return nil
	}

	itr, err := cli.NewGrpcInteractor(addr, defaultGrpcTimeout)
	if err != nil {
		log.Printf("Cannot build interactor: %v\r\n", err)
		return nil
	}

	list, err := itr.MinerList(context.Background())
	if err != nil {
		return nil
	}

	wrk := []*storage.Worker{}
	for addr := range list.Info {
		wrk = append(wrk, &storage.Worker{Addr: addr})
	}

	return wrk
}
