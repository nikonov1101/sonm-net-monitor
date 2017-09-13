package main

import (
	"log"
	"sync"
	"time"

	"context"
	"crypto/ecdsa"
	"strings"

	"github.com/ethereum/go-ethereum/whisper/whisperv2"
	cli "github.com/sonm-io/core/cmd/cli/commands"
	"github.com/sonm-io/core/common"
	frd "github.com/sonm-io/core/fusrodah/miner"
)

const (
	hubRequestTimeout = 10 * time.Second
	discoveryPeriod   = 30 * time.Second
	//hubInfoExpirationPeriod = 1 * time.Minute
	//hubInfoCleanupPeriod    = 30 * time.Second
)

type (
	hub struct {
		addr    string
		id      string
		pubKey  *ecdsa.PublicKey
		pinged  bool
		workers []*worker
	}

	worker struct {
		addr string
	}
)

func (h *hub) isAnon() bool {
	return h.pubKey == nil
}

func (h *hub) getClientAddr() string {
	ipport := strings.Split(h.addr, ":")
	if len(ipport) == 2 {
		// use default client port
		return ipport[0] + ":10001"
	}

	log.Printf("Cannot parse address: %s\r\n", h.addr)
	return ""
}

func (h *hub) toPtr() *hub {
	h2 := &hub{}
	*h2 = *h
	return h2
}

type nodeStorage struct {
	mx   sync.RWMutex
	hubs map[string]*hub
}

func (ns *nodeStorage) PutHub(h *hub) {
	ns.mx.Lock()
	defer ns.mx.Unlock()

	log.Printf("[STORAGE] hub %s has %d workers\r\n", h.addr, len(h.workers))

	_, ok := ns.hubs[h.addr]
	if !ok {
		ns.hubs[h.addr] = h
	}
}

func (ns *nodeStorage) GetHubs() []*hub {
	ns.mx.RLock()
	defer ns.mx.RUnlock()

	hubs := make([]*hub, 0, len(ns.hubs))
	for _, hu := range ns.hubs {
		hubs = append(hubs, hu)
	}

	return hubs
}

func (ns *nodeStorage) GetWorkerForHub(h hub) []*worker {
	ns.mx.RLock()
	defer ns.mx.RUnlock()

	hb, ok := ns.hubs[h.addr]
	if !ok {
		return nil
	}

	return hb.workers
}

func (ns *nodeStorage) GetHubCount() int {
	ns.mx.RLock()
	defer ns.mx.RUnlock()

	return len(ns.hubs)
}

func (ns *nodeStorage) GetWorkerCount() int {
	ns.mx.RLock()
	defer ns.mx.RUnlock()

	total := 0
	for _, hb := range ns.hubs {
		total += len(hb.workers)
	}

	return total
}

// todo: implement cleanup for expired nodes
//func (ns *nodeStorage) cleanExpiredNodes() {
//	t := time.NewTicker(hubInfoCleanupPeriod)
//	defer t.Stop()
//
//	ns.mx.Lock()
//	defer ns.mx.Unlock()
//
//	deadline := time.Now().Add(-1 * hubInfoExpirationPeriod)
//
//	for {
//		select {
//		case <-t.C:
//			for h := range ns.hubs {
//				if h.ts.Before(deadline) {
//					log.Printf("Deadlive reached for hub = %s\r\n", h.addr)
//					// todo: remove node
//				} else {
//					log.Printf("Hub %s has at least %s to expiration\r\n", h.addr, h.ts.Sub(deadline))
//				}
//			}
//		}
//	}
//}

func newNodeStorage() *nodeStorage {
	ns := &nodeStorage{
		mx:   sync.RWMutex{},
		hubs: make(map[string]*hub),
	}
	// go ns.cleanExpiredNodes()

	return ns
}

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

func startHubDiscovery(p2p *frd.Server, storage *nodeStorage) {
	hubChan := make(chan *hub)
	p2p.Frd.AddHandling(nil, nil, func(msg *whisperv2.Message) {
		hubKey := msg.Recover()
		h := &hub{
			addr:   string(msg.Payload),
			pubKey: hubKey,
		}
		hubChan <- h
	}, common.TopicMinerDiscover)

	t := time.NewTicker(discoveryPeriod)
	defer t.Stop()

	show := time.NewTicker(time.Minute)
	defer show.Stop()

	for {
		select {
		case h := <-hubChan:
			go func(hb *hub) {
				checkAndStore(hb.toPtr(), storage)
			}(h)
		case <-t.C:
			p2p.Frd.Send(p2p.GetPubKeyString(), true, common.TopicHubDiscover)
		case <-show.C:
			log.Printf("[!!!] Hubs: %d  Wrk: %d\r\n\r\n", storage.GetHubCount(), storage.GetWorkerCount())
		}
	}
}

func checkAndStore(h *hub, storage *nodeStorage) {
	isAvailable := checkHubAvailability(h)
	h.pinged = isAvailable
	if isAvailable {
		h.workers = queryWorkers(h)
	}

	storage.PutHub(h)
}

func checkHubAvailability(h *hub) bool {
	addr := h.getClientAddr()
	if addr == "" {
		return false
	}

	itr, err := cli.NewGrpcInteractor(addr, hubRequestTimeout)
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

func queryWorkers(h *hub) []*worker {
	addr := h.getClientAddr()
	if addr == "" {
		return nil
	}

	itr, err := cli.NewGrpcInteractor(addr, hubRequestTimeout)
	if err != nil {
		log.Printf("Cannot build interactor: %v\r\n", err)
		return nil
	}

	list, err := itr.MinerList(context.Background())
	if err != nil {
		return nil
	}

	wrk := []*worker{}
	for addr := range list.Info {
		wrk = append(wrk, &worker{addr: addr})
	}

	return wrk
}

func main() {
	p2p := startP2PServer()
	storage := newNodeStorage()

	startHubDiscovery(p2p, storage)
}
