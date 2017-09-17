package main

import (
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"context"
	"crypto/ecdsa"
	"encoding/json"
	"strings"

	"github.com/ethereum/go-ethereum/whisper/whisperv2"
	geo "github.com/oschwald/geoip2-golang"
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

var (
	geodb *geo.Reader
)

type (
	pos struct {
		Country string  `json:"country"`
		City    string  `json:"city"`
		Lat     float64 `json:"lat"`
		Lon     float64 `json:"lon"`
	}

	hub struct {
		Addr      string    `json:"Addr"`
		Available bool      `json:"available"`
		Workers   []*worker `json:"workers"`
		Pos       *pos      `json:"geo"`
		pubKey    *ecdsa.PublicKey
	}

	worker struct {
		Addr string `json:"addr"`
	}
)

func (h *hub) isAnon() bool {
	return h.pubKey == nil
}

func (h *hub) getClientAddr() string {
	ip := h.getIP()
	if ip == nil {
		return ""
	}

	return ip.String() + ":10001"
}

func (h *hub) getIP() net.IP {
	ipport := strings.Split(h.Addr, ":")
	if len(ipport) != 2 {
		return nil
	}

	return net.ParseIP(ipport[0])
}

func (h *hub) toPtr() *hub {
	h2 := &hub{}
	*h2 = *h
	return h2
}

type NodeStorage interface {
	StoreHub(*hub) error
	GetHubs() ([]*hub, error)
	GetHubsCount() (int, error)
	GetWorkersCount() (int, error)
}

// inMemNodeStorage implements NodeStorage interface
type inMemNodeStorage struct {
	mx   sync.RWMutex
	hubs map[string]*hub
}

func (ns *inMemNodeStorage) StoreHub(h *hub) error {
	ns.mx.Lock()
	defer ns.mx.Unlock()

	h2, ok := ns.hubs[h.Addr]
	if !ok {
		// if hub doesn't exists in storage - query for geoIP and save it
		if pos, err := queryGeoIP(h.getIP().String()); err == nil {
			h.Pos = pos
		}
		ns.hubs[h.Addr] = h
	} else {
		// if Hub already present in storage - update fields that can be changed and save record back
		h2.Workers = h.Workers
		h2.Available = h.Available
		h2.pubKey = h.pubKey
		ns.hubs[h2.Addr] = h2
	}

	return nil
}

func (ns *inMemNodeStorage) GetHubs() ([]*hub, error) {
	ns.mx.RLock()
	defer ns.mx.RUnlock()

	hubs := make([]*hub, 0, len(ns.hubs))
	for _, hu := range ns.hubs {
		hubs = append(hubs, hu)
	}

	return hubs, nil
}

func (ns *inMemNodeStorage) GetHubsCount() (int, error) {
	ns.mx.RLock()
	defer ns.mx.RUnlock()

	return len(ns.hubs), nil
}

func (ns *inMemNodeStorage) GetWorkersCount() (int, error) {
	ns.mx.RLock()
	defer ns.mx.RUnlock()

	total := 0
	for _, hb := range ns.hubs {
		total += len(hb.Workers)
	}

	return total, nil
}

func (ns *inMemNodeStorage) getAvailableHubCount() int {
	ns.mx.RLock()
	defer ns.mx.RUnlock()

	c := 0
	for _, h := range ns.hubs {
		if h.Available {
			c++
		}
	}

	return c
}

func (ns *inMemNodeStorage) dump() []byte {
	ns.mx.RLock()
	defer ns.mx.RUnlock()

	b, _ := json.MarshalIndent(ns.hubs, "", "    ")
	return b
}

// todo: implement cleanup for expired nodes
//func (ns *inMemNodeStorage) cleanExpiredNodes() {
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
//					log.Printf("Deadlive reached for hub = %s\r\n", h.Addr)
//					// todo: remove node
//				} else {
//					log.Printf("Hub %s has at least %s to expiration\r\n", h.Addr, h.ts.Sub(deadline))
//				}
//			}
//		}
//	}
//}

func newNodeStorage() *inMemNodeStorage {
	ns := &inMemNodeStorage{
		mx:   sync.RWMutex{},
		hubs: make(map[string]*hub),
	}

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

func startHubDiscovery(p2p *frd.Server, storage *inMemNodeStorage) {
	hubChan := make(chan *hub)
	p2p.Frd.AddHandling(nil, nil, func(msg *whisperv2.Message) {
		hubKey := msg.Recover()
		h := &hub{
			Addr:   string(msg.Payload),
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
			hubCount, _ := storage.GetHubsCount()
			workerCount, _ := storage.GetWorkersCount()
			log.Printf("[!!!] Hubs: %d  Wrk: %d\r\n", hubCount, workerCount)
			log.Println(string(storage.dump()))
		}
	}
}

func checkAndStore(h *hub, storage *inMemNodeStorage) {
	isAvailable := checkHubAvailability(h)
	h.Available = isAvailable
	if isAvailable {
		h.Workers = queryWorkers(h)
	}

	storage.StoreHub(h)
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
		wrk = append(wrk, &worker{Addr: addr})
	}

	return wrk
}

func initGeoDB() *geo.Reader {
	db, err := geo.Open("geo.mmdb")
	if err != nil {
		panic("Cannot open geoip db: " + err.Error())
	}

	return db
}

func queryGeoIP(s string) (*pos, error) {
	ip := net.ParseIP(s)
	if ip == nil {
		return nil, fmt.Errorf("Cannot parse \"%s\" to net.IP", s)
	}

	rec, err := geodb.City(ip)
	if err != nil {
		return nil, err
	}

	return &pos{
		Lat:     rec.Location.Latitude,
		Lon:     rec.Location.Longitude,
		City:    rec.City.Names["en"],
		Country: rec.Country.Names["en"],
	}, nil
}

func main() {
	geodb = initGeoDB()
	defer geodb.Close()

	p2p := startP2PServer()
	storage := newNodeStorage()

	startHubDiscovery(p2p, storage)
}
