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
	inf "github.com/influxdata/influxdb/client/v2"
	"github.com/mmcloughlin/geohash"
	geo "github.com/oschwald/geoip2-golang"
	cli "github.com/sonm-io/core/cmd/cli/commands"
	"github.com/sonm-io/core/common"
	frd "github.com/sonm-io/core/fusrodah/miner"
)

const (
	hubRequestTimeout = 10 * time.Second
	discoveryPeriod   = 30 * time.Second
	// todo: read this params from config or cli flag
	influxdbAddr       = "http://10.0.1.1:8086"
	influxdbDB         = "sonm"
	influxdbUser       = "admin"
	influxdbPassword   = "admin"
	influxdbSavePeriod = 1 * time.Minute

	//hubInfoExpirationPeriod = 1 * time.Minute
	//hubInfoCleanupPeriod    = 30 * time.Second
)

var (
	geodb     *geo.Reader
	infClient inf.Client
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

type nodeStorage struct {
	mx   sync.RWMutex
	hubs map[string]*hub
}

func (ns *nodeStorage) PutHub(h *hub) {
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

	hb, ok := ns.hubs[h.Addr]
	if !ok {
		return nil
	}

	return hb.Workers
}

func (ns *nodeStorage) getHubCount() int {
	ns.mx.RLock()
	defer ns.mx.RUnlock()

	return len(ns.hubs)
}

func (ns *nodeStorage) getAvailableHubCount() int {
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

func (ns *nodeStorage) getWorkerCount() int {
	ns.mx.RLock()
	defer ns.mx.RUnlock()

	total := 0
	for _, hb := range ns.hubs {
		total += len(hb.Workers)
	}

	return total
}

func (ns *nodeStorage) dump() []byte {
	ns.mx.RLock()
	defer ns.mx.RUnlock()

	b, _ := json.MarshalIndent(ns.hubs, "", "    ")
	return b
}

func (ns *nodeStorage) saveToInflux(t time.Duration) {
	tk := time.NewTicker(t)
	for {
		select {
		case <-tk.C:
			err := ns.saveNodesCount()
			if err != nil {
				log.Printf("[INFLX] Cannot save node counters to influx: %v\r\n", err)
			}
		}
	}
}

func (ns *nodeStorage) getNodesCountFields() map[string]interface{} {
	return map[string]interface{}{
		"hubs":           ns.getHubCount(),
		"hubs_available": ns.getAvailableHubCount(),
		"workers":        ns.getWorkerCount(),
	}
}

func (ns *nodeStorage) getNodesCoordinatesPoints() ([]*inf.Point, error) {
	ns.mx.RLock()
	defer ns.mx.RUnlock()

	points := make([]*inf.Point, 0, len(ns.hubs))
	for _, h := range ns.hubs {
		tags := map[string]string{}
		fields := map[string]interface{}{
			"geo":   geohash.Encode(h.Pos.Lat, h.Pos.Lon),
			"place": h.Pos.City,
		}

		pt, err := inf.NewPoint("nodes_map", tags, fields, time.Now())
		if err != nil {
			return nil, err
		}

		points = append(points, pt)
	}

	return points, nil
}

func (ns *nodeStorage) saveNodesCount() error {
	// build coordinates points
	coPoints, err := ns.getNodesCoordinatesPoints()
	if err != nil {
		return err
	}

	bp, err := inf.NewBatchPoints(inf.BatchPointsConfig{
		Database:  influxdbDB,
		Precision: "s",
	})
	if err != nil {
		return err
	}

	// Create count point
	tags := map[string]string{}
	fields := ns.getNodesCountFields()

	pt, err := inf.NewPoint("network", tags, fields, time.Now())
	if err != nil {
		return err
	}

	bp.AddPoint(pt)
	bp.AddPoints(coPoints)

	// Write the batch
	if err := infClient.Write(bp); err != nil {
		return err
	}

	return nil
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
//					log.Printf("Deadlive reached for hub = %s\r\n", h.Addr)
//					// todo: remove node
//				} else {
//					log.Printf("Hub %s has at least %s to expiration\r\n", h.Addr, h.ts.Sub(deadline))
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

	go ns.saveToInflux(influxdbSavePeriod)

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
			log.Printf("[!!!] Hubs: %d  Wrk: %d\r\n", storage.getHubCount(), storage.getWorkerCount())
			log.Println(string(storage.dump()))
		}
	}
}

func checkAndStore(h *hub, storage *nodeStorage) {
	isAvailable := checkHubAvailability(h)
	h.Available = isAvailable
	if isAvailable {
		h.Workers = queryWorkers(h)
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

func initInfluxClient() {
	// Create a new HTTPClient
	var err error
	infClient, err = inf.NewHTTPClient(inf.HTTPConfig{
		Addr:     influxdbAddr,
		Username: influxdbUser,
		Password: influxdbPassword,
	})
	if err != nil {
		panic("Cannot init influx client: " + err.Error())
	}
}

func main() {
	initInfluxClient()
	geodb = initGeoDB()
	defer geodb.Close()

	p2p := startP2PServer()
	storage := newNodeStorage()

	startHubDiscovery(p2p, storage)
}
