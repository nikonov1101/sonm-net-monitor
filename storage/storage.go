package storage

import (
	"fmt"
	"net"
	"sync"
	"time"

	"crypto/ecdsa"
	"encoding/json"
	"strings"

	geo "github.com/oschwald/geoip2-golang"
)

const (
	defaultNodeTTL       = 1 * time.Minute
	defaultCleanupPeriod = 30 * time.Second
)

var (
	geodb *geo.Reader
)

func init() {
	db, err := geo.Open("geo.mmdb")
	if err != nil {
		panic("Cannot open geoip db: " + err.Error())
	}

	geodb = db
}

type Pos struct {
	Country string  `json:"country"`
	City    string  `json:"city"`
	Lat     float64 `json:"lat"`
	Lon     float64 `json:"lon"`
}

type Hub struct {
	Addr      string           `json:"addr"`
	Available bool             `json:"available"`
	Workers   []*Worker        `json:"workers"`
	Pos       *Pos             `json:"geo"`
	PubKey    *ecdsa.PublicKey `json:"-"`
}

type Worker struct {
	Addr string `json:"addr"`
}

func (h *Hub) GetClientAddr() string {
	ip := h.GetIP()
	if ip == nil {
		return ""
	}

	return ip.String() + ":10001"
}

func (h *Hub) GetIP() net.IP {
	ipport := strings.Split(h.Addr, ":")
	if len(ipport) != 2 {
		return nil
	}

	return net.ParseIP(ipport[0])
}

func (h *Hub) ToPtr() *Hub {
	h2 := &Hub{}
	*h2 = *h
	return h2
}

type NodeStorage interface {
	StoreHub(*Hub) error
	GetHubs() ([]*Hub, error)
	GetHubsCount() (int, error)
	GetWorkersCount() (int, error)
	Dump() []byte
}

// inMemNodeStorage implements NodeStorage interface
type inMemNodeStorage struct {
	mx     sync.RWMutex
	hubs   map[string]*Hub
	config *StorageConfig
}

func (ns *inMemNodeStorage) StoreHub(h *Hub) error {
	ns.mx.Lock()
	defer ns.mx.Unlock()

	h2, ok := ns.hubs[h.Addr]
	if !ok {
		// if Hub doesn't exists in storage - query for geoIP and save it
		if ns.config.QueryGeoIP {
			if pos, err := queryGeoIP(h.GetIP().String()); err == nil {
				h.Pos = pos
			}
		}
		ns.hubs[h.Addr] = h
	} else {
		// if Hub already present in storage - update fields that can be changed and save record back
		h2.Workers = h.Workers
		h2.Available = h.Available
		h2.PubKey = h.PubKey
		ns.hubs[h2.Addr] = h2
	}

	return nil
}

func (ns *inMemNodeStorage) GetHubs() ([]*Hub, error) {
	ns.mx.RLock()
	defer ns.mx.RUnlock()

	hubs := make([]*Hub, 0, len(ns.hubs))
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

func (ns *inMemNodeStorage) Dump() []byte {
	ns.mx.RLock()
	defer ns.mx.RUnlock()

	b, _ := json.MarshalIndent(ns.hubs, "", "    ")
	return b
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

// todo(sshaman1101): implement cleanup for expired nodes
//func (ns *inMemNodeStorage) cleanExpiredNodes() {
//	t := time.NewTicker(defaultCleanupPeriod)
//	defer t.Stop()
//
//	ns.mx.Lock()
//	defer ns.mx.Unlock()
//
//	deadline := time.Now().Add(-1 * defaultNodeTTL)
//
//	for {
//		select {
//		case <-t.C:
//			for h := range ns.hubs {
//				if h.ts.Before(deadline) {
//					log.Printf("Deadlive reached for Hub = %s\r\n", h.Addr)
//					// todo: remove node
//				} else {
//					log.Printf("Hub %s has at least %s to expiration\r\n", h.Addr, h.ts.Sub(deadline))
//				}
//			}
//		}
//	}
//}

type StorageConfig struct {
	NodeTTL       time.Duration
	CleanupPeriod time.Duration
	QueryGeoIP    bool
}

func DefaultStorageConfig() *StorageConfig {
	return &StorageConfig{
		QueryGeoIP:    true,
		NodeTTL:       defaultNodeTTL,
		CleanupPeriod: defaultCleanupPeriod,
	}
}

func NewNodeStorage(config *StorageConfig) NodeStorage {
	ns := &inMemNodeStorage{
		mx:     sync.RWMutex{},
		hubs:   make(map[string]*Hub),
		config: config,
	}

	return ns
}

func queryGeoIP(s string) (*Pos, error) {
	ip := net.ParseIP(s)
	if ip == nil {
		return nil, fmt.Errorf("Cannot parse \"%s\" to net.IP", s)
	}

	rec, err := geodb.City(ip)
	if err != nil {
		return nil, err
	}

	return &Pos{
		Lat:     rec.Location.Latitude,
		Lon:     rec.Location.Longitude,
		City:    rec.City.Names["en"],
		Country: rec.Country.Names["en"],
	}, nil
}
