package storage

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

/*
	GetWorkersCount() (int, error)

	cleanup test
	concurrent write test
*/

func TestInMemNodeStorage_StoreHub(t *testing.T) {
	cfg := DefaultStorageConfig()
	cfg.QueryGeoIP = false
	cfg.CleanupEnabled = false
	s := NewNodeStorage(cfg)

	s.StoreHub(&Hub{Addr: "1.2.3.4:10001", Available: true})
	h, err := s.GetHubs()
	assert.NoError(t, err)
	assert.Len(t, h, 1)

	count, err := s.GetHubsCount()
	assert.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestInMemNodeStorage_StoreHub2(t *testing.T) {
	cfg := DefaultStorageConfig()
	cfg.QueryGeoIP = false
	cfg.CleanupEnabled = false
	s := NewNodeStorage(cfg)

	wg := sync.WaitGroup{}
	for i := 1; i < 10000; i++ {
		wg.Add(1)
		go func(j int) {
			defer wg.Done()

			s.StoreHub(&Hub{Addr: "1.2.3.4:10001", Available: true})
		}(i)

	}
	wg.Wait()

	h, err := s.GetHubs()
	assert.NoError(t, err)
	assert.Len(t, h, 1)

	count, err := s.GetHubsCount()
	assert.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestInMemNodeStorage_StoreHub3(t *testing.T) {
	cfg := DefaultStorageConfig()
	cfg.QueryGeoIP = false
	cfg.CleanupEnabled = false
	s := NewNodeStorage(cfg)

	steps := 10000

	wg := sync.WaitGroup{}
	for i := 0; i < steps; i++ {
		wg.Add(1)
		go func(j int) {
			defer wg.Done()

			s.StoreHub(&Hub{Addr: fmt.Sprintf("1.2.3.4:%d", j), Available: true})
		}(i)

	}
	wg.Wait()

	h, err := s.GetHubs()
	assert.NoError(t, err)
	assert.Len(t, h, steps)

	count, err := s.GetHubsCount()
	assert.NoError(t, err)
	assert.Equal(t, steps, count)
}

func TestInMemNodeStorage_GetWorkersCount(t *testing.T) {
	cfg := DefaultStorageConfig()
	cfg.QueryGeoIP = false
	cfg.CleanupEnabled = false
	s := NewNodeStorage(cfg)

	wrk := []*Worker{
		{Addr: "1.2.3.4"},
		{Addr: "1.2.3.5"},
		{Addr: "1.2.3.6"},
	}

	s.StoreHub(&Hub{Addr: "1.2.3.4:10001", Available: true, Workers: wrk})

	h, err := s.GetHubs()
	assert.NoError(t, err)
	assert.Len(t, h, 1)

	cc, err := s.GetWorkersCount()
	assert.NoError(t, err)
	assert.Equal(t, 3, cc)
}

func TestInMemNodeStorage_GetWorkersCount2(t *testing.T) {
	cfg := DefaultStorageConfig()
	cfg.QueryGeoIP = false
	cfg.CleanupEnabled = false
	s := NewNodeStorage(cfg)

	wrk := []*Worker{
		{Addr: "1.2.3.4"},
		{Addr: "1.2.3.5"},
		{Addr: "1.2.3.6"},
	}

	wg := sync.WaitGroup{}
	for i := 0; i < 10000; i++ {
		wg.Add(1)
		go func(j int) {
			defer wg.Done()
			s.StoreHub(&Hub{Addr: "1.2.3.4:10001", Available: true, Workers: wrk})
		}(i)

	}
	wg.Wait()

	h, err := s.GetHubs()
	assert.NoError(t, err)
	assert.Len(t, h, 1)

	cc, err := s.GetWorkersCount()
	assert.NoError(t, err)
	assert.Equal(t, 3, cc)
}

func TestInMemNodeStorage_GetWorkersCount3(t *testing.T) {
	cfg := DefaultStorageConfig()
	cfg.QueryGeoIP = false
	cfg.CleanupEnabled = false
	s := NewNodeStorage(cfg)

	wrk := []*Worker{
		{Addr: "1.2.3.4"},
		{Addr: "1.2.3.5"},
		{Addr: "1.2.3.6"},
	}

	steps := 10000

	wg := sync.WaitGroup{}
	for i := 0; i < steps; i++ {
		wg.Add(1)
		go func(j int) {
			defer wg.Done()
			s.StoreHub(&Hub{Addr: fmt.Sprintf("1.2.3.4:%d", j), Available: true, Workers: wrk})
		}(i)

	}
	wg.Wait()

	h, err := s.GetHubs()
	assert.NoError(t, err)
	assert.Len(t, h, steps)

	cc, err := s.GetWorkersCount()
	assert.NoError(t, err)
	assert.Equal(t, len(wrk)*steps, cc)
}

func TestInMemNodeStorage_GetWorkersCount4(t *testing.T) {
	cfg := DefaultStorageConfig()
	cfg.QueryGeoIP = false
	cfg.CleanupEnabled = false
	s := NewNodeStorage(cfg)

	steps := 10000
	wrkSteps := 1000

	wg := sync.WaitGroup{}
	for i := 0; i < steps; i++ {
		wg.Add(1)
		go func(j int) {
			defer wg.Done()

			wrk := []*Worker{}
			for k := 0; k < wrkSteps; k++ {
				wrk = append(wrk, &Worker{Addr: fmt.Sprintf("5.5.5.5:%d", k)})
			}

			s.StoreHub(&Hub{Addr: fmt.Sprintf("1.2.3.4:%d", j), Available: true, Workers: wrk})
		}(i)

	}
	wg.Wait()

	h, err := s.GetHubs()
	assert.NoError(t, err)
	assert.Len(t, h, steps)

	cc, err := s.GetWorkersCount()
	assert.NoError(t, err)
	assert.Equal(t, steps*wrkSteps, cc)
}

func TestInMemNodeStorage_ExpirationCleanup(t *testing.T) {
	cfg := &StorageConfig{
		QueryGeoIP:     false,
		CleanupEnabled: true,
		NodeTTL:        1 * time.Second,
		CleanupPeriod:  1 * time.Second,
	}

	s := NewNodeStorage(cfg)
	for i := 0; i < 3; i++ {
		s.StoreHub(&Hub{Addr: fmt.Sprintf("1.2.3.4:%d", i)})
	}

	// wait for expiration
	time.Sleep(2 * time.Second)

	count, err := s.GetHubsCount()
	assert.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestInMemNodeStorage_ExpirationCleanup2(t *testing.T) {
	cfg := &StorageConfig{
		QueryGeoIP:     false,
		CleanupEnabled: true,
		NodeTTL:        2 * time.Second,
		CleanupPeriod:  1 * time.Second,
	}

	s := NewNodeStorage(cfg)
	for i := 0; i < 3; i++ {
		s.StoreHub(&Hub{Addr: fmt.Sprintf("1.2.3.4:%d", i)})
	}

	// wait for expiration
	time.Sleep(1500 * time.Millisecond)
	s.StoreHub(&Hub{Addr: "1.2.3.4:1111"})
	time.Sleep(1 * time.Second)

	count, err := s.GetHubsCount()
	assert.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestInMemNodeStorage_ExpirationCleanup3(t *testing.T) {
	cfg := &StorageConfig{
		QueryGeoIP:     false,
		CleanupEnabled: true,
		NodeTTL:        1 * time.Hour,
		CleanupPeriod:  1 * time.Second,
	}

	s := NewNodeStorage(cfg)
	s.StoreHub(&Hub{Addr: "1.2.3.4:1111"})
	// skip some cleanup cycles
	time.Sleep(3 * time.Second)

	count, err := s.GetHubsCount()
	assert.NoError(t, err)
	assert.Equal(t, 1, count)
}
