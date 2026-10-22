package nlb

import (
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type Backend struct {
	Address string 
	Alive bool 
	mux sync.RWMutex
}

func (b *Backend) setAlive(alive bool) {
	b.mux.Lock()
	b.Alive = alive
	b.mux.Unlock()
}

func (b *Backend) isAlive() bool {
	b.mux.RLock()
	alive := b.Alive
	b.mux.RUnlock()
	return alive
}

type ServerPool struct {
	backends []*Backend
	current uint64
}

func (s *ServerPool) AddBackend(addr string) {
	s.backends = append(s.backends, &Backend{
		Address: addr,
		Alive: true,
	})
}

func (s *ServerPool) GetNextPeer() *Backend {
	n := len(s.backends)
	if n == 0 {
		return nil
	}

	start := int(atomic.AddUint64(&s.current, 1) - 1) % n 

	for i := 0; i<n ; i++ {
		idx := (start + i) % n
		if s.backends[idx].isAlive() {
			return s.backends[idx]
		}
	}
	return nil
}

func (s *ServerPool) HealthCheck() {
	for  _, b := range s.backends {
		conn, err := net.DialTimeout("tcp", b.Address, 2*time.Second) 
		alive := err == nil 
		if conn != nil {
			conn.Close()
		}

		b.setAlive(alive)

		status := "up" 
		if !alive {
			status = "down"
		}

		log.Printf("%s [%s]", b.Address, status)
	}
}

func 