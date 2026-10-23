package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Backend struct {
	Address string
	Alive   bool
	mux     sync.RWMutex
}

func (b *Backend) SetAlive(alive bool) {
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
	current  uint64
}

func (s *ServerPool) AddBackend(addr string) {
	s.backends = append(s.backends, &Backend{
		Address: addr,
		Alive:   true,
	})
}

func (s *ServerPool) GetNextPeer() *Backend {
	n := len(s.backends)
	if n == 0 {
		return nil
	}

	start := int(atomic.AddUint64(&s.current, 1)-1) % n

	for i := 0; i < n; i++ {
		idx := (start + i) % n
		if s.backends[idx].isAlive() {
			return s.backends[idx]
		}
	}
	return nil
}

func (s *ServerPool) HealthCheck() {
	for _, b := range s.backends {
		conn, err := net.DialTimeout("tcp", b.Address, 2*time.Second)
		alive := err == nil
		if conn != nil {
			conn.Close()
		}

		b.SetAlive(alive)

		status := "up"
		if !alive {
			status = "down"
		}

		log.Printf("%s [%s]", b.Address, status)
	}
}

func (s *ServerPool) handle(client net.Conn) {
	defer client.Close()

	peer := s.GetNextPeer()
	if peer == nil {
		log.Println("none avialable")
		return
	}

	upstream, err := net.DialTimeout("tcp", peer.Address, 2*time.Second)
	if err != nil {
		log.Printf("backend %s err: %v", peer.Address, err)
		peer.SetAlive(false)
		return
	}
	defer upstream.Close()

	log.Println("forwarding to", peer.Address)

	done := make(chan struct{})

	go func() {
		_, _ = io.Copy(upstream, client)
		if tcp, ok := upstream.(*net.TCPConn); ok {
			_ = tcp.CloseWrite()
		}
		close(done)
	}()

	_, _ = io.Copy(client, upstream)
	if tcp, ok := client.(*net.TCPConn); ok {
		_ = tcp.CloseWrite()
	}

	<-done
}

func main() {
	var backendList string
	var port int

	flag.StringVar(&backendList, "backend", "", "backends")
	flag.IntVar(&port, "port", 3030, "port to listen on")
	flag.Parse()

	if backendList == "" {
		log.Fatal("backends needed")
	}

	var serverPool ServerPool

	for _, addr := range strings.Split(backendList, ",") {
		addr = strings.TrimSpace(addr)
		if addr == "" {
			continue
		}

		serverPool.AddBackend(addr)
		log.Printf("conf backend: %s", addr)
	}

	if len(serverPool.backends) == 0 {
		log.Fatal("no backends available")
	}

	serverPool.HealthCheck()

	go func() {
		ticker := time.NewTicker(time.Minute * 2)
		defer ticker.Stop()

		for range ticker.C {
			log.Println("starting health check....")
			serverPool.HealthCheck()
			log.Println("health check done")
		}
	}()

	list, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatal(err)
	}
	defer list.Close()

	log.Printf("tcp lb at : %d", port)

	for {
		client, err := list.Accept()
		if err != nil {
			log.Println("accept err:", err)
			continue
		}

		go serverPool.handle(client)
	}

}
