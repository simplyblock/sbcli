package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

// Supervisor keeps one Worker per storage node of every arbitrated cluster, starting and
// stopping them as clusters enable or disable arbitration and nodes join, leave or move.
type Supervisor struct {
	Store    Store
	TLS      TLSConfig
	Client   *http.Client
	Interval time.Duration
	Log      *slog.Logger
	// NewCaller is replaceable in tests.
	NewCaller func(Node) Caller

	mu      sync.Mutex
	running map[string]*running
}

type running struct {
	fingerprint string
	cancel      context.CancelFunc
	done        chan struct{}
}

func (s *Supervisor) caller(n Node) Caller {
	if s.NewCaller != nil {
		return s.NewCaller(n)
	}
	return &HTTPCaller{
		URL:      fmt.Sprintf("%s://%s:%d/", s.TLS.Scheme(), n.MgmtIP, n.RPCPort),
		Username: n.RPCUsername, Password: n.RPCPassword, Client: s.Client,
	}
}

// fingerprint changes when the node must be reached differently, which restarts its worker.
func fingerprint(n Node) string {
	return fmt.Sprintf("%s|%s|%d|%s|%s", n.ClusterID, n.MgmtIP, n.RPCPort, n.RPCUsername, n.RPCPassword)
}

// Reconcile brings the set of workers in line with the store once.
func (s *Supervisor) Reconcile(ctx context.Context) error {
	clusters, err := s.Store.ArbitratedClusters()
	if err != nil {
		return err
	}
	want := map[string]Node{}
	for _, c := range clusters {
		nodes, err := s.Store.Nodes(c.UUID)
		if err != nil {
			return err
		}
		for _, n := range nodes {
			if n.Status == nodeStatusRemoved || n.MgmtIP == "" || n.RPCPort <= 0 {
				continue
			}
			want[n.UUID] = n
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.running == nil {
		s.running = map[string]*running{}
	}
	for id, r := range s.running {
		n, ok := want[id]
		if !ok || fingerprint(n) != r.fingerprint {
			r.cancel()
			<-r.done
			delete(s.running, id)
			s.log().Info("worker stopped", "node", id)
		}
	}
	for id, n := range want {
		if _, ok := s.running[id]; ok {
			continue
		}
		wctx, cancel := context.WithCancel(ctx)
		r := &running{fingerprint: fingerprint(n), cancel: cancel, done: make(chan struct{})}
		w := &Worker{ClusterID: n.ClusterID, NodeID: n.UUID, RPC: s.caller(n), Store: s.Store, Log: s.log()}
		go func() {
			defer close(r.done)
			w.Run(wctx)
		}()
		s.running[id] = r
		s.log().Info("worker started", "cluster", n.ClusterID, "node", id)
	}
	return nil
}

// Workers is the number of running workers.
func (s *Supervisor) Workers() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.running)
}

// Run reconciles every Interval until ctx ends, then stops all workers.
func (s *Supervisor) Run(ctx context.Context) {
	interval := s.Interval
	if interval == 0 {
		interval = 5 * time.Second
	}
	for {
		if err := s.Reconcile(ctx); err != nil {
			s.log().Warn("reconcile failed", "err", err)
		}
		if sleepCtx(ctx, interval) != nil {
			break
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, r := range s.running {
		r.cancel()
		<-r.done
		delete(s.running, id)
	}
}

func (s *Supervisor) log() *slog.Logger {
	if s.Log != nil {
		return s.Log
	}
	return slog.Default()
}
