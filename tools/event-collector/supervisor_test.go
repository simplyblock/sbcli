package main

import (
	"context"
	"testing"
	"time"
)

func TestSupervisorStartsStopsAndRestartsWorkers(t *testing.T) {
	st := newMemStore()
	st.clusters = []Cluster{{UUID: "cl", TwoNodeArbitration: true}}
	st.nodes["cl"] = []Node{
		{UUID: "a", ClusterID: "cl", MgmtIP: "10.0.0.1", RPCPort: 8080},
		{UUID: "b", ClusterID: "cl", MgmtIP: "10.0.0.2", RPCPort: 8080},
		{UUID: "gone", ClusterID: "cl", MgmtIP: "10.0.0.3", RPCPort: 8080, Status: nodeStatusRemoved},
	}
	callers := map[string]int{}
	s := &Supervisor{Store: st, NewCaller: func(n Node) Caller { callers[n.UUID]++; return &scripted{} }}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := s.Reconcile(ctx); err != nil || s.Workers() != 2 {
		t.Fatalf("workers %d err %v", s.Workers(), err)
	}
	// unchanged: no restart
	_ = s.Reconcile(ctx)
	if callers["a"] != 1 {
		t.Fatalf("a restarted without a change: %v", callers)
	}
	// an address change restarts the worker
	st.nodes["cl"][0].MgmtIP = "10.0.0.9"
	_ = s.Reconcile(ctx)
	if callers["a"] != 2 || s.Workers() != 2 {
		t.Fatalf("callers %v workers %d", callers, s.Workers())
	}
	// disabling arbitration stops every worker
	st.clusters = nil
	done := make(chan struct{})
	go func() { _ = s.Reconcile(ctx); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("stopping workers hung")
	}
	if s.Workers() != 0 {
		t.Fatalf("workers %d", s.Workers())
	}
}
