package main

import (
	"context"
	"errors"
	"testing"
	"time"
)

func newWorker(rpc Caller, st Store) *Worker {
	return &Worker{ClusterID: "cl", NodeID: "n1", RPC: rpc, Store: st, Now: fixedNow,
		Timeout: 50 * time.Millisecond}
}

func ev(seq int, status string) map[string]any {
	return map[string]any{"seq": seq, "event_type": "device_status", "jm_vuid": 3, "status": status}
}

func key(instance string, seq int64) string {
	return Event{ClusterID: "cl", NodeID: "n1", Instance: instance, Seq: seq}.Key()
}

func TestFirstCallResyncsThenQueuesEventsInOrder(t *testing.T) {
	rpc, st := &scripted{}, newMemStore()
	rpc.push("jc_wait_events", waitResult{Instance: "i1", Seq: 40})
	rpc.push("jc_ha_status", map[string]any{"instance": "i1", "seq": 40, "epoch": 7})
	rpc.push("jc_wait_events", waitResult{Instance: "i1", Seq: 42,
		Events: []map[string]any{ev(41, "remote_jm_unhealthy"), ev(42, "ha_hold_started")}})
	w := newWorker(rpc, st)
	for i := 0; i < 2; i++ {
		if err := w.Once(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if st.count() != 3 {
		t.Fatalf("want resync + 2 events, got %d", st.count())
	}
	resync := st.events[key("i1", 40)]
	if resync.Payload["status"] != statusResync || resync.Payload["reason"] != "instance" {
		t.Fatalf("resync payload %v", resync.Payload)
	}
	args := rpc.waitArgs()
	if args[0].Instance != "" || args[0].AfterSeq != 0 || args[1].Instance != "i1" || args[1].AfterSeq != 40 {
		t.Fatalf("acknowledgements %+v", args)
	}
	if w.afterSeq != 42 {
		t.Fatalf("afterSeq %d", w.afterSeq)
	}
	got := st.events[key("i1", 41)]
	if got.Payload["instance"] != "i1" || got.ReceivedAt != fixedNow().UnixMilli() {
		t.Fatalf("event %+v", got)
	}
}

func TestRedeliveredEventsAreSkippedAndEmptyAnswerAdvancesSeq(t *testing.T) {
	rpc, st := &scripted{}, newMemStore()
	w := newWorker(rpc, st)
	w.instance, w.afterSeq = "i1", 42
	rpc.push("jc_wait_events", waitResult{Instance: "i1", Seq: 43, Events: []map[string]any{ev(42, "x"), ev(43, "y")}})
	rpc.push("jc_wait_events", waitResult{Instance: "i1", Seq: 50})
	_ = w.Once(context.Background())
	_ = w.Once(context.Background())
	if st.count() != 1 || w.afterSeq != 50 {
		t.Fatalf("count %d afterSeq %d", st.count(), w.afterSeq)
	}
}

func TestOverflowResyncs(t *testing.T) {
	rpc, st := &scripted{}, newMemStore()
	w := newWorker(rpc, st)
	w.instance, w.afterSeq = "i1", 10
	rpc.push("jc_wait_events", waitResult{Instance: "i1", Seq: 2000, Overflow: true})
	rpc.push("jc_ha_status", map[string]any{"instance": "i1", "seq": 2000})
	if err := w.Once(context.Background()); err != nil {
		t.Fatal(err)
	}
	e := st.events[key("i1", 2000)]
	if e.Payload["reason"] != "overflow" || w.afterSeq != 2000 {
		t.Fatalf("payload %v afterSeq %d", e.Payload, w.afterSeq)
	}
}

func TestNodeRestartResyncsWithTheNewInstance(t *testing.T) {
	rpc, st := &scripted{}, newMemStore()
	w := newWorker(rpc, st)
	w.instance, w.afterSeq = "old", 99
	rpc.push("jc_wait_events", waitResult{Instance: "new", Seq: 3})
	rpc.push("jc_ha_status", map[string]any{"instance": "new", "seq": 3})
	_ = w.Once(context.Background())
	if w.instance != "new" || w.afterSeq != 3 {
		t.Fatalf("instance %q afterSeq %d", w.instance, w.afterSeq)
	}
}

func TestSupersededAnswerChangesNothing(t *testing.T) {
	rpc, st := &scripted{}, newMemStore()
	w := newWorker(rpc, st)
	w.instance, w.afterSeq = "i1", 5
	rpc.push("jc_wait_events", waitResult{Instance: "i1", Seq: 9, Superseded: true})
	_ = w.Once(context.Background())
	if st.count() != 0 || w.afterSeq != 5 {
		t.Fatalf("count %d afterSeq %d", st.count(), w.afterSeq)
	}
}

func TestFailedStoreWriteKeepsTheAcknowledgement(t *testing.T) {
	rpc, st := &scripted{}, newMemStore()
	w := newWorker(rpc, st)
	w.instance, w.afterSeq = "i1", 1
	st.putErr = errors.New("fdb down")
	rpc.push("jc_wait_events", waitResult{Instance: "i1", Seq: 2, Events: []map[string]any{ev(2, "x")}})
	if err := w.Once(context.Background()); err == nil {
		t.Fatal("want an error")
	}
	if w.afterSeq != 1 {
		t.Fatalf("afterSeq advanced to %d: the event would be lost", w.afterSeq)
	}
}

func TestRunBacksOffDoublingToTheCap(t *testing.T) {
	rpc, st := &scripted{}, newMemStore()
	for i := 0; i < 9; i++ {
		rpc.push("jc_wait_events", errDown)
	}
	var sleeps []time.Duration
	ctx, cancel := context.WithCancel(context.Background())
	w := newWorker(rpc, st)
	w.Sleep = func(_ context.Context, d time.Duration) error {
		sleeps = append(sleeps, d)
		if len(sleeps) == 9 {
			cancel()
			return context.Canceled
		}
		return nil
	}
	w.Run(ctx)
	want := []time.Duration{100, 200, 400, 800, 1600, 3200, 5000, 5000, 5000}
	for i, d := range want {
		if sleeps[i] != d*time.Millisecond {
			t.Fatalf("sleep %d = %v, want %v (%v)", i, sleeps[i], d*time.Millisecond, sleeps)
		}
	}
}

func TestEventKeyMatchesThePythonModel(t *testing.T) {
	e := Event{ClusterID: "c", NodeID: "n", Instance: "i", Seq: 5}
	if e.Key() != "object/ArbitrationEvent/c/n/i/00000000000000000005" {
		t.Fatal(e.Key())
	}
	r := e.Record()
	if r["name"] != "ArbitrationEvent" || r["object_type"] != "object" {
		t.Fatalf("%v", r)
	}
}
