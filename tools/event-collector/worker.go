package main

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"time"
)

// Defaults from the contract (sections 3.7 and 4).
const (
	defaultWaitTimeout = 20 * time.Second
	defaultMaxEvents   = 256
	backoffMin         = 100 * time.Millisecond
	backoffMax         = 5 * time.Second
	// Slack on top of the long-poll timeout before the HTTP request gives up.
	httpSlack = 10 * time.Second
)

// statusResync is the synthetic event carrying a full jc_ha_status (arbitration/events.py).
const statusResync = "ha_resync"

type waitArgs struct {
	Instance  string `json:"instance"`
	AfterSeq  int64  `json:"after_seq"`
	TimeoutMS int64  `json:"timeout_ms"`
	Max       int    `json:"max"`
}

type waitResult struct {
	Instance   string           `json:"instance"`
	Seq        int64            `json:"seq"`
	Overflow   bool             `json:"overflow"`
	Superseded bool             `json:"superseded"`
	Events     []map[string]any `json:"events"`
}

// Worker holds the long-poll for one node.
type Worker struct {
	ClusterID string
	NodeID    string
	RPC       Caller
	Store     Store
	Log       *slog.Logger
	Now       func() time.Time
	Sleep     func(context.Context, time.Duration) error
	Timeout   time.Duration
	Max       int

	instance string
	afterSeq int64
}

func (w *Worker) defaults() {
	if w.Now == nil {
		w.Now = time.Now
	}
	if w.Sleep == nil {
		w.Sleep = sleepCtx
	}
	if w.Timeout == 0 {
		w.Timeout = defaultWaitTimeout
	}
	if w.Max == 0 {
		w.Max = defaultMaxEvents
	}
	if w.Log == nil {
		w.Log = slog.Default()
	}
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// Run loops until ctx ends. Transport errors back off from 100 ms, doubling, up to 5 s, and
// the next call re-sends the last instance and after_seq (at-least-once delivery).
func (w *Worker) Run(ctx context.Context) {
	w.defaults()
	backoff := backoffMin
	for ctx.Err() == nil {
		if err := w.Once(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			w.Log.Warn("jc_wait_events failed", "node", w.NodeID, "err", err, "retry_in", backoff)
			if w.Sleep(ctx, backoff) != nil {
				return
			}
			backoff = min(backoff*2, backoffMax)
			continue
		}
		backoff = backoffMin
	}
}

// Once performs one jc_wait_events call and queues what it returns. The acknowledgement
// (after_seq) advances only after every event is stored, so a failed write is redelivered.
func (w *Worker) Once(ctx context.Context) error {
	w.defaults()
	callCtx, cancel := context.WithTimeout(ctx, w.Timeout+httpSlack)
	defer cancel()
	var res waitResult
	err := w.RPC.Call(callCtx, "jc_wait_events",
		waitArgs{Instance: w.instance, AfterSeq: w.afterSeq, TimeoutMS: w.Timeout.Milliseconds(), Max: w.Max}, &res)
	if err != nil {
		return err
	}
	if res.Superseded {
		// Another waiter took over (e.g. this collector restarted elsewhere); keep going.
		return nil
	}
	if res.Instance != w.instance || res.Overflow {
		reason := "instance"
		if res.Overflow && res.Instance == w.instance {
			reason = "overflow"
		}
		return w.resync(ctx, res, reason)
	}
	for _, ev := range res.Events {
		seq := toInt64(ev["seq"])
		if seq <= w.afterSeq {
			continue
		}
		if err := w.put(res.Instance, seq, ev); err != nil {
			return err
		}
		w.afterSeq = seq
	}
	if res.Seq > w.afterSeq && len(res.Events) == 0 {
		// An empty answer reports the node's current seq: nothing newer is pending.
		w.afterSeq = res.Seq
	}
	return nil
}

// resync handles a node restart (instance change) or a dropped event (overflow): the full
// jc_ha_status goes to the arbiter as one ha_resync event, and the loop continues from its seq.
func (w *Worker) resync(ctx context.Context, res waitResult, reason string) error {
	callCtx, cancel := context.WithTimeout(ctx, httpSlack)
	defer cancel()
	var status map[string]any
	if err := w.RPC.Call(callCtx, "jc_ha_status", nil, &status); err != nil {
		return err
	}
	instance := res.Instance
	if s, ok := status["instance"].(string); ok && s != "" {
		instance = s
	}
	seq := toInt64(status["seq"])
	if seq == 0 {
		seq = res.Seq
	}
	payload := map[string]any{
		"status": statusResync, "instance": instance, "seq": seq,
		"reason": reason, "ha_status": status,
	}
	if err := w.put(instance, seq, payload); err != nil {
		return err
	}
	w.Log.Info("resynced", "node", w.NodeID, "reason", reason, "instance", instance, "seq", seq)
	w.instance, w.afterSeq = instance, seq
	return nil
}

func (w *Worker) put(instance string, seq int64, payload map[string]any) error {
	if _, ok := payload["instance"]; !ok {
		payload["instance"] = instance
	}
	ev := Event{ClusterID: w.ClusterID, NodeID: w.NodeID, Instance: instance, Seq: seq,
		ReceivedAt: w.Now().UnixMilli(), Payload: payload}
	if err := w.Store.PutEvent(ev); err != nil {
		return errors.Join(errors.New("queue event"), err)
	}
	return nil
}

func toInt64(v any) int64 {
	switch x := v.(type) {
	case float64:
		return int64(x)
	case int64:
		return x
	case int:
		return int64(x)
	case string:
		n, _ := strconv.ParseInt(x, 10, 64)
		return n
	}
	return 0
}
