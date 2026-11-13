package main

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"
)

// memStore is an in-memory Store keyed like FDB.
type memStore struct {
	mu       sync.Mutex
	clusters []Cluster
	nodes    map[string][]Node
	events   map[string]Event
	putErr   error
}

func newMemStore() *memStore {
	return &memStore{nodes: map[string][]Node{}, events: map[string]Event{}}
}

func (m *memStore) ArbitratedClusters() ([]Cluster, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]Cluster(nil), m.clusters...), nil
}

func (m *memStore) Nodes(id string) ([]Node, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]Node(nil), m.nodes[id]...), nil
}

func (m *memStore) PutEvent(e Event) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.putErr != nil {
		return m.putErr
	}
	m.events[e.Key()] = e
	return nil
}

func (m *memStore) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.events)
}

// scripted is a Caller answering from a queue per method and recording the calls.
// With nothing queued it blocks like a long-poll until the context ends.
type scripted struct {
	mu      sync.Mutex
	answers map[string][]any // value or error
	calls   []call
}

type call struct {
	method string
	params any
}

func (s *scripted) push(method string, v any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.answers == nil {
		s.answers = map[string][]any{}
	}
	s.answers[method] = append(s.answers[method], v)
}

func (s *scripted) Call(ctx context.Context, method string, params any, out any) error {
	s.mu.Lock()
	s.calls = append(s.calls, call{method, params})
	q := s.answers[method]
	if len(q) == 0 {
		s.mu.Unlock()
		<-ctx.Done()
		return ctx.Err()
	}
	v := q[0]
	s.answers[method] = q[1:]
	s.mu.Unlock()
	if err, ok := v.(error); ok {
		return err
	}
	raw, _ := json.Marshal(v)
	return json.Unmarshal(raw, out)
}

func (s *scripted) waitArgs() []waitArgs {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []waitArgs
	for _, c := range s.calls {
		if a, ok := c.params.(waitArgs); ok {
			out = append(out, a)
		}
	}
	return out
}

var errDown = errors.New("connection refused")

func fixedNow() time.Time { return time.UnixMilli(1_700_000_000_000) }
