package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

// FileStore is a test sink: the topology comes from a JSON file and every event is
// appended as one JSON line, so the collector runs without the control plane or
// FoundationDB (two-node integration tests). Selected with
// SB_COLLECTOR_STORE=file:<topology.json>:<events.jsonl>.
//
// topology.json: {"clusters":[{"uuid":"c1","two_node_arbitration":true}],
// "nodes":[{"uuid":"n1","cluster_id":"c1","status":"online","mgmt_ip":"10.0.0.1",
// "rpc_port":8080,"rpc_username":"u","rpc_password":"p"}]}
// The topology file is re-read on every call, so a test can add, remove or move nodes.
type FileStore struct {
	TopologyPath string
	EventsPath   string
	mu           sync.Mutex
}

type fileTopology struct {
	Clusters []Cluster `json:"clusters"`
	Nodes    []Node    `json:"nodes"`
}

func (f *FileStore) topology() (fileTopology, error) {
	var t fileTopology
	b, err := os.ReadFile(f.TopologyPath)
	if err != nil {
		return t, err
	}
	err = json.Unmarshal(b, &t)
	return t, err
}

// ArbitratedClusters returns the clusters with two_node_arbitration on.
func (f *FileStore) ArbitratedClusters() ([]Cluster, error) {
	t, err := f.topology()
	if err != nil {
		return nil, err
	}
	var out []Cluster
	for _, c := range t.Clusters {
		if c.TwoNodeArbitration {
			out = append(out, c)
		}
	}
	return out, nil
}

// Nodes returns the nodes of one cluster.
func (f *FileStore) Nodes(clusterID string) ([]Node, error) {
	t, err := f.topology()
	if err != nil {
		return nil, err
	}
	var out []Node
	for _, n := range t.Nodes {
		if n.ClusterID == clusterID {
			out = append(out, n)
		}
	}
	return out, nil
}

// PutEvent appends the event with the time it was written, one JSON object per line.
func (f *FileStore) PutEvent(e Event) error {
	line, err := json.Marshal(map[string]any{
		"key": e.Key(), "record": e.Record(), "written_at_ms": time.Now().UnixMilli(),
	})
	if err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	fh, err := os.OpenFile(f.EventsPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer fh.Close()
	_, err = fh.Write(append(line, '\n'))
	return err
}

// fileStoreFromEnv returns the file store when SB_COLLECTOR_STORE selects it.
func fileStoreFromEnv() (Store, bool, error) {
	v := os.Getenv("SB_COLLECTOR_STORE")
	if !strings.HasPrefix(v, "file:") {
		return nil, false, nil
	}
	parts := strings.SplitN(strings.TrimPrefix(v, "file:"), ":", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return nil, true, fmt.Errorf("SB_COLLECTOR_STORE=file:<topology.json>:<events.jsonl>, got %q", v)
	}
	return &FileStore{TopologyPath: parts[0], EventsPath: parts[1]}, true, nil
}
