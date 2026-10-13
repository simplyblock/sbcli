// Package main is the two-node HA event collector (docs/design/two-node-arbitration.md, section 4).
//
// One goroutine per storage node of every cluster with two_node_arbitration on holds a
// jc_wait_events long-poll against the node's SPDK JSON-RPC endpoint and queues each event
// as an ArbitrationEvent record in FoundationDB, where the Python arbiter consumes it.
// The connection is always opened by the control plane; nodes never call back.
package main

import "fmt"

// Cluster is the part of a simplyblock Cluster record the collector needs.
type Cluster struct {
	UUID               string `json:"uuid"`
	TwoNodeArbitration bool   `json:"two_node_arbitration"`
}

// Node is the part of a StorageNode record the collector needs.
type Node struct {
	UUID        string `json:"uuid"`
	ClusterID   string `json:"cluster_id"`
	Status      string `json:"status"`
	MgmtIP      string `json:"mgmt_ip"`
	RPCPort     int    `json:"rpc_port"`
	RPCUsername string `json:"rpc_username"`
	RPCPassword string `json:"rpc_password"`
}

// Event is one queued ArbitrationEvent, serialised like the Python model
// (simplyblock_core/models/arbitration.py).
type Event struct {
	ClusterID  string         `json:"cluster_id"`
	NodeID     string         `json:"node_id"`
	Instance   string         `json:"instance"`
	Seq        int64          `json:"seq"`
	ReceivedAt int64          `json:"received_at"`
	Payload    map[string]any `json:"payload"`
}

// Key is the record's FDB key: object/ArbitrationEvent/{cluster}/{node}/{instance}/{seq:020d}.
// A redelivered event overwrites its own key, so the queue never holds duplicates.
func (e Event) Key() string {
	return fmt.Sprintf("object/ArbitrationEvent/%s/%s/%s/%020d", e.ClusterID, e.NodeID, e.Instance, e.Seq)
}

// Record is the JSON value written for the event, matching BaseModel.to_dict().
func (e Event) Record() map[string]any {
	return map[string]any{
		"id": "", "uuid": "", "name": "ArbitrationEvent", "status": "", "deleted": false,
		"updated_at": "", "create_dt": "", "remove_dt": "", "object_type": "object",
		"cluster_id": e.ClusterID, "node_id": e.NodeID, "instance": e.Instance,
		"seq": e.Seq, "received_at": e.ReceivedAt, "payload": e.Payload,
	}
}

// Store reads the cluster topology and queues events.
type Store interface {
	ArbitratedClusters() ([]Cluster, error)
	Nodes(clusterID string) ([]Node, error)
	PutEvent(Event) error
}

const nodeStatusRemoved = "removed"
