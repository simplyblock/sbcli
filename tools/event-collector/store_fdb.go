//go:build fdb

package main

import (
	"encoding/json"
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

// fdbAPIVersion matches the Python control plane (constants.KVD_DB_VERSION).
const fdbAPIVersion = 730

// fdbStore reads and writes the same raw keys as simplyblock_core.models.base_model:
// object/<ClassName>/<id>, JSON values.
type fdbStore struct {
	db fdb.Database
}

func newStore(clusterFile string) (Store, error) {
	if err := fdb.APIVersion(fdbAPIVersion); err != nil {
		return nil, fmt.Errorf("fdb api version: %w", err)
	}
	db, err := fdb.OpenDatabase(clusterFile)
	if err != nil {
		return nil, fmt.Errorf("fdb open %s: %w", clusterFile, err)
	}
	return &fdbStore{db: db}, nil
}

func (s *fdbStore) scan(prefix string) ([][]byte, error) {
	kr, err := fdb.PrefixRange([]byte(prefix))
	if err != nil {
		return nil, err
	}
	out, err := s.db.ReadTransact(func(rt fdb.ReadTransaction) (any, error) {
		kvs, err := rt.GetRange(kr, fdb.RangeOptions{}).GetSliceWithError()
		if err != nil {
			return nil, err
		}
		vals := make([][]byte, 0, len(kvs))
		for _, kv := range kvs {
			vals = append(vals, kv.Value)
		}
		return vals, nil
	})
	if err != nil {
		return nil, err
	}
	return out.([][]byte), nil
}

func (s *fdbStore) ArbitratedClusters() ([]Cluster, error) {
	vals, err := s.scan("object/Cluster/")
	if err != nil {
		return nil, err
	}
	var out []Cluster
	for _, v := range vals {
		var c Cluster
		if json.Unmarshal(v, &c) == nil && c.TwoNodeArbitration && c.UUID != "" {
			out = append(out, c)
		}
	}
	return out, nil
}

func (s *fdbStore) Nodes(clusterID string) ([]Node, error) {
	vals, err := s.scan("object/StorageNode/")
	if err != nil {
		return nil, err
	}
	var out []Node
	for _, v := range vals {
		var n Node
		if json.Unmarshal(v, &n) == nil && n.ClusterID == clusterID {
			out = append(out, n)
		}
	}
	return out, nil
}

func (s *fdbStore) PutEvent(e Event) error {
	raw, err := json.Marshal(e.Record())
	if err != nil {
		return err
	}
	_, err = s.db.Transact(func(tr fdb.Transaction) (any, error) {
		tr.Set(fdb.Key(e.Key()), raw)
		return nil, nil
	})
	return err
}
