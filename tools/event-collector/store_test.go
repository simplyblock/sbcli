package main

import "testing"

// Mode pacemaker (docs/design/two-node-kubernetes-fencing.md, section 4.2): the node agent
// owns the single jc_wait_events waiter, so the collector must leave the cluster alone.
func TestClusterArbitratedExcludesPacemakerAuthority(t *testing.T) {
	cases := []struct {
		c    Cluster
		want bool
	}{
		{Cluster{UUID: "a", TwoNodeArbitration: true}, true},
		{Cluster{UUID: "b", TwoNodeArbitration: true, FencingAuthority: "witness"}, true},
		{Cluster{UUID: "c", TwoNodeArbitration: true, FencingAuthority: "pacemaker"}, false},
		{Cluster{UUID: "d", TwoNodeArbitration: false}, false},
	}
	for _, tc := range cases {
		if got := tc.c.Arbitrated(); got != tc.want {
			t.Errorf("%s: Arbitrated() = %v, want %v", tc.c.UUID, got, tc.want)
		}
	}
}
