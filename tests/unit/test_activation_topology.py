"""What a cluster's activation records about its topology. A fresh activation
of a cluster with one online node is non-HA whatever ha_type it carries, and
volume creation has to make the same decision; the activation records it so
creation reads it instead of deciding again. A reactivation never records it:
an HA cluster reactivated with one node online is degraded, not single-node."""

from types import SimpleNamespace

from simplyblock_core import cluster_ops


def _cluster():
    cluster = SimpleNamespace(activated_single_node=False, writes=0)
    cluster.write_to_db = lambda *_a, **_k: setattr(cluster, "writes", cluster.writes + 1)
    return cluster


def test_a_fresh_single_node_activation_is_recorded():
    cluster = _cluster()
    cluster_ops._record_activation_topology(cluster, single_node=True, fresh=True)
    assert cluster.activated_single_node is True
    assert cluster.writes == 1


def test_a_reactivation_records_nothing():
    cluster = _cluster()
    cluster_ops._record_activation_topology(cluster, single_node=True, fresh=False)
    assert cluster.activated_single_node is False
    assert cluster.writes == 0


def test_a_fresh_ha_activation_records_ha():
    cluster = _cluster()
    cluster.activated_single_node = True
    cluster_ops._record_activation_topology(cluster, single_node=False, fresh=True)
    assert cluster.activated_single_node is False
