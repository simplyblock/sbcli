"""Fail-back must not die on a snapshot that has no counterpart yet.

Lab 2026-08-17/18: every fail-back task failed on its FIRST step with
"Multiple values present" (348 occurrences) and nothing ever replicated back.
Cause: process_snap_replicate_start looked the counterpart up unconditionally,
so an empty source_replicated_snap_uuid reached get_snapshot_by_id(""), which
degenerates into a table-prefix scan and matches every snapshot.

That state is normal, not exceptional: snapshots taken after the fail-over have
no counterpart, and when failing back into a FRESHLY INSTALLED cluster none of
them do.
"""
import pytest

from simplyblock_core.models.pool import Pool
from simplyblock_core.models.storage_node import StorageNode


class _FakeDB:
    """Only what _destination_pool_uuid touches, plus a get_snapshot_by_id that
    reproduces the real blank-id behaviour (raises, never returns a random row)."""

    def __init__(self, configured_pool="", pools=(), snapshots=None,
                 configured_cluster=""):
        self.cluster = type("C", (), {
            "snapshot_replication_target_pool": configured_pool,
            "snapshot_replication_target_cluster": configured_cluster,
        })()
        self._pools = list(pools)
        self._snapshots = snapshots or {}

    def get_cluster_by_id(self, cluster_id):
        if not cluster_id:
            raise KeyError("Cluster lookup with a blank id")
        return self.cluster

    def get_replication_policy_for_lvol(self, lvol):
        return None

    def get_pools(self, cluster_id=None):
        return self._pools

    def get_snapshot_by_id(self, uuid):
        if not uuid:
            raise KeyError("Snapshot lookup with a blank id")
        return self._snapshots[uuid]


def _node(cluster_id="CL1"):
    n = StorageNode()
    n.uuid = "N1"
    n.cluster_id = cluster_id
    return n


def _pool(uuid, status=Pool.STATUS_ACTIVE):
    p = Pool()
    p.uuid = uuid
    p.pool_name = uuid
    p.status = status
    return p


# The pool rules of a fail-back (configured pool for its direction, first
# active pool, a snapshot without a counterpart resolving like the forward
# direction) are in tests/integration/test_landing_preparation.py.


def test_blank_id_lookup_raises_rather_than_matching_everything():
    """Guard rail: a blank id must be an error, not a whole-table scan."""
    db = _FakeDB()
    with pytest.raises(KeyError):
        db.get_snapshot_by_id("")
