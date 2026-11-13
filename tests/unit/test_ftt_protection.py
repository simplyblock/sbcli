"""
test_ftt_protection.py – unit tests for _check_ftt_allows_node_removal.

Tests cover all FTT scenarios:
  - FTT=1 (npcs=1): block if any node not online or journal replicating
  - FTT=2 (npcs=2, ft=2): allow one not-online, block at two
  - npcs=2, ft=1: like FTT=2 plus primary-secondary pair constraint
  - Resource floors (simplyblock_core.floors) are judged last: the removal is
    refused when the cluster without the node would be below a floor. The
    FTT tests below patch the floors out (their fixtures model the FTT rule
    with as few nodes as it takes); TestFloorGate exercises the gate itself.
  - Journal replication counts as one additional not-online node
  - REMOVED and IN_CREATION nodes are excluded from counts
"""

import unittest
from unittest.mock import MagicMock, patch

import pytest

from simplyblock_core import storage_node_ops
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCConnectionError
from simplyblock_core.storage_node_ops import _check_ftt_allows_node_removal
from tests._mocks import unique_ip

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _cluster(ha_type="ha", npcs=1, ndcs=2, ft=1, rebalancing=False, enable_failure_domain=False):
    cl = Cluster()
    cl.uuid = "cluster-1"
    cl.ha_type = ha_type
    cl.distr_npcs = npcs
    cl.distr_ndcs = ndcs
    cl.max_fault_tolerance = ft
    cl.is_re_balancing = rebalancing
    cl.enable_failure_domain = enable_failure_domain
    cl.status = Cluster.STATUS_ACTIVE
    return cl


def _node(node_id, status=StorageNode.STATUS_ONLINE, cluster_id="cluster-1",
          secondary_id="", secondary_id_2="", jm_vuid=8881, lvstore="LVS_1",
          failure_domain=-1):
    n = MagicMock(spec=StorageNode)
    n.uuid = node_id
    n.get_id = MagicMock(return_value=node_id)
    n.status = status
    n.cluster_id = cluster_id
    n.secondary_node_id = secondary_id
    n.tertiary_node_id = secondary_id_2
    n.jm_vuid = jm_vuid
    n.lvstore = lvstore
    n.mgmt_ip = unique_ip(node_id)
    n.failure_domain = failure_domain
    # rpc_client mock: journal replication not active by default
    rpc = MagicMock()
    rpc.get_lvstore = MagicMock(return_value={"name": lvstore})
    rpc.jc_get_jm_status = MagicMock(return_value={"jm1": True})
    n.rpc_client = MagicMock(return_value=rpc)
    return n


def _db(cluster, nodes):
    db = MagicMock()
    db.get_cluster_by_id = MagicMock(return_value=cluster)
    db.get_storage_nodes_by_cluster_id = MagicMock(return_value=nodes)

    def get_node(nid):
        for n in nodes:
            if n.get_id() == nid:
                return n
        raise KeyError(nid)
    db.get_storage_node_by_id = MagicMock(side_effect=get_node)
    return db


def _set_jm_replicating(node):
    """Make a node report active journal replication."""
    rpc = node.rpc_client()
    rpc.jc_get_jm_status.return_value = {"jm1": False}


def _with_devices(node, count=1):
    """Give a mock node ``count`` online data devices (the floor gate counts them)."""
    devs = []
    for i in range(count):
        d = MagicMock(spec=NVMeDevice)
        d.status = NVMeDevice.STATUS_ONLINE
        d.get_id = MagicMock(return_value=f"{node.get_id()}-dev-{i}")
        devs.append(d)
    node.nvme_devices = devs
    return node


@pytest.fixture(autouse=True)
def _floors_out_of_the_ftt_tests(request):
    """The FTT fixtures are sized for the FTT rule alone; the resource floors
    (n+2k nodes, one data device per node, ...) are a separate gate with its
    own tests (TestFloorGate), so they are patched out everywhere else."""
    if request.cls is not None and request.cls.__name__ == "TestFloorGate":
        yield
        return
    with patch.object(storage_node_ops.floors, "evaluate", return_value=[]):
        yield


# ---------------------------------------------------------------------------
# Non-HA clusters
# ---------------------------------------------------------------------------

class TestNonHA(unittest.TestCase):

    def test_single_mode_always_allowed(self):
        cl = _cluster(ha_type="single")
        n1 = _node("n1")
        db = _db(cl, [n1])
        allowed, reason = _check_ftt_allows_node_removal("n1", db)
        self.assertTrue(allowed)


# ---------------------------------------------------------------------------
# FTT=1 (npcs=1)
# ---------------------------------------------------------------------------

class TestFTT1(unittest.TestCase):

    def test_all_online_allows_removal(self):
        cl = _cluster(npcs=1, ft=1)
        nodes = [_node("n1"), _node("n2"), _node("n3")]
        db = _db(cl, nodes)
        allowed, _ = _check_ftt_allows_node_removal("n1", db)
        self.assertTrue(allowed)

    def test_one_offline_blocks_removal(self):
        cl = _cluster(npcs=1, ft=1)
        nodes = [_node("n1"), _node("n2"), _node("n3", status=StorageNode.STATUS_OFFLINE)]
        db = _db(cl, nodes)
        allowed, reason = _check_ftt_allows_node_removal("n1", db)
        self.assertFalse(allowed)
        self.assertIn("FTT=1", reason)

    def test_one_suspended_blocks_removal(self):
        cl = _cluster(npcs=1, ft=1)
        nodes = [_node("n1"), _node("n2"), _node("n3", status=StorageNode.STATUS_SUSPENDED)]
        db = _db(cl, nodes)
        allowed, reason = _check_ftt_allows_node_removal("n1", db)
        self.assertFalse(allowed)
        self.assertIn("FTT=1", reason)

    def test_one_down_blocks_removal(self):
        cl = _cluster(npcs=1, ft=1)
        nodes = [_node("n1"), _node("n2"), _node("n3", status=StorageNode.STATUS_DOWN)]
        db = _db(cl, nodes)
        allowed, reason = _check_ftt_allows_node_removal("n1", db)
        self.assertFalse(allowed)

    def test_journal_replication_blocks_removal(self):
        cl = _cluster(npcs=1, ft=1)
        nodes = [_node("n1"), _node("n2"), _node("n3")]
        _set_jm_replicating(nodes[1])  # n2 is replicating
        db = _db(cl, nodes)
        allowed, reason = _check_ftt_allows_node_removal("n1", db)
        self.assertFalse(allowed)
        self.assertIn("journal replication", reason)

    def test_removed_node_does_not_count_as_offline(self):
        cl = _cluster(npcs=1, ft=1)
        nodes = [_node("n1"), _node("n2"), _node("n3", status=StorageNode.STATUS_REMOVED)]
        db = _db(cl, nodes)
        allowed, _ = _check_ftt_allows_node_removal("n1", db)
        self.assertTrue(allowed)

    def test_in_creation_node_does_not_count_as_offline(self):
        cl = _cluster(npcs=1, ft=1)
        nodes = [_node("n1"), _node("n2"), _node("n3", status=StorageNode.STATUS_IN_CREATION)]
        db = _db(cl, nodes)
        allowed, _ = _check_ftt_allows_node_removal("n1", db)
        self.assertTrue(allowed)

    def test_target_node_not_counted_in_offline(self):
        """The node being removed should not count against itself."""
        cl = _cluster(npcs=1, ft=1)
        nodes = [_node("n1"), _node("n2")]
        db = _db(cl, nodes)
        # n1 is being removed — n2 is online, so this should be allowed
        allowed, _ = _check_ftt_allows_node_removal("n1", db)
        self.assertTrue(allowed)


# ---------------------------------------------------------------------------
# FTT=2 (npcs=2, ft=2)
# ---------------------------------------------------------------------------

class TestFTT2(unittest.TestCase):

    def test_all_online_allows_removal(self):
        cl = _cluster(npcs=2, ft=2)
        nodes = [_node(f"n{i}") for i in range(5)]
        db = _db(cl, nodes)
        allowed, _ = _check_ftt_allows_node_removal("n0", db)
        self.assertTrue(allowed)

    def test_one_offline_allows_removal(self):
        cl = _cluster(npcs=2, ft=2)
        nodes = [_node("n1"), _node("n2"), _node("n3"),
                 _node("n4", status=StorageNode.STATUS_OFFLINE), _node("n5")]
        db = _db(cl, nodes)
        allowed, _ = _check_ftt_allows_node_removal("n1", db)
        self.assertTrue(allowed)

    def test_two_offline_blocks_removal(self):
        cl = _cluster(npcs=2, ft=2)
        nodes = [_node("n1"), _node("n2"), _node("n3"),
                 _node("n4", status=StorageNode.STATUS_OFFLINE),
                 _node("n5", status=StorageNode.STATUS_SUSPENDED)]
        db = _db(cl, nodes)
        allowed, reason = _check_ftt_allows_node_removal("n1", db)
        self.assertFalse(allowed)
        self.assertIn("FTT=2", reason)

    def test_one_offline_plus_jm_replication_blocks(self):
        """One offline + journal replication = effective count of 2 → block."""
        cl = _cluster(npcs=2, ft=2)
        nodes = [_node("n1"), _node("n2"), _node("n3"),
                 _node("n4", status=StorageNode.STATUS_OFFLINE), _node("n5")]
        _set_jm_replicating(nodes[1])  # n2 replicating
        db = _db(cl, nodes)
        allowed, reason = _check_ftt_allows_node_removal("n1", db)
        self.assertFalse(allowed)
        self.assertIn("journal replication", reason)

    def test_jm_replication_alone_allows_one_removal(self):
        """Journal replication = effective 1 not-online → still room for one more in FTT=2."""
        cl = _cluster(npcs=2, ft=2)
        nodes = [_node(f"n{i}") for i in range(5)]
        _set_jm_replicating(nodes[1])
        db = _db(cl, nodes)
        allowed, _ = _check_ftt_allows_node_removal("n0", db)
        self.assertTrue(allowed)


# ---------------------------------------------------------------------------
# npcs=2, ft=1 (primary-secondary pair constraint)
# ---------------------------------------------------------------------------

class TestNpcs2Ft1(unittest.TestCase):

    def test_all_online_allows_removal(self):
        cl = _cluster(npcs=2, ft=1)
        nodes = [_node("n1", secondary_id="n2"), _node("n2"), _node("n3"), _node("n4")]
        db = _db(cl, nodes)
        allowed, _ = _check_ftt_allows_node_removal("n1", db)
        self.assertTrue(allowed)

    def test_unrelated_offline_allows_removal(self):
        """An offline node that is NOT the pair of the target → allowed."""
        cl = _cluster(npcs=2, ft=1)
        nodes = [_node("n1", secondary_id="n2"), _node("n2"),
                 _node("n3", status=StorageNode.STATUS_OFFLINE), _node("n4")]
        db = _db(cl, nodes)
        allowed, _ = _check_ftt_allows_node_removal("n1", db)
        self.assertTrue(allowed)

    def test_secondary_offline_blocks_primary_removal(self):
        """If n1's secondary (n2) is offline, n1 cannot be removed."""
        cl = _cluster(npcs=2, ft=1)
        nodes = [_node("n1", secondary_id="n2"),
                 _node("n2", status=StorageNode.STATUS_OFFLINE),
                 _node("n3"), _node("n4")]
        db = _db(cl, nodes)
        allowed, reason = _check_ftt_allows_node_removal("n1", db)
        self.assertFalse(allowed)
        self.assertIn("secondary", reason)
        self.assertIn("n2", reason)

    def test_primary_offline_blocks_secondary_removal(self):
        """If primary n1 is offline, its secondary n2 cannot be removed."""
        cl = _cluster(npcs=2, ft=1)
        nodes = [_node("n1", secondary_id="n2", status=StorageNode.STATUS_OFFLINE),
                 _node("n2"), _node("n3"), _node("n4")]
        db = _db(cl, nodes)
        allowed, reason = _check_ftt_allows_node_removal("n2", db)
        self.assertFalse(allowed)
        self.assertIn("secondary of not-online primary", reason)
        self.assertIn("n1", reason)

    def test_secondary_id_2_offline_blocks(self):
        """Also works for tertiary_node_id."""
        cl = _cluster(npcs=2, ft=1)
        nodes = [_node("n1", secondary_id="n2", secondary_id_2="n3"),
                 _node("n2"), _node("n3", status=StorageNode.STATUS_OFFLINE), _node("n4")]
        db = _db(cl, nodes)
        allowed, reason = _check_ftt_allows_node_removal("n1", db)
        self.assertFalse(allowed)
        self.assertIn("n3", reason)

    def test_two_unrelated_offline_blocks(self):
        """Two offline nodes (even if unrelated to pair) → blocked by capacity rule."""
        cl = _cluster(npcs=2, ft=1)
        nodes = [_node("n1", secondary_id="n5"),
                 _node("n2"), _node("n3", status=StorageNode.STATUS_OFFLINE),
                 _node("n4", status=StorageNode.STATUS_SUSPENDED), _node("n5")]
        db = _db(cl, nodes)
        allowed, reason = _check_ftt_allows_node_removal("n1", db)
        self.assertFalse(allowed)
        self.assertIn("npcs=2/ft=1", reason)
class TestEdgeCases(unittest.TestCase):

    def test_node_not_found(self):
        db = MagicMock()
        db.get_storage_node_by_id = MagicMock(side_effect=KeyError("nope"))
        allowed, reason = _check_ftt_allows_node_removal("missing", db)
        self.assertFalse(allowed)
        self.assertIn("not found", reason)

    def test_jm_rpc_failure_treated_as_no_replication(self):
        """If RPC to check journal status fails, assume no replication (don't block)."""
        cl = _cluster(npcs=1, ft=1)
        nodes = [_node("n1"), _node("n2")]
        nodes[1].rpc_client().jc_get_jm_status.side_effect = Exception("RPC failed")
        db = _db(cl, nodes)
        allowed, _ = _check_ftt_allows_node_removal("n1", db)
        self.assertTrue(allowed)

    def test_multiple_offline_statuses(self):
        """Various not-online statuses all count: offline, suspended, down, unreachable."""
        cl = _cluster(npcs=2, ft=2)
        nodes = [
            _node("n1"),
            _node("n2", status=StorageNode.STATUS_OFFLINE),
            _node("n3", status=StorageNode.STATUS_DOWN),
            _node("n4"), _node("n5"),
        ]
        db = _db(cl, nodes)
        allowed, reason = _check_ftt_allows_node_removal("n1", db)
        self.assertFalse(allowed)
        self.assertIn("2 not-online", reason)


# ---------------------------------------------------------------------------
# FTT=1 additional scenarios
# ---------------------------------------------------------------------------

class TestFTT1Additional(unittest.TestCase):

    def test_unreachable_counts_as_not_online(self):
        cl = _cluster(npcs=1, ft=1)
        nodes = [_node("n1"), _node("n2"), _node("n3", status=StorageNode.STATUS_UNREACHABLE)]
        db = _db(cl, nodes)
        allowed, reason = _check_ftt_allows_node_removal("n1", db)
        self.assertFalse(allowed)

    def test_schedulable_counts_as_not_online(self):
        cl = _cluster(npcs=1, ft=1)
        nodes = [_node("n1"), _node("n2"), _node("n3", status=StorageNode.STATUS_SCHEDULABLE)]
        db = _db(cl, nodes)
        allowed, reason = _check_ftt_allows_node_removal("n1", db)
        self.assertFalse(allowed)

    def test_in_shutdown_counts_as_not_online(self):
        cl = _cluster(npcs=1, ft=1)
        nodes = [_node("n1"), _node("n2"), _node("n3", status=StorageNode.STATUS_IN_SHUTDOWN)]
        db = _db(cl, nodes)
        allowed, reason = _check_ftt_allows_node_removal("n1", db)
        self.assertFalse(allowed)

    def test_restarting_counts_as_not_online(self):
        cl = _cluster(npcs=1, ft=1)
        nodes = [_node("n1"), _node("n2"), _node("n3", status=StorageNode.STATUS_RESTARTING)]
        db = _db(cl, nodes)
        allowed, reason = _check_ftt_allows_node_removal("n1", db)
        self.assertFalse(allowed)

    def test_two_nodes_both_online_allows(self):
        """Minimal cluster: 2 online nodes, removing one is allowed for FTT=1."""
        cl = _cluster(npcs=1, ft=1)
        nodes = [_node("n1"), _node("n2")]
        db = _db(cl, nodes)
        allowed, _ = _check_ftt_allows_node_removal("n1", db)
        self.assertTrue(allowed)

    def test_jm_replication_on_target_node_not_counted(self):
        """Journal replication on the node being removed should not block removal."""
        cl = _cluster(npcs=1, ft=1)
        nodes = [_node("n1"), _node("n2")]
        _set_jm_replicating(nodes[0])  # n1 is the target, its replication shouldn't matter
        db = _db(cl, nodes)
        allowed, _ = _check_ftt_allows_node_removal("n1", db)
        self.assertTrue(allowed)

    def test_jm_replication_plus_offline_both_count(self):
        """Offline node + journal replication = 2 not-online → blocked even for FTT=1."""
        cl = _cluster(npcs=1, ft=1)
        nodes = [_node("n1"), _node("n2"), _node("n3"),
                 _node("n4", status=StorageNode.STATUS_OFFLINE)]
        _set_jm_replicating(nodes[1])
        db = _db(cl, nodes)
        allowed, reason = _check_ftt_allows_node_removal("n1", db)
        self.assertFalse(allowed)
        self.assertIn("journal replication", reason)


# ---------------------------------------------------------------------------
# FTT=2 additional scenarios
# ---------------------------------------------------------------------------

class TestFTT2Additional(unittest.TestCase):

    def test_two_jm_replications_blocks(self):
        """Only one jm_replication_active flag, but it still counts as +1 with 1 offline = 2."""
        cl = _cluster(npcs=2, ft=2)
        nodes = [_node("n1"), _node("n2"), _node("n3"),
                 _node("n4", status=StorageNode.STATUS_SUSPENDED), _node("n5")]
        _set_jm_replicating(nodes[1])
        db = _db(cl, nodes)
        allowed, reason = _check_ftt_allows_node_removal("n1", db)
        self.assertFalse(allowed)

    def test_removed_plus_offline_only_offline_counts(self):
        """Removed node doesn't count; only the offline one does. FTT=2 allows one."""
        cl = _cluster(npcs=2, ft=2)
        nodes = [_node("n1"), _node("n2"), _node("n3"),
                 _node("n4", status=StorageNode.STATUS_REMOVED),
                 _node("n5", status=StorageNode.STATUS_OFFLINE)]
        db = _db(cl, nodes)
        allowed, _ = _check_ftt_allows_node_removal("n1", db)
        self.assertTrue(allowed)

    def test_three_offline_blocks(self):
        cl = _cluster(npcs=2, ft=2)
        nodes = [_node("n1"), _node("n2"),
                 _node("n3", status=StorageNode.STATUS_OFFLINE),
                 _node("n4", status=StorageNode.STATUS_DOWN),
                 _node("n5", status=StorageNode.STATUS_SUSPENDED)]
        db = _db(cl, nodes)
        allowed, reason = _check_ftt_allows_node_removal("n1", db)
        self.assertFalse(allowed)


# ---------------------------------------------------------------------------
# npcs=2, ft=1 additional scenarios
# ---------------------------------------------------------------------------

class TestNpcs2Ft1Additional(unittest.TestCase):

    def test_secondary_suspended_blocks_primary(self):
        """Suspended secondary also blocks its primary."""
        cl = _cluster(npcs=2, ft=1)
        nodes = [_node("n1", secondary_id="n2"),
                 _node("n2", status=StorageNode.STATUS_SUSPENDED),
                 _node("n3"), _node("n4")]
        db = _db(cl, nodes)
        allowed, reason = _check_ftt_allows_node_removal("n1", db)
        self.assertFalse(allowed)
        self.assertIn("secondary", reason)

    def test_primary_down_blocks_secondary(self):
        cl = _cluster(npcs=2, ft=1)
        nodes = [_node("n1", secondary_id="n2", status=StorageNode.STATUS_DOWN),
                 _node("n2"), _node("n3"), _node("n4")]
        db = _db(cl, nodes)
        allowed, reason = _check_ftt_allows_node_removal("n2", db)
        self.assertFalse(allowed)
        self.assertIn("primary", reason)

    def test_both_secondaries_online_allows(self):
        """Node with two secondaries, both online → allowed."""
        cl = _cluster(npcs=2, ft=1)
        nodes = [_node("n1", secondary_id="n2", secondary_id_2="n3"),
                 _node("n2"), _node("n3"), _node("n4")]
        db = _db(cl, nodes)
        allowed, _ = _check_ftt_allows_node_removal("n1", db)
        self.assertTrue(allowed)

    def test_second_secondary_offline_blocks(self):
        """secondary_id_2 offline blocks primary removal."""
        cl = _cluster(npcs=2, ft=1)
        nodes = [_node("n1", secondary_id="n2", secondary_id_2="n3"),
                 _node("n2"),
                 _node("n3", status=StorageNode.STATUS_OFFLINE),
                 _node("n4")]
        db = _db(cl, nodes)
        allowed, reason = _check_ftt_allows_node_removal("n1", db)
        self.assertFalse(allowed)
        self.assertIn("n3", reason)

    def test_node_is_secondary_of_two_primaries_one_offline(self):
        """Node n3 is secondary of both n1 and n2. n1 is offline → n3 cannot be removed."""
        cl = _cluster(npcs=2, ft=1)
        nodes = [_node("n1", secondary_id="n3", status=StorageNode.STATUS_OFFLINE),
                 _node("n2", secondary_id="n3"),
                 _node("n3"), _node("n4")]
        db = _db(cl, nodes)
        allowed, reason = _check_ftt_allows_node_removal("n3", db)
        self.assertFalse(allowed)
        self.assertIn("n1", reason)

    def test_jm_replication_counts_but_pair_still_checked(self):
        """With jm replication (count=1), if secondary is also offline → count=2 → blocked by capacity before pair check."""
        cl = _cluster(npcs=2, ft=1)
        nodes = [_node("n1", secondary_id="n2"),
                 _node("n2", status=StorageNode.STATUS_OFFLINE),
                 _node("n3"), _node("n4")]
        _set_jm_replicating(nodes[2])  # n3 replicating
        db = _db(cl, nodes)
        allowed, reason = _check_ftt_allows_node_removal("n1", db)
        self.assertFalse(allowed)


# ---------------------------------------------------------------------------
# Failure-domain-aware capacity check
#
# With FD enabled, the risk budget is npcs, spent per domain at a rate of
# min(nodes_down_in_that_domain, chunks_per_domain), where chunks_per_domain
# = ceil((ndcs+npcs)/domains_available). A domain that already has
# chunks_per_domain nodes down has maxed its contribution -- further nodes in
# THAT SAME domain are free -- but a node in a domain that hasn't maxed out
# yet is only allowed if the combined risk across every affected domain still
# leaves room in the npcs budget. This mirrors the operator's fdDrainGate
# (nodedrain_controller.go) and reduces to "up to npcs whole domains are
# free" when there are >= ndcs+npcs domains (chunks_per_domain == 1).
#
# Confirmed against the backend team's stated requirements (2026-08, Dmitrii
# Iakovlev): for a 2+2 layout,
#   - 2 failure domains: safe combos are "1 whole FD down" (any node count
#     within it) OR "1 node in each of the 2 FDs" -- nothing beyond that.
#   - 3 failure domains: only 1 FD may be fully down, not 2.
#   - 4 failure domains: 2 FDs may be fully down (the well-provisioned case).
# ---------------------------------------------------------------------------

class TestFailureDomainAwareCapacity(unittest.TestCase):

    def test_piling_onto_active_domain_always_allowed(self):
        """Two nodes already down in domain 1 (at cap already) -- removing a
        THIRD node also in domain 1 must still be allowed."""
        cl = _cluster(npcs=2, ndcs=2, ft=2, enable_failure_domain=True)
        nodes = [
            _node("n1", failure_domain=1),
            _node("n2", status=StorageNode.STATUS_OFFLINE, failure_domain=1),
            _node("n3", status=StorageNode.STATUS_OFFLINE, failure_domain=1),
            _node("n4", failure_domain=2),
            _node("n5", failure_domain=3),
            _node("n6", failure_domain=4),
        ]
        db = _db(cl, nodes)
        allowed, _ = _check_ftt_allows_node_removal("n1", db)
        self.assertTrue(allowed)

    def test_well_provisioned_new_domain_blocked_at_cap(self):
        """ndcs=2/npcs=2 needs 4 domains for full isolation; 4 are available.
        Domains 1 and 2 already have not-online nodes (cap=npcs=2 domains
        active) -- removing a node in a THIRD domain must be blocked."""
        cl = _cluster(npcs=2, ndcs=2, ft=2, enable_failure_domain=True)
        nodes = [
            _node("n1", failure_domain=3),
            _node("n2", status=StorageNode.STATUS_OFFLINE, failure_domain=1),
            _node("n3", status=StorageNode.STATUS_OFFLINE, failure_domain=2),
            _node("n4", failure_domain=4),
            _node("n5", failure_domain=1),
            _node("n6", failure_domain=2),
        ]
        db = _db(cl, nodes)
        allowed, reason = _check_ftt_allows_node_removal("n1", db)
        self.assertFalse(allowed)
        self.assertIn("domain", reason)

    def test_well_provisioned_new_domain_within_cap_allowed(self):
        """Same layout, but only ONE domain active so far -- opening a
        second domain is still within the npcs=2 domain budget."""
        cl = _cluster(npcs=2, ndcs=2, ft=2, enable_failure_domain=True)
        nodes = [
            _node("n1", failure_domain=3),
            _node("n2", status=StorageNode.STATUS_OFFLINE, failure_domain=1),
            _node("n3", failure_domain=2),
            _node("n4", failure_domain=4),
        ]
        db = _db(cl, nodes)
        allowed, _ = _check_ftt_allows_node_removal("n1", db)
        self.assertTrue(allowed)

    def test_under_provisioned_second_domain_within_node_budget_allowed(self):
        """ndcs=2/npcs=2 needs 4 domains, but only 2 exist -- under-provisioned.
        One node down in domain 1; removing a node in domain 2 (opening a
        second domain) is still allowed while raw not-online count (1) is
        under the npcs=2 node budget."""
        cl = _cluster(npcs=2, ndcs=2, ft=2, enable_failure_domain=True)
        nodes = [
            _node("n1", failure_domain=2),
            _node("n2", status=StorageNode.STATUS_OFFLINE, failure_domain=1),
            _node("n3", failure_domain=1),
            _node("n4", failure_domain=2),
        ]
        db = _db(cl, nodes)
        allowed, _ = _check_ftt_allows_node_removal("n1", db)
        self.assertTrue(allowed)

    def test_under_provisioned_second_domain_blocked_at_node_budget(self):
        """Same under-provisioned layout, but the node-count budget (npcs=2)
        is already spent -- opening a second domain must now be blocked."""
        cl = _cluster(npcs=2, ndcs=2, ft=2, enable_failure_domain=True)
        nodes = [
            _node("n1", failure_domain=2),
            _node("n2", status=StorageNode.STATUS_OFFLINE, failure_domain=1),
            _node("n3", status=StorageNode.STATUS_OFFLINE, failure_domain=1),
            _node("n4", failure_domain=2),
        ]
        db = _db(cl, nodes)
        allowed, reason = _check_ftt_allows_node_removal("n1", db)
        self.assertFalse(allowed)
        self.assertIn("risk budget", reason)

    # --- Dmitrii's confirmed scenarios: 2 FD / 2+2 ---------------------------
    # chunks_per_domain = ceil(4/2) = 2. Safe: "1 whole FD" or "1 node per FD",
    # nothing more -- verified against the specific combinations he ruled out.

    def test_2fd_one_whole_fd_down_is_safe(self):
        """Domain 1 already has 3-of-4 nodes down; the 4th is still allowed
        (piling within a single domain, which may go fully down)."""
        cl = _cluster(npcs=2, ndcs=2, ft=2, enable_failure_domain=True)
        nodes = [
            _node("n1", failure_domain=1),
            _node("n2", status=StorageNode.STATUS_OFFLINE, failure_domain=1),
            _node("n3", status=StorageNode.STATUS_OFFLINE, failure_domain=1),
            _node("n4", status=StorageNode.STATUS_OFFLINE, failure_domain=1),
            _node("n5", failure_domain=2), _node("n6", failure_domain=2),
            _node("n7", failure_domain=2), _node("n8", failure_domain=2),
        ]
        db = _db(cl, nodes)
        allowed, _ = _check_ftt_allows_node_removal("n1", db)
        self.assertTrue(allowed)

    def test_2fd_one_node_per_fd_is_safe(self):
        """1 node down in domain 1; removing 1 node in domain 2 is safe."""
        cl = _cluster(npcs=2, ndcs=2, ft=2, enable_failure_domain=True)
        nodes = [
            _node("n1", failure_domain=2),
            _node("n2", status=StorageNode.STATUS_OFFLINE, failure_domain=1),
            _node("n3", failure_domain=1),
            _node("n4", failure_domain=2),
        ]
        db = _db(cl, nodes)
        allowed, _ = _check_ftt_allows_node_removal("n1", db)
        self.assertTrue(allowed)

    def test_2fd_whole_fd_plus_other_fd_node_is_unsafe(self):
        """Domain 1 fully down (4 nodes) -- a node in domain 2 must now be
        blocked (not one of the two safe combinations)."""
        cl = _cluster(npcs=2, ndcs=2, ft=2, enable_failure_domain=True)
        nodes = [
            _node("n1", failure_domain=2),
            _node("n2", status=StorageNode.STATUS_OFFLINE, failure_domain=1),
            _node("n3", status=StorageNode.STATUS_OFFLINE, failure_domain=1),
            _node("n4", status=StorageNode.STATUS_OFFLINE, failure_domain=1),
            _node("n5", status=StorageNode.STATUS_OFFLINE, failure_domain=1),
            _node("n6", failure_domain=2), _node("n7", failure_domain=2), _node("n8", failure_domain=2),
        ]
        db = _db(cl, nodes)
        allowed, _ = _check_ftt_allows_node_removal("n1", db)
        self.assertFalse(allowed)

    def test_2fd_one_per_fd_plus_second_node_in_same_fd_is_unsafe(self):
        """1 node down in EACH of domains 1 and 2 already (the safe combo) --
        piling a SECOND node onto domain 2 must now be blocked, even though
        domain 2 is already 'active'. This is the exact gap the old
        unconditional-piling logic missed."""
        cl = _cluster(npcs=2, ndcs=2, ft=2, enable_failure_domain=True)
        nodes = [
            _node("n1", failure_domain=2),
            _node("n2", status=StorageNode.STATUS_OFFLINE, failure_domain=1),
            _node("n3", status=StorageNode.STATUS_OFFLINE, failure_domain=2),
            _node("n4", failure_domain=1),
        ]
        db = _db(cl, nodes)
        allowed, _ = _check_ftt_allows_node_removal("n1", db)
        self.assertFalse(allowed)

    def test_2fd_one_per_fd_plus_second_node_in_other_fd_is_unsafe(self):
        """Same setup, but piling the extra node onto domain 1 (the first,
        'already active' domain) instead of domain 2 -- also unsafe."""
        cl = _cluster(npcs=2, ndcs=2, ft=2, enable_failure_domain=True)
        nodes = [
            _node("n1", failure_domain=1),
            _node("n2", status=StorageNode.STATUS_OFFLINE, failure_domain=1),
            _node("n3", status=StorageNode.STATUS_OFFLINE, failure_domain=2),
            _node("n4", failure_domain=2),
        ]
        db = _db(cl, nodes)
        allowed, _ = _check_ftt_allows_node_removal("n1", db)
        self.assertFalse(allowed)

    # --- Dmitrii's confirmed scenarios: 3 FD / 2+2 ---------------------------
    # chunks_per_domain = ceil(4/3) = 2 -- same per-domain cap as 2 FD, but
    # spread across 3 domains. Confirmed: 1 FD fully down is safe, 2 FDs is not.

    def test_3fd_one_whole_fd_down_is_safe(self):
        cl = _cluster(npcs=2, ndcs=2, ft=2, enable_failure_domain=True)
        nodes = [
            _node("n1", failure_domain=1),
            _node("n2", status=StorageNode.STATUS_OFFLINE, failure_domain=1),
            _node("n3", status=StorageNode.STATUS_OFFLINE, failure_domain=1),
            _node("n4", status=StorageNode.STATUS_OFFLINE, failure_domain=1),
            _node("n5", failure_domain=2), _node("n6", failure_domain=3),
        ]
        db = _db(cl, nodes)
        allowed, _ = _check_ftt_allows_node_removal("n1", db)
        self.assertTrue(allowed)

    def test_3fd_two_whole_fds_down_is_unsafe(self):
        """Domain 1 fully down already (2+ nodes, maxing its chunks_per_domain
        budget) -- a node in domain 2 must be blocked."""
        cl = _cluster(npcs=2, ndcs=2, ft=2, enable_failure_domain=True)
        nodes = [
            _node("n1", failure_domain=2),
            _node("n2", status=StorageNode.STATUS_OFFLINE, failure_domain=1),
            _node("n3", status=StorageNode.STATUS_OFFLINE, failure_domain=1),
            _node("n4", failure_domain=2), _node("n5", failure_domain=3),
        ]
        db = _db(cl, nodes)
        allowed, _ = _check_ftt_allows_node_removal("n1", db)
        self.assertFalse(allowed)

    # --- Dmitrii's confirmed scenarios: 4 FD / 2+2 (well-provisioned) --------
    # chunks_per_domain = ceil(4/4) = 1. Confirmed: 2 whole FDs down is safe.

    def test_4fd_two_whole_fds_down_is_safe(self):
        """Domain 1 already has 3-of-4 nodes down (maxed, chunks_per_domain=1
        means it only ever contributes 1 to the budget regardless of count);
        removing a node in domain 2 -- opening the second domain -- is still
        within the npcs=2 budget."""
        cl = _cluster(npcs=2, ndcs=2, ft=2, enable_failure_domain=True)
        nodes = [
            _node("n1", failure_domain=2),
            _node("n2", status=StorageNode.STATUS_OFFLINE, failure_domain=1),
            _node("n3", status=StorageNode.STATUS_OFFLINE, failure_domain=1),
            _node("n4", status=StorageNode.STATUS_OFFLINE, failure_domain=1),
            _node("n5", failure_domain=2), _node("n6", failure_domain=3), _node("n7", failure_domain=4),
        ]
        db = _db(cl, nodes)
        allowed, _ = _check_ftt_allows_node_removal("n1", db)
        self.assertTrue(allowed)

    def test_4fd_three_whole_fds_down_is_unsafe(self):
        """Domains 1 and 2 already have a node down each (budget=2/2 spent);
        removing a node in a THIRD domain (3) must be blocked."""
        cl = _cluster(npcs=2, ndcs=2, ft=2, enable_failure_domain=True)
        nodes = [
            _node("n1", failure_domain=3),
            _node("n2", status=StorageNode.STATUS_OFFLINE, failure_domain=1),
            _node("n3", status=StorageNode.STATUS_OFFLINE, failure_domain=2),
            _node("n4", failure_domain=4),
        ]
        db = _db(cl, nodes)
        allowed, _ = _check_ftt_allows_node_removal("n1", db)
        self.assertFalse(allowed)

    def test_fd_enabled_but_node_unassigned_falls_back_to_node_count(self):
        """FD enabled cluster-wide, but this specific node has no domain
        assignment (-1) -- must fall back to the plain node-count cap."""
        cl = _cluster(npcs=1, ndcs=2, ft=1, enable_failure_domain=True)
        nodes = [
            _node("n1", failure_domain=-1),
            _node("n2", status=StorageNode.STATUS_OFFLINE, failure_domain=1),
            _node("n3", failure_domain=1),
        ]
        db = _db(cl, nodes)
        allowed, reason = _check_ftt_allows_node_removal("n1", db)
        self.assertFalse(allowed)
        self.assertIn("not-online", reason)

    def test_fd_disabled_ignores_domains_uses_node_count(self):
        """enable_failure_domain=False: domain tags present but irrelevant --
        behaves exactly like the non-FD npcs=1 case."""
        cl = _cluster(npcs=1, ndcs=2, ft=1, enable_failure_domain=False)
        nodes = [
            _node("n1", failure_domain=1),
            _node("n2", status=StorageNode.STATUS_OFFLINE, failure_domain=2),
            _node("n3", failure_domain=3),
        ]
        db = _db(cl, nodes)
        allowed, reason = _check_ftt_allows_node_removal("n1", db)
        self.assertFalse(allowed)
        self.assertIn("FTT=1", reason)
class TestReasonStrings(unittest.TestCase):

    def test_ftt1_reason_includes_count(self):
        cl = _cluster(npcs=1, ft=1)
        nodes = [_node("n1"), _node("n2"),
                 _node("n3", status=StorageNode.STATUS_OFFLINE),
                 _node("n4", status=StorageNode.STATUS_DOWN)]
        db = _db(cl, nodes)
        allowed, reason = _check_ftt_allows_node_removal("n1", db)
        self.assertFalse(allowed)
        self.assertIn("2 not-online", reason)

    def test_ftt2_reason_includes_count(self):
        cl = _cluster(npcs=2, ft=2)
        nodes = [_node("n1"), _node("n2"),
                 _node("n3", status=StorageNode.STATUS_OFFLINE),
                 _node("n4", status=StorageNode.STATUS_DOWN),
                 _node("n5")]
        db = _db(cl, nodes)
        allowed, reason = _check_ftt_allows_node_removal("n1", db)
        self.assertFalse(allowed)
        self.assertIn("2 not-online", reason)

    def test_pair_reason_includes_node_ids(self):
        cl = _cluster(npcs=2, ft=1)
        nodes = [_node("n1", secondary_id="n2"),
                 _node("n2", status=StorageNode.STATUS_OFFLINE),
                 _node("n3"), _node("n4")]
        db = _db(cl, nodes)
        allowed, reason = _check_ftt_allows_node_removal("n1", db)
        self.assertFalse(allowed)
        self.assertIn("n1", reason)
        self.assertIn("n2", reason)
        self.assertIn("offline", reason)


# ---------------------------------------------------------------------------
# A secondary of the removed node that SPDK fenced after the shutdown
# ---------------------------------------------------------------------------

#: Substrings the operator reads as "ask again later" (operator main,
#: internal/controllers/node/remove.go, ``passingRefusals``). The fenced-
#: secondary refusal must contain NONE of them, or the operator keeps
#: re-asking a question whose answer cannot change (run 57 removal 3: 3h).
OPERATOR_PASSING_REFUSALS = (
    "wait for rebalancing",
    "is rebalancing",
    "active task(s) on the node",
    "not-online node",
    "is not online",
    "risk budget already committed",
    "peer node(s) not online",
)

EVENT = "simplyblock_core.storage_node_ops.storage_events.snode_removal_aborted_fenced_secondary"


def _fence(node, port):
    """Make ``node`` report ``port`` fenced in reject mode and serve every
    lvstore on that port."""
    node.get_lvol_subsys_port = MagicMock(return_value=port)
    node.rpc_client().nvmf_get_blocked_ports = MagicMock(return_value={port})


class TestFencedOwnSecondary(unittest.TestCase):
    """FTT=1, n1 is being removed and is already offline; n2 is its secondary."""

    def _nodes(self, removed_status=StorageNode.STATUS_OFFLINE,
               secondary_status=StorageNode.STATUS_DOWN):
        return [
            _node("n1", status=removed_status, secondary_id="n2", lvstore="LVS_10"),
            _node("n2", status=secondary_status, lvstore="LVS_3"),
            _node("n3"),
        ]

    def test_fenced_secondary_is_a_terminal_refusal(self):
        nodes = self._nodes()
        _fence(nodes[1], 4440)
        db = _db(_cluster(npcs=1, ft=1), nodes)
        with patch(EVENT) as event:
            allowed, reason = _check_ftt_allows_node_removal("n1", db)
        self.assertFalse(allowed)
        self.assertIn("aborted", reason)
        self.assertIn("n2", reason)
        self.assertIn("4440", reason)
        self.assertIn("LVS_10", reason)
        self.assertIn("restart node n1", reason)
        for passing in OPERATOR_PASSING_REFUSALS:
            self.assertNotIn(passing, reason.lower(), passing)
        event.assert_called_once()
        node, sec, port = event.call_args.args
        self.assertEqual((node.get_id(), sec.get_id(), port), ("n1", "n2", 4440))

    def test_fence_before_the_shutdown_stays_a_passing_refusal(self):
        """While the primary is still online the monitors can release the
        fence, so the generic refusal (which the operator waits out) is the
        right one."""
        nodes = self._nodes(removed_status=StorageNode.STATUS_ONLINE)
        _fence(nodes[1], 4440)
        db = _db(_cluster(npcs=1, ft=1), nodes)
        with patch(EVENT) as event:
            allowed, reason = _check_ftt_allows_node_removal("n1", db)
        self.assertFalse(allowed)
        self.assertIn("not-online node", reason)
        self.assertNotIn("aborted", reason)
        event.assert_not_called()

    def test_down_secondary_without_a_fence_stays_a_passing_refusal(self):
        nodes = self._nodes()
        nodes[1].get_lvol_subsys_port = MagicMock(return_value=4440)
        nodes[1].rpc_client().nvmf_get_blocked_ports = MagicMock(return_value=set())
        db = _db(_cluster(npcs=1, ft=1), nodes)
        with patch(EVENT) as event:
            allowed, reason = _check_ftt_allows_node_removal("n1", db)
        self.assertFalse(allowed)
        self.assertIn("not-online node", reason)
        event.assert_not_called()

    def test_fence_on_a_node_that_is_not_our_secondary_is_not_ours(self):
        nodes = self._nodes(secondary_status=StorageNode.STATUS_ONLINE)
        nodes[2].status = StorageNode.STATUS_DOWN
        _fence(nodes[2], 4442)
        db = _db(_cluster(npcs=1, ft=1), nodes)
        with patch(EVENT) as event:
            allowed, reason = _check_ftt_allows_node_removal("n1", db)
        self.assertFalse(allowed)
        self.assertIn("not-online node", reason)
        event.assert_not_called()

    def test_a_fence_on_another_lvstore_port_is_not_ours(self):
        """n2 also fences its own LVS_3 port; that is not this removal's
        business."""
        nodes = self._nodes()
        nodes[1].get_lvol_subsys_port = MagicMock(
            side_effect=lambda lvs: {"LVS_10": 4440, "LVS_3": 4434}[lvs])
        nodes[1].rpc_client().nvmf_get_blocked_ports = MagicMock(return_value={4434})
        db = _db(_cluster(npcs=1, ft=1), nodes)
        allowed, reason = _check_ftt_allows_node_removal("n1", db)
        self.assertFalse(allowed)
        self.assertIn("not-online node", reason)

    def test_unreadable_secondary_stays_a_passing_refusal(self):
        """A secondary whose SPDK cannot be asked is a node that may merely
        be unreachable; that is the generic refusal's case."""
        nodes = self._nodes()
        nodes[1].get_lvol_subsys_port = MagicMock(return_value=4440)
        nodes[1].rpc_client().nvmf_get_blocked_ports = MagicMock(
            side_effect=RPCConnectionError("proxy unreachable"))
        db = _db(_cluster(npcs=1, ft=1), nodes)
        with patch(EVENT) as event:
            allowed, reason = _check_ftt_allows_node_removal("n1", db)
        self.assertFalse(allowed)
        self.assertIn("not-online node", reason)
        event.assert_not_called()

    def test_tertiary_counts_as_own_secondary(self):
        nodes = [
            _node("n1", status=StorageNode.STATUS_OFFLINE, secondary_id="n2",
                  secondary_id_2="n3", lvstore="LVS_10"),
            _node("n2"),
            _node("n3", status=StorageNode.STATUS_DOWN),
            _node("n4"),
        ]
        _fence(nodes[2], 4440)
        db = _db(_cluster(npcs=2, ft=2), nodes)
        with patch(EVENT):
            allowed, reason = _check_ftt_allows_node_removal("n1", db)
        self.assertFalse(allowed)
        self.assertIn("aborted", reason)
        self.assertIn("n3", reason)


if __name__ == "__main__":
    unittest.main()


# ---------------------------------------------------------------------------
# Resource floors: the cluster after the removal must stay at or above them
# ---------------------------------------------------------------------------

class TestFloorGate(unittest.TestCase):
    """2+1: node floor n+2k = 4, data-device floor 4 (n+k = 3 with
    raid_devices), one data device per node."""

    def _nodes(self, count, devices=1):
        return [_with_devices(_node(f"n{i}"), devices) for i in range(count)]

    def test_removal_that_leaves_the_node_floor_is_allowed(self):
        cl = _cluster(npcs=1, ndcs=2, ft=1)
        db = _db(cl, self._nodes(5, devices=2))
        allowed, reason = _check_ftt_allows_node_removal("n1", db)
        self.assertTrue(allowed, reason)

    def test_removal_below_the_node_floor_is_refused_and_named(self):
        cl = _cluster(npcs=1, ndcs=2, ft=1)
        db = _db(cl, self._nodes(4, devices=2))
        allowed, reason = _check_ftt_allows_node_removal("n1", db)
        self.assertFalse(allowed)
        self.assertIn("nodes online 3 < floor 4 (n+2k)", reason)

    def test_removal_below_the_data_device_floor_is_refused(self):
        cl = _cluster(npcs=1, ndcs=2, ft=1)
        nodes = self._nodes(5, devices=1)
        # Without n1 four devices remain -- the floor. Take one away.
        nodes[4].nvme_devices = []
        db = _db(cl, nodes)
        allowed, reason = _check_ftt_allows_node_removal("n1", db)
        self.assertFalse(allowed)
        self.assertIn("node n4 has 0 online data devices < floor 1", reason)
        self.assertIn("data devices online 3 < floor 4 (n+2k)", reason)

    def test_raid_devices_lowers_the_data_device_floor(self):
        cl = _cluster(npcs=1, ndcs=2, ft=1)
        cl.raid_devices = True
        nodes = self._nodes(5, devices=1)
        nodes[4].nvme_devices = []
        db = _db(cl, nodes)
        allowed, reason = _check_ftt_allows_node_removal("n1", db)
        # The per-node floor still refuses; the cluster-wide one no longer does.
        self.assertFalse(allowed)
        self.assertIn("node n4 has 0 online data devices", reason)
        self.assertNotIn("data devices online", reason)

    def test_journal_device_does_not_count(self):
        cl = _cluster(npcs=1, ndcs=2, ft=1)
        nodes = self._nodes(5, devices=1)
        jm = MagicMock(spec=NVMeDevice)
        jm.status = NVMeDevice.STATUS_JM
        jm.get_id = MagicMock(return_value="n4-jm")
        nodes[4].nvme_devices = [jm]
        db = _db(cl, nodes)
        allowed, reason = _check_ftt_allows_node_removal("n1", db)
        self.assertFalse(allowed)
        self.assertIn("node n4 has 0 online data devices", reason)

    def test_rebalancing_no_longer_adds_a_headroom_rule(self):
        """The former headroom formula (active nodes - (ndcs+npcs)) is gone:
        with the floors satisfied a rebalance does not block the removal."""
        cl = _cluster(npcs=1, ndcs=2, ft=1, rebalancing=True)
        db = _db(cl, self._nodes(5, devices=2))
        allowed, reason = _check_ftt_allows_node_removal("n1", db)
        self.assertTrue(allowed, reason)

    def test_failure_domain_floor(self):
        cl = _cluster(npcs=1, ndcs=2, ft=1, enable_failure_domain=True)
        # Six nodes in three domains: removing n0 leaves domains {0,1,2} = n+k.
        nodes = [_with_devices(_node(f"n{i}", failure_domain=i % 3), 1) for i in range(6)]
        db = _db(cl, nodes)
        allowed, reason = _check_ftt_allows_node_removal("n0", db)
        self.assertTrue(allowed, reason)
        # Two domains only: below the n+k = 3 domain floor.
        nodes = [_with_devices(_node(f"n{i}", failure_domain=i % 2), 1) for i in range(6)]
        db = _db(cl, nodes)
        allowed, reason = _check_ftt_allows_node_removal("n0", db)
        self.assertFalse(allowed)
        self.assertIn("failure domains online 2 < floor 3 (n+k)", reason)
