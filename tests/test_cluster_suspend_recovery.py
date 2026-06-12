# coding=utf-8
"""
test_cluster_suspend_recovery.py — regression tests for the
auto-recovery deadlock that left clusters DEGRADED instead of SUSPENDED
when more than ``distr_npcs`` nodes went non-online together.

Three interlocking bugs were fixed in
``simplyblock_core/services/storage_node_monitor.py``:

1. ``get_next_cluster_status`` only marked a node "affected" when its
   ``nvme_devices[*].status`` were not ONLINE. ``set_node_down`` does
   NOT flip device statuses (a DOWN node still has SPDK + devices alive,
   only the client port is blocked), and ``set_node_unreachable`` does
   not either. So a state like {2 OFFLINE, 2 DOWN} on a 1×2 cluster
   produced ``affected_nodes == 2 == k`` -> DEGRADED, when by FTT
   semantics it should be SUSPENDED.

2. ``add_node_to_auto_restart`` refuses to queue when
   ``offline_nodes > distr_npcs`` *unless* the cluster is already
   SUSPENDED. With bug #1 the cluster never suspends -> the queue is
   never accepted -> no auto-recovery. ``set_node_offline`` only queues
   once (it guards on the OFFLINE no-op), so no path retries the queue
   after the first refusal. Fix: ``update_cluster_status`` now scans for
   OFFLINE/SCHEDULABLE nodes without an active restart task and
   re-queues them every monitor tick.

3. ``_check_data_plane_and_escalate`` used to require all peers to vote
   "disconnected" to escalate UNREACHABLE -> OFFLINE. In a severe
   outage, every peer is itself non-online and ``_count_data_plane_votes``
   returns ``(0, 0)`` -> no escalation -> stuck UNREACHABLE. Fix: when
   there are no peer votes, fall back to a direct
   ``snode_api.spdk_process_is_up`` probe on the node itself.

Plus a robustness fix for ``set_node_offline``: each follow-up step
(``update_cluster_status``, ANA failover, auto-restart queue) is its
own try/except so an exception in one cannot strand the node OFFLINE
with no restart queued.
"""

import unittest
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch

from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.models.nvme_device import NVMeDevice


def _down_ts(seconds_ago):
    """ISO timestamp `seconds_ago` in the past, for node.down_since."""
    return (datetime.now(timezone.utc) - timedelta(seconds=seconds_ago)).isoformat()


# storage_node_monitor.DOWN_SUSPEND_GRACE_SEC == 60: a DOWN node only counts
# toward the suspend threshold once it has been DOWN at least that long.
#
# These are *sentinels*, not precomputed timestamps: the production grace check
# compares ``down_since`` against the real wall clock, so a timestamp frozen at
# module-import time silently crosses the 60s grace once the surrounding suite
# runs long enough (the full suite takes minutes), flipping a "transient" node
# into a "sustained" one and failing TestDownGraceWindow. _node() resolves these
# to a *fresh* offset at test-execution time, right before the assertion runs.
_SUSTAINED_DOWN = "__sustained_down__"   # well past the 60s grace -> counts
_TRANSIENT_DOWN = "__transient_down__"   # inside the grace window -> does NOT count


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _dev(status=NVMeDevice.STATUS_ONLINE, uuid="dev-x"):
    d = MagicMock(spec=NVMeDevice)
    d.status = status
    d.get_id = MagicMock(return_value=uuid)
    return d


def _node(uuid, status=StorageNode.STATUS_ONLINE, mgmt_ip=None,
          n_online_devs=2, n_offline_devs=0, cluster_id="cluster-1",
          jm_vuid=999, rpc_port=8080, online_since="", down_since=_SUSTAINED_DOWN):
    """Build a mock StorageNode with a populated nvme_devices list.

    The default 2 online / 0 offline devices matches a healthy node and
    also matches what ``set_node_down`` leaves behind (devices stay
    ONLINE even though node status flipped to DOWN).

    ``down_since`` defaults to a sustained (>grace) timestamp so DOWN nodes
    count toward the suspend bucket unless a test explicitly makes them
    transient — this preserves the pre-grace-window assertions.
    """
    n = MagicMock(spec=StorageNode)
    n.status = status
    n.cluster_id = cluster_id
    n.mgmt_ip = mgmt_ip or f"10.0.0.{abs(hash(uuid)) % 250 + 1}"
    # Default False: a bare MagicMock attribute is truthy, which would make
    # the re-queue scan treat every node as a deliberate shutdown and skip it.
    n.auto_restart_disabled = False
    n.jm_vuid = jm_vuid
    n.rpc_port = rpc_port
    n.online_since = online_since
    # Resolve down_since sentinels to a fresh timestamp at call time so the
    # grace-window comparison is stable regardless of how long the suite runs.
    if down_since == _SUSTAINED_DOWN:
        down_since = _down_ts(300)   # well past 60s grace -> counts
    elif down_since == _TRANSIENT_DOWN:
        down_since = _down_ts(5)     # inside 60s grace -> does NOT count
    n.down_since = down_since
    n.lvstore = "LVS_X"
    n.lvstore_status = "ready"
    n.get_id = MagicMock(return_value=uuid)
    n.nvme_devices = (
        [_dev(NVMeDevice.STATUS_ONLINE, f"{uuid}-on-{i}") for i in range(n_online_devs)] +
        [_dev(NVMeDevice.STATUS_UNAVAILABLE, f"{uuid}-off-{i}") for i in range(n_offline_devs)]
    )
    return n


def _cluster(status=Cluster.STATUS_ACTIVE, distr_ndcs=1, distr_npcs=2,
             strict_anti_affinity=False):
    c = MagicMock(spec=Cluster)
    c.uuid = "cluster-1"
    c.status = status
    c.distr_ndcs = distr_ndcs
    c.distr_npcs = distr_npcs
    c.strict_node_anti_affinity = strict_anti_affinity
    return c


# ===========================================================================
# Fix 1: get_next_cluster_status counts non-ONLINE nodes as affected
# ===========================================================================


class TestGetNextClusterStatusCountsNonOnline(unittest.TestCase):
    """When a node is non-ONLINE its data plane is unavailable to clients
    even if its NVMe device records still say ONLINE. The cluster
    state machine must count these toward the FTT bucket — otherwise
    multi-node DOWN/UNREACHABLE outages leave the cluster DEGRADED and
    auto-restart stays blocked by the peer-count guard.
    """

    def _run(self, nodes, cluster):
        from simplyblock_core.services import storage_node_monitor as mod
        with patch.object(mod, "db") as mock_db:
            mock_db.get_cluster_by_id.return_value = cluster
            mock_db.get_primary_storage_nodes_by_cluster_id.return_value = nodes
            return mod.get_next_cluster_status("cluster-1")

    # ---- baseline behavior preserved ------------------------------------

    def test_all_online_active(self):
        # Healthy 1x2 cluster: 4 online nodes, devices all online.
        nodes = [_node(f"n{i}") for i in range(4)]
        c = _cluster(distr_ndcs=1, distr_npcs=2)
        self.assertEqual(self._run(nodes, c), Cluster.STATUS_ACTIVE)

    def test_one_offline_node_zero_online_devs_degraded(self):
        # 1 of 4 OFFLINE (devices unavailable) — affected_nodes == 1, k == 2,
        # so affected_nodes < k -> ACTIVE (no degradation: still within FTT).
        # The DEGRADED gate is "affected_nodes == k". Switch to k=1 to
        # exercise that specific gate.
        nodes = [_node(f"n{i}") for i in range(4)]
        nodes[0] = _node("n0", status=StorageNode.STATUS_OFFLINE,
                         n_online_devs=0, n_offline_devs=2)
        c = _cluster(distr_ndcs=1, distr_npcs=1)
        self.assertEqual(self._run(nodes, c), Cluster.STATUS_DEGRADED)

    # ---- the new behavior: DOWN/UNREACHABLE/SCHEDULABLE contribute ------

    def test_down_node_with_online_devices_counts_as_affected(self):
        # Reproduces test case 2's final state on a 1x2 cluster
        # (distr_ndcs=1, distr_npcs=2): 2 nodes OFFLINE (devs 2/0) and
        # 2 nodes DOWN (devs 2/2). Without the fix, affected_nodes=2==k
        # and we'd return DEGRADED. With the fix, the DOWN nodes also
        # count -> affected_nodes=4 > k=2 -> SUSPENDED.
        nodes = [
            _node("n0", status=StorageNode.STATUS_OFFLINE,
                  n_online_devs=0, n_offline_devs=2),
            _node("n1", status=StorageNode.STATUS_OFFLINE,
                  n_online_devs=0, n_offline_devs=2),
            # DOWN nodes: set_node_down does not flip devices, so they
            # remain ONLINE in the DB.
            _node("n2", status=StorageNode.STATUS_DOWN,
                  n_online_devs=2, n_offline_devs=0),
            _node("n3", status=StorageNode.STATUS_DOWN,
                  n_online_devs=2, n_offline_devs=0),
        ]
        c = _cluster(distr_ndcs=1, distr_npcs=2)
        self.assertEqual(self._run(nodes, c), Cluster.STATUS_SUSPENDED)

    def test_test_case_1_state_three_not_online_suspended(self):
        # Test case 1's final state: 2 OFFLINE (2/0 devs), 1 DOWN (2/2),
        # 1 ONLINE on a 1x2 (k=2) cluster. Three nodes are not online;
        # by user's FTT semantics this must be SUSPENDED.
        nodes = [
            _node("n0", status=StorageNode.STATUS_OFFLINE,
                  n_online_devs=0, n_offline_devs=2),
            _node("n1", status=StorageNode.STATUS_OFFLINE,
                  n_online_devs=0, n_offline_devs=2),
            _node("n2", status=StorageNode.STATUS_DOWN,
                  n_online_devs=2, n_offline_devs=0),
            _node("n3", status=StorageNode.STATUS_ONLINE),
        ]
        c = _cluster(distr_ndcs=1, distr_npcs=2)
        self.assertEqual(self._run(nodes, c), Cluster.STATUS_SUSPENDED)

    def test_unreachable_node_counts_as_affected(self):
        # Same story for UNREACHABLE — mgmt-plane gone, data records
        # may still claim ONLINE because escalation hasn't fired yet.
        nodes = [
            _node("n0", status=StorageNode.STATUS_UNREACHABLE),
            _node("n1", status=StorageNode.STATUS_UNREACHABLE),
            _node("n2", status=StorageNode.STATUS_UNREACHABLE),
            _node("n3", status=StorageNode.STATUS_ONLINE),
        ]
        c = _cluster(distr_ndcs=1, distr_npcs=2)
        # 3 unreachable > k=2 -> SUSPENDED
        self.assertEqual(self._run(nodes, c), Cluster.STATUS_SUSPENDED)

    def test_schedulable_node_counts_as_affected(self):
        # SCHEDULABLE means SPDK RPC double-timed-out — SPDK is sick.
        # Treat as affected.
        nodes = [
            _node("n0", status=StorageNode.STATUS_SCHEDULABLE),
            _node("n1", status=StorageNode.STATUS_SCHEDULABLE),
            _node("n2", status=StorageNode.STATUS_SCHEDULABLE),
            _node("n3", status=StorageNode.STATUS_ONLINE),
        ]
        c = _cluster(distr_ndcs=1, distr_npcs=2)
        self.assertEqual(self._run(nodes, c), Cluster.STATUS_SUSPENDED)

    def test_removed_node_does_not_count_as_affected(self):
        # REMOVED nodes are excluded from the cluster — they should not
        # contribute to the affected count.
        nodes = [
            _node("n0", status=StorageNode.STATUS_REMOVED,
                  n_online_devs=0, n_offline_devs=0),
            _node("n1", status=StorageNode.STATUS_ONLINE),
            _node("n2", status=StorageNode.STATUS_ONLINE),
            _node("n3", status=StorageNode.STATUS_ONLINE),
        ]
        c = _cluster(distr_ndcs=1, distr_npcs=2)
        # 0 affected, 3 online -> ACTIVE
        self.assertEqual(self._run(nodes, c), Cluster.STATUS_ACTIVE)


# ===========================================================================
# DOWN grace window: a transient DOWN must not tip the cluster into suspend
# ===========================================================================


class TestDownGraceWindow(unittest.TestCase):
    """A DOWN node is temporary (SPDK + devices alive, only the client port is
    blocked) and commonly self-heals in seconds. It must only count toward the
    suspend threshold after DOWN_SUSPEND_GRACE_SEC (60s) — so a brief blip on a
    third physical node can't tip an otherwise-survivable outage into a full
    cluster suspend (incident 2026-06-08: cbc62adc DOWN ~6.5s suspended the
    cluster). A sustained DOWN still counts so auto-restart can recover.
    """

    def _run(self, nodes, cluster):
        from simplyblock_core.services import storage_node_monitor as mod
        with patch.object(mod, "db") as mock_db:
            mock_db.get_cluster_by_id.return_value = cluster
            mock_db.get_primary_storage_nodes_by_cluster_id.return_value = nodes
            return mod.get_next_cluster_status("cluster-1")

    def test_transient_down_third_node_does_not_suspend(self):
        # The incident shape: 2 physical nodes genuinely OFFLINE (affected==k,
        # survivable) + a 3rd node only briefly DOWN. The transient DOWN must
        # NOT be counted -> cluster stays DEGRADED, not SUSPENDED.
        nodes = [
            _node("off-1", status=StorageNode.STATUS_OFFLINE,
                  mgmt_ip="10.0.0.1", n_online_devs=0, n_offline_devs=2),
            _node("off-2", status=StorageNode.STATUS_OFFLINE,
                  mgmt_ip="10.0.0.2", n_online_devs=0, n_offline_devs=2),
            _node("down-transient", status=StorageNode.STATUS_DOWN,
                  mgmt_ip="10.0.0.3", n_online_devs=2, n_offline_devs=0,
                  down_since=_TRANSIENT_DOWN),
            _node("on-1", status=StorageNode.STATUS_ONLINE, mgmt_ip="10.0.0.4"),
        ]
        c = _cluster(distr_ndcs=1, distr_npcs=2)
        self.assertEqual(self._run(nodes, c), Cluster.STATUS_DEGRADED)

    def test_sustained_down_third_node_suspends(self):
        # Same shape but the 3rd node has been DOWN past the grace window:
        # now it counts -> affected 3 > k=2 -> SUSPENDED (so auto-restart fires).
        nodes = [
            _node("off-1", status=StorageNode.STATUS_OFFLINE,
                  mgmt_ip="10.0.0.1", n_online_devs=0, n_offline_devs=2),
            _node("off-2", status=StorageNode.STATUS_OFFLINE,
                  mgmt_ip="10.0.0.2", n_online_devs=0, n_offline_devs=2),
            _node("down-sustained", status=StorageNode.STATUS_DOWN,
                  mgmt_ip="10.0.0.3", n_online_devs=2, n_offline_devs=0,
                  down_since=_SUSTAINED_DOWN),
            _node("on-1", status=StorageNode.STATUS_ONLINE, mgmt_ip="10.0.0.4"),
        ]
        c = _cluster(distr_ndcs=1, distr_npcs=2)
        self.assertEqual(self._run(nodes, c), Cluster.STATUS_SUSPENDED)

    def test_transient_down_missing_timestamp_counts(self):
        # Conservative fallback: a DOWN node with a blank down_since (legacy row
        # / never stamped) is treated as sustained and still counts.
        nodes = [
            _node("off-1", status=StorageNode.STATUS_OFFLINE,
                  mgmt_ip="10.0.0.1", n_online_devs=0, n_offline_devs=2),
            _node("off-2", status=StorageNode.STATUS_OFFLINE,
                  mgmt_ip="10.0.0.2", n_online_devs=0, n_offline_devs=2),
            _node("down-blank", status=StorageNode.STATUS_DOWN,
                  mgmt_ip="10.0.0.3", n_online_devs=2, n_offline_devs=0,
                  down_since=""),
            _node("on-1", status=StorageNode.STATUS_ONLINE, mgmt_ip="10.0.0.4"),
        ]
        c = _cluster(distr_ndcs=1, distr_npcs=2)
        self.assertEqual(self._run(nodes, c), Cluster.STATUS_SUSPENDED)

    def test_lone_transient_down_stays_active(self):
        # A single transient DOWN with everything else healthy: not counted,
        # cluster is ACTIVE (no degradation at all).
        nodes = [
            _node("down-transient", status=StorageNode.STATUS_DOWN,
                  mgmt_ip="10.0.0.1", n_online_devs=2, n_offline_devs=0,
                  down_since=_TRANSIENT_DOWN),
            _node("on-1", status=StorageNode.STATUS_ONLINE, mgmt_ip="10.0.0.2"),
            _node("on-2", status=StorageNode.STATUS_ONLINE, mgmt_ip="10.0.0.3"),
            _node("on-3", status=StorageNode.STATUS_ONLINE, mgmt_ip="10.0.0.4"),
        ]
        c = _cluster(distr_ndcs=1, distr_npcs=2)
        self.assertEqual(self._run(nodes, c), Cluster.STATUS_ACTIVE)


# ===========================================================================
# Fix 2: update_cluster_status re-queues stuck OFFLINE/SCHEDULABLE nodes
# ===========================================================================


class TestUpdateClusterStatusRequeuesOffline(unittest.TestCase):
    """When set_node_offline's auto-restart queue was refused (because the
    cluster was DEGRADED at the moment with too many peers offline), no
    code path retries the queue afterwards. The monitor's
    update_cluster_status now scans for OFFLINE/SCHEDULABLE nodes with
    no active restart task and re-queues them.
    """

    def _patched_mod(self, cluster, nodes, active_restart_tasks=None):
        """Patch storage_node_monitor's db + cluster_ops + tasks_controller
        and return (mod, mocks_dict)."""
        from simplyblock_core.services import storage_node_monitor as mod

        # active_restart_tasks: mapping of node_id -> bool (True = has active task)
        active = active_restart_tasks or {}

        db_p = patch.object(mod, "db")
        co_p = patch.object(mod, "cluster_ops")
        tc_p = patch.object(mod, "tasks_controller")
        mock_db = db_p.start()
        mock_co = co_p.start()
        mock_tc = tc_p.start()
        self.addCleanup(db_p.stop)
        self.addCleanup(co_p.stop)
        self.addCleanup(tc_p.stop)

        mock_db.get_cluster_by_id.return_value = cluster
        # get_next_cluster_status uses these:
        mock_db.get_primary_storage_nodes_by_cluster_id.return_value = nodes
        mock_db.get_storage_nodes_by_cluster_id.return_value = nodes
        mock_db.get_job_tasks.return_value = []  # no rebalancing tasks

        mock_tc.get_active_node_restart_task.side_effect = \
            lambda cid, nid: active.get(nid, False)
        mock_tc.add_node_to_auto_restart = MagicMock(return_value="task-uuid")

        return mod, {"db": mock_db, "co": mock_co, "tc": mock_tc}

    def test_offline_node_without_active_task_is_requeued(self):
        # 2 OFFLINE + 2 DOWN on a 1x2 cluster -> next status is SUSPENDED
        # (per Fix 1). Neither OFFLINE node has an active restart task.
        # The monitor must call add_node_to_auto_restart for each.
        nodes = [
            _node("off-1", status=StorageNode.STATUS_OFFLINE,
                  n_online_devs=0, n_offline_devs=2),
            _node("off-2", status=StorageNode.STATUS_OFFLINE,
                  n_online_devs=0, n_offline_devs=2),
            _node("down-1", status=StorageNode.STATUS_DOWN),
            _node("down-2", status=StorageNode.STATUS_DOWN),
        ]
        c = _cluster(status=Cluster.STATUS_SUSPENDED, distr_ndcs=1, distr_npcs=2)
        mod, mocks = self._patched_mod(c, nodes)
        # next status will compute SUSPENDED, current is SUSPENDED,
        # so update_cluster_status would call cluster_activate only if
        # can_activate is True — it isn't here (nodes are not ONLINE).
        # That's fine; what we care about is the re-queue scan.
        mod.update_cluster_status("cluster-1")

        called_for = [
            call.args[0].get_id()
            for call in mocks["tc"].add_node_to_auto_restart.call_args_list
        ]
        self.assertIn("off-1", called_for)
        self.assertIn("off-2", called_for)
        # DOWN nodes must NOT be re-queued for auto-restart — DOWN is not
        # an auto-restart trigger (SPDK alive, only port blocked).
        self.assertNotIn("down-1", called_for)
        self.assertNotIn("down-2", called_for)

    def test_offline_node_with_active_task_not_requeued(self):
        nodes = [
            _node("off-1", status=StorageNode.STATUS_OFFLINE,
                  n_online_devs=0, n_offline_devs=2),
            _node("on-1", status=StorageNode.STATUS_ONLINE),
        ]
        c = _cluster(status=Cluster.STATUS_DEGRADED, distr_ndcs=1, distr_npcs=2)
        mod, mocks = self._patched_mod(
            c, nodes, active_restart_tasks={"off-1": True})
        mod.update_cluster_status("cluster-1")
        mocks["tc"].add_node_to_auto_restart.assert_not_called()

    def test_schedulable_node_requeued(self):
        nodes = [
            _node("sched-1", status=StorageNode.STATUS_SCHEDULABLE),
            _node("on-1", status=StorageNode.STATUS_ONLINE),
        ]
        c = _cluster(status=Cluster.STATUS_DEGRADED, distr_ndcs=1, distr_npcs=2)
        mod, mocks = self._patched_mod(c, nodes)
        mod.update_cluster_status("cluster-1")
        called_for = [
            call.args[0].get_id()
            for call in mocks["tc"].add_node_to_auto_restart.call_args_list
        ]
        self.assertEqual(called_for, ["sched-1"])

    def test_deliberately_shut_down_offline_node_not_requeued(self):
        # An OFFLINE node carrying auto_restart_disabled=True was stopped on
        # purpose via `sn shutdown`; the re-queue scan must leave it alone
        # even though it has no active restart task.
        off = _node("off-1", status=StorageNode.STATUS_OFFLINE,
                    n_online_devs=0, n_offline_devs=2)
        off.auto_restart_disabled = True
        nodes = [off, _node("on-1", status=StorageNode.STATUS_ONLINE)]
        c = _cluster(status=Cluster.STATUS_DEGRADED, distr_ndcs=1, distr_npcs=2)
        mod, mocks = self._patched_mod(c, nodes)
        mod.update_cluster_status("cluster-1")
        mocks["tc"].add_node_to_auto_restart.assert_not_called()

    def test_online_nodes_never_requeued(self):
        nodes = [_node(f"n{i}", status=StorageNode.STATUS_ONLINE) for i in range(3)]
        c = _cluster(status=Cluster.STATUS_ACTIVE, distr_ndcs=1, distr_npcs=2)
        mod, mocks = self._patched_mod(c, nodes)
        mod.update_cluster_status("cluster-1")
        mocks["tc"].add_node_to_auto_restart.assert_not_called()

    def test_requeue_exception_does_not_prevent_other_nodes(self):
        nodes = [
            _node("off-1", status=StorageNode.STATUS_OFFLINE,
                  n_online_devs=0, n_offline_devs=2),
            _node("off-2", status=StorageNode.STATUS_OFFLINE,
                  n_online_devs=0, n_offline_devs=2),
        ]
        c = _cluster(status=Cluster.STATUS_SUSPENDED, distr_ndcs=1, distr_npcs=2)
        mod, mocks = self._patched_mod(c, nodes)

        # First call raises, second must still run.
        mocks["tc"].add_node_to_auto_restart.side_effect = [
            RuntimeError("FDB blip"), "task-uuid",
        ]
        mod.update_cluster_status("cluster-1")
        self.assertEqual(mocks["tc"].add_node_to_auto_restart.call_count, 2)


# ===========================================================================
# Fix 3: _check_data_plane_and_escalate falls back to SnodeAPI when no peers
# ===========================================================================


class TestDataPlaneEscalateNoPeersFallback(unittest.TestCase):
    """When no online peers exist (every other node is non-online too),
    the peer-quorum data-plane check returns (0, 0) -> abstain. Without
    the fallback, an UNREACHABLE node whose SPDK actually died stays
    UNREACHABLE forever. Fix: probe the node's own SnodeAPI directly;
    if SPDK is confirmed gone, escalate to OFFLINE.
    """

    def _patched_mod(self, target_node, votes, spdk_up=None, spdk_exc=None):
        from simplyblock_core.services import storage_node_monitor as mod

        db_p = patch.object(mod, "db")
        votes_p = patch.object(mod, "_count_data_plane_votes", return_value=votes)
        sno_p = patch.object(mod, "set_node_offline")
        mock_db = db_p.start()
        votes_p.start()
        mock_sno = sno_p.start()
        self.addCleanup(db_p.stop)
        self.addCleanup(votes_p.stop)
        self.addCleanup(sno_p.stop)

        mock_db.get_storage_node_by_id.return_value = target_node

        # snode_api.spdk_process_is_up(rpc_port, cluster_id) -> (bool, msg)
        if spdk_exc is not None:
            target_node.client = MagicMock(side_effect=spdk_exc)
        else:
            api = MagicMock()
            api.spdk_process_is_up.return_value = (spdk_up, "")
            target_node.client = MagicMock(return_value=api)

        return mod, mock_sno

    def test_peer_quorum_unanimous_disconnect_escalates(self):
        # Baseline: with peer votes, fallback is not consulted.
        target = _node("t", status=StorageNode.STATUS_UNREACHABLE)
        mod, mock_sno = self._patched_mod(target, votes=(3, 3))
        mod._check_data_plane_and_escalate(target)
        mock_sno.assert_called_once()
        # The SnodeAPI fallback should not have been used.
        target.client.assert_not_called() if hasattr(target.client, "assert_not_called") \
            else self.assertFalse(target.client.called)

    def test_partial_peer_disconnect_does_not_escalate(self):
        target = _node("t", status=StorageNode.STATUS_UNREACHABLE)
        mod, mock_sno = self._patched_mod(target, votes=(2, 3))
        mod._check_data_plane_and_escalate(target)
        mock_sno.assert_not_called()
        # Fallback must NOT fire when peers cast votes — those votes are
        # the authoritative signal.
        self.assertFalse(target.client.called)

    def test_no_peers_and_spdk_dead_escalates(self):
        # The headline case: cluster has lost too many nodes to vote, but
        # the node's own SnodeAPI confirms SPDK is gone -> escalate to
        # OFFLINE so auto-restart can fire.
        target = _node("t", status=StorageNode.STATUS_UNREACHABLE)
        mod, mock_sno = self._patched_mod(target, votes=(0, 0), spdk_up=False)
        mod._check_data_plane_and_escalate(target)
        mock_sno.assert_called_once()

    def test_no_peers_and_spdk_alive_does_not_escalate(self):
        # If SnodeAPI says SPDK is alive, the node is in a strange but
        # potentially recoverable state — do not destructively flip to
        # OFFLINE just because the cluster is too damaged to vote.
        target = _node("t", status=StorageNode.STATUS_UNREACHABLE)
        mod, mock_sno = self._patched_mod(target, votes=(0, 0), spdk_up=True)
        mod._check_data_plane_and_escalate(target)
        mock_sno.assert_not_called()

    def test_no_peers_and_snodeapi_unreachable_does_not_escalate(self):
        # SnodeAPI itself is down too -> we have no signal. Stay
        # conservative: do not escalate. The next monitor tick will retry.
        target = _node("t", status=StorageNode.STATUS_UNREACHABLE)
        mod, mock_sno = self._patched_mod(
            target, votes=(0, 0), spdk_exc=ConnectionError("boom"))
        mod._check_data_plane_and_escalate(target)
        mock_sno.assert_not_called()

    def test_restarting_node_skipped(self):
        # Sanity: the existing skip for RESTARTING is preserved.
        target = _node("t", status=StorageNode.STATUS_RESTARTING)
        mod, mock_sno = self._patched_mod(target, votes=(3, 3))
        mod._check_data_plane_and_escalate(target)
        mock_sno.assert_not_called()


# ===========================================================================
# Fix 4: set_node_offline always reaches add_node_to_auto_restart
# ===========================================================================


class TestSetNodeOfflineRobustness(unittest.TestCase):
    """An exception in update_cluster_status or trigger_ana_failover_for_node
    must not strand the node OFFLINE without a queued auto-restart. The
    fix isolates each follow-up step in its own try/except.
    """

    def _patched_mod(self, node, peers=None,
                     update_cluster_status_exc=None,
                     ana_exc=None):
        from simplyblock_core.services import storage_node_monitor as mod

        db_p = patch.object(mod, "db")
        ops_p = patch.object(mod, "storage_node_ops")
        dc_p = patch.object(mod, "device_controller")
        tc_p = patch.object(mod, "tasks_controller")
        ucs_p = patch.object(mod, "update_cluster_status")
        mock_db = db_p.start()
        mock_ops = ops_p.start()
        mock_dc = dc_p.start()
        mock_tc = tc_p.start()
        mock_ucs = ucs_p.start()
        self.addCleanup(db_p.stop)
        self.addCleanup(ops_p.stop)
        self.addCleanup(dc_p.stop)
        self.addCleanup(tc_p.stop)
        self.addCleanup(ucs_p.stop)

        mock_db.get_storage_node_by_id.return_value = node
        mock_db.get_storage_nodes_by_cluster_id.return_value = peers or [node]

        if update_cluster_status_exc:
            mock_ucs.side_effect = update_cluster_status_exc
        if ana_exc:
            mock_ops.trigger_ana_failover_for_node.side_effect = ana_exc

        return mod, {
            "ops": mock_ops, "dc": mock_dc, "tc": mock_tc,
            "ucs": mock_ucs, "db": mock_db,
        }

    def test_set_node_offline_queues_auto_restart_on_happy_path(self):
        node = _node("n", status=StorageNode.STATUS_DOWN,
                     n_online_devs=2, n_offline_devs=0)
        mod, mocks = self._patched_mod(node)
        mod.set_node_offline(node)

        mocks["ops"].set_node_status.assert_called_once()
        args, _ = mocks["ops"].set_node_status.call_args
        self.assertEqual(args[0], "n")
        self.assertEqual(args[1], StorageNode.STATUS_OFFLINE)
        # All ONLINE devs flipped to unavailable.
        self.assertEqual(mocks["dc"].device_set_unavailable.call_count, 2)
        mocks["ucs"].assert_called_once()
        mocks["ops"].trigger_ana_failover_for_node.assert_called_once()
        mocks["tc"].add_node_to_auto_restart.assert_called_once_with(node)

    def test_update_cluster_status_exception_does_not_prevent_auto_restart(self):
        # Headline case: update_cluster_status throws but the auto-restart
        # queue call must still run.
        node = _node("n", status=StorageNode.STATUS_DOWN,
                     n_online_devs=2, n_offline_devs=0)
        mod, mocks = self._patched_mod(
            node, update_cluster_status_exc=RuntimeError("cluster activate failed"))
        mod.set_node_offline(node)

        # Status was flipped, devs were marked unavailable, then UCS raised.
        mocks["ops"].set_node_status.assert_called_once()
        self.assertEqual(mocks["dc"].device_set_unavailable.call_count, 2)
        mocks["ucs"].assert_called_once()
        # Critical: auto-restart still queued.
        mocks["tc"].add_node_to_auto_restart.assert_called_once_with(node)

    def test_ana_failover_exception_does_not_prevent_auto_restart(self):
        node = _node("n", status=StorageNode.STATUS_DOWN,
                     n_online_devs=2, n_offline_devs=0)
        mod, mocks = self._patched_mod(
            node, ana_exc=RuntimeError("ANA RPC timeout"))
        mod.set_node_offline(node)

        mocks["ops"].trigger_ana_failover_for_node.assert_called_once()
        mocks["tc"].add_node_to_auto_restart.assert_called_once_with(node)

    def test_skip_when_already_offline(self):
        # The guard at the top of set_node_offline still skips OFFLINE
        # nodes so we don't churn a node already in the right state.
        node = _node("n", status=StorageNode.STATUS_OFFLINE,
                     n_online_devs=0, n_offline_devs=2)
        mod, mocks = self._patched_mod(node)
        mod.set_node_offline(node)
        mocks["ops"].set_node_status.assert_not_called()
        mocks["tc"].add_node_to_auto_restart.assert_not_called()

    def test_skip_when_in_shutdown_or_restarting(self):
        for st in (StorageNode.STATUS_IN_SHUTDOWN, StorageNode.STATUS_RESTARTING):
            with self.subTest(status=st):
                node = _node("n", status=st)
                mod, mocks = self._patched_mod(node)
                mod.set_node_offline(node)
                mocks["ops"].set_node_status.assert_not_called()
                mocks["tc"].add_node_to_auto_restart.assert_not_called()


if __name__ == "__main__":
    unittest.main()
