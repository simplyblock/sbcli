# coding=utf-8
"""
test_activation_fixes.py – unit tests covering the cluster_activate fixes:

1. _connect_to_remote_jm_devs no longer skips attaches based on
   _peer_reachable_via_jm_quorum. During activation the peers' JC quorums
   are still bootstrapping, so that probe cannot answer correctly and
   previously caused every intended remote_jm member to be skipped.

2. get_secondary_nodes_2 enforces host-disjointness: tertiary cannot share
   mgmt_ip with primary OR with the already-picked secondary. A single host
   outage must never take out two of the four HA journal members.

All external dependencies (FDB, RPC, SPDK) are mocked.
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.models.nvme_device import JMDevice, NVMeDevice
from simplyblock_core.models.iface import IFace


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _node(uuid, cluster_id="cluster-1", mgmt_ip=None,
          status=StorageNode.STATUS_ONLINE, rpc_port=8080,
          is_secondary_node=False,
          lvstore_stack_secondary="", lvstore_stack_tertiary="",
          jm_vuid=0, jm_ids=None, jm_device=None):
    n = StorageNode()
    n.uuid = uuid
    n.cluster_id = cluster_id
    n.status = status
    n.hostname = f"host-{uuid}"
    n.mgmt_ip = mgmt_ip if mgmt_ip is not None else f"10.0.0.{(hash(uuid) % 254) + 1}"
    n.rpc_port = rpc_port
    n.rpc_username = "user"
    n.rpc_password = "pass"
    n.is_secondary_node = is_secondary_node
    n.lvstore_stack_secondary = lvstore_stack_secondary
    n.lvstore_stack_tertiary = lvstore_stack_tertiary
    n.jm_vuid = jm_vuid
    n.jm_ids = jm_ids or []
    n.jm_device = jm_device
    n.remote_jm_devices = []
    n.nvme_devices = []
    n.data_nics = [IFace()]
    n.data_nics[0].ip4_address = n.mgmt_ip
    n.data_nics[0].trtype = "TCP"
    n.active_tcp = True
    n.active_rdma = False
    return n


def _jm_device(uuid, node_id, alceml_name=None):
    jd = JMDevice()
    jd.uuid = uuid
    jd.node_id = node_id
    jd.status = NVMeDevice.STATUS_ONLINE
    jd.alceml_name = alceml_name or f"alceml_{uuid}"
    jd.jm_bdev = f"jm_{uuid}"
    jd.size = 1 << 30
    jd.nvmf_multipath = False
    return jd


# ===========================================================================
# 1. _connect_to_remote_jm_devs — no quorum-skip gate during activation
# ===========================================================================

class TestConnectToRemoteJmDevs(unittest.TestCase):
    """Task 6: the JM-quorum reachability gate previously skipped every
    intended remote_jm attach during cluster_activate because peers had no
    bootstrapped JC quorum yet. The gate has been removed from this code
    path; attaches must proceed regardless of quorum-probe results.
    """

    def _patch_stack(self, this_node, all_nodes, existing_bdevs=None):
        """Patch everything _connect_to_remote_jm_devs touches and return the
        mocked RPC client + connect_device mock so tests can introspect calls.
        """
        existing_bdevs = existing_bdevs or []
        mock_db = MagicMock()
        mock_db.get_storage_nodes_by_cluster_id.return_value = all_nodes
        mock_db.get_storage_nodes.return_value = all_nodes

        def _get_jm_dev(jm_id):
            for n in all_nodes:
                if n.jm_device and n.jm_device.get_id() == jm_id:
                    return n.jm_device
            return None

        mock_db.get_jm_device_by_id.side_effect = _get_jm_dev

        mock_rpc = MagicMock()
        mock_rpc.get_bdevs.return_value = existing_bdevs
        # After connect_device succeeds, the expected remote bdev is "found".
        # Simulate that by always returning a non-empty list when asked about
        # a specific bdev.
        def _get_bdevs(name=None):
            if name is None:
                return existing_bdevs
            return [{"name": name}]
        mock_rpc.get_bdevs.side_effect = _get_bdevs

        return mock_db, mock_rpc

    @patch("simplyblock_core.storage_node_ops.time.sleep", return_value=None)
    @patch("simplyblock_core.storage_node_ops.connect_device")
    @patch("simplyblock_core.models.storage_node.RPCClient")
    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_attaches_proceed_when_quorum_probe_would_fail(
            self, MockDBCtrl, MockRPC, mock_connect_device, _mock_sleep):
        """Even if every peer would report quorum-unreachable (i.e. during
        activation bootstrap), all intended remote_jm members must still be
        attached locally. This is the regression the fix addresses.
        """
        import simplyblock_core.storage_node_ops as ops

        this_node = _node("this", mgmt_ip="10.0.0.1", jm_vuid=8612)
        peer_a = _node("peer-a", mgmt_ip="10.0.0.2", jm_vuid=0,
                       jm_device=_jm_device("jm-a", "peer-a"))
        peer_b = _node("peer-b", mgmt_ip="10.0.0.3", jm_vuid=0,
                       jm_device=_jm_device("jm-b", "peer-b"))
        peer_c = _node("peer-c", mgmt_ip="10.0.0.4", jm_vuid=0,
                       jm_device=_jm_device("jm-c", "peer-c"))

        mock_db, mock_rpc = self._patch_stack(
            this_node, [this_node, peer_a, peer_b, peer_c])
        MockDBCtrl.return_value = mock_db
        MockRPC.return_value = mock_rpc
        mock_connect_device.side_effect = lambda name, *a, **kw: f"{name}n1"

        # Even if _peer_reachable_via_jm_quorum would return False for every
        # target, the attaches must still go out.
        with patch.object(ops, "_peer_reachable_via_jm_quorum", return_value=False):
            result = ops._connect_to_remote_jm_devs(
                this_node, jm_ids=["jm-a", "jm-b", "jm-c"])

        # All 3 peer JMs must be attached (previously 0 attaches because the
        # quorum gate skipped every peer).
        self.assertEqual(len(result), 3,
            f"Expected 3 remote_jm attaches; got {len(result)}")
        attached_ids = {rjd.uuid for rjd in result}
        self.assertEqual(attached_ids, {"jm-a", "jm-b", "jm-c"})
        self.assertEqual(mock_connect_device.call_count, 3,
            "connect_device must be called once per remote JM peer")

    @patch("simplyblock_core.storage_node_ops.time.sleep", return_value=None)
    @patch("simplyblock_core.storage_node_ops.connect_device")
    @patch("simplyblock_core.models.storage_node.RPCClient")
    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_local_jm_device_is_not_reattached(
            self, MockDBCtrl, MockRPC, mock_connect_device, _mock_sleep):
        """this_node's own JM must be skipped — it's already local, attaching
        to itself would be a bug.
        """
        import simplyblock_core.storage_node_ops as ops

        this_node = _node("this", mgmt_ip="10.0.0.1", jm_vuid=8612,
                          jm_device=_jm_device("jm-self", "this"))
        peer = _node("peer", mgmt_ip="10.0.0.2", jm_vuid=0,
                     jm_device=_jm_device("jm-peer", "peer"))

        mock_db, mock_rpc = self._patch_stack(this_node, [this_node, peer])
        MockDBCtrl.return_value = mock_db
        MockRPC.return_value = mock_rpc
        mock_connect_device.side_effect = lambda name, *a, **kw: f"{name}n1"

        result = ops._connect_to_remote_jm_devs(
            this_node, jm_ids=["jm-self", "jm-peer"])

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].uuid, "jm-peer")

    @patch("simplyblock_core.storage_node_ops.time.sleep", return_value=None)
    @patch("simplyblock_core.storage_node_ops.connect_device")
    @patch("simplyblock_core.models.storage_node.RPCClient")
    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_offline_peer_jm_is_skipped(
            self, MockDBCtrl, MockRPC, mock_connect_device, _mock_sleep):
        """A peer whose node status is not in {ONLINE, DOWN, RESTARTING} must
        be skipped — that's an intentional filter unrelated to the quorum gate.
        """
        import simplyblock_core.storage_node_ops as ops

        this_node = _node("this", mgmt_ip="10.0.0.1", jm_vuid=8612)
        healthy = _node("healthy", mgmt_ip="10.0.0.2",
                        jm_device=_jm_device("jm-healthy", "healthy"))
        # "removed" is not in the allowed_node_statuses list
        removed = _node("removed", mgmt_ip="10.0.0.3",
                        status="removed",
                        jm_device=_jm_device("jm-removed", "removed"))

        mock_db, mock_rpc = self._patch_stack(
            this_node, [this_node, healthy, removed])
        MockDBCtrl.return_value = mock_db
        MockRPC.return_value = mock_rpc
        mock_connect_device.side_effect = lambda name, *a, **kw: f"{name}n1"

        result = ops._connect_to_remote_jm_devs(
            this_node, jm_ids=["jm-healthy", "jm-removed"])

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].uuid, "jm-healthy")


# ===========================================================================
# 2. get_secondary_nodes_2 — host-disjointness from primary AND secondary
# ===========================================================================

class TestTertiaryHostDisjointness(unittest.TestCase):
    """Task 10: tertiary must be picked on a different physical host from
    both the primary and the already-picked first secondary. Otherwise a
    single host outage would take out two of four HA journal members and
    violate the cluster's fault-tolerance guarantee.
    """

    def _setup(self, nodes):
        mock_db = MagicMock()
        mock_db.get_storage_nodes_by_cluster_id.return_value = nodes
        return mock_db

    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_excludes_secondary_host_sibling(self, MockDBCtrl):
        """When the secondary sits on host B, host B's *other* storage node
        (same mgmt_ip, different id) must not be a tertiary candidate.
        """
        import simplyblock_core.storage_node_ops as ops

        # 3 physical hosts × 2 storage nodes each
        primary   = _node("p",     mgmt_ip="10.0.0.1")
        p_sibling = _node("p_sib", mgmt_ip="10.0.0.1")
        sec       = _node("s",     mgmt_ip="10.0.0.2")
        s_sibling = _node("s_sib", mgmt_ip="10.0.0.2")
        node_c1   = _node("c1",    mgmt_ip="10.0.0.3")
        node_c2   = _node("c2",    mgmt_ip="10.0.0.3")

        MockDBCtrl.return_value = self._setup(
            [primary, p_sibling, sec, s_sibling, node_c1, node_c2])

        candidates = ops.get_secondary_nodes_2(
            primary,
            exclude_ids=["s"],
            exclude_mgmt_ips=[sec.mgmt_ip],
        )

        ids = set(candidates)
        self.assertNotIn("p_sib", ids, "primary's host sibling must be excluded")
        self.assertNotIn("s_sib", ids, "secondary's host sibling must be excluded")
        self.assertNotIn("s", ids, "secondary itself must be excluded")
        self.assertTrue(ids.issubset({"c1", "c2"}),
                        f"only host C's nodes are valid; got {ids}")
        self.assertTrue(len(ids) >= 1, "at least one host-C candidate must remain")

    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_excludes_primary_host(self, MockDBCtrl):
        """Pre-existing behavior: tertiary must not share the primary's host.
        Kept as a regression test so the new exclude_mgmt_ips logic doesn't
        accidentally drop the current_node filter.
        """
        import simplyblock_core.storage_node_ops as ops

        primary   = _node("p",     mgmt_ip="10.0.0.1")
        p_sibling = _node("p_sib", mgmt_ip="10.0.0.1")
        sec       = _node("s",     mgmt_ip="10.0.0.2")
        node_c    = _node("c",     mgmt_ip="10.0.0.3")

        MockDBCtrl.return_value = self._setup([primary, p_sibling, sec, node_c])

        candidates = ops.get_secondary_nodes_2(
            primary,
            exclude_ids=["s"],
            exclude_mgmt_ips=[sec.mgmt_ip],
        )
        self.assertNotIn("p_sib", candidates,
                         "primary host must be excluded via current_node.mgmt_ip")

    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_no_host_c_raises_no_candidate(self, MockDBCtrl):
        """If every remaining node sits on primary's or secondary's host, the
        function must return an empty list. Caller in cluster_ops.py then
        raises 'not enough nodes for dual fault tolerance', matching the
        hard-constraint requirement.
        """
        import simplyblock_core.storage_node_ops as ops

        # 2 hosts only, 2 nodes each — no host C exists
        primary   = _node("p",     mgmt_ip="10.0.0.1")
        p_sibling = _node("p_sib", mgmt_ip="10.0.0.1")
        sec       = _node("s",     mgmt_ip="10.0.0.2")
        s_sibling = _node("s_sib", mgmt_ip="10.0.0.2")

        MockDBCtrl.return_value = self._setup(
            [primary, p_sibling, sec, s_sibling])

        candidates = ops.get_secondary_nodes_2(
            primary,
            exclude_ids=["s"],
            exclude_mgmt_ips=[sec.mgmt_ip],
        )
        # Note: the len==2 fast-path does not apply here (4 nodes).
        self.assertEqual(candidates, [],
            "no host-disjoint candidate must be returned when none exists")

    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_backward_compat_without_exclude_mgmt_ips(self, MockDBCtrl):
        """Calls that omit exclude_mgmt_ips keep the old semantics (only the
        primary's host is excluded; secondary's host sibling is eligible).
        This is the behavior the bug-fix removes when exclude_mgmt_ips is
        passed; without it, the old (buggy) behavior is preserved so internal
        callers not yet updated don't break.
        """
        import simplyblock_core.storage_node_ops as ops

        primary = _node("p", mgmt_ip="10.0.0.1")
        sec     = _node("s", mgmt_ip="10.0.0.2")
        s_sib   = _node("s_sib", mgmt_ip="10.0.0.2")  # same host as sec
        node_c  = _node("c", mgmt_ip="10.0.0.3")

        MockDBCtrl.return_value = self._setup([primary, sec, s_sib, node_c])

        # Function returns the first eligible node after the primary — in
        # iteration order that is s_sib. Without exclude_mgmt_ips, s_sib is
        # NOT filtered; that proves the new filter only activates when the
        # caller opts in.
        candidates = ops.get_secondary_nodes_2(primary, exclude_ids=["s"])
        self.assertIn("s_sib", candidates,
            "without exclude_mgmt_ips, secondary's host sibling must be eligible "
            "(pre-fix behavior preserved for unmigrated callers)")

        # Same setup, but with the new exclude_mgmt_ips parameter — s_sib
        # must now be filtered out, leaving only host-C candidate(s).
        candidates = ops.get_secondary_nodes_2(
            primary, exclude_ids=["s"], exclude_mgmt_ips=[sec.mgmt_ip])
        self.assertNotIn("s_sib", candidates)
        self.assertIn("c", candidates)

    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_only_online_candidates_returned(self, MockDBCtrl):
        """Offline nodes are never tertiary candidates regardless of host."""
        import simplyblock_core.storage_node_ops as ops

        primary = _node("p", mgmt_ip="10.0.0.1")
        sec     = _node("s", mgmt_ip="10.0.0.2")
        off     = _node("off", mgmt_ip="10.0.0.3",
                        status=StorageNode.STATUS_OFFLINE)
        online  = _node("on", mgmt_ip="10.0.0.4")

        MockDBCtrl.return_value = self._setup([primary, sec, off, online])

        candidates = ops.get_secondary_nodes_2(
            primary, exclude_ids=["s"], exclude_mgmt_ips=[sec.mgmt_ip])
        self.assertIn("on", candidates)
        self.assertNotIn("off", candidates)


# ===========================================================================
# 3. NVMe attach-controller timeout caps
# ===========================================================================

class TestAttachControllerTimeoutCap(unittest.TestCase):
    """Task 12: bdev_nvme_attach_controller must always be called through an
    RPC client with timeout <= 1s and retry=0. A reachable SPDK peer replies
    in microseconds; a longer wait is an unreachable path we want to fail
    fast on so per-peer iteration stays bounded.
    """

    @patch("simplyblock_core.storage_node_ops.time.sleep", return_value=None)
    @patch("simplyblock_core.models.storage_node.RPCClient")
    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_connect_device_caps_attach_timeout_at_1s(
            self, MockDBCtrl, MockRPC, _sleep):
        """connect_device must build its attach RPC client with timeout<=1."""
        import simplyblock_core.storage_node_ops as ops

        node = _node("n", mgmt_ip="10.0.0.1")
        device = NVMeDevice()
        device.uuid = "dev-1"
        device.alceml_name = "alceml-1"
        device.nvmf_ip = "10.0.0.2"
        device.nvmf_port = 4420
        device.nvmf_nqn = "nqn.test"
        device.node_id = "other-node"
        device.nvmf_multipath = False

        # node.rpc_client() returns a mock; bdev_nvme_controller_list -> None
        # to force the attach path.
        node_rpc = MagicMock()
        node_rpc.bdev_nvme_controller_list.return_value = None
        node_rpc.get_bdevs.return_value = [{"name": "remote-fake-n1"}]
        def _rpc_client(*_args, **kwargs):
            if "timeout" in kwargs:
                return attach_rpc
            return node_rpc

        node.rpc_client = MagicMock(side_effect=_rpc_client)

        attach_rpc = MagicMock()
        attach_rpc.bdev_nvme_attach_controller.return_value = ["remote-fake-n1"]

        mock_db = MagicMock()
        mock_db.get_storage_node_by_id.return_value = node
        MockDBCtrl.return_value = mock_db

        # No caller-specified timeout: should default-cap at 1.
        ops.connect_device("remote-fake", device, node,
                           bdev_names=[], reattach=False)
        timeouts_used = [c.kwargs.get("timeout") for c in node.rpc_client.call_args_list
                         if "timeout" in c.kwargs]
        self.assertTrue(timeouts_used,
                        "a short-timeout attach RPC client must be built")
        self.assertLessEqual(max(timeouts_used), 1,
            f"attach RPC timeout must be <= 1s; got {timeouts_used!r}")

        # Caller passes 5 — must be clamped to 1.
        node.rpc_client.reset_mock()
        ops.connect_device("remote-fake", device, node,
                           bdev_names=[], reattach=False, attach_timeout=5)
        timeouts_used = [c.kwargs.get("timeout") for c in node.rpc_client.call_args_list
                         if "timeout" in c.kwargs]
        self.assertLessEqual(max(timeouts_used), 1,
            "excessive attach_timeout must be clamped to 1s")

        # Caller passes 0.3 — must be kept (lower than cap).
        node.rpc_client.reset_mock()
        ops.connect_device("remote-fake", device, node,
                           bdev_names=[], reattach=False, attach_timeout=0.3)
        timeouts_used = [c.kwargs.get("timeout") for c in node.rpc_client.call_args_list
                         if "timeout" in c.kwargs]
        self.assertIn(0.3, timeouts_used,
            f"caller-supplied sub-cap timeout must be preserved; got {timeouts_used!r}")

    # The former test_connect_to_hublvol_caps_attach_timeout_at_1s pinned a
    # per-call RPC timeout clamp that connect_to_hublvol applied around its
    # own bdev_nvme_attach_controller invocation. That wrapper was removed
    # when hublvol (re)attach was routed through
    # HublvolReconnectCoordinator: SPDK's ctrlr_loss_timeout_sec /
    # reconnect_delay_sec / fast_io_fail_timeout_sec now control reconnect
    # behaviour server-side, which makes a separate client-side RPC timeout
    # redundant and lets a short peer blip recover via reset instead of
    # destroy-and-reattach. The SPDK-side timeouts are pinned in
    # tests/test_hublvol_reconnect_coordinator.py::
    # NoExistingController::test_attach_passes_hublvol_ctrlr_timeouts.


# ===========================================================================
# 4. bdev_nvme_set_options retry counts (uniform, NIC-count-independent)
# ===========================================================================

class TestBdevNvmeSetOptionsRetries(unittest.TestCase):
    """``bdev_retry_count`` must be non-zero so SPDK's bdev_nvme retries an
    aborted IO on the alternate path of an NVMe-oF multipath bdev (see
    SPDK NVMe multipath docs). Hublvol bdevs are multipath whenever an
    FTT≥1 cluster exists, regardless of how many local data NICs the
    node has — so the retries are now applied unconditionally instead
    of being gated on ``len(data_nics) > 1``.

    Worst-case retry budget:
        (1 + BDEV_RETRY) * (1 + TRANSPORT_RETRY) = 3 * 2 = 6
    transport submissions per failing IO before EIO bubbles to the caller.
    """

    def test_retries_are_unconditional_and_nonzero(self):
        from simplyblock_core.rpc_client import RPCClient
        from simplyblock_core import constants

        self.assertEqual(constants.BDEV_RETRY, 2)
        self.assertEqual(constants.TRANSPORT_RETRY, 1)

        client = RPCClient.__new__(RPCClient)
        captured = {}

        def _fake_request(method, params=None):
            captured[method] = params
            return True

        client._request = _fake_request

        client.bdev_nvme_set_options()
        self.assertEqual(captured["bdev_nvme_set_options"]["bdev_retry_count"], 2)
        self.assertEqual(captured["bdev_nvme_set_options"]["transport_retry_count"], 1)


if __name__ == "__main__":
    unittest.main()
