# coding=utf-8
"""
test_failure_domain.py – unit tests for the failure-domain feature.

Failure domains are an operator-defined, deploy-time-only grouping of storage
nodes (rack/cabinet/DC), identified by a 32-bit integer id (default -1 = unset;
a value >= 0 activates the feature for the node). When the cluster has
enable_failure_domain set, every node carries a failure_domain id and placement
spreads data/parity chunks, HA journal copies and secondary/tertiary nodes
across distinct domains, with a best-effort fallback to host-disjoint placement.

The data plane consumes the id at node level inside the distrib cluster map
(map_cluster[node]["failure_domain"]) via distr_send_cluster_map /
distr_add_nodes.

Tests cover:
  - model defaults (Cluster / StorageNode)
  - get_secondary_nodes domain-disjoint preference + fallback (incl. domain 0)
  - get_secondary_nodes_2 domain-disjoint preference (incl. secondary's
    domain) + fallback
  - get_sorted_ha_jms domain-disjoint preference + fallback + host invariant
  - get_distr_cluster_map emits the node-level int only when enabled

All external dependencies (FDB, RPC) are mocked.
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.models.nvme_device import NVMeDevice, JMDevice


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _cluster(enable_failure_domain=False, distr_npcs=2, cluster_id="cluster-1"):
    c = Cluster()
    c.uuid = cluster_id
    c.ha_type = "ha"
    c.distr_ndcs = 1
    c.distr_npcs = distr_npcs
    c.enable_failure_domain = enable_failure_domain
    return c


def _node(uuid, mgmt_ip, failure_domain=-1, status=StorageNode.STATUS_ONLINE,
          cluster_id="cluster-1", is_secondary_node=False,
          lvstore_stack_secondary="", lvstore_stack_tertiary="",
          jm_device=None, jm_ids=None, ha_jm_count=3, physical_label=0):
    n = StorageNode()
    n.uuid = uuid
    n.status = status
    n.cluster_id = cluster_id
    n.mgmt_ip = mgmt_ip
    n.failure_domain = failure_domain
    n.is_secondary_node = is_secondary_node
    n.lvstore_stack_secondary = lvstore_stack_secondary
    n.lvstore_stack_tertiary = lvstore_stack_tertiary
    n.jm_device = jm_device
    n.jm_ids = jm_ids or []
    n.ha_jm_count = ha_jm_count
    n.physical_label = physical_label
    return n


def _jm(uuid):
    j = JMDevice()
    j.uuid = uuid
    j.status = JMDevice.STATUS_ONLINE
    j.jm_bdev = f"jm_{uuid}"
    return j


def _dev(status=NVMeDevice.STATUS_ONLINE):
    d = NVMeDevice()
    d.status = status
    return d


# ===========================================================================
# 1. Model defaults
# ===========================================================================

class TestModelDefaults(unittest.TestCase):

    def test_cluster_default_disabled(self):
        assert Cluster().enable_failure_domain is False

    def test_storage_node_default_unset(self):
        assert StorageNode().failure_domain == -1

    def test_values_round_trip(self):
        c = _cluster(enable_failure_domain=True)
        assert c.to_dict()["enable_failure_domain"] is True
        n = _node("n1", "10.0.0.1", failure_domain=0)
        assert n.to_dict()["failure_domain"] == 0


# ===========================================================================
# 2. get_secondary_nodes
# ===========================================================================

class TestGetSecondaryNodes(unittest.TestCase):

    def _mock_db(self, cluster, nodes):
        mock_db = MagicMock()
        mock_db.get_cluster_by_id.return_value = cluster
        mock_db.get_storage_nodes_by_cluster_id.return_value = nodes
        return mock_db

    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_disabled_ignores_failure_domain(self, MockDBCtrl):
        import simplyblock_core.storage_node_ops as ops
        primary = _node("primary", "10.0.0.1", failure_domain=0)
        # Same domain as primary; with the feature OFF this is still eligible.
        sec = _node("sec-1", "10.0.0.2", failure_domain=0, is_secondary_node=True)
        MockDBCtrl.return_value = self._mock_db(_cluster(False), [primary, sec])

        result = ops.get_secondary_nodes(primary)
        assert "sec-1" in result

    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_enabled_prefers_other_domain(self, MockDBCtrl):
        import simplyblock_core.storage_node_ops as ops
        # Domain 0 must behave like any other id (regression guard against
        # truthiness checks that would treat 0 as "unset").
        primary = _node("primary", "10.0.0.1", failure_domain=0)
        same = _node("same", "10.0.0.2", failure_domain=0, is_secondary_node=True)
        other = _node("other", "10.0.0.3", failure_domain=1, is_secondary_node=True)
        MockDBCtrl.return_value = self._mock_db(_cluster(True), [primary, same, other])

        result = ops.get_secondary_nodes(primary)
        assert "other" in result
        assert "same" not in result

    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_enabled_falls_back_when_no_other_domain(self, MockDBCtrl):
        import simplyblock_core.storage_node_ops as ops
        # Three nodes (avoids the 2-node early-return shortcut), all in the
        # same domain as the primary, so no domain-disjoint candidate exists.
        primary = _node("primary", "10.0.0.1", failure_domain=0)
        cand1 = _node("cand1", "10.0.0.2", failure_domain=0)
        cand2 = _node("cand2", "10.0.0.3", failure_domain=0)
        MockDBCtrl.return_value = self._mock_db(_cluster(True), [primary, cand1, cand2])

        with self.assertLogs("root.simplyblock_core.storage_node_ops", level="WARNING") as cm:
            result = ops.get_secondary_nodes(primary)
        assert result  # fallback offers a same-domain (host-disjoint) candidate
        assert any("falling back" in m for m in cm.output)


# ===========================================================================
# 3. get_secondary_nodes_2 (tertiary)
# ===========================================================================

class TestGetSecondaryNodes2(unittest.TestCase):

    def _mock_db(self, cluster, nodes):
        mock_db = MagicMock()
        mock_db.get_cluster_by_id.return_value = cluster
        mock_db.get_storage_nodes_by_cluster_id.return_value = nodes
        return mock_db

    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_excludes_primary_and_secondary_domains(self, MockDBCtrl):
        import simplyblock_core.storage_node_ops as ops
        primary = _node("primary", "10.0.0.1", failure_domain=0)
        # secondary is domain 1; caller passes it via exclude_failure_domains
        in_sec_domain = _node("c1", "10.0.0.3", failure_domain=1)
        in_prim_domain = _node("c2", "10.0.0.4", failure_domain=0)
        fresh = _node("c3", "10.0.0.5", failure_domain=2)
        MockDBCtrl.return_value = self._mock_db(
            _cluster(True), [primary, in_sec_domain, in_prim_domain, fresh])

        result = ops.get_secondary_nodes_2(
            primary, exclude_mgmt_ips=["10.0.0.2"], exclude_failure_domains=[1])
        assert "c3" in result
        assert "c1" not in result  # secondary's domain
        assert "c2" not in result  # primary's domain

    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_falls_back_when_only_shared_domains(self, MockDBCtrl):
        import simplyblock_core.storage_node_ops as ops
        # Three nodes (avoids the 2-node early-return shortcut). Every candidate
        # is in either the primary's (0) or the secondary's (1) domain, so the
        # domain-disjoint pass finds nothing and falls back.
        primary = _node("primary", "10.0.0.1", failure_domain=0)
        n_b = _node("n_b", "10.0.0.4", failure_domain=1)
        n_a = _node("n_a", "10.0.0.5", failure_domain=0)
        MockDBCtrl.return_value = self._mock_db(_cluster(True), [primary, n_b, n_a])

        with self.assertLogs("root.simplyblock_core.storage_node_ops", level="WARNING") as cm:
            result = ops.get_secondary_nodes_2(
                primary, exclude_mgmt_ips=["10.0.0.2"], exclude_failure_domains=[1])
        assert result
        assert any("falling back" in m for m in cm.output)


# ===========================================================================
# 4. get_sorted_ha_jms
# ===========================================================================

class TestGetSortedHaJms(unittest.TestCase):

    def _mock_db(self, cluster, nodes):
        mock_db = MagicMock()
        mock_db.get_cluster_by_id.return_value = cluster
        mock_db.get_storage_nodes_by_cluster_id.return_value = nodes
        return mock_db

    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_prefers_distinct_domains(self, MockDBCtrl):
        import simplyblock_core.storage_node_ops as ops
        current = _node("current", "10.0.0.1", failure_domain=0, ha_jm_count=3)
        b = _node("b", "10.0.0.2", failure_domain=1, jm_device=_jm("jb"))
        c = _node("c", "10.0.0.3", failure_domain=2, jm_device=_jm("jc"))
        d = _node("d", "10.0.0.4", failure_domain=1, jm_device=_jm("jd"))
        MockDBCtrl.return_value = self._mock_db(_cluster(True), [current, b, c, d])

        result = ops.get_sorted_ha_jms(current)
        # target = ha_jm_count - 1 = 2; should pick one per distinct domain.
        assert result == ["jb", "jc"]

    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_falls_back_to_host_disjoint(self, MockDBCtrl):
        import simplyblock_core.storage_node_ops as ops
        current = _node("current", "10.0.0.1", failure_domain=0, ha_jm_count=3)
        # current's own domain (0) is skipped on the first pass; only one other
        # domain (1) exists, so the second copy needs the fallback.
        b = _node("b", "10.0.0.2", failure_domain=0, jm_device=_jm("jb"))
        c = _node("c", "10.0.0.3", failure_domain=1, jm_device=_jm("jc"))
        MockDBCtrl.return_value = self._mock_db(_cluster(True), [current, b, c])

        with self.assertLogs("root.simplyblock_core.storage_node_ops", level="WARNING") as cm:
            result = ops.get_sorted_ha_jms(current)
        assert set(result) == {"jb", "jc"}
        assert len(result) == 2
        assert any("failure" in m for m in cm.output)

    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_never_two_copies_on_same_host(self, MockDBCtrl):
        import simplyblock_core.storage_node_ops as ops
        current = _node("current", "10.0.0.1", failure_domain=0, ha_jm_count=3)
        # Two JMs on the SAME host (mgmt_ip) but different domains — host
        # disjointness is a hard invariant, so only one may be selected.
        b = _node("b", "10.0.0.2", failure_domain=1, jm_device=_jm("jb"))
        c = _node("c", "10.0.0.2", failure_domain=2, jm_device=_jm("jc"))
        MockDBCtrl.return_value = self._mock_db(_cluster(True), [current, b, c])

        result = ops.get_sorted_ha_jms(current)
        assert len(result) == 1


# ===========================================================================
# 5. get_distr_cluster_map
# ===========================================================================

class TestDistrClusterMap(unittest.TestCase):

    def _target_node(self, failure_domain=0):
        node = _node("tnode", "10.0.0.1", failure_domain=failure_domain)
        dev = NVMeDevice()
        dev.uuid = "dev-1"
        dev.status = NVMeDevice.STATUS_ONLINE
        dev.cluster_device_order = 0
        dev.alceml_bdev = "alceml_0"
        dev.size = 1073741824  # 1 GiB
        dev.physical_label = 1
        node.nvme_devices = [dev]
        node.remote_devices = []
        return node

    @patch("simplyblock_core.distr_controller.DBController")
    def test_emits_node_level_int_when_enabled(self, MockDBCtrl):
        import simplyblock_core.distr_controller as dc
        node = self._target_node(failure_domain=0)  # domain 0 must be emitted
        mock_db = MagicMock()
        mock_db.get_cluster_by_id.return_value = _cluster(True)
        MockDBCtrl.return_value = mock_db

        cl_map = dc.get_distr_cluster_map([node], node)
        node_entry = cl_map["map_cluster"]["tnode"]
        assert node_entry["failure_domain"] == 0
        # The tag is node-level, not inside the per-device entries.
        assert "failure_domain" not in node_entry["devices"][0]

    @patch("simplyblock_core.distr_controller.DBController")
    def test_omits_tag_when_disabled(self, MockDBCtrl):
        import simplyblock_core.distr_controller as dc
        node = self._target_node(failure_domain=0)
        mock_db = MagicMock()
        mock_db.get_cluster_by_id.return_value = _cluster(False)
        MockDBCtrl.return_value = mock_db

        cl_map = dc.get_distr_cluster_map([node], node)
        assert "failure_domain" not in cl_map["map_cluster"]["tnode"]

    @patch("simplyblock_core.distr_controller.DBController")
    def test_parse_node_line_ignores_failure_domain_suffix(self, MockDBCtrl):
        """Regression: the data plane appends a trailing 'failure_domain=N' to
        node lines in the distr cluster map. The node-status regex must capture
        ONLY the status token, not greedily swallow the suffix — otherwise the
        parsed status ('online  failure_domain=0') never equals the DB's
        'online' and Health flips False cluster-wide (incident 2026-06-25)."""
        import simplyblock_core.distr_controller as dc

        snode = _node("n1", "10.0.0.1", failure_domain=0)
        snode.status = StorageNode.STATUS_ONLINE
        nodes = {"n1": snode}
        # Non-empty so parse_distr_cluster_map does not fall back to a DB
        # rebuild (it does that only when nodes OR devices is empty).
        devices = {"ignored": MagicMock()}
        # New data-plane format carries the failure_domain suffix.
        map_string = "uuid_node=n1  status=online  failure_domain=0"
        results, passed = dc.parse_distr_cluster_map(map_string, nodes, devices)
        assert passed is True, results
        assert results[0]["Found Status"] == "online"

        # Legacy format (no suffix) must still parse.
        results, passed = dc.parse_distr_cluster_map(
            "uuid_node=n1  status=online", nodes, devices)
        assert passed is True, results

        # A genuine status mismatch must still be detected.
        snode.status = StorageNode.STATUS_OFFLINE
        _, passed = dc.parse_distr_cluster_map(map_string, nodes, devices)
        assert passed is False


# ===========================================================================
# 5. FD-aware cluster suspend criteria (storage_node_monitor)
#
# Contract for clusters created with --enable-failure-domain:
#   (A) losing a whole failure domain (any nodes/devices) -> DEGRADED, never
#       SUSPENDED;
#   (B) a whole domain + one extra node on one other domain -> DEGRADED, but
#       only when FTT (distr_npcs) == 2 AND there are >= ndcs domains;
#   anything broader -> SUSPENDED.
# With --enable-failure-domain off, the flat per-node logic is unchanged.
# Failure-domain ids are 32-bit ints (-1 = unset, >= 0 a real domain; 0 valid).
# ===========================================================================
class TestFDAwareClusterStatus(unittest.TestCase):

    def setUp(self):
        with patch("simplyblock_core.db_controller.DBController"):
            from simplyblock_core.services import storage_node_monitor as snm
        self.snm = snm
        self._patches = [
            patch.object(snm, "is_new_migrated_node", return_value=False),
        ]
        for p in self._patches:
            p.start()

    def tearDown(self):
        for p in self._patches:
            p.stop()

    def _fd_cluster(self, enable=True, npcs=2, ndcs=2):
        c = _cluster(enable_failure_domain=enable, distr_npcs=npcs)
        c.distr_ndcs = ndcs
        return c

    def _on(self, uuid, ip, fd):
        n = _node(uuid, ip, failure_domain=fd, status=StorageNode.STATUS_ONLINE)
        n.nvme_devices = [_dev(NVMeDevice.STATUS_ONLINE)]
        # online nodes hit rpc_client for the JM-replication probe; stub it to
        # report "no lvstore" so the probe is skipped (no jm_replication_tasks).
        n.rpc_client = MagicMock()
        n.rpc_client.return_value.bdev_lvol_get_lvstores.return_value = []
        return n

    def _off(self, uuid, ip, fd):
        # An abrupt host loss (host_reboot): node flips OFFLINE while its
        # devices are still flagged ONLINE in the DB.
        n = _node(uuid, ip, failure_domain=fd, status=StorageNode.STATUS_OFFLINE)
        n.nvme_devices = [_dev(NVMeDevice.STATUS_ONLINE)]
        return n

    def _status(self, cluster, nodes):
        mock_db = MagicMock()
        mock_db.get_cluster_by_id.return_value = cluster
        mock_db.get_primary_storage_nodes_by_cluster_id.return_value = nodes
        self.snm.db = mock_db
        return self.snm.get_next_cluster_status(cluster.get_id())

    def _two_domain_nodes(self, off_a=0, off_b=0):
        """3 nodes in domain 0, 3 in domain 1; first off_a / off_b offline."""
        nodes = []
        for i in range(3):
            mk = self._off if i < off_a else self._on
            nodes.append(mk(f"a{i}", f"10.0.0.{i}", 0))
        for i in range(3):
            mk = self._off if i < off_b else self._on
            nodes.append(mk(f"b{i}", f"10.0.1.{i}", 1))
        return nodes

    # --- (A) whole-domain loss is tolerated ---------------------------------
    def test_whole_domain_reboot_degraded_not_suspended(self):
        # The exact soak scenario: reboot all 3 nodes of domain 0.
        cluster = self._fd_cluster(npcs=2, ndcs=2)
        status = self._status(cluster, self._two_domain_nodes(off_a=3))
        assert status == Cluster.STATUS_DEGRADED

    def test_whole_domain_tolerated_even_with_npcs1(self):
        # Case (A) is unconditional — single-domain loss never suspends.
        cluster = self._fd_cluster(npcs=1, ndcs=2)
        status = self._status(cluster, self._two_domain_nodes(off_a=3))
        assert status == Cluster.STATUS_DEGRADED

    # --- (B) whole domain + one extra node ----------------------------------
    def test_whole_domain_plus_one_node_degraded(self):
        cluster = self._fd_cluster(npcs=2, ndcs=2)
        status = self._status(cluster, self._two_domain_nodes(off_a=3, off_b=1))
        assert status == Cluster.STATUS_DEGRADED

    def test_one_per_domain_degraded(self):
        cluster = self._fd_cluster(npcs=2, ndcs=2)
        status = self._status(cluster, self._two_domain_nodes(off_a=1, off_b=1))
        assert status == Cluster.STATUS_DEGRADED

    # --- beyond the contract -> SUSPENDED -----------------------------------
    def test_whole_domain_plus_two_nodes_suspended(self):
        cluster = self._fd_cluster(npcs=2, ndcs=2)
        status = self._status(cluster, self._two_domain_nodes(off_a=3, off_b=2))
        assert status == Cluster.STATUS_SUSPENDED

    def test_case_b_blocked_when_npcs_not_two(self):
        # FTT != 2: the "+1 node on another domain" allowance does not apply.
        cluster = self._fd_cluster(npcs=1, ndcs=2)
        status = self._status(cluster, self._two_domain_nodes(off_a=3, off_b=1))
        assert status == Cluster.STATUS_SUSPENDED

    # --- no damage / disabled / single-domain layouts -----------------------
    def test_all_online_active(self):
        cluster = self._fd_cluster(npcs=2, ndcs=2)
        status = self._status(cluster, self._two_domain_nodes())
        assert status == Cluster.STATUS_ACTIVE

    def test_disabled_falls_back_to_legacy_suspend(self):
        # enable_failure_domain off: 3 offline nodes (> npcs) -> legacy SUSPEND.
        cluster = self._fd_cluster(enable=False, npcs=2, ndcs=2)
        status = self._status(cluster, self._two_domain_nodes(off_a=3))
        assert status == Cluster.STATUS_SUSPENDED

    def test_single_domain_layout_falls_back_to_legacy(self):
        # FD enabled but every node tagged the same domain: not a usable FD
        # layout, so the helper defers and the legacy logic suspends.
        cluster = self._fd_cluster(npcs=2, ndcs=2)
        nodes = [self._off(f"a{i}", f"10.0.0.{i}", 0) for i in range(3)]
        nodes += [self._on(f"b{i}", f"10.0.1.{i}", 0) for i in range(3)]
        status = self._status(cluster, nodes)
        assert status == Cluster.STATUS_SUSPENDED


# ===========================================================================
# 6. HA-JM count requirement (req #4: FTT=1 + failure domains needs 4 JMs)
# ===========================================================================
class TestRequiredHaJmCount(unittest.TestCase):

    def setUp(self):
        import simplyblock_core.storage_node_ops as ops
        self.ops = ops

    def _cl(self, ftt, fd):
        c = Cluster()
        c.max_fault_tolerance = ftt
        c.enable_failure_domain = fd
        return c

    def test_ftt2_always_four(self):
        assert self.ops.get_required_ha_jm_count(self._cl(2, False)) == 4
        assert self.ops.get_required_ha_jm_count(self._cl(2, True)) == 4

    def test_ftt1_no_fd_three(self):
        assert self.ops.get_required_ha_jm_count(self._cl(1, False)) == 3

    def test_ftt1_with_fd_four(self):
        # The reverse-quorum fix: 3 journals across 2 domains can lose 2-of-3.
        assert self.ops.get_required_ha_jm_count(self._cl(1, True)) == 4


# ===========================================================================
# 7. HA-JM domain balance (req #2: <=2 JMs per domain for 2 domains; spread)
# ===========================================================================
class TestJmDomainBalance(unittest.TestCase):

    def _mock_db(self, cluster, nodes):
        mock_db = MagicMock()
        mock_db.get_cluster_by_id.return_value = cluster
        mock_db.get_storage_nodes_by_cluster_id.return_value = nodes
        return mock_db

    def _fd_of(self, nodes):
        return {n.jm_device.get_id(): n.failure_domain for n in nodes if n.jm_device}

    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_two_domains_four_jms_split_2_2(self, MockDBCtrl):
        import simplyblock_core.storage_node_ops as ops
        # current is in fd0 (its local JM occupies fd0). 4-JM set => at most 2
        # per domain, so exactly one more in fd0 and two in fd1.
        current = _node("current", "10.0.0.1", failure_domain=0, ha_jm_count=4)
        nodes = [current,
                 _node("a", "10.0.0.2", failure_domain=0, jm_device=_jm("ja")),
                 _node("b", "10.0.0.3", failure_domain=0, jm_device=_jm("jb")),
                 _node("c", "10.0.0.4", failure_domain=1, jm_device=_jm("jc")),
                 _node("d", "10.0.0.5", failure_domain=1, jm_device=_jm("jd"))]
        MockDBCtrl.return_value = self._mock_db(_cluster(True), nodes)
        result = ops.get_sorted_ha_jms(current)
        fd_of = self._fd_of(nodes)
        # full journal set = local (fd0) + selected remotes
        fd_tally = {0: 1}  # local
        for jm in result:
            fd_tally[fd_of[jm]] = fd_tally.get(fd_of[jm], 0) + 1
        assert len(result) == 3
        assert max(fd_tally.values()) <= 2, fd_tally
        assert fd_tally == {0: 2, 1: 2}, fd_tally

    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_four_domains_one_per_domain(self, MockDBCtrl):
        import simplyblock_core.storage_node_ops as ops
        current = _node("current", "10.0.0.1", failure_domain=0, ha_jm_count=4)
        nodes = [current,
                 _node("a", "10.0.0.2", failure_domain=1, jm_device=_jm("ja")),
                 _node("b", "10.0.0.3", failure_domain=2, jm_device=_jm("jb")),
                 _node("c", "10.0.0.4", failure_domain=3, jm_device=_jm("jc")),
                 _node("d", "10.0.0.5", failure_domain=1, jm_device=_jm("jd"))]
        MockDBCtrl.return_value = self._mock_db(_cluster(True), nodes)
        result = ops.get_sorted_ha_jms(current)
        fd_of = self._fd_of(nodes)
        chosen_fds = sorted(fd_of[jm] for jm in result)
        # spread across the three other domains, one each
        assert chosen_fds == [1, 2, 3], chosen_fds

    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_no_domain_exceeds_quorum_cap(self, MockDBCtrl):
        import simplyblock_core.storage_node_ops as ops
        # 3 in fd0, 3 in fd1; the OLD code produced 3-1 splits. Verify the new
        # code never lets the lost-domain side exceed 2 (so >=2 JMs survive).
        current = _node("current", "10.0.0.1", failure_domain=1, ha_jm_count=4)
        nodes = [current,
                 _node("a", "10.0.0.2", failure_domain=0, jm_device=_jm("ja")),
                 _node("b", "10.0.0.3", failure_domain=0, jm_device=_jm("jb")),
                 _node("e", "10.0.0.6", failure_domain=0, jm_device=_jm("je")),
                 _node("c", "10.0.0.4", failure_domain=1, jm_device=_jm("jc")),
                 _node("d", "10.0.0.5", failure_domain=1, jm_device=_jm("jd"))]
        MockDBCtrl.return_value = self._mock_db(_cluster(True), nodes)
        result = ops.get_sorted_ha_jms(current)
        fd_of = self._fd_of(nodes)
        fd_tally = {1: 1}  # local in fd1
        for jm in result:
            fd_tally[fd_of[jm]] = fd_tally.get(fd_of[jm], 0) + 1
        assert max(fd_tally.values()) <= 2, fd_tally


# ===========================================================================
# 8. Secondary/tertiary physical-label anti-affinity (req #1)
# ===========================================================================
class TestPlacementPhysicalLabel(unittest.TestCase):

    def _mock_db(self, cluster, nodes):
        mock_db = MagicMock()
        mock_db.get_cluster_by_id.return_value = cluster
        mock_db.get_storage_nodes_by_cluster_id.return_value = nodes
        return mock_db

    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_secondary_prefers_distinct_label(self, MockDBCtrl):
        import simplyblock_core.storage_node_ops as ops
        # FD disabled so only the physical label distinguishes candidates.
        primary = _node("primary", "10.0.0.1", physical_label=5)
        same = _node("same", "10.0.0.2", physical_label=5)        # same label
        other = _node("other", "10.0.0.3", physical_label=7)      # distinct
        MockDBCtrl.return_value = self._mock_db(_cluster(False), [primary, same, other])
        result = ops.get_secondary_nodes(primary)
        assert "other" in result
        assert "same" not in result

    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_secondary_falls_back_when_only_same_label(self, MockDBCtrl):
        import simplyblock_core.storage_node_ops as ops
        primary = _node("primary", "10.0.0.1", physical_label=5)
        c1 = _node("c1", "10.0.0.2", physical_label=5)
        c2 = _node("c2", "10.0.0.3", physical_label=5)
        MockDBCtrl.return_value = self._mock_db(_cluster(False), [primary, c1, c2])
        result = ops.get_secondary_nodes(primary)
        assert result  # best-effort: returns same-label host-disjoint candidates

    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_tertiary_excludes_secondary_label(self, MockDBCtrl):
        import simplyblock_core.storage_node_ops as ops
        primary = _node("primary", "10.0.0.1", physical_label=1)
        # secondary has label 2 (passed via exclude_physical_labels)
        in_sec_label = _node("c1", "10.0.0.3", physical_label=2)
        in_prim_label = _node("c2", "10.0.0.4", physical_label=1)
        fresh = _node("c3", "10.0.0.5", physical_label=3)
        MockDBCtrl.return_value = self._mock_db(
            _cluster(False), [primary, in_sec_label, in_prim_label, fresh])
        result = ops.get_secondary_nodes_2(
            primary, exclude_mgmt_ips=["10.0.0.2"], exclude_physical_labels=[2])
        assert "c3" in result
        assert "c1" not in result  # secondary's label
        assert "c2" not in result  # primary's label


if __name__ == "__main__":
    unittest.main()
