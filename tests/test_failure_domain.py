# coding=utf-8
"""
test_failure_domain.py – unit tests for the failure-domain feature.

Failure domains are an operator-defined, deploy-time-only grouping of storage
nodes (rack/cabinet/DC). When the cluster has enable_failure_domain set, every
node carries a failure_domain tag and placement spreads data/parity chunks,
HA journal copies and secondary/tertiary nodes across distinct domains, with a
best-effort fallback to host-disjoint placement.

Tests cover:
  - model defaults (Cluster / StorageNode / NVMeDevice)
  - get_secondary_nodes domain-disjoint preference + fallback
  - get_secondary_nodes_2 domain-disjoint preference (incl. secondary's
    domain) + fallback
  - get_sorted_ha_jms domain-disjoint preference + fallback
  - get_distr_cluster_map emits the tag only when the feature is enabled
  - bdev_distrib_create RPC parameter injection (provisional name)

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


def _node(uuid, mgmt_ip, failure_domain="", status=StorageNode.STATUS_ONLINE,
          cluster_id="cluster-1", is_secondary_node=False,
          lvstore_stack_secondary="", lvstore_stack_tertiary="",
          jm_device=None, jm_ids=None, ha_jm_count=3):
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
    return n


def _jm(uuid):
    j = JMDevice()
    j.uuid = uuid
    j.status = JMDevice.STATUS_ONLINE
    j.jm_bdev = f"jm_{uuid}"
    return j


# ===========================================================================
# 1. Model defaults
# ===========================================================================

class TestModelDefaults(unittest.TestCase):

    def test_cluster_default_disabled(self):
        assert Cluster().enable_failure_domain is False

    def test_storage_node_default_empty(self):
        assert StorageNode().failure_domain == ""

    def test_nvme_device_default_empty(self):
        assert NVMeDevice().failure_domain == ""

    def test_values_round_trip(self):
        c = _cluster(enable_failure_domain=True)
        assert c.to_dict()["enable_failure_domain"] is True
        n = _node("n1", "10.0.0.1", failure_domain="rack-a")
        assert n.to_dict()["failure_domain"] == "rack-a"


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
        primary = _node("primary", "10.0.0.1", failure_domain="rack-a")
        # Same domain as primary; with the feature OFF this is still eligible.
        sec = _node("sec-1", "10.0.0.2", failure_domain="rack-a", is_secondary_node=True)
        MockDBCtrl.return_value = self._mock_db(_cluster(False), [primary, sec])

        result = ops.get_secondary_nodes(primary)
        assert "sec-1" in result

    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_enabled_prefers_other_domain(self, MockDBCtrl):
        import simplyblock_core.storage_node_ops as ops
        primary = _node("primary", "10.0.0.1", failure_domain="rack-a")
        same = _node("same", "10.0.0.2", failure_domain="rack-a", is_secondary_node=True)
        other = _node("other", "10.0.0.3", failure_domain="rack-b", is_secondary_node=True)
        MockDBCtrl.return_value = self._mock_db(_cluster(True), [primary, same, other])

        result = ops.get_secondary_nodes(primary)
        # Only the different-domain node should be offered.
        assert "other" in result
        assert "same" not in result

    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_enabled_falls_back_when_no_other_domain(self, MockDBCtrl):
        import simplyblock_core.storage_node_ops as ops
        # Three nodes (avoids the 2-node early-return shortcut), all in the
        # same domain as the primary, so no domain-disjoint candidate exists.
        primary = _node("primary", "10.0.0.1", failure_domain="rack-a")
        cand1 = _node("cand1", "10.0.0.2", failure_domain="rack-a")
        cand2 = _node("cand2", "10.0.0.3", failure_domain="rack-a")
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
        primary = _node("primary", "10.0.0.1", failure_domain="rack-a")
        # secondary is rack-b; caller passes it via exclude_failure_domains
        in_sec_domain = _node("c1", "10.0.0.3", failure_domain="rack-b")
        in_prim_domain = _node("c2", "10.0.0.4", failure_domain="rack-a")
        fresh = _node("c3", "10.0.0.5", failure_domain="rack-c")
        MockDBCtrl.return_value = self._mock_db(
            _cluster(True), [primary, in_sec_domain, in_prim_domain, fresh])

        result = ops.get_secondary_nodes_2(
            primary, exclude_mgmt_ips=["10.0.0.2"], exclude_failure_domains=["rack-b"])
        assert "c3" in result
        assert "c1" not in result  # secondary's domain
        assert "c2" not in result  # primary's domain

    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_falls_back_when_only_shared_domains(self, MockDBCtrl):
        import simplyblock_core.storage_node_ops as ops
        # Three nodes (avoids the 2-node early-return shortcut). Every candidate
        # is in either the primary's (rack-a) or the secondary's (rack-b)
        # domain, so the domain-disjoint pass finds nothing and falls back.
        primary = _node("primary", "10.0.0.1", failure_domain="rack-a")
        n_b = _node("n_b", "10.0.0.4", failure_domain="rack-b")
        n_a = _node("n_a", "10.0.0.5", failure_domain="rack-a")
        MockDBCtrl.return_value = self._mock_db(_cluster(True), [primary, n_b, n_a])

        with self.assertLogs("root.simplyblock_core.storage_node_ops", level="WARNING") as cm:
            result = ops.get_secondary_nodes_2(
                primary, exclude_mgmt_ips=["10.0.0.2"], exclude_failure_domains=["rack-b"])
        # Both remaining domains are forbidden, so best-effort fallback offers a
        # host-disjoint candidate anyway, with a warning.
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
        current = _node("current", "10.0.0.1", failure_domain="rack-a", ha_jm_count=3)
        b = _node("b", "10.0.0.2", failure_domain="rack-b", jm_device=_jm("jb"))
        c = _node("c", "10.0.0.3", failure_domain="rack-c", jm_device=_jm("jc"))
        d = _node("d", "10.0.0.4", failure_domain="rack-b", jm_device=_jm("jd"))
        MockDBCtrl.return_value = self._mock_db(_cluster(True), [current, b, c, d])

        result = ops.get_sorted_ha_jms(current)
        # target = ha_jm_count - 1 = 2; should pick one per distinct domain.
        assert result == ["jb", "jc"]

    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_falls_back_to_host_disjoint(self, MockDBCtrl):
        import simplyblock_core.storage_node_ops as ops
        current = _node("current", "10.0.0.1", failure_domain="rack-a", ha_jm_count=3)
        # current's own domain (rack-a) is skipped on the first pass; only one
        # other domain (rack-b) exists, so the second copy needs the fallback.
        b = _node("b", "10.0.0.2", failure_domain="rack-a", jm_device=_jm("jb"))
        c = _node("c", "10.0.0.3", failure_domain="rack-b", jm_device=_jm("jc"))
        MockDBCtrl.return_value = self._mock_db(_cluster(True), [current, b, c])

        with self.assertLogs("root.simplyblock_core.storage_node_ops", level="WARNING") as cm:
            result = ops.get_sorted_ha_jms(current)
        assert set(result) == {"jb", "jc"}
        assert len(result) == 2
        assert any("failure" in m for m in cm.output)

    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_never_two_copies_on_same_host(self, MockDBCtrl):
        import simplyblock_core.storage_node_ops as ops
        current = _node("current", "10.0.0.1", failure_domain="rack-a", ha_jm_count=3)
        # Two JMs on the SAME host (mgmt_ip) but different domains — host
        # disjointness is a hard invariant, so only one may be selected.
        b = _node("b", "10.0.0.2", failure_domain="rack-b", jm_device=_jm("jb"))
        c = _node("c", "10.0.0.2", failure_domain="rack-c", jm_device=_jm("jc"))
        MockDBCtrl.return_value = self._mock_db(_cluster(True), [current, b, c])

        result = ops.get_sorted_ha_jms(current)
        assert len(result) == 1


# ===========================================================================
# 5. get_distr_cluster_map
# ===========================================================================

class TestDistrClusterMap(unittest.TestCase):

    def _target_node(self, failure_domain="rack-a"):
        node = _node("tnode", "10.0.0.1", failure_domain=failure_domain)
        dev = NVMeDevice()
        dev.uuid = "dev-1"
        dev.status = NVMeDevice.STATUS_ONLINE
        dev.cluster_device_order = 0
        dev.alceml_bdev = "alceml_0"
        dev.size = 1073741824  # 1 GiB
        dev.physical_label = 1
        dev.failure_domain = failure_domain
        node.nvme_devices = [dev]
        node.remote_devices = []
        return node

    @patch("simplyblock_core.distr_controller.DBController")
    def test_emits_tag_when_enabled(self, MockDBCtrl):
        import simplyblock_core.distr_controller as dc
        node = self._target_node("rack-a")
        mock_db = MagicMock()
        mock_db.get_cluster_by_id.return_value = _cluster(True)
        MockDBCtrl.return_value = mock_db

        cl_map = dc.get_distr_cluster_map([node], node)
        dev_entry = cl_map["map_cluster"]["tnode"]["devices"][0]
        assert dev_entry["failure_domain"] == "rack-a"

    @patch("simplyblock_core.distr_controller.DBController")
    def test_omits_tag_when_disabled(self, MockDBCtrl):
        import simplyblock_core.distr_controller as dc
        node = self._target_node("rack-a")
        mock_db = MagicMock()
        mock_db.get_cluster_by_id.return_value = _cluster(False)
        MockDBCtrl.return_value = mock_db

        cl_map = dc.get_distr_cluster_map([node], node)
        dev_entry = cl_map["map_cluster"]["tnode"]["devices"][0]
        assert "failure_domain" not in dev_entry


# ===========================================================================
# 6. bdev_distrib_create RPC parameter (PROVISIONAL name)
# ===========================================================================

class TestBdevDistribCreateParam(unittest.TestCase):

    def _client(self):
        from simplyblock_core.rpc_client import RPCClient
        client = RPCClient.__new__(RPCClient)
        client.get_bdevs = MagicMock(side_effect=Exception("not found"))
        captured = {}
        client._request = lambda method, params: captured.setdefault("params", params)
        return client, captured

    def test_param_injected_when_enabled(self):
        client, captured = self._client()
        client.bdev_distrib_create(
            "distrib_1", 1, 1, 1, 10, 4096, ["jm0"], 4096,
            failure_domain_enabled=True)
        assert captured["params"].get("failure_domain_enabled") is True

    def test_param_absent_when_disabled(self):
        client, captured = self._client()
        client.bdev_distrib_create(
            "distrib_1", 1, 1, 1, 10, 4096, ["jm0"], 4096)
        assert "failure_domain_enabled" not in captured["params"]


if __name__ == "__main__":
    unittest.main()
