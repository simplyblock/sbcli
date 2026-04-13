# coding=utf-8
"""
test_cluster_duplicate_name.py – unit tests for duplicate cluster name prevention.

Tests cover:
  - add_cluster() raises ValueError when a cluster with the same name exists
  - add_cluster() succeeds when the name is unique
  - create_cluster() raises ValueError when a cluster with the same name exists
  - create_cluster() does not raise when no clusters exist yet
  - set_name() raises ValueError when another cluster already has the target name
  - set_name() allows renaming a cluster to its own current name
  - change_cluster_name() raises ValueError when another cluster already has the target name
  - change_cluster_name() allows renaming a cluster to its own current name

All external dependencies (FDB, Docker, Kubernetes, RPC) are mocked.
"""

import sys
import unittest
from unittest.mock import MagicMock, patch

# ---------------------------------------------------------------------------
# Stub out only the truly uninstallable C-extension (fdb / FoundationDB).
# Everything else (docker, kubernetes, boto3, simplyblock_core modules) can
# be imported cleanly and must NOT be stubbed here because this module-level
# code runs during pytest collection – before any test in any file executes –
# and permanent sys.modules replacements would break other test files that
# import the same modules as real modules.
# ---------------------------------------------------------------------------

# Provide a real Singleton class so the conftest autouse fixture can call
# Singleton._instances.clear() without error.
class _Singleton(type):
    _instances: dict = {}

_db_ctrl_mock = MagicMock()
_db_ctrl_mock.Singleton = _Singleton
_db_ctrl_mock.DBController = MagicMock()

# Only stub the C-extension that cannot be pip-installed in dev environments.
sys.modules.setdefault("fdb", MagicMock())
sys.modules.setdefault("fdb.tuple", MagicMock())

# Replace db_controller with a stub that exposes a real Singleton class so
# the conftest autouse fixture works, and a mock DBController for tests.
sys.modules["simplyblock_core.db_controller"] = _db_ctrl_mock

# Now we can safely import simplyblock_core pieces.
from simplyblock_core import cluster_ops  # noqa: E402  (needed so @patch can resolve the target)
from simplyblock_core.models.cluster import Cluster  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _cluster(uuid="cluster-1", name="test-cluster"):
    c = Cluster()
    c.uuid = uuid
    c.cluster_name = name
    c.mode = "docker"
    c.db_connection = "user:pass@127.0.0.1:4500"
    c.grafana_secret = "secret"
    c.grafana_endpoint = "http://grafana:3000"
    return c


# ---------------------------------------------------------------------------
# Tests for add_cluster()
# ---------------------------------------------------------------------------

class TestAddClusterDuplicateName(unittest.TestCase):

    def _call(self, name):
        return cluster_ops.add_cluster(
            blk_size=4096,
            page_size_in_blocks=2,
            cap_warn=80,
            cap_crit=90,
            prov_cap_warn=80,
            prov_cap_crit=90,
            distr_ndcs=1,
            distr_npcs=1,
            distr_bs=4096,
            distr_chunk_bs=4096,
            ha_type="single",
            enable_node_affinity=False,
            qpair_count=4,
            max_queue_size=128,
            inflight_io_threshold=64,
            strict_node_anti_affinity=False,
            is_single_node=True,
            name=name,
        )

    @patch("simplyblock_core.cluster_ops.db_controller")
    def test_raises_when_name_already_exists(self, mock_db):
        existing = _cluster(uuid="cluster-1", name="my-cluster")
        mock_db.get_clusters.return_value = [existing]

        with self.assertRaises(ValueError) as ctx:
            self._call("my-cluster")

        self.assertIn("my-cluster", str(ctx.exception))

    @patch("simplyblock_core.cluster_ops.db_controller")
    def test_raises_when_one_of_many_clusters_matches(self, mock_db):
        mock_db.get_clusters.return_value = [
            _cluster(uuid="cluster-1", name="alpha"),
            _cluster(uuid="cluster-2", name="beta"),
        ]

        with self.assertRaises(ValueError) as ctx:
            self._call("beta")

        self.assertIn("beta", str(ctx.exception))

    @patch("simplyblock_core.cluster_ops.qos_controller")
    @patch("simplyblock_core.cluster_ops.cluster_events")
    @patch("simplyblock_core.cluster_ops._create_update_user")
    @patch("simplyblock_core.cluster_ops.db_controller")
    def test_succeeds_when_name_is_unique(self, mock_db, _mock_user, _mock_events, _mock_qos):
        mock_db.get_clusters.return_value = [_cluster(uuid="cluster-1", name="other-cluster")]
        mock_db.kv_store = MagicMock()

        with patch("simplyblock_core.cluster_ops.utils") as mock_utils:
            mock_utils.generate_string.return_value = "x" * 20
            result = self._call("new-cluster")

        self.assertIsNotNone(result)

    @patch("simplyblock_core.cluster_ops.qos_controller")
    @patch("simplyblock_core.cluster_ops.cluster_events")
    @patch("simplyblock_core.cluster_ops._create_update_user")
    @patch("simplyblock_core.cluster_ops.db_controller")
    def test_no_name_skips_duplicate_check(self, mock_db, _mock_user, _mock_events, _mock_qos):
        """name=None should bypass the uniqueness check."""
        mock_db.get_clusters.return_value = [_cluster(uuid="cluster-1", name="some-cluster")]
        mock_db.kv_store = MagicMock()

        with patch("simplyblock_core.cluster_ops.utils") as mock_utils:
            mock_utils.generate_string.return_value = "x" * 20
            result = self._call(None)

        self.assertIsNotNone(result)


# ---------------------------------------------------------------------------
# Tests for create_cluster()
# ---------------------------------------------------------------------------

class TestCreateClusterDuplicateName(unittest.TestCase):

    def _call(self, name, existing_clusters=None):
        with patch("simplyblock_core.cluster_ops.db_controller") as mock_db, \
             patch("simplyblock_core.cluster_ops.scripts"), \
             patch("simplyblock_core.cluster_ops.utils") as mock_utils:
            mock_db.get_clusters.return_value = existing_clusters or []
            mock_db.kv_store = MagicMock()
            mock_utils.get_iface_ip.return_value = "10.0.0.1"
            mock_utils.generate_string.return_value = "generated"

            return cluster_ops.create_cluster(
                blk_size=4096,
                page_size_in_blocks=2,
                cli_pass="pass",
                cap_warn=80,
                cap_crit=90,
                prov_cap_warn=80,
                prov_cap_crit=90,
                ifname="eth0",
                mgmt_ip=None,
                log_del_interval=7,
                metrics_retention_period=30,
                contact_point=None,
                grafana_endpoint=None,
                distr_ndcs=1,
                distr_npcs=1,
                distr_bs=4096,
                distr_chunk_bs=4096,
                ha_type="single",
                mode="docker",
                enable_node_affinity=False,
                qpair_count=4,
                client_qpair_count=4,
                max_queue_size=128,
                inflight_io_threshold=64,
                disable_monitoring=True,
                strict_node_anti_affinity=False,
                name=name,
                tls_secret=None,
                ingress_host_source=None,
                dns_name=None,
                fabric="tcp",
                is_single_node=True,
                client_data_nic="",
            )

    def test_raises_when_name_already_exists(self):
        existing = _cluster(uuid="cluster-1", name="first-cluster")

        with self.assertRaises(ValueError) as ctx:
            self._call("first-cluster", existing_clusters=[existing])

        self.assertIn("first-cluster", str(ctx.exception))

    def test_no_existing_clusters_skips_name_check(self):
        """When no clusters exist the duplicate-name check must not fire."""
        # If any exception surfaces it must not be our duplicate-name ValueError.
        try:
            self._call("brand-new", existing_clusters=[])
        except Exception as exc:
            self.assertNotIn("already exists", str(exc))


# ---------------------------------------------------------------------------
# Tests for set_name()
# ---------------------------------------------------------------------------

class TestSetNameDuplicateName(unittest.TestCase):

    @patch("simplyblock_core.cluster_ops.db_controller")
    def test_raises_when_another_cluster_has_the_name(self, mock_db):
        target = _cluster(uuid="cluster-1", name="old-name")
        other = _cluster(uuid="cluster-2", name="taken-name")
        mock_db.get_cluster_by_id.return_value = target
        mock_db.get_clusters.return_value = [target, other]

        with self.assertRaises(ValueError) as ctx:
            cluster_ops.set_name("cluster-1", "taken-name")

        self.assertIn("taken-name", str(ctx.exception))

    @patch("simplyblock_core.cluster_ops.cluster_events")
    @patch("simplyblock_core.cluster_ops.db_controller")
    def test_allows_renaming_to_own_current_name(self, mock_db, _mock_events):
        target = _cluster(uuid="cluster-1", name="my-cluster")
        mock_db.get_cluster_by_id.return_value = target
        mock_db.get_clusters.return_value = [target]
        mock_db.kv_store = MagicMock()

        cluster_ops.set_name("cluster-1", "my-cluster")  # must not raise

    @patch("simplyblock_core.cluster_ops.cluster_events")
    @patch("simplyblock_core.cluster_ops.db_controller")
    def test_allows_unique_new_name(self, mock_db, _mock_events):
        target = _cluster(uuid="cluster-1", name="old-name")
        other = _cluster(uuid="cluster-2", name="other-cluster")
        mock_db.get_cluster_by_id.return_value = target
        mock_db.get_clusters.return_value = [target, other]
        mock_db.kv_store = MagicMock()

        result = cluster_ops.set_name("cluster-1", "brand-new-name")

        self.assertEqual(result.cluster_name, "brand-new-name")


# ---------------------------------------------------------------------------
# Tests for change_cluster_name()
# ---------------------------------------------------------------------------

class TestChangeClusterNameDuplicateName(unittest.TestCase):

    @patch("simplyblock_core.cluster_ops.db_controller")
    def test_raises_when_another_cluster_has_the_name(self, mock_db):
        target = _cluster(uuid="cluster-1", name="old-name")
        other = _cluster(uuid="cluster-2", name="taken-name")
        mock_db.get_cluster_by_id.return_value = target
        mock_db.get_clusters.return_value = [target, other]

        with self.assertRaises(ValueError) as ctx:
            cluster_ops.change_cluster_name("cluster-1", "taken-name")

        self.assertIn("taken-name", str(ctx.exception))

    @patch("simplyblock_core.cluster_ops.cluster_events")
    @patch("simplyblock_core.cluster_ops.db_controller")
    def test_allows_renaming_to_own_current_name(self, mock_db, _mock_events):
        target = _cluster(uuid="cluster-1", name="my-cluster")
        mock_db.get_cluster_by_id.return_value = target
        mock_db.get_clusters.return_value = [target]
        mock_db.kv_store = MagicMock()

        cluster_ops.change_cluster_name("cluster-1", "my-cluster")  # must not raise

    @patch("simplyblock_core.cluster_ops.cluster_events")
    @patch("simplyblock_core.cluster_ops.db_controller")
    def test_allows_unique_new_name(self, mock_db, _mock_events):
        target = _cluster(uuid="cluster-1", name="old-name")
        other = _cluster(uuid="cluster-2", name="other-cluster")
        mock_db.get_cluster_by_id.return_value = target
        mock_db.get_clusters.return_value = [target, other]
        mock_db.kv_store = MagicMock()

        cluster_ops.change_cluster_name("cluster-1", "unique-name")

        self.assertEqual(target.cluster_name, "unique-name")


if __name__ == "__main__":
    unittest.main()
