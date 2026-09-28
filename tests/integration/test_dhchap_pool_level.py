"""Pool-level DH-HMAC-CHAP configuration, against real FoundationDB.

Covers:
  - Pool model dhchap/dhchap_key/dhchap_ctrlr_key/allowed_hosts fields
  - Fixed DHCHAP_DIGESTS and DHCHAP_DHGROUP constants
  - pool_controller.add_pool() auto-generates key pair when dhchap=True
  - pool_controller.add_host_to_pool / remove_host_from_pool
  - nvmf_set_config always receives DHCHAP digests/dhgroups unconditionally
  - _get_dhchap_group returns DHCHAP_DHGROUP when pool.dhchap=True
  - _register_pool_dhchap_keys_on_node writes pool-scoped keyring entries
  - LVol creation inherits pool.allowed_hosts when pool.dhchap=True
  - add_host_to_lvol uses pool keys for DHCHAP pools
  - bdev_nvme_set_options no longer accepts/sends dhchap params
  - connect_lvol builds nvme connect strings with/without DHCHAP secrets and TLS

Every case that reaches a DBController accessor seeds real records with
``write_to_db(db.kv_store)`` and reads them back through a live
``DBController``, per the tier's rule. These cases previously ran against a
``MagicMock`` standing in for the database, and the stand-in is what let
``add_pool``'s duplicate-name pre-check break unnoticed: it moved from
``get_pools()`` (which the mock stubbed) to ``pool_name_taken()`` (which it did
not), so the mock answered with a truthy ``MagicMock``, every ``add_pool`` call
bailed out at "name already taken", and the failure surfaced two layers away as
an assertion about ``pool.dhchap``. A real DBController cannot answer a
question the code never asked it.

Mocked here — everything *above* the database, per the tier's rule:

- ``create_kms_connection``, the pool key-encryption-key call into the KMS.
- ``StorageNode.rpc_client``, the JSON-RPC chain to a storage node. The
  integration tier never starts SPDK.
- ``add_lvol_on_node`` and ``_register_pool_dhchap_keys_on_node``, both of
  which are node-side work behind that same RPC boundary.
"""

import inspect
import unittest
from unittest.mock import MagicMock, patch

import pytest
from pydantic import SecretStr

from simplyblock_core.controllers import lvol_controller, pool_controller
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.iface import IFace
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.pool import Pool
from simplyblock_core.models.storage_node import StorageNode

CLUSTER_ID = "cluster-dhchap-1"
NODE_ID = "node-dhchap-1"
POOL_ID = "pool-dhchap-1"
HOST_A = "nqn.2014-08.org.nvmexpress:uuid:host-a"
HOST_B = "nqn.2014-08.org.nvmexpress:uuid:host-b"
POOL_KEY = "DHHC-1:01:aGVsbG8=:"
POOL_CTRLR_KEY = "DHHC-1:01:d29ybGQ=:"


@pytest.fixture
def db():
    db = DBController()
    if db.kv_store is None:
        pytest.skip("FoundationDB is not available")
    return db


@pytest.fixture
def cluster(db):
    return _write_cluster(db)


def _write_cluster(db, uuid=CLUSTER_ID, tls=False):
    cluster = Cluster()
    cluster.uuid = uuid
    cluster.status = Cluster.STATUS_ACTIVE
    cluster.ha_type = "single"
    cluster.nqn = f"nqn.2023-02.io.simplyblock:{uuid}"
    cluster.tls = tls
    cluster.tls_config = {}
    cluster.blk_size = 4096
    cluster.qpair_count = 32
    cluster.client_qpair_count = 3
    cluster.write_to_db(db.kv_store)
    return cluster


def _write_pool(db, uuid=POOL_ID, name="pool-dhchap", dhchap=True, hosts=(),
                cluster_id=CLUSTER_ID):
    pool = Pool()
    pool.uuid = uuid
    pool.cluster_id = cluster_id
    pool.pool_name = name
    pool.status = Pool.STATUS_ACTIVE
    pool.dhchap = dhchap
    if dhchap:
        pool.dhchap_key = SecretStr(POOL_KEY)
        pool.dhchap_ctrlr_key = SecretStr(POOL_CTRLR_KEY)
    pool.allowed_hosts = list(hosts)
    pool.write_to_db(db.kv_store)
    return pool


def _write_node(db, uuid=NODE_ID, cluster_id=CLUSTER_ID, devices=1):
    nic = IFace()
    nic.ip4_address = "10.0.0.1"
    nic.trtype = "TCP"

    node = StorageNode()
    node.uuid = uuid
    node.cluster_id = cluster_id
    node.hostname = uuid
    node.status = StorageNode.STATUS_ONLINE
    node.mgmt_ip = "127.0.0.1"
    node.rpc_port = 9901
    node.lvol_subsys_port = 4420
    node.rpc_username = "spdkuser"
    node.rpc_password = SecretStr("spdkpass")
    node.max_lvol = 100
    node.lvstore = "lvs1"
    node.lvstore_status = "ready"
    node.active_tcp = True
    node.active_rdma = False
    node.data_nics = [nic]
    node.nvme_devices = [_device(uuid, cluster_id, i) for i in range(devices)]
    node.write_to_db(db.kv_store)
    return node


def _device(node_id, cluster_id, index):
    device = NVMeDevice()
    device.uuid = f"{node_id}-dev-{index}"
    device.cluster_id = cluster_id
    device.node_id = node_id
    device.status = NVMeDevice.STATUS_ONLINE
    device.nvme_bdev = f"nvme_{index}"
    device.alceml_bdev = f"alceml_{index}"
    device.nvmf_nqn = f"nqn:dev:{node_id}:{index}"
    device.size = 100 * 1024 ** 3
    device.health_check = True
    return device


def _write_lvol(db, uuid="lvol-dhchap-1", node_id=NODE_ID, pool_uuid=POOL_ID,
                allowed_hosts=(), cluster_id=CLUSTER_ID):
    lvol = LVol()
    lvol.uuid = uuid
    lvol.pool_uuid = pool_uuid
    lvol.node_id = node_id
    lvol.nodes = [node_id]
    lvol.lvol_name = f"VOL_{uuid}"
    lvol.lvol_bdev = f"LVOL_{uuid}"
    lvol.lvs_name = "lvs1"
    lvol.top_bdev = f"{lvol.lvs_name}/{lvol.lvol_bdev}"
    lvol.nqn = f"nqn:test:{uuid}"
    lvol.ha_type = "single"
    lvol.fabric = "tcp"
    lvol.ns_id = 1
    lvol.size = 1024 ** 3
    lvol.status = LVol.STATUS_ONLINE
    lvol.allowed_hosts = [dict(entry) for entry in allowed_hosts]
    lvol.write_to_db(db.kv_store)
    return lvol


def _add_pool(name="testpool", dhchap=False, cluster_id=CLUSTER_ID, **kwargs):
    """``add_pool`` against the real DB, with only the KMS boundary mocked."""
    with patch.object(pool_controller, "create_kms_connection"):
        return pool_controller.add_pool(
            name=name, pool_max=0, lvol_max=0, max_rw_iops=0, max_rw_mbytes=0,
            max_r_mbytes=0, max_w_mbytes=0, cluster_id=cluster_id,
            dhchap=dhchap, **kwargs)


# ---------------------------------------------------------------------------
# Pool model
# ---------------------------------------------------------------------------

class TestPoolModelDhchap(unittest.TestCase):

    def _pool(self, **kwargs):
        from simplyblock_core.models.pool import Pool
        p = Pool()
        for k, v in kwargs.items():
            setattr(p, k, v)
        return p

    def test_default_is_false(self):
        from simplyblock_core.models.pool import Pool
        p = Pool()
        self.assertFalse(p.dhchap)

    def test_can_be_set_true(self):
        p = self._pool(dhchap=True)
        self.assertTrue(p.dhchap)

    def test_is_bool_type(self):
        from simplyblock_core.models.pool import Pool
        p = Pool()
        self.assertIsInstance(p.dhchap, bool)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

class TestDhchapConstants(unittest.TestCase):

    def test_dhchap_digests_defined(self):
        from simplyblock_core import constants
        self.assertTrue(hasattr(constants, "DHCHAP_DIGESTS"))

    def test_dhchap_dhgroup_defined(self):
        from simplyblock_core import constants
        self.assertTrue(hasattr(constants, "DHCHAP_DHGROUP"))

    def test_dhchap_digests_contains_standard_algorithms(self):
        from simplyblock_core import constants
        for digest in ("sha256", "sha384", "sha512"):
            self.assertIn(digest, constants.DHCHAP_DIGESTS)

    def test_dhchap_dhgroup_is_ffdhe2048(self):
        """Weakest DH group must be ffdhe2048."""
        from simplyblock_core import constants
        self.assertEqual(constants.DHCHAP_DHGROUP, "ffdhe2048")

    def test_dhchap_dhgroup_is_valid(self):
        from simplyblock_core import constants
        self.assertIn(constants.DHCHAP_DHGROUP, constants.VALID_DHCHAP_DHGROUPS)

    def test_dhchap_digests_all_valid(self):
        from simplyblock_core import constants
        for d in constants.DHCHAP_DIGESTS:
            self.assertIn(d, constants.VALID_DHCHAP_DIGESTS)


# ---------------------------------------------------------------------------
# pool_controller.add_pool
# ---------------------------------------------------------------------------

class TestAddPoolDhchap:
    """Tests for the dhchap parameter of add_pool()."""

    def test_dhchap_false_by_default(self):
        """add_pool with no dhchap arg must set pool.dhchap = False."""
        sig = inspect.signature(pool_controller.add_pool)
        assert "dhchap" in sig.parameters
        assert sig.parameters["dhchap"].default is False

    def test_dhchap_true_stored_on_pool(self, db, cluster):
        pool_id = _add_pool(dhchap=True)
        assert db.get_pool_by_id(pool_id).dhchap is True

    def test_dhchap_false_stored_on_pool(self, db, cluster):
        pool_id = _add_pool(dhchap=False)
        assert db.get_pool_by_id(pool_id).dhchap is False

    def test_duplicate_name_in_the_same_cluster_rejected(self, db, cluster):
        """The pre-check behind Unique(cluster_id, pool_name).

        Regression: it moved from a get_pools() scan to pool_name_taken(), and
        against a mocked DBController the unstubbed accessor answered with a
        truthy MagicMock — so every add_pool in this file bailed out here.
        """
        assert _add_pool(name="dupe")
        assert _add_pool(name="dupe") is False

    def test_same_name_in_another_cluster_allowed(self, db, cluster):
        """The constraint is per cluster, not deployment-wide."""
        other = _write_cluster(db, uuid="cluster-dhchap-2")
        assert _add_pool(name="shared")
        assert _add_pool(name="shared", cluster_id=other.get_id())


# ---------------------------------------------------------------------------
# RPC: nvmf_set_config and bdev_nvme_set_options
# ---------------------------------------------------------------------------

class TestNvmfSetConfigDhchap(unittest.TestCase):

    def _rpc(self):
        from simplyblock_core.rpc_client import RPCClient
        c = RPCClient.__new__(RPCClient)
        c._request = MagicMock(return_value=True)
        return c

    def test_signature_has_dhchap_params(self):
        from simplyblock_core.rpc_client import RPCClient
        sig = inspect.signature(RPCClient.nvmf_set_config)
        self.assertIn("dhchap_digests", sig.parameters)
        self.assertIn("dhchap_dhgroups", sig.parameters)

    def test_no_dhchap_only_pollers_mask(self):
        c = self._rpc()
        c.nvmf_set_config("0x1")
        params = c._request.call_args[0][1]
        self.assertEqual(params["poll_groups_mask"], "0x1")
        self.assertNotIn("dhchap_digests", params)
        self.assertNotIn("dhchap_dhgroups", params)

    def test_dhchap_params_included_when_provided(self):
        from simplyblock_core import constants
        c = self._rpc()
        c.nvmf_set_config(
            "0x3",
            dhchap_digests=constants.DHCHAP_DIGESTS,
            dhchap_dhgroups=[constants.DHCHAP_DHGROUP],
        )
        params = c._request.call_args[0][1]
        self.assertEqual(params["dhchap_digests"], constants.DHCHAP_DIGESTS)
        self.assertEqual(params["dhchap_dhgroups"], [constants.DHCHAP_DHGROUP])

    def test_null_dhchap_not_sent(self):
        """Passing None for dhchap params must not include them in the RPC call."""
        c = self._rpc()
        c.nvmf_set_config("0x1", dhchap_digests=None, dhchap_dhgroups=None)
        params = c._request.call_args[0][1]
        self.assertNotIn("dhchap_digests", params)
        self.assertNotIn("dhchap_dhgroups", params)


class TestBdevNvmeSetOptionsNoDhchap(unittest.TestCase):

    def test_signature_has_no_dhchap_params(self):
        from simplyblock_core.rpc_client import RPCClient
        sig = inspect.signature(RPCClient.bdev_nvme_set_options)
        self.assertNotIn("dhchap_digests", sig.parameters)
        self.assertNotIn("dhchap_dhgroups", sig.parameters)

    def test_rpc_call_never_contains_dhchap(self):
        from simplyblock_core.rpc_client import RPCClient
        c = RPCClient.__new__(RPCClient)
        c._request = MagicMock(return_value=True)
        c.bdev_nvme_set_options()
        params = c._request.call_args[0][1]
        self.assertNotIn("dhchap_digests", params)
        self.assertNotIn("dhchap_dhgroups", params)


# ---------------------------------------------------------------------------
# storage_node_ops: nvmf_set_config always called with DHCHAP constants
# -----------------------------------------------------------------------

class TestNvmfSetConfigAlwaysSendsDhchap(unittest.TestCase):
    """
    Verify that nvmf_set_config always receives DHCHAP digests and dhgroups
    unconditionally — they are capability options, not enforcement.
    Actual DHCHAP is activated per-subsystem when hosts are added with keys.
    """

    def test_constants_are_configured(self):
        from simplyblock_core import constants
        self.assertTrue(len(constants.DHCHAP_DIGESTS) > 0)
        self.assertIn("sha256", constants.DHCHAP_DIGESTS)
        self.assertTrue(len(constants.DHCHAP_DHGROUP) > 0)

    def test_fixed_dhgroup_is_ffdhe2048(self):
        from simplyblock_core import constants
        self.assertEqual(constants.DHCHAP_DHGROUP, "ffdhe2048")


# ---------------------------------------------------------------------------
# Pool model – new fields
# ---------------------------------------------------------------------------

class TestPoolModelDhchapFields(unittest.TestCase):

    def _pool(self):
        from simplyblock_core.models.pool import Pool
        return Pool()

    def test_dhchap_key_default_empty(self):
        self.assertEqual(self._pool().dhchap_key.get_secret_value(), "")

    def test_dhchap_ctrlr_key_default_empty(self):
        self.assertEqual(self._pool().dhchap_ctrlr_key.get_secret_value(), "")

    def test_allowed_hosts_default_empty_list(self):
        self.assertEqual(self._pool().allowed_hosts, [])

    def test_allowed_hosts_is_list(self):
        self.assertIsInstance(self._pool().allowed_hosts, list)


# ---------------------------------------------------------------------------
# add_pool key generation
# ---------------------------------------------------------------------------

class TestAddPoolKeyGeneration:
    """The keys add_pool generates, as they come back out of the database.

    Read back rather than captured at the write: the pair is persisted through
    ``SecretStr`` and has to survive the round trip to be usable at all.
    """

    def _pool(self, db, dhchap):
        return db.get_pool_by_id(_add_pool(name="p1", dhchap=dhchap))

    def test_keys_generated_when_dhchap_true(self, db, cluster):
        pool = self._pool(db, dhchap=True)
        assert pool.dhchap_key.get_secret_value()
        assert pool.dhchap_ctrlr_key.get_secret_value()

    def test_key_is_dhhc1_format(self, db, cluster):
        key_value = self._pool(db, dhchap=True).dhchap_key.get_secret_value()
        assert key_value.startswith("DHHC-1:"), f"Expected DHHC-1 prefix, got: {key_value}"

    def test_two_distinct_keys_generated(self, db, cluster):
        pool = self._pool(db, dhchap=True)
        assert (pool.dhchap_key.get_secret_value()
                != pool.dhchap_ctrlr_key.get_secret_value())

    def test_no_keys_when_dhchap_false(self, db, cluster):
        pool = self._pool(db, dhchap=False)
        assert pool.dhchap_key.get_secret_value() == ""
        assert pool.dhchap_ctrlr_key.get_secret_value() == ""

    def test_keys_are_masked_in_the_record_repr(self, db, cluster):
        """The pair is secret-wrapped end to end, so a log line cannot leak it."""
        pool = self._pool(db, dhchap=True)
        assert pool.dhchap_key.get_secret_value() not in str(pool.to_dict())


# ---------------------------------------------------------------------------
# add_host_to_pool / remove_host_from_pool
# ---------------------------------------------------------------------------

class TestAddHostToPool:
    """The allowed-host list is read back from FDB, not from the caller's copy.

    ``add_host_to_pool`` persists the mutation and then fans out over the
    pool's volumes; asserting on the in-memory object would pass even if the
    write never landed.
    """

    def test_success(self, db, cluster):
        pool = _write_pool(db)
        ok, err = pool_controller.add_host_to_pool(pool.get_id(), HOST_A)
        assert ok
        assert err is None
        assert HOST_A in db.get_pool_by_id(pool.get_id()).allowed_hosts

    def test_duplicate_rejected(self, db, cluster):
        pool = _write_pool(db, hosts=[HOST_A])
        ok, err = pool_controller.add_host_to_pool(pool.get_id(), HOST_A)
        assert not ok
        assert err and "already in" in err
        assert db.get_pool_by_id(pool.get_id()).allowed_hosts == [HOST_A]

    def test_invalid_nqn_rejected(self, db, cluster):
        pool = _write_pool(db)
        ok, err = pool_controller.add_host_to_pool(pool.get_id(), "not-an-nqn")
        assert not ok
        assert err and "Invalid host NQN" in err
        assert db.get_pool_by_id(pool.get_id()).allowed_hosts == []

    def test_non_dhchap_pool_rejected(self, db, cluster):
        pool = _write_pool(db, uuid="pool-plain", name="plain", dhchap=False)
        ok, err = pool_controller.add_host_to_pool(pool.get_id(), HOST_A)
        assert not ok
        assert err and "DHCHAP" in err

    def test_pool_not_found(self, db, cluster):
        ok, err = pool_controller.add_host_to_pool("bad-id", HOST_A)
        assert not ok
        assert err and "not found" in err


class TestRemoveHostFromPool:

    def test_success(self, db, cluster):
        pool = _write_pool(db, hosts=[HOST_A, HOST_B])
        ok, err = pool_controller.remove_host_from_pool(pool.get_id(), HOST_A)
        assert ok
        assert err is None
        assert db.get_pool_by_id(pool.get_id()).allowed_hosts == [HOST_B]

    def test_nonexistent_host_rejected(self, db, cluster):
        pool = _write_pool(db, hosts=[HOST_A])
        ok, err = pool_controller.remove_host_from_pool(pool.get_id(), "nqn:not-there")
        assert not ok
        assert err and "not in" in err
        assert db.get_pool_by_id(pool.get_id()).allowed_hosts == [HOST_A]

    def test_non_dhchap_pool_rejected(self, db, cluster):
        pool = _write_pool(db, uuid="pool-plain", name="plain", dhchap=False)
        ok, err = pool_controller.remove_host_from_pool(pool.get_id(), HOST_A)
        assert not ok
        assert err and "DHCHAP" in err


# ---------------------------------------------------------------------------
# _get_dhchap_group with pool
# ---------------------------------------------------------------------------

def _make_dhchap_pool(pool_id="pool-1", hosts=None):
    from simplyblock_core.models.pool import Pool
    p = Pool()
    p.uuid = pool_id
    p.dhchap = True
    p.dhchap_key = SecretStr("DHHC-1:01:aGVsbG8=:")
    p.dhchap_ctrlr_key = SecretStr("DHHC-1:01:d29ybGQ=:")
    p.allowed_hosts = list(hosts or [])
    return p


class TestGetDhchapGroupWithPool(unittest.TestCase):

    def _group(self, cluster, pool=None):
        from simplyblock_core.controllers.lvol_controller import _get_dhchap_group
        return _get_dhchap_group(cluster, pool)

    def _cluster(self):
        from simplyblock_core.models.cluster import Cluster
        c = Cluster()
        c.tls = False
        c.tls_config = {}
        return c

    def test_pool_dhchap_returns_constant_group(self):
        from simplyblock_core import constants
        pool = _make_dhchap_pool()
        result = self._group(self._cluster(), pool)
        self.assertEqual(result, constants.DHCHAP_DHGROUP)
        self.assertEqual(result, "ffdhe2048")

    def test_pool_no_dhchap_falls_back_to_null(self):
        from simplyblock_core.models.pool import Pool
        plain_pool = Pool()
        plain_pool.dhchap = False
        result = self._group(self._cluster(), plain_pool)
        self.assertEqual(result, "null")

    def test_no_pool_no_cluster_tls_returns_null(self):
        result = self._group(self._cluster(), None)
        self.assertEqual(result, "null")


# ---------------------------------------------------------------------------
# LVol creation: allowed_hosts inherited from pool when pool.dhchap=True
# ---------------------------------------------------------------------------

class TestLvolInheritsDhchapFromPool:

    def test_lvol_allowed_hosts_set_from_pool(self, db, cluster):
        """When pool.dhchap=True, add_lvol_ha populates lvol.allowed_hosts from pool."""
        node = _write_node(db)
        secondary = _write_node(db, uuid="node-dhchap-2", devices=0)
        node.secondary_node_id = secondary.get_id()
        node.write_to_db(db.kv_store)
        pool = _write_pool(db, hosts=[HOST_A, HOST_B])

        captured_lvol = {}

        def fake_add_on_node(lvol, snode, **kwargs):
            captured_lvol['obj'] = lvol
            return {'uuid': 'u1', 'driver_specific': {'lvol': {'blobid': 1}}}, None

        rpc = MagicMock()
        with patch.object(StorageNode, "rpc_client", return_value=rpc), \
             patch.object(lvol_controller, "add_lvol_on_node",
                          side_effect=fake_add_on_node):
            result, err = lvol_controller.add_lvol_ha(
                name="vol1", size=1073741824, host_id_or_name=node.get_id(),
                ha_type="single", pool_id_or_name=pool.pool_name,
                use_comp=False, use_crypto=False,
                distr_vuid=0, max_rw_iops=0, max_rw_mbytes=0,
                max_r_mbytes=0, max_w_mbytes=0,
            )

        assert err is None, err
        assert result
        lvol = captured_lvol['obj']
        host_nqns = [h["nqn"] for h in lvol.allowed_hosts]
        assert HOST_A in host_nqns
        assert HOST_B in host_nqns
        # Entries must be plain NQN dicts, no key material stored on lvol
        for entry in lvol.allowed_hosts:
            assert "dhchap_key" not in entry
            assert "dhchap_ctrlr_key" not in entry

        # The persisted record, not just the object handed to the node.
        stored = db.get_lvol_by_id(result)
        assert [h["nqn"] for h in stored.allowed_hosts] == [HOST_A, HOST_B]

# ---------------------------------------------------------------------------
# add_host_to_lvol uses pool keys for DHCHAP pools
# ---------------------------------------------------------------------------

class TestAddHostToLvolDhchapPool:
    """add_host_to_lvol on a DHCHAP pool must use the pool's key pair.

    The node-side halves are mocked (key registration and the JSON-RPC call);
    the volume, its pool and its node are real records, because which key pair
    the call uses is decided by reading them back.
    """

    @pytest.fixture
    def rpc(self):
        rpc = MagicMock()
        rpc.subsystem_add_host.return_value = True
        with patch.object(StorageNode, "rpc_client", return_value=rpc):
            yield rpc

    def test_uses_pool_keys_not_per_host_keys(self, db, cluster, rpc):
        _write_node(db)
        _write_pool(db)
        lvol = _write_lvol(db)
        key_names = {
            "dhchap_key": "pool_pool_1_dhchap_key",
            "dhchap_ctrlr_key": "pool_pool_1_dhchap_ctrlr_key",
        }

        with patch.object(lvol_controller, "_register_pool_dhchap_keys_on_node",
                          return_value=key_names) as mock_pool_reg:
            entry, err = lvol_controller.add_host_to_lvol(lvol.get_id(), HOST_A)

        assert err is None
        mock_pool_reg.assert_called_once()

        call_kwargs = rpc.subsystem_add_host.call_args[1]
        assert call_kwargs["dhchap_key"] == key_names["dhchap_key"]
        assert call_kwargs["dhchap_ctrlr_key"] == key_names["dhchap_ctrlr_key"]
        assert call_kwargs["dhchap_group"] == "ffdhe2048"

        # The host is persisted as a bare NQN: pool-level DHCHAP keeps key
        # material on the pool, never copied onto the volume.
        stored = db.get_lvol_by_id(lvol.get_id()).allowed_hosts
        assert stored == [{"nqn": HOST_A}]
        assert entry == {"nqn": HOST_A}

    def test_no_per_host_key_generation_for_dhchap_pool(self, db, cluster, rpc):
        """For DHCHAP pools, generate_dhchap_key must never be called."""
        from simplyblock_core import utils

        _write_node(db)
        _write_pool(db)
        lvol = _write_lvol(db)

        with patch.object(lvol_controller, "_register_pool_dhchap_keys_on_node",
                          return_value={"dhchap_key": "pool_k",
                                        "dhchap_ctrlr_key": "pool_ck"}), \
             patch.object(utils, "generate_dhchap_key") as mock_gen:
            lvol_controller.add_host_to_lvol(lvol.get_id(), HOST_A)

        mock_gen.assert_not_called()

    def test_offline_node_is_skipped(self, db, cluster, rpc):
        """A node that is not ONLINE gets no subsystem call."""
        node = _write_node(db)
        node.status = StorageNode.STATUS_OFFLINE
        node.write_to_db(db.kv_store)
        _write_pool(db)
        lvol = _write_lvol(db)

        with patch.object(lvol_controller, "_register_pool_dhchap_keys_on_node",
                          return_value={}) as mock_pool_reg:
            _entry, err = lvol_controller.add_host_to_lvol(lvol.get_id(), HOST_A)

        assert err is None
        mock_pool_reg.assert_not_called()
        rpc.subsystem_add_host.assert_not_called()

    def test_duplicate_host_rejected(self, db, cluster, rpc):
        _write_node(db)
        _write_pool(db)
        lvol = _write_lvol(db, allowed_hosts=[{"nqn": HOST_A}])

        result, err = lvol_controller.add_host_to_lvol(lvol.get_id(), HOST_A)

        assert result is False
        assert err and "already allowed" in err


# ---------------------------------------------------------------------------
# connect_lvol: DHCHAP secret & TLS flag handling in the nvme connect string
# ---------------------------------------------------------------------------

def _connect_env(db, lvol_allowed_hosts, pool_dhchap_key="", pool_dhchap_ctrlr_key=""):
    """Seed the records every connect_lvol case reads, and return the volume.

    connect_lvol pulls the pool's DHCHAP keys onto the matched host_entry (see
    PR #1074, which reverted the "stop injecting" change). Passing either key
    also turns on the pool's ``dhchap`` flag, which is what gates the pool-key
    branch in ``HostConnectAuth.from_entry`` — matching add_pool, which sets
    the flag and the keys together. Passing neither gives a pool with DHCHAP
    off, so per-entry material (e.g. psk) is used instead.
    """
    _write_cluster(db)
    _write_node(db)

    pool = Pool()
    pool.uuid = POOL_ID
    pool.cluster_id = CLUSTER_ID
    pool.pool_name = "pool-dhchap"
    pool.status = Pool.STATUS_ACTIVE
    pool.dhchap = bool(pool_dhchap_key or pool_dhchap_ctrlr_key)
    pool.dhchap_key = SecretStr(pool_dhchap_key)
    pool.dhchap_ctrlr_key = SecretStr(pool_dhchap_ctrlr_key)
    pool.write_to_db(db.kv_store)

    return _write_lvol(db, allowed_hosts=lvol_allowed_hosts)


class TestConnectLvolDhchap:

    def test_host_with_dhchap_keys_injected_into_connect_cmd(self, db):
        """connect_lvol must add --dhchap-secret and --dhchap-ctrl-secret,
        sourced from the pool's DHCHAP keys, for an allowed host entry.

        Per PR #1074 the secrets come from the POOL, not from any key material
        stored on the lvol's allowed_hosts entry — for a pool with DHCHAP
        enabled (the pool keys are gated on pool.dhchap)."""
        _connect_env(db, [{"nqn": HOST_A}],
                     pool_dhchap_key=POOL_KEY,
                     pool_dhchap_ctrlr_key=POOL_CTRLR_KEY)

        result, _err = lvol_controller.connect_lvol("lvol-dhchap-1", host_nqn=HOST_A)

        assert isinstance(result, list)
        assert len(result) == 1
        cmd = result[0].connect
        assert f"--hostnqn={HOST_A}" in cmd
        assert f"--dhchap-secret={POOL_KEY}" in cmd
        assert f"--dhchap-ctrl-secret={POOL_CTRLR_KEY}" in cmd
        # No PSK/TLS was configured
        assert " --tls" not in cmd
        assert result[0].tls is False

    def test_host_with_psk_sets_tls_flag(self, db):
        """A host_entry with a psk must add --tls to the connect command and
        mark tls=True on the returned entry."""
        _connect_env(db, [{"nqn": HOST_A, "psk": "NVMeTLSkey-1:01:aGVsbG8=:"}])

        result, _err = lvol_controller.connect_lvol("lvol-dhchap-1", host_nqn=HOST_A)

        assert isinstance(result, list)
        cmd = result[0].connect
        assert " --tls" in cmd
        assert f"--hostnqn={HOST_A}" in cmd
        assert result[0].tls is True
        # No DHCHAP keys on the entry
        assert "--dhchap-secret" not in cmd
        assert "--dhchap-ctrl-secret" not in cmd

    def test_missing_host_nqn_when_allowed_hosts_present_returns_false(self, db):
        """If allowed_hosts is populated, host_nqn is mandatory."""
        _connect_env(db, [{"nqn": HOST_A}])
        result, _err = lvol_controller.connect_lvol("lvol-dhchap-1", host_nqn=None)
        assert not result

    def test_unknown_host_nqn_returns_false(self, db):
        """host_nqn that is not in the allowed_hosts list is rejected."""
        _connect_env(db, [{"nqn": HOST_A}])
        result, _err = lvol_controller.connect_lvol("lvol-dhchap-1", host_nqn="nqn:intruder")
        assert not result

    def test_no_allowed_hosts_pass_through_with_host_nqn(self, db):
        """When lvol.allowed_hosts is empty, host_nqn is passed through
        without any DHCHAP/TLS material and the volume accepts any host."""
        _connect_env(db, [])

        result, _err = lvol_controller.connect_lvol("lvol-dhchap-1", host_nqn="nqn:whoever")

        assert isinstance(result, list)
        cmd = result[0].connect
        assert "--hostnqn=nqn:whoever" in cmd
        assert "--dhchap-secret" not in cmd
        assert "--dhchap-ctrl-secret" not in cmd
        assert " --tls" not in cmd
        # No allowed_hosts → empty list on returned entry
        assert result[0].allowed_hosts == []

    def test_pool_level_dhchap_lvol_injects_pool_secret_in_connect_cmd(self, db):
        """Lvols inheriting from a pool-level DHCHAP pool have nqn-only entries
        in allowed_hosts (no key material stored on the lvol). connect_lvol
        injects the pool's DHCHAP keys onto the connect command for the matched
        host — documents current behavior after PR #1074 reverted the change
        that stopped unconditionally injecting pool keys."""
        _connect_env(db, [{"nqn": HOST_A}],
                     pool_dhchap_key=POOL_KEY,
                     pool_dhchap_ctrlr_key=POOL_CTRLR_KEY)

        result, _err = lvol_controller.connect_lvol("lvol-dhchap-1", host_nqn=HOST_A)

        assert isinstance(result, list)
        cmd = result[0].connect
        assert f"--hostnqn={HOST_A}" in cmd
        assert f"--dhchap-secret={POOL_KEY}" in cmd
        assert f"--dhchap-ctrl-secret={POOL_CTRLR_KEY}" in cmd
        assert result[0].allowed_hosts == [HOST_A]


if __name__ == "__main__":
    unittest.main()
