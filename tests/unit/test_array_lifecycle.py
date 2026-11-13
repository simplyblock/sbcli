"""Multi-attach array LUN mode, phase 4: array plugins, credentials in the
KMS, registration, the LUN lifecycle, and the AWS EBS io2 plugin.

  - KMS generic secrets (LocalKMS, HCPClient): stored plaintext only at the
    persistence boundary, read back as SecretStr, own key prefix.
  - plugin interface: connect with KMS credentials, ownership tags, default
    fence = forced detach; FakeArrayPlugin implements everything in memory.
  - array_controller: register / get / delete (credentials never on the
    cluster record), node-add LUN provisioning, attachment reconcile on role
    changes (never detaches the serving node, never attaches a fenced one),
    growth by factor, node release, cluster delete.
  - lun_takeover fences by the array's own LUN / host ids.
  - aws_ebs against botocore's Stubber.
  - v2 API and CLI front-ends.
"""

import json
import os
import tempfile
import unittest
from contextlib import nullcontext
from typing import ClassVar
from unittest.mock import MagicMock, patch

from pydantic import SecretStr

from simplyblock_core import array, multi_attach, utils
from simplyblock_core.array import aws_ebs
from simplyblock_core.array.fake import FakeArrayPlugin
from simplyblock_core.controllers import array_controller, device_controller
from simplyblock_core.kms import KMSException, array_secret_path
from simplyblock_core.kms._fdb import LocalKMS
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode

PLUGIN = "fake-p4"
CID = "cluster-4"
GIB = 1024 ** 3


# ------------------------------------------------------------------ fakes

class FakeKV:
    def __init__(self):
        self.data = {}

    def set(self, k, v):
        self.data[k] = v

    def get(self, k):
        return self.data.get(k)

    def clear(self, k):
        self.data.pop(k, None)


class FakeKMS:
    """A KMS holding secrets in memory (as plaintext, like the stores)."""
    store: ClassVar[dict] = {}

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return None

    def put_secret(self, path, data):
        from simplyblock_core.kms._base import secret_plaintext
        FakeKMS.store[path] = secret_plaintext(data)

    def get_secret(self, path):
        from simplyblock_core.kms._base import secret_wrapped
        if path not in FakeKMS.store:
            raise KMSException(f"No secret found at {path}")
        return secret_wrapped(FakeKMS.store[path])

    def delete_secret(self, path):
        FakeKMS.store.pop(path, None)


class FakeDB:
    def __init__(self, cluster, nodes):
        self.cluster = cluster
        self.nodes = {n.get_id(): n for n in nodes}

    def get_cluster_by_id(self, _cid):
        return self.cluster

    def get_storage_nodes_by_cluster_id(self, _cid):
        return list(self.nodes.values())

    def get_storage_node_by_id(self, nid):
        return self.nodes[nid]

    def atomic_update(self, obj, fn):
        fresh = self.cluster if isinstance(obj, Cluster) else self.nodes[obj.get_id()]
        fn(fresh)
        return fresh


def _cluster(plugin=PLUGIN, multi=True, lun_size=0):
    cl = Cluster()
    cl.uuid = CID
    cl.multi_attach = multi
    cl.max_fault_tolerance = 2
    cl.array_plugin = plugin
    cl.array_lun_size = lun_size
    cl.status = Cluster.STATUS_ACTIVE
    return cl


def _node(nid, sec="", ter="", status=StorageNode.STATUS_ONLINE):
    n = StorageNode()
    n.uuid = nid
    n.cluster_id = CID
    n.hostname = f"{nid}_5260"
    n.rpc_port = 5260
    n.status = status
    n.secondary_node_id, n.tertiary_node_id = sec, ter
    n.agent = MagicMock()
    n.agent.lun_info.return_value = ([{"by_id_path": f"/dev/disk/by-id/lun-{nid}",
                                       "by_id_paths": [], "device_path": "/dev/sdx"}], None)
    n.client = lambda *a, _c=n.agent, **k: _c
    return n


def _lun_device(owner, lun_id, wwn, size=10 * GIB):
    d = NVMeDevice()
    d.uuid = f"dev-{owner.get_id()}"
    d.node_id = owner.get_id()
    d.cluster_id = CID
    d.status = NVMeDevice.STATUS_ONLINE
    d.multi_attach = True
    d.array_lun_id = lun_id
    d.lun_wwn = wwn
    d.size = size
    d.node_paths = {owner.get_id(): {"path": f"/dev/disk/by-id/own-{owner.get_id()}", "bdev": "aio"}}
    owner.nvme_devices = [d]
    return d


class _Case(unittest.TestCase):
    def setUp(self):
        FakeArrayPlugin.reset()
        FakeKMS.store = {}
        array.register_plugin(PLUGIN, FakeArrayPlugin)
        self.addCleanup(array.unregister_plugin, PLUGIN)
        for target, attr, kw in (
                (array_controller, "_event", {}),
                (array_controller, "_sleep", {}),
                ("simplyblock_core.kms.create_kms_connection", None, {"side_effect": lambda c: FakeKMS()}),
                ("simplyblock_core.models.lock.DbLock", None, {"side_effect": lambda *a, **k: nullcontext()})):
            p = patch(target, **kw) if attr is None else patch.object(target, attr, **kw)
            p.start()
            self.addCleanup(p.stop)

    def use_db(self, db):
        p = patch.object(array_controller, "_db", return_value=db)
        p.start()
        self.addCleanup(p.stop)
        return db

    def plugin(self, cluster):
        return array.plugin_for_cluster(cluster)


# -------------------------------------------------------------------- KMS

class TestLocalKmsSecrets(unittest.TestCase):

    def _kms(self):
        kv = FakeKV()
        with patch("simplyblock_core.db_controller.DBController") as dbc:
            dbc.return_value.kv_store = kv
            return LocalKMS(MagicMock()), kv

    def test_round_trip_plaintext_only_at_rest(self):
        kms, kv = self._kms()
        kms.put_secret("cluster/c/array/a", {"key": SecretStr("s3cr3t"), "user": "u"})
        raw = kv.data[b"secrets/cluster/c/array/a"]
        self.assertEqual(json.loads(raw), {"key": "s3cr3t", "user": "u"})
        got = kms.get_secret("cluster/c/array/a")
        self.assertIsInstance(got["key"], SecretStr)
        self.assertEqual(got["key"].get_secret_value(), "s3cr3t")
        self.assertNotIn("s3cr3t", repr(got))

    def test_own_prefix_and_delete(self):
        kms, kv = self._kms()
        kms.import_data_encryption_keys("p", "kek", (SecretStr("aa"), SecretStr("bb")))
        kms.put_secret("p", {"x": "y"})
        self.assertIn(b"keys/p", kv.data)
        self.assertIn(b"secrets/p", kv.data)
        kms.delete_secret("p")
        self.assertNotIn(b"secrets/p", kv.data)
        self.assertIn(b"keys/p", kv.data)
        kms.delete_secret("p")  # idempotent
        with self.assertRaises(KMSException):
            kms.get_secret("p")

    def test_refuses_non_string_values(self):
        kms, _ = self._kms()
        with self.assertRaises(TypeError):
            kms.put_secret("p", {"x": {"nested": "secret"}})


class TestHcpSecrets(unittest.TestCase):

    def _client(self):
        from simplyblock_core.kms._hcp import HCPClient
        c = HCPClient.__new__(HCPClient)
        c.kv_mount = "kv"
        c.client = MagicMock()
        return c

    def test_put_get_delete(self):
        import hvac.exceptions
        c = self._client()
        c.put_secret("cluster/c/array/a", {"k": SecretStr("v")})
        c.client.secrets.kv.v2.create_or_update_secret.assert_called_once_with(
            path="cluster/c/array/a", secret={"k": "v"}, mount_point="kv")
        c.client.secrets.kv.v2.read_secret_version.return_value = {"data": {"data": {"k": "v"}}}
        got = c.get_secret("cluster/c/array/a")
        self.assertEqual(got["k"].get_secret_value(), "v")
        c.client.secrets.kv.v2.read_secret_version.side_effect = hvac.exceptions.InvalidPath()
        with self.assertRaises(KMSException):
            c.get_secret("x")
        c.client.secrets.kv.v2.delete_metadata_and_all_versions.side_effect = hvac.exceptions.InvalidPath()
        c.delete_secret("x")  # a missing secret is deleted already
        c.client.secrets.kv.v2.delete_metadata_and_all_versions.side_effect = hvac.exceptions.Forbidden()
        with self.assertRaises(KMSException):
            c.delete_secret("x")


# ------------------------------------------------------- plugin interface

class TestPluginInterface(_Case):

    def test_plugin_for_cluster_connects_with_kms_credentials(self):
        cl = _cluster()
        cl.array_id, cl.array_has_credentials = "arr-1", True
        cl.array_endpoint, cl.array_options = "https://array", {"pool": "p"}
        FakeKMS.store[array_secret_path(CID, "arr-1")] = {"password": "pw"}
        plugin = array.plugin_for_cluster(cl)
        endpoint, creds, opts = FakeArrayPlugin.connected[-1]
        self.assertEqual((endpoint, opts), ("https://array", {"pool": "p"}))
        self.assertEqual(creds["password"].get_secret_value(), "pw")
        self.assertEqual(plugin.cluster_id, CID)
        self.assertNotIn("pw", repr(plugin))

    def test_no_kms_read_without_credentials(self):
        cl = _cluster()
        with patch("simplyblock_core.kms.create_kms_connection") as kms:
            array.plugin_for_cluster(cl)
            kms.assert_not_called()

    def test_unreadable_credentials_are_an_array_error(self):
        cl = _cluster()
        cl.array_id, cl.array_has_credentials = "arr-1", True
        with self.assertRaisesRegex(array.ArrayError, "KMS"):
            array.plugin_for_cluster(cl)

    def test_connect_failure_is_an_array_error(self):
        FakeArrayPlugin.fail.add("connect")
        with self.assertRaisesRegex(array.ArrayError, "could not connect"):
            array.plugin_for_cluster(_cluster())

    def test_builtin_aws_ebs_without_entry_point(self):
        with patch.object(array, "_entry_point", return_value=None):
            self.assertIs(array.plugin_class("aws-ebs"), aws_ebs.AwsEbsPlugin)

    def test_default_fence_is_forced_detach(self):
        class Minimal(FakeArrayPlugin):
            fence = array.ArrayPlugin.fence
            unfence = array.ArrayPlugin.unfence
        p = Minimal(cluster_id=CID)
        lun = p.create_lun("l", GIB, {})
        p.attach(lun["id"], "h1")
        self.assertTrue(p.fence(lun["id"], "h1"))
        self.assertIn(("detach", lun["id"], "h1", True), FakeArrayPlugin.calls)
        self.assertEqual(p.get(lun["id"])["hosts"], [])
        self.assertTrue(p.unfence(lun["id"], "h1"))
        self.assertEqual(p.get(lun["id"])["hosts"], ["h1"])

    def test_fake_lifecycle_and_ownership(self):
        p = self.plugin(_cluster())
        lun = p.create_lun("node-a", 5 * GIB, {"x": "1"})
        self.assertEqual(p.create_lun("node-a", 5 * GIB, {})["id"], lun["id"])  # idempotent
        self.assertEqual(FakeArrayPlugin.luns[lun["id"]]["tags"][array.TAG_CLUSTER_ID], CID)
        p.attach(lun["id"], "h1")
        p.attach(lun["id"], "h1")
        p.attach(lun["wwn"], "h2")  # by WWN
        self.assertEqual(p.get(lun["id"])["hosts"], ["h1", "h2"])
        p.resize(lun["id"], 8 * GIB)
        with self.assertRaises(array.ArrayError):
            p.resize(lun["id"], GIB)
        p.detach(lun["id"], "h2")
        p.detach(lun["id"], "h2")
        self.assertEqual([x["id"] for x in p.list_luns()], [lun["id"]])
        p.delete_lun(lun["id"])
        p.delete_lun(lun["id"])
        self.assertEqual(p.list_luns(), [])

    def test_foreign_lun_is_never_touched(self):
        p = self.plugin(_cluster())
        FakeArrayPlugin().add_foreign_lun("lun-x", tags={array.TAG_CLUSTER_ID: "other"})
        for op in (lambda: p.attach("lun-x", "h"), lambda: p.detach("lun-x", "h"),
                   lambda: p.resize("lun-x", GIB * 9), lambda: p.delete_lun("lun-x"),
                   lambda: p.get("lun-x")):
            with self.assertRaises(array.ForeignLunError):
                op()
        self.assertIn("lun-x", FakeArrayPlugin.luns)
        self.assertEqual(p.list_luns(), [])


# ------------------------------------------------------------ registration

class TestRegistration(_Case):

    def test_register_stores_credentials_in_kms_only(self):
        db = self.use_db(FakeDB(_cluster(plugin=""), []))
        view = array_controller.register_array(
            CID, PLUGIN, "https://array", {"az": "z"}, {"password": "pw"}, lun_size=GIB)
        cl = db.cluster
        self.assertTrue(cl.array_id)
        self.assertEqual((cl.array_plugin, cl.array_endpoint, cl.array_options, cl.array_lun_size),
                         (PLUGIN, "https://array", {"az": "z"}, GIB))
        self.assertTrue(cl.array_has_credentials)
        self.assertEqual(FakeKMS.store[array_secret_path(CID, cl.array_id)], {"password": "pw"})
        self.assertNotIn("pw", json.dumps(cl.to_dict(unwrap_secrets=True), default=str))
        self.assertNotIn("credentials", view)
        self.assertNotIn("pw", json.dumps(view))
        self.assertEqual(view["capabilities"]["max_attachments"], 16)
        self.assertEqual(array_controller.get_arrays(CID)[0]["id"], cl.array_id)
        # The connect saw the credentials wrapped.
        self.assertIsInstance(FakeArrayPlugin.connected[-1][1]["password"], SecretStr)

    def test_reregister_keeps_id_and_drops_old_secret(self):
        db = self.use_db(FakeDB(_cluster(plugin=""), []))
        array_controller.register_array(CID, PLUGIN, credentials={"password": "pw"})
        aid = db.cluster.array_id
        array_controller.register_array(CID, PLUGIN)
        self.assertEqual(db.cluster.array_id, aid)
        self.assertFalse(db.cluster.array_has_credentials)
        self.assertEqual(FakeKMS.store, {})

    def test_refusals_save_nothing(self):
        db = self.use_db(FakeDB(_cluster(plugin="", multi=False), []))
        with self.assertRaisesRegex(ValueError, "multi-attach"):
            array_controller.register_array(CID, PLUGIN)
        db.cluster.multi_attach = True
        FakeArrayPlugin.fail.add("connect")
        with self.assertRaisesRegex(ValueError, "connect"):
            array_controller.register_array(CID, PLUGIN, credentials={"p": "x"})
        FakeArrayPlugin.fail.clear()
        FakeArrayPlugin.max_attachments = 2  # FTT2 needs 3 hosts per LUN
        with self.assertRaisesRegex(ValueError, "at most 2"):
            array_controller.register_array(CID, PLUGIN)
        with self.assertRaisesRegex(ValueError, "no array plugin"):
            array_controller.register_array(CID, "missing")
        self.assertEqual((db.cluster.array_plugin, db.cluster.array_id, FakeKMS.store), ("", "", {}))

    def test_delete(self):
        a = _node("a")
        db = self.use_db(FakeDB(_cluster(plugin=""), [a]))
        array_controller.register_array(CID, PLUGIN, credentials={"p": "x"})
        aid = db.cluster.array_id
        with self.assertRaises(KeyError):
            array_controller.delete_array(CID, "other")
        _lun_device(a, "lun-1", "w")
        with self.assertRaisesRegex(ValueError, "LUN"):
            array_controller.delete_array(CID, aid)
        array_controller.delete_array(CID, aid, force=True)
        self.assertEqual((db.cluster.array_plugin, db.cluster.array_id), ("", ""))
        self.assertEqual(FakeKMS.store, {})
        self.assertEqual(array_controller.get_arrays(CID), [])


# ------------------------------------------------------------- node add

class TestProvisioning(_Case):

    def _api(self, appear_after=1):
        api = MagicMock()
        calls = {"n": 0}

        def inv():
            calls["n"] += 1
            disks = [{"type": "disk", "name": "nvme0n1", "serial": "local", "size": GIB}]
            if calls["n"] > appear_after:
                lun = next(iter(FakeArrayPlugin.luns.values()))
                disks.append({"type": "disk", "name": "nvme3n1", "serial": lun["wwn"],
                              "wwn": "eui.x", "size": lun["size"], "by_id_path": "/dev/disk/by-id/x",
                              "numa_node": 0})
            return disks, None
        api.get_blockdevices.side_effect = inv
        return api

    def test_provision_creates_attaches_and_waits(self):
        cl = _cluster(lun_size=20 * GIB)
        info = {"hostname": "worker-1", "system_id": "sys",
                "cloud_instance": {"cloud": "amazon", "id": "i-0abc"}}
        entry, host = array_controller.provision_node_lun(
            cl, self._api(), info, {"socket": 0}, [{"serial": "local", "name": "nvme0n1"}])
        lun = next(iter(FakeArrayPlugin.luns.values()))
        self.assertEqual(host, "host-worker-1")
        self.assertEqual(FakeArrayPlugin.hosts[host]["initiators"]["ec2_instance_id"], "i-0abc")
        self.assertEqual(lun["name"], "simplyblock-worker-1-s0")
        self.assertEqual(lun["size"], 20 * GIB)
        self.assertEqual(lun["hosts"], [host])
        self.assertEqual((entry["serial"], entry["lun_wwn"], entry["array_lun_id"], entry["name"]),
                         (lun["wwn"], lun["wwn"], lun["id"], "nvme3n1"))
        # A retried add reuses the LUN.
        entry2, _ = array_controller.provision_node_lun(
            cl, self._api(appear_after=0), info, {"socket": 0}, [entry])
        self.assertEqual(entry2["array_lun_id"], lun["id"])
        self.assertEqual(len(FakeArrayPlugin.luns), 1)

    def test_lun_that_never_appears_fails(self):
        cl = _cluster(lun_size=GIB)
        with self.assertRaisesRegex(array_controller.ArrayLifecycleError, "did not appear"):
            array_controller.provision_node_lun(cl, self._api(appear_after=10 ** 6),
                                                {"hostname": "h"}, {}, [], timeout=0)

    def test_array_failure_fails(self):
        FakeArrayPlugin.fail.add("create_lun")
        with self.assertRaisesRegex(array_controller.ArrayLifecycleError, "array"):
            array_controller.provision_node_lun(_cluster(lun_size=GIB), self._api(),
                                                {"hostname": "h"}, {}, [])

    def test_config_entry_and_journal_flag(self):
        entry = {"serial": "vol0a", "array_lun_id": "vol-0a", "lun_wwn": "vol0a"}
        out = array_controller.add_lun_to_config([{"serial": "local"}], entry)
        self.assertEqual(out, [{"serial": "local", "journal": True}, entry])
        out = array_controller.add_lun_to_config(out, entry)  # re-add: replaced, not doubled
        self.assertEqual(len(out), 2)
        two = array_controller.add_lun_to_config([{"serial": "a"}, {"serial": "b"}], entry)
        self.assertFalse(any(e.get("journal") for e in two))

    def test_resolve_and_mark_carry_the_array_identity(self):
        inv = [{"name": "nvme3n1", "device_path": "/dev/nvme3n1", "serial": "vol0a", "wwn": "eui.1",
                "size": GIB, "type": "disk"}]
        resolved, missing = utils.resolve_lblk_entries(
            [{"serial": "vol0a", "lun_wwn": "vol0a", "array_lun_id": "vol-0a"}], inv)
        self.assertEqual(missing, [])
        self.assertEqual((resolved[0]["lun_wwn"], resolved[0]["array_lun_id"]), ("vol0a", "vol-0a"))
        node = _node("a")
        dev = NVMeDevice()
        dev.serial_number = "vol0a"
        multi_attach.mark_lun_devices(_cluster(), node, [dev], resolved)
        self.assertEqual((dev.lun_wwn, dev.array_lun_id), ("vol0a", "vol-0a"))
        self.assertEqual(multi_attach.array_lun_ref(dev), "vol-0a")

    def test_gates(self):
        self.assertFalse(array_controller.provisions_luns(_cluster(lun_size=0)))
        self.assertFalse(array_controller.provisions_luns(_cluster(plugin="", lun_size=GIB)))
        self.assertFalse(array_controller.provisions_luns(_cluster(multi=False, lun_size=GIB)))
        self.assertTrue(array_controller.provisions_luns(_cluster(lun_size=GIB)))


# ------------------------------------------------------------ reconcile

class TestReconcile(_Case):

    def _bed(self):
        a = _node("a", "b", "c")
        b, c, d = _node("b"), _node("c"), _node("d")
        db = self.use_db(FakeDB(_cluster(), [a, b, c, d]))
        p = self.plugin(db.cluster)
        lun = p.create_lun("a", 10 * GIB, {})
        a.array_host_id = p.ensure_host("a", {})
        p.attach(lun["id"], a.array_host_id)
        dev = _lun_device(a, lun["id"], lun["wwn"])
        return db, p, a, dev

    def test_attaches_roles_and_records_paths(self):
        db, p, a, dev = self._bed()
        rep = array_controller.reconcile_lun_attachments(CID)
        self.assertEqual(rep["errors"], {})
        self.assertEqual(sorted(p.get(dev.array_lun_id)["hosts"]), ["host-a", "host-b", "host-c"])
        self.assertEqual(sorted(dev.array_attached_node_ids), ["a", "b", "c"])
        self.assertEqual(dev.node_paths["b"]["path"], "/dev/disk/by-id/lun-b")
        self.assertIn("a", dev.node_paths)
        self.assertEqual(db.nodes["b"].array_host_id, "host-b")
        # Steady state: no array call at all.
        FakeArrayPlugin.calls.clear()
        FakeArrayPlugin.connected.clear()
        array_controller.reconcile_lun_attachments(CID)
        self.assertEqual((FakeArrayPlugin.calls, FakeArrayPlugin.connected), ([], []))

    def test_role_move_attaches_first_then_detaches(self):
        db, p, a, dev = self._bed()
        array_controller.reconcile_lun_attachments(CID)
        a.tertiary_node_id = "d"
        FakeArrayPlugin.calls.clear()
        array_controller.reconcile_lun_attachments(CID)
        ops = [c[:3] for c in FakeArrayPlugin.calls if c[0] in ("attach", "detach")]
        self.assertEqual(ops, [("attach", dev.array_lun_id, "host-d"),
                               ("detach", dev.array_lun_id, "host-c")])
        self.assertNotIn("c", dev.node_paths)
        self.assertIn("d", dev.node_paths)

    def test_serving_node_is_never_detached_fenced_never_attached(self):
        db, p, a, dev = self._bed()
        array_controller.reconcile_lun_attachments(CID)
        # A takeover moved the LUN to c and fenced b.
        a.status = StorageNode.STATUS_OFFLINE
        dev.serving_node_id = "c"
        dev.fenced_node_ids = ["b"]
        p.fence(dev.array_lun_id, "host-b")
        a.secondary_node_id, a.tertiary_node_id = "d", ""
        array_controller.reconcile_lun_attachments(CID)
        hosts = p.get(dev.array_lun_id)["hosts"]
        self.assertIn("host-c", hosts)        # serving, lost its role: kept
        self.assertNotIn("host-b", hosts)     # fenced: not re-attached
        self.assertIn("host-d", hosts)
        self.assertIn("c", dev.node_paths)

    def test_not_visible_yet_is_retried(self):
        db, p, a, dev = self._bed()
        db.nodes["b"].agent.lun_info.return_value = ([], None)
        rep = array_controller.reconcile_lun_attachments(CID, path_timeout=0)
        self.assertIn(dev.get_id(), rep["errors"])
        self.assertNotIn("b", dev.node_paths)
        db.nodes["b"].agent.lun_info.return_value = ([{"by_id_path": "/dev/disk/by-id/z"}], None)
        array_controller.reconcile_lun_attachments(CID)
        self.assertEqual(dev.node_paths["b"]["path"], "/dev/disk/by-id/z")

    def test_noop_without_array_and_for_operator_luns(self):
        db, p, a, dev = self._bed()
        db.cluster.array_plugin = ""
        FakeArrayPlugin.calls.clear()
        array_controller.reconcile_lun_attachments(CID)
        db.cluster.array_plugin = PLUGIN
        dev.array_lun_id = ""   # presented by the operator: not ours
        array_controller.reconcile_lun_attachments(CID)
        self.assertEqual(FakeArrayPlugin.calls, [])

    def test_failure_of_one_lun_is_reported(self):
        db, p, a, dev = self._bed()
        FakeArrayPlugin.fail.add("attach")
        rep = array_controller.reconcile_after_role_change(CID)
        self.assertIn(dev.get_id(), rep["errors"])
        array_controller._event.assert_called()


# --------------------------------------------------------------- growth

class TestGrowth(_Case):

    def test_grow_by_factor_and_wait_visible(self):
        a = _node("a", "b")
        b = _node("b")
        db = self.use_db(FakeDB(_cluster(), [a, b]))
        p = self.plugin(db.cluster)
        lun = p.create_lun("a", 10 * GIB, {})
        dev = _lun_device(a, lun["id"], lun["wwn"])
        dev.node_paths["b"] = {"path": "/dev/disk/by-id/lun-b", "bdev": ""}
        a.agent.rescan_block_device.return_value = ({"size": 15 * GIB}, None)
        b.agent.rescan_block_device.side_effect = [({"size": 10 * GIB}, None), ({"size": 15 * GIB}, None)]
        out = array_controller.grow_luns(CID, 1.5)
        self.assertEqual(out["errors"], {})
        self.assertEqual(out["luns"][dev.get_id()]["new_size"], 15 * GIB)
        self.assertEqual(FakeArrayPlugin.luns[lun["id"]]["size"], 15 * GIB)
        self.assertEqual(b.agent.rescan_block_device.call_count, 2)

    def test_refusals_and_errors(self):
        a = _node("a")
        db = self.use_db(FakeDB(_cluster(), [a]))
        for bad in (1, 0.5, "x", float("inf")):
            with self.assertRaises(ValueError):
                array_controller.grow_luns(CID, bad)
        p = self.plugin(db.cluster)
        lun = p.create_lun("a", GIB, {})
        dev = _lun_device(a, lun["id"], lun["wwn"])
        a.agent.rescan_block_device.return_value = ({"size": GIB}, None)
        out = array_controller.grow_luns(CID, 2, visible_timeout=0)
        self.assertIn("not visible", out["errors"][dev.get_id()])
        FakeArrayPlugin.fail.add("resize")
        out = array_controller.grow_luns(CID, 2)
        self.assertIn("array resize", out["errors"][dev.get_id()])
        db.cluster.array_plugin = ""
        with self.assertRaisesRegex(ValueError, "array"):
            array_controller.grow_luns(CID, 2)

    def test_a_failed_lun_grows_nothing(self):
        # Growth is uniform (PR #1572): a LUN that did not grow at the array
        # would cap the factor at 1, so nothing on the cluster is grown.
        with patch.object(array_controller, "grow_luns",
                          return_value={"luns": {}, "errors": {"d1": "array resize failed"}}),              patch.object(device_controller, "_grow_cluster_devices") as inner:
            out = device_controller.grow_cluster_devices(CID, lun_factor=2)
        inner.assert_not_called()
        self.assertFalse(out["grown"])
        self.assertEqual(out["errors"], {"d1": "array resize failed"})
        self.assertEqual(out["luns"], {})

    def test_grown_luns_then_uniform_growth(self):
        luns = {"d1": {"lun_id": "vol-1", "old_size": 1, "new_size": 2}}
        with patch.object(array_controller, "grow_luns", return_value={"luns": luns, "errors": {}}),              patch.object(device_controller, "_grow_cluster_devices",
                          return_value={"grown": True, "factor": 2.0, "devices": [], "errors": {},
                                        "reason": "", "shrunk": []}) as inner:
            out = device_controller.grow_cluster_devices(CID, lun_factor=2)
        inner.assert_called_once_with(CID)
        self.assertTrue(out["grown"])
        self.assertEqual(out["luns"], luns)

    def test_served_elsewhere_is_not_resized_on_the_owner(self):
        dev = NVMeDevice()
        dev.multi_attach, dev.node_id, dev.serving_node_id = True, "a", "b"
        dev.status, dev.bdev_type, dev.alceml_bdev, dev.nvme_bdev = NVMeDevice.STATUS_ONLINE, "aio", "al", "nv"
        snode = StorageNode()
        snode.uuid, snode.status = "a", StorageNode.STATUS_ONLINE
        cluster = MagicMock(device_mode=device_controller.constants.DEVICE_MODE_LBLK)
        with patch.object(device_controller.tasks_controller, "get_active_dev_restart_task", return_value=None):
            with self.assertRaisesRegex(ValueError, "served by node b"):
                device_controller._check_resizable(cluster, dev, snode)


# --------------------------------------------------------- release / delete

class TestReleaseAndDelete(_Case):

    def test_release_node_detaches_without_deleting(self):
        a, b = _node("a", "b"), _node("b")
        db = self.use_db(FakeDB(_cluster(), [a, b]))
        p = self.plugin(db.cluster)
        la = p.create_lun("a", GIB, {})
        lb = p.create_lun("b", GIB, {})
        a.array_host_id, b.array_host_id = "host-a", "host-b"
        for lun in (la, lb):
            p.attach(lun["id"], "host-a")
            p.attach(lun["id"], "host-b")
        _lun_device(a, la["id"], la["wwn"])
        _lun_device(b, lb["id"], lb["wwn"])
        array_controller.release_node_luns(a)
        self.assertEqual(p.get(la["id"])["hosts"], [])
        self.assertEqual(p.get(lb["id"])["hosts"], ["host-b"])
        self.assertEqual(len(FakeArrayPlugin.luns), 2)

    def test_release_never_detaches_a_lun_the_node_serves(self):
        a, b = _node("a"), _node("b")
        db = self.use_db(FakeDB(_cluster(), [a, b]))
        p = self.plugin(db.cluster)
        lb = p.create_lun("b", GIB, {})
        a.array_host_id = "host-a"
        p.attach(lb["id"], "host-a")
        dev = _lun_device(b, lb["id"], lb["wwn"])
        dev.serving_node_id = "a"
        array_controller.release_node_luns(a)
        self.assertEqual(p.get(lb["id"])["hosts"], ["host-a"])

    def test_delete_cluster_deletes_only_its_luns_and_secret(self):
        cl = _cluster()
        cl.array_id, cl.array_has_credentials = "arr", True
        FakeKMS.store[array_secret_path(CID, "arr")] = {"k": "v"}
        p = self.plugin(cl)
        lun = p.create_lun("a", GIB, {})
        p.attach(lun["id"], "h")
        FakeArrayPlugin().add_foreign_lun("lun-foreign", tags={array.TAG_CLUSTER_ID: "other"})
        array_controller.delete_cluster_luns(cl)
        self.assertEqual(sorted(FakeArrayPlugin.luns), ["lun-foreign"])
        self.assertEqual(FakeKMS.store, {})

    def test_delete_cluster_failure(self):
        cl = _cluster()
        p = self.plugin(cl)
        p.create_lun("a", GIB, {})
        FakeArrayPlugin.fail.add("delete_lun")
        with self.assertRaisesRegex(ValueError, "could not be deleted"):
            array_controller.delete_cluster_luns(cl)
        array_controller.delete_cluster_luns(cl, force=True)  # logged, goes on

    def test_noop_on_regular_cluster(self):
        array_controller.delete_cluster_luns(_cluster(multi=False))
        array_controller.release_node_luns(_node("a"))
        self.assertEqual(FakeArrayPlugin.calls, [])


# ------------------------------------------------------------ takeover

class TestTakeoverFenceIds(unittest.TestCase):

    def test_fence_uses_array_ids(self):
        from simplyblock_core.controllers import lun_takeover
        FakeArrayPlugin.reset()
        array.register_plugin(PLUGIN, FakeArrayPlugin)
        self.addCleanup(array.unregister_plugin, PLUGIN)
        failed = _node("a")
        failed.array_host_id = "i-0abc"
        dev = NVMeDevice()
        dev.lun_wwn, dev.array_lun_id = "vol0abc", "vol-0abc"
        lun_takeover._fence(_cluster(), dev, failed)
        self.assertEqual(FakeArrayPlugin.calls, [("fence", "vol-0abc", "i-0abc")])
        failed.array_host_id, dev.array_lun_id = "", ""
        lun_takeover._fence(_cluster(), dev, failed)
        self.assertEqual(FakeArrayPlugin.calls[-1], ("fence", "vol0abc", "a"))


# ------------------------------------------------------------ AWS EBS

class TestAwsEbs(unittest.TestCase):

    def setUp(self):
        import boto3
        from botocore.stub import Stubber
        self.ec2 = boto3.session.Session(
            aws_access_key_id="x", aws_secret_access_key="y", region_name="us-east-1").client("ec2")
        self.stub = Stubber(self.ec2)
        self.stub.activate()
        self.addCleanup(self.stub.deactivate)
        self.p = aws_ebs.AwsEbsPlugin(options={"az": "us-east-1a", "poll_interval": 0, "timeout": 5},
                                      cluster_id=CID)
        self.p.ec2 = self.ec2
        self.p._sleep = lambda s: None

    def _vol(self, vid="vol-0abc", state="available", size=10, atts=(), tags=None):
        if tags is None:
            tags = {array.TAG_CLUSTER_ID: CID}
        return {"VolumeId": vid, "State": state, "Size": size, "AvailabilityZone": "us-east-1a",
                "Attachments": [{"InstanceId": i, "State": s, "VolumeId": vid} for i, s in atts],
                "Tags": [{"Key": k, "Value": v} for k, v in tags.items()]}

    def _describe(self, vol, vid="vol-0abc"):
        self.stub.add_response("describe_volumes", {"Volumes": [vol]}, {"VolumeIds": [vid]})

    def test_ids(self):
        self.assertEqual(aws_ebs.volume_wwn("vol-0abc"), "vol0abc")
        self.assertEqual(aws_ebs.volume_id_of("vol0abc"), "vol-0abc")
        self.assertEqual(aws_ebs.volume_id_of("vol-0abc"), "vol-0abc")
        with self.assertRaises(array.ArrayError):
            aws_ebs.volume_id_of("0x600a")

    def test_create_new_volume(self):
        self.stub.add_response("describe_volumes", {"Volumes": []})
        self.stub.add_response("create_volume", {"VolumeId": "vol-0abc", "State": "creating"}, {
            "AvailabilityZone": "us-east-1a", "Size": 20, "VolumeType": "io2", "Iops": 3000,
            "MultiAttachEnabled": True, "Encrypted": True,
            "TagSpecifications": [{"ResourceType": "volume", "Tags": [
                {"Key": "Name", "Value": "lun-a"},
                {"Key": "role", "Value": "data"},
                {"Key": array.TAG_CLUSTER_ID, "Value": CID},
                {"Key": array.TAG_LUN_NAME, "Value": "lun-a"}]}]})
        self._describe(self._vol(state="creating", size=20))
        self._describe(self._vol(size=20))
        out = self.p.create_lun("lun-a", int(19.5 * GIB), {"role": "data"})
        self.assertEqual(out, {"id": "vol-0abc", "wwn": "vol0abc", "size": 20 * GIB})
        self.stub.assert_no_pending_responses()

    def test_create_finds_existing(self):
        self.stub.add_response("describe_volumes", {"Volumes": [self._vol()]})
        self._describe(self._vol())
        self.assertEqual(self.p.create_lun("lun-a", GIB, {})["id"], "vol-0abc")
        self.stub.assert_no_pending_responses()

    def test_attach_picks_a_free_device_and_waits(self):
        self._describe(self._vol(atts=[("i-other", "attached")]))
        self.stub.add_response("describe_instances", {"Reservations": [{"Instances": [{
            "InstanceId": "i-1", "RootDeviceName": "/dev/xvda",
            "BlockDeviceMappings": [{"DeviceName": "/dev/sdf"}]}]}]}, {"InstanceIds": ["i-1"]})
        self.stub.add_response("attach_volume", {}, {"VolumeId": "vol-0abc", "InstanceId": "i-1",
                                                     "Device": "/dev/sdg"})
        self._describe(self._vol(atts=[("i-other", "attached"), ("i-1", "attaching")]))
        self._describe(self._vol(atts=[("i-other", "attached"), ("i-1", "attached")]))
        self.p.attach("vol0abc", "i-1")
        self.stub.assert_no_pending_responses()

    def test_attach_already_attached_is_a_noop(self):
        self._describe(self._vol(atts=[("i-1", "attached")]))
        self._describe(self._vol(atts=[("i-1", "attached")]))
        self.p.attach("vol-0abc", "i-1")
        self.stub.assert_no_pending_responses()

    def test_fence_is_forced_detach_and_waits_until_gone(self):
        self._describe(self._vol(atts=[("i-1", "attached"), ("i-2", "attached")]))
        self.stub.add_response("detach_volume", {}, {"VolumeId": "vol-0abc", "InstanceId": "i-1",
                                                     "Force": True})
        self._describe(self._vol(atts=[("i-1", "detaching"), ("i-2", "attached")]))
        self._describe(self._vol(atts=[("i-2", "attached")]))
        self.assertTrue(self.p.fence("vol-0abc", "i-1"))
        self.stub.assert_no_pending_responses()

    def test_detach_not_attached_is_a_noop(self):
        self._describe(self._vol(atts=[("i-2", "attached")]))
        self.p.detach("vol-0abc", "i-1")
        self.stub.assert_no_pending_responses()

    def test_detach_timeout(self):
        self.p.options["timeout"] = 0
        self._describe(self._vol(atts=[("i-1", "attached")]))
        self.stub.add_response("detach_volume", {}, {"VolumeId": "vol-0abc", "InstanceId": "i-1",
                                                     "Force": False})
        self._describe(self._vol(atts=[("i-1", "busy")]))
        with self.assertRaisesRegex(array.ArrayError, "timed out"):
            self.p.detach("vol-0abc", "i-1")

    def test_resize_grows_and_waits_for_optimizing(self):
        self._describe(self._vol(size=10))
        self.stub.add_response("modify_volume", {}, {"VolumeId": "vol-0abc", "Size": 15})
        self.stub.add_response("describe_volumes_modifications", {"VolumesModifications": [
            {"VolumeId": "vol-0abc", "ModificationState": "modifying"}]}, {"VolumeIds": ["vol-0abc"]})
        self.stub.add_response("describe_volumes_modifications", {"VolumesModifications": [
            {"VolumeId": "vol-0abc", "ModificationState": "optimizing"}]}, {"VolumeIds": ["vol-0abc"]})
        self.p.resize("vol-0abc", 15 * GIB)
        self.stub.assert_no_pending_responses()

    def test_resize_never_shrinks(self):
        self._describe(self._vol(size=10))
        with self.assertRaisesRegex(array.ArrayError, "shrinking"):
            self.p.resize("vol-0abc", 5 * GIB)

    def test_resize_failed_modification(self):
        self._describe(self._vol(size=10))
        self.stub.add_response("modify_volume", {}, {"VolumeId": "vol-0abc", "Size": 11})
        self.stub.add_response("describe_volumes_modifications", {"VolumesModifications": [
            {"VolumeId": "vol-0abc", "ModificationState": "failed", "StatusMessage": "nope"}]})
        with self.assertRaisesRegex(array.ArrayError, "failed"):
            self.p.resize("vol-0abc", 11 * GIB)

    def test_foreign_volume_is_refused(self):
        for tags in ({}, {array.TAG_CLUSTER_ID: "other"}):
            self._describe(self._vol(atts=[("i-1", "attached")], tags=tags))
            with self.assertRaises(array.ForeignLunError):
                self.p.detach("vol-0abc", "i-1", force=True)
            self._describe(self._vol(tags=tags))
            with self.assertRaises(array.ForeignLunError):
                self.p.delete_lun("vol-0abc")
        self.stub.assert_no_pending_responses()

    def test_get_and_list(self):
        self._describe(self._vol(size=8, atts=[("i-1", "attached"), ("i-2", "detached")]))
        self.assertEqual(self.p.get("vol-0abc"),
                         {"id": "vol-0abc", "wwn": "vol0abc", "size": 8 * GIB, "hosts": ["i-1"],
                          "state": "available"})
        self.stub.add_response("describe_volumes", {"Volumes": [self._vol(), self._vol("vol-1", "deleting")]},
                               {"Filters": [{"Name": f"tag:{array.TAG_CLUSTER_ID}", "Values": [CID]}]})
        self.assertEqual([x["id"] for x in self.p.list_luns()], ["vol-0abc"])

    def test_delete_detaches_then_deletes(self):
        self._describe(self._vol(state="in-use", atts=[("i-1", "attached")]))
        self._describe(self._vol(state="in-use", atts=[("i-1", "attached")]))
        self.stub.add_response("detach_volume", {}, {"VolumeId": "vol-0abc", "InstanceId": "i-1",
                                                     "Force": True})
        self._describe(self._vol())
        self._describe(self._vol())
        self.stub.add_response("delete_volume", {}, {"VolumeId": "vol-0abc"})
        self.p.delete_lun("vol-0abc")
        self.stub.assert_no_pending_responses()

    def test_delete_missing_is_a_noop(self):
        self.stub.add_client_error("describe_volumes", "InvalidVolume.NotFound")
        self.p.delete_lun("vol-0abc")

    def test_ensure_host_checks_zone(self):
        self.stub.add_response("describe_instances", {"Reservations": [{"Instances": [{
            "InstanceId": "i-1", "Placement": {"AvailabilityZone": "us-east-1a"}}]}]})
        self.assertEqual(self.p.ensure_host("w1", {"ec2_instance_id": "i-1"}), "i-1")
        self.stub.add_response("describe_instances", {"Reservations": [{"Instances": [{
            "InstanceId": "i-2", "Placement": {"AvailabilityZone": "us-east-1b"}}]}]})
        with self.assertRaisesRegex(array.ArrayError, "cannot cross zones"):
            self.p.ensure_host("w2", {"ec2_instance_id": "i-2"})
        with self.assertRaisesRegex(array.ArrayError, "no EC2 instance id"):
            self.p.ensure_host("w3", {"hostname": "w3"})

    def test_ensure_host_by_private_ip(self):
        # Docker deployments start the node agent with WITHOUT_CLOUD_INFO, so
        # the instance is found by the node's private IP within the zone.
        expected = {"Filters": [
            {"Name": "private-ip-address", "Values": ["172.31.36.102"]},
            {"Name": "availability-zone", "Values": ["us-east-1a"]},
            {"Name": "instance-state-name", "Values": ["pending", "running"]}]}
        self.stub.add_response("describe_instances", {"Reservations": [{"Instances": [{
            "InstanceId": "i-7"}]}]}, expected)
        self.stub.add_response("describe_instances", {"Reservations": [{"Instances": [{
            "InstanceId": "i-7", "Placement": {"AvailabilityZone": "us-east-1a"}}]}]})
        self.assertEqual(self.p.ensure_host("w1", {"hostname": "w1", "ip": "172.31.36.102"}), "i-7")
        self.stub.assert_no_pending_responses()

    def test_ensure_host_by_ip_refuses_none_or_several(self):
        self.stub.add_response("describe_instances", {"Reservations": []})
        with self.assertRaisesRegex(array.ArrayError, "0 running instances"):
            self.p.ensure_host("w1", {"ip": "10.0.0.9"})
        self.stub.add_response("describe_instances", {"Reservations": [
            {"Instances": [{"InstanceId": "i-1"}, {"InstanceId": "i-2"}]}]})
        with self.assertRaisesRegex(array.ArrayError, "2 running instances"):
            self.p.ensure_host("w1", {"ip": "10.0.0.9"})

    def test_host_initiators_carry_the_ip(self):
        out = array_controller.host_initiators("w1", {}, "", "172.31.36.102")
        self.assertEqual(out, {"hostname": "w1", "ip": "172.31.36.102"})

    def test_connect(self):
        p = aws_ebs.AwsEbsPlugin(cluster_id=CID)
        with self.assertRaisesRegex(array.ArrayError, "'az'"):
            p.connect("", {}, {})
        with self.assertRaisesRegex(array.ArrayError, "go together"):
            p.connect("", {"aws_access_key_id": SecretStr("k")}, {"az": "us-east-1a"})
        with self.assertRaisesRegex(array.ArrayError, "volume_type"):
            p.connect("", {}, {"az": "us-east-1a", "volume_type": "gp3"})
        with patch("boto3.session.Session") as sess:
            client = sess.return_value.client.return_value
            client.describe_availability_zones.return_value = {"AvailabilityZones": [{"ZoneName": "us-east-1a"}]}
            p.connect("", {"aws_access_key_id": SecretStr("k"), "aws_secret_access_key": SecretStr("s")},
                      {"az": "us-east-1a"})
        sess.assert_called_once_with(aws_access_key_id="k", aws_secret_access_key="s",
                                     aws_session_token=None, region_name="us-east-1")
        sess.return_value.client.assert_called_once_with("ec2", endpoint_url=None)
        self.assertEqual(p.capabilities()["max_attachments"], 16)
        self.assertNotIn("s", repr(p).replace("AwsEbsPlugin", "").replace("cluster", ""))

    def test_instance_role_without_credentials(self):
        p = aws_ebs.AwsEbsPlugin(cluster_id=CID)
        with patch("boto3.session.Session") as sess:
            sess.return_value.client.return_value.describe_availability_zones.return_value = {
                "AvailabilityZones": [{}]}
            p.connect("https://ec2.vpce", {}, {"az": "eu-west-1b"})
        sess.assert_called_once_with(aws_access_key_id=None, aws_secret_access_key=None,
                                     aws_session_token=None, region_name="eu-west-1")
        sess.return_value.client.assert_called_once_with("ec2", endpoint_url="https://ec2.vpce")


# ----------------------------------------------------------- front-ends

class TestApi(unittest.TestCase):

    def _client(self):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        from simplyblock_web.api.v2._dependencies import _lookup_cluster
        from simplyblock_web.api.v2.cluster import array as array_api
        app = FastAPI()
        app.include_router(array_api.api, prefix="/clusters/{cluster_id}/arrays")
        app.dependency_overrides[_lookup_cluster] = lambda: _cluster()
        return TestClient(app)

    def test_post_get_delete(self):
        view = {"id": "arr", "plugin": "aws-ebs", "endpoint": "", "options": {"az": "z"},
                "lun_size": GIB, "has_credentials": True, "capabilities": {"max_attachments": 16}}
        cid = "00000000-0000-0000-0000-000000000001"
        with patch.object(array_controller, "register_array", return_value=view) as reg, \
             patch.object(array_controller, "get_arrays", return_value=[dict(view, capabilities=None)]), \
             patch.object(array_controller, "delete_array") as dele:
            c = self._client()
            r = c.post(f"/clusters/{cid}/arrays/", json={
                "plugin": "aws-ebs", "options": {"az": "z"}, "lun_size": "1G",
                "credentials": {"aws_secret_access_key": "TOPSECRET"}})
            self.assertEqual(r.status_code, 201, r.text)
            self.assertNotIn("TOPSECRET", r.text)
            args = reg.call_args.args
            self.assertIsInstance(args[4]["aws_secret_access_key"], SecretStr)
            self.assertEqual(args[5], utils.parse_size("1G"))
            r = c.get(f"/clusters/{cid}/arrays/")
            self.assertEqual(r.json()[0]["id"], "arr")
            self.assertNotIn("credentials", r.json()[0])
            self.assertEqual(c.get(f"/clusters/{cid}/arrays/arr").status_code, 200)
            self.assertEqual(c.get(f"/clusters/{cid}/arrays/nope").status_code, 404)
            self.assertEqual(c.delete(f"/clusters/{cid}/arrays/arr?force=true").status_code, 204)
            dele.assert_called_once_with(_cluster().get_id(), "arr", force=True)
            dele.side_effect = ValueError("LUNs exist")
            self.assertEqual(c.delete(f"/clusters/{cid}/arrays/arr").status_code, 409)
            reg.side_effect = ValueError("bad plugin")
            self.assertEqual(c.post(f"/clusters/{cid}/arrays/", json={"plugin": "x"}).status_code, 422)


class TestCli(unittest.TestCase):

    def test_set_array_reads_and_wraps_credentials(self):
        from simplyblock_cli.clibase import CLIWrapperBase
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
            json.dump({"aws_access_key_id": "AK", "aws_secret_access_key": "SK"}, f)
        self.addCleanup(os.unlink, f.name)
        args = MagicMock(cluster_id=CID, plugin="aws-ebs", endpoint="", options='{"az": "z"}',
                         credentials_file=f.name, lun_size=GIB, json=True)
        with patch.object(array_controller, "register_array", return_value={"id": "arr"}) as reg:
            CLIWrapperBase.cluster__set_array(MagicMock(), "set-array", args)
        a = reg.call_args.args
        self.assertEqual((a[0], a[1], a[3], a[5]), (CID, "aws-ebs", {"az": "z"}, GIB))
        self.assertEqual(a[4]["aws_secret_access_key"].get_secret_value(), "SK")
        self.assertNotIn("SK", repr(a[4]))

    def test_bad_credentials_file_does_not_echo_it(self):
        from simplyblock_cli.clibase import CLIWrapperBase
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
            f.write("SECRET-NOT-JSON")
        self.addCleanup(os.unlink, f.name)
        args = MagicMock(options=None, credentials_file=f.name)
        with self.assertRaises(ValueError) as cm:
            CLIWrapperBase.cluster__set_array(MagicMock(), "set-array", args)
        self.assertNotIn("SECRET", str(cm.exception))


# ------------------------------------------------------------- agent

class TestAgent(unittest.TestCase):

    def test_amazon_info_reports_the_zone(self):
        from simplyblock_web.api.internal import _node_info
        doc = {"instanceId": "i-1", "instanceType": "m5", "privateIp": "10.0.0.1",
               "availabilityZone": "us-east-1a", "region": "us-east-1"}
        token = MagicMock(text="tok")
        ident = MagicMock()
        ident.json.return_value = doc
        with patch.object(_node_info.requests, "put", return_value=token) as put,                 patch.object(_node_info.requests, "get", return_value=ident) as get:
            info = _node_info._amazon_info()
        self.assertEqual((info["id"], info["availability_zone"], info["region"]),
                         ("i-1", "us-east-1a", "us-east-1"))
        self.assertIn("/latest/api/token", put.call_args.args[0])
        self.assertEqual(get.call_args.kwargs["headers"], {"X-aws-ec2-metadata-token": "tok"})

    def test_amazon_info_none_off_aws(self):
        from simplyblock_web.api.internal import _node_info
        with patch.object(_node_info.requests, "put", side_effect=OSError("no route")):
            self.assertIsNone(_node_info._amazon_info())

    def test_rescan_dm_multipath_map(self):
        from simplyblock_web import node_utils
        root = tempfile.mkdtemp()
        self.addCleanup(__import__("shutil").rmtree, root)

        def mk(rel, content=""):
            path = os.path.join(root, rel)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w") as fh:
                fh.write(content)
        mk("sys/class/block/dm-2/size", "4096")
        mk("sys/class/block/dm-2/dm/name", "mpatha")
        mk("sys/class/block/dm-2/dm/uuid", "mpath-3600a0")
        mk("sys/class/block/dm-2/slaves/sdb/x")
        mk("sys/class/block/dm-2/slaves/sdc/x")
        mk("sys/class/block/sdb/device/rescan")
        mk("sys/class/block/sdc/device/rescan")

        def _map(p):
            return os.path.join(root, p.lstrip("/")) if p.startswith("/sys/") else p
        real_exists, real_isdir, real_listdir = os.path.exists, os.path.isdir, os.listdir
        writes = []
        with patch.object(node_utils.os.path, "realpath", return_value="/dev/dm-2"), \
             patch.object(node_utils.os.path, "exists", side_effect=lambda p: real_exists(_map(p))), \
             patch.object(node_utils.os.path, "isdir", side_effect=lambda p: real_isdir(_map(p))), \
             patch.object(node_utils.os, "listdir", side_effect=lambda p: real_listdir(_map(p))), \
             patch.object(node_utils, "_read_sysfs",
                          side_effect=lambda p: open(_map(p)).read().strip() if real_exists(_map(p)) else ""), \
             patch.object(node_utils, "_write_sysfs", side_effect=lambda p, v: writes.append(p)), \
             patch.object(node_utils.shell_utils, "run_command", return_value=("ok", "", 0)) as run:
            res = node_utils.rescan_block_device("/dev/mapper/mpatha")
        self.assertEqual(writes, ["/sys/class/block/sdb/device/rescan", "/sys/class/block/sdc/device/rescan"])
        run.assert_called_once_with("multipathd resize map mpatha")
        self.assertEqual(res["size"], 4096 * 512)
        self.assertIn("multipathd:mpatha", res["rescanned"])


if __name__ == "__main__":
    unittest.main()
