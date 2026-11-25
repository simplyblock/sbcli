"""Multi-attach warm standby (Cluster.multi_attach_warm_standby, opt-in).

  - lun_takeover.ensure_warm_standbys: a standby alceml (standby=true,
    open-only, the reservation key) on attached non-serving nodes only; never
    exported; never on fenced / serving / unattached nodes or on clusters
    without the flag; closed when the node is no longer a standby; refused
    beyond the node's memory budget, with a cluster event.
  - the takeover reuses a warm standby: no alceml create, the export is
    built, the activation is awaited after the path switch; a standby that
    does not activate falls back to the array fence and a cold open.
  - memory: warm-standby LUNs count in the node's restart budget.
  - plumbing: cluster flag, monitor driver.
"""

import os
import time
import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core import (
    cluster_ops,
    constants,
    multi_attach,
    storage_node_ops,
    utils,
)
from simplyblock_core.array.fake import FakeArrayPlugin
from simplyblock_core.controllers import device_controller, lun_takeover
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.services import storage_node_monitor
from tests.unit.test_lun_takeover import BY_ID, DEV_ID, SIZE, WWN, _bed, _TakeoverCase

ALCEML = f"alceml_{DEV_ID}"
BASE = utils.aio_bdev_name_for_serial(WWN)


def _warm_bed(flag=True, reservations=True):
    """_bed with the owner node-a ONLINE and serving its LUN; node-b / node-c
    its LVS secondary / tertiary (attached standbys), node-d a peer."""
    db, dev = _bed()
    db.cluster.multi_attach_warm_standby = flag
    dev.lun_reservations = reservations
    db.nodes["node-a"].status = StorageNode.STATUS_ONLINE
    return db, dev


def _role(spdk, fn):
    spdk.alceml_get_role = fn


def _standby_role(_name):
    return {"role": "standby", "state": "standby", "standby_at_open": True, "warm": True,
            "refreshes_count": 7}


class _WarmCase(_TakeoverCase):

    def ensure(self, db, nid):
        self.m_DBController.return_value = db
        return lun_takeover.ensure_warm_standbys(db.nodes[nid])


# ------------------------------------------------------ keeping standbys

class TestEnsureWarmStandbys(_WarmCase):

    def test_created_on_attached_non_serving_nodes_only(self):
        db, _ = _warm_bed()
        for nid in ("node-a", "node-b", "node-c", "node-d"):
            self.ensure(db, nid)
        for nid in ("node-b", "node-c"):
            spdk = db.nodes[nid].spdk
            calls = spdk.ops("bdev_alceml_create")
            self.assertEqual(len(calls), 1, nid)
            kw = calls[0][2]
            self.assertEqual(calls[0][1][:3], (ALCEML, BASE, DEV_ID))
            self.assertIs(kw["standby"], True)
            self.assertEqual(kw["pba_init_mode"], lun_takeover.ALCEML_OPEN_ONLY)
            self.assertEqual(kw["reservation_key"], multi_attach.reservation_key(db.nodes[nid]))
            self.assertEqual(kw["refresh_interval_ms"], constants.MULTI_ATTACH_WARM_REFRESH_MS)
            self.assertEqual(kw["warm_verify"], constants.MULTI_ATTACH_WARM_VERIFY)
            self.assertEqual(kw["stuck_io_timeout_ms"], constants.MULTI_ATTACH_ALCEML_STUCK_IO_MS)
            # Over the node's own path to the LUN (its agent's lun_info).
            self.assertEqual(spdk.ops("bdev_aio_create")[0][1][:2], (BASE, BY_ID))
            # Never exported: a peer's I/O would activate it.
            self.assertEqual(spdk.ops("bdev_PT_NoExcl_create"), [])
            self.assertEqual(spdk.ops("subsystem_create"), [])
            self.assertEqual(spdk.ops("nvmf_subsystem_add_ns"), [])
        # The serving node and an unattached peer get nothing.
        self.assertEqual(db.nodes["node-a"].spdk.calls, [])
        self.assertEqual(db.nodes["node-d"].spdk.calls, [])
        db.nodes["node-d"].agent.lun_info.assert_not_called()

    def test_idempotent(self):
        db, _ = _warm_bed()
        self.assertEqual(self.ensure(db, "node-b")["created"], [DEV_ID])
        _role(db.nodes["node-b"].spdk, _standby_role)
        self.assertEqual(self.ensure(db, "node-b"), {"created": [], "removed": [], "over_budget": []})
        self.assertEqual(len(db.nodes["node-b"].spdk.ops("bdev_alceml_create")), 1)

    def test_not_on_a_fenced_node(self):
        db, dev = _warm_bed()
        dev.fenced_node_ids = ["node-c"]
        self.ensure(db, "node-c")
        self.assertEqual(db.nodes["node-c"].spdk.calls, [])

    def test_not_on_the_serving_node_after_a_takeover(self):
        db, dev = _warm_bed()
        dev.serving_node_id = "node-b"
        dev.fenced_node_ids = ["node-a"]
        self.ensure(db, "node-b")
        self.assertEqual(db.nodes["node-b"].spdk.ops("bdev_alceml_create"), [])
        # The owner, fenced, gets none either; the tertiary does.
        self.ensure(db, "node-a")
        self.assertEqual(db.nodes["node-a"].spdk.ops("bdev_alceml_create"), [])
        self.ensure(db, "node-c")
        self.assertEqual(len(db.nodes["node-c"].spdk.ops("bdev_alceml_create")), 1)

    def test_not_on_offline_node_or_device(self):
        db, dev = _warm_bed()
        db.nodes["node-b"].status = StorageNode.STATUS_OFFLINE
        self.ensure(db, "node-b")
        self.assertEqual(db.nodes["node-b"].spdk.calls, [])
        db.nodes["node-b"].status = StorageNode.STATUS_ONLINE
        dev.status = "unavailable"
        self.ensure(db, "node-b")
        self.assertEqual(db.nodes["node-b"].spdk.ops("bdev_alceml_create"), [])

    def test_not_without_the_flag_or_reservations(self):
        for flag, res in ((False, True), (True, False)):
            with self.subTest(flag=flag, reservations=res):
                db, _ = _warm_bed(flag=flag, reservations=res)
                for nid in db.nodes:
                    self.ensure(db, nid)
                    self.assertEqual(db.nodes[nid].spdk.calls, [])
                    db.nodes[nid].agent.lun_info.assert_not_called()

    def test_not_while_a_takeover_of_the_lun_runs(self):
        db, _ = _warm_bed()
        lock = lun_takeover._local_lock(DEV_ID)
        with lock:
            self.ensure(db, "node-b")
        self.assertEqual(db.nodes["node-b"].spdk.ops("bdev_alceml_create"), [])

    def test_lun_not_visible_opens_nothing(self):
        db, _ = _warm_bed()
        db.nodes["node-b"].agent.lun_info.return_value = ([], None)
        self.assertEqual(self.ensure(db, "node-b")["created"], [])
        self.assertEqual(db.nodes["node-b"].spdk.calls, [])

    def test_failed_open_closes_the_base_bdev(self):
        db, _ = _warm_bed()
        b = db.nodes["node-b"]
        b.spdk.fail.add("bdev_alceml_create")
        self.assertEqual(self.ensure(db, "node-b")["created"], [])
        self.assertNotIn(BASE, b.spdk.bdevs)
        self.assertTrue(b.spdk.ops("bdev_aio_delete"))

    def test_removed_standby_role_cleans_up(self):
        db, dev = _warm_bed()
        c = db.nodes["node-c"]
        self.ensure(db, "node-c")
        self.assertIn(ALCEML, c.spdk.bdevs)
        _role(c.spdk, _standby_role)
        # node-c is no longer the owner's tertiary: not attached any more.
        db.nodes["node-a"].tertiary_node_id = ""
        report = self.ensure(db, "node-c")
        self.assertEqual(report["removed"], [DEV_ID])
        self.assertNotIn(ALCEML, c.spdk.bdevs)
        self.assertNotIn(BASE, c.spdk.bdevs)
        deletes = [op for op, *_ in c.spdk.calls if op.endswith("_delete")]
        self.assertEqual(deletes, ["bdev_alceml_delete", "bdev_aio_delete"])

    def test_fenced_standby_is_closed(self):
        db, dev = _warm_bed()
        b = db.nodes["node-b"]
        self.ensure(db, "node-b")
        _role(b.spdk, _standby_role)
        dev.fenced_node_ids = ["node-b"]
        self.assertEqual(self.ensure(db, "node-b")["removed"], [DEV_ID])
        self.assertNotIn(ALCEML, b.spdk.bdevs)

    def test_exported_or_active_stack_is_never_closed(self):
        db, dev = _warm_bed()
        b = db.nodes["node-b"]
        dev.fenced_node_ids = ["node-b"]
        b.spdk.bdevs[ALCEML] = {"name": ALCEML}
        _role(b.spdk, lambda _n: {"role": "active", "state": "active"})
        self.assertEqual(self.ensure(db, "node-b")["removed"], [])
        _role(b.spdk, _standby_role)
        b.spdk.bdevs[f"{ALCEML}_PT"] = {"name": f"{ALCEML}_PT"}
        self.assertEqual(self.ensure(db, "node-b")["removed"], [])
        self.assertIn(ALCEML, b.spdk.bdevs)

    def test_owner_standby_keeps_its_base_bdev(self):
        # The owner, no longer serving and not a standby: its alceml closes,
        # its own base bdev stays (its restart path owns it).
        db, dev = _warm_bed()
        a = db.nodes["node-a"]
        dev.serving_node_id = "node-b"
        dev.fenced_node_ids = ["node-a"]
        a.spdk.bdevs[BASE] = {"name": BASE}
        a.spdk.bdevs[ALCEML] = {"name": ALCEML}
        _role(a.spdk, _standby_role)
        self.assertEqual(self.ensure(db, "node-a")["removed"], [DEV_ID])
        self.assertNotIn(ALCEML, a.spdk.bdevs)
        self.assertIn(BASE, a.spdk.bdevs)


class TestRetireKeepsWarmStandby(_WarmCase):
    """retire_stale_local_stack (node back ONLINE without a restart) keeps
    the warm standby of a LUN served elsewhere; a fenced node's is retired."""

    def _owner_standby(self):
        db, dev = _warm_bed()
        a = db.nodes["node-a"]
        dev.serving_node_id = "node-b"
        a.spdk.bdevs[ALCEML] = {"name": ALCEML}
        _role(a.spdk, _standby_role)
        p = patch("simplyblock_core.db_controller.DBController", return_value=db)
        p.start()
        self.addCleanup(p.stop)
        return db, dev, a

    def test_kept(self):
        _db, _dev, a = self._owner_standby()
        self.assertEqual(lun_takeover.retire_stale_local_stack(a), [])
        self.assertIn(ALCEML, a.spdk.bdevs)

    def test_retired_when_fenced_or_deposed(self):
        _db, dev, a = self._owner_standby()
        dev.fenced_node_ids = ["node-a"]
        self.assertEqual(lun_takeover.retire_stale_local_stack(a), [DEV_ID])
        self.assertNotIn(ALCEML, a.spdk.bdevs)
        # A deposed former active (not opened standby) is stale as well.
        _db, dev, a = self._owner_standby()
        _role(a.spdk, lambda _n: {"role": "standby", "state": "deposed", "standby_at_open": False})
        self.assertEqual(lun_takeover.retire_stale_local_stack(a), [DEV_ID])


# --------------------------------------------------------------- memory

class TestWarmStandbyMemory(_WarmCase):

    def test_over_budget_is_refused_with_one_event(self):
        db, _ = _warm_bed()
        b = db.nodes["node-b"]
        b.minimum_sys_memory = utils.sys_memory_for_capacity(0)   # no room for a page map
        report = self.ensure(db, "node-b")
        self.assertEqual(report["over_budget"], [DEV_ID])
        self.assertEqual(b.spdk.ops("bdev_alceml_create"), [])
        budget_events = [e for e in self.events if "memory budget" in e["message"]]
        self.assertEqual(len(budget_events), 1)
        self.assertEqual(budget_events[0]["node_id"], "node-b")
        # Not repeated while nothing changes.
        self.ensure(db, "node-b")
        self.assertEqual(len([e for e in self.events if "memory budget" in e["message"]]), 1)

    def test_within_budget_is_opened(self):
        db, _ = _warm_bed()
        b = db.nodes["node-b"]
        b.minimum_sys_memory = utils.sys_memory_for_capacity(SIZE)
        self.assertEqual(self.ensure(db, "node-b")["created"], [DEV_ID])
        self.assertFalse([e for e in self.events if "memory budget" in e["message"]])

    def test_restart_budget_counts_warm_standby_luns(self):
        db, dev = _warm_bed()
        b, d = db.nodes["node-b"], db.nodes["node-d"]
        self.assertEqual(multi_attach.warm_standby_capacity(db.cluster, "node-b", list(db.nodes.values())),
                         SIZE)
        self.assertEqual(storage_node_ops._warm_standby_capacity(db, b), SIZE)
        # A device unavailable for a while still counts: its standby returns.
        dev.status = "unavailable"
        self.assertEqual(storage_node_ops._warm_standby_capacity(db, b), SIZE)
        # Not a standby (unattached peer, serving node), not a warm cluster.
        self.assertEqual(storage_node_ops._warm_standby_capacity(db, d), 0)
        self.assertEqual(storage_node_ops._warm_standby_capacity(db, db.nodes["node-a"]), 0)
        db.cluster.multi_attach_warm_standby = False
        self.assertEqual(storage_node_ops._warm_standby_capacity(db, b), 0)


# ------------------------------------------------------------- takeover

class TestWarmTakeover(_WarmCase):

    def setUp(self):
        super().setUp()
        p = patch.object(device_controller, "device_set_unavailable", return_value=True)
        self.m_unavailable = p.start()
        self.addCleanup(p.stop)

    def _with_standby(self):
        """The secondary holds a warm standby (opened by ensure_warm_standbys);
        then the serving owner fails."""
        db, dev = _warm_bed()
        self.ensure(db, "node-b")
        b = db.nodes["node-b"]
        self.assertIn(ALCEML, b.spdk.bdevs)
        b.spdk.calls.clear()
        db.nodes["node-a"].status = StorageNode.STATUS_OFFLINE
        return db, dev, b

    def test_reuses_the_warm_standby(self):
        db, _, b = self._with_standby()
        seen = []

        def role(_name):
            # The peers' I/O reaches it once the paths are switched.
            switched = bool(b.spdk.ops("attach"))
            seen.append(switched)
            if switched:
                return {"role": "active", "state": "active", "epoch": 3}
            return _standby_role(_name)
        _role(b.spdk, role)
        self.assertEqual(self.run_takeover(db), lun_takeover.TAKEN_OVER)
        self.assertEqual(b.spdk.ops("bdev_alceml_create"), [])
        self.assertEqual(b.spdk.ops("bdev_alceml_delete"), [])
        self.assertEqual(len(b.spdk.ops("bdev_PT_NoExcl_create")), 1)
        self.assertEqual(len(b.spdk.ops("subsystem_create")), 1)
        self.assertEqual(len(b.spdk.ops("nvmf_subsystem_add_ns")), 1)
        # Checked once before the export (standby), then awaited after the
        # path switch until active.
        self.assertEqual(seen[0], False)
        self.assertEqual(seen[-1], True)
        dev = self.dev(db)
        self.assertEqual(dev.serving_node(), "node-b")
        self.assertIn("node-a", dev.fenced_node_ids)
        self.m_unavailable.assert_not_called()

    def test_activation_failure_falls_back_to_a_cold_open(self):
        db, _, b = self._with_standby()

        def role(_name):
            # The standby never activates; the instance reopened active does.
            if b.spdk.ops("bdev_alceml_create"):
                return {"role": "active", "state": "active"}
            return _standby_role(_name)
        _role(b.spdk, role)
        with patch.object(constants, "MULTI_ATTACH_ACTIVATION_WAIT_SEC", 0.2):
            self.assertEqual(self.run_takeover(db), lun_takeover.TAKEN_OVER)
        # The failed node is fenced at the array (synchronously).
        self.assertIn(("fence", WWN, "host-node-a"), FakeArrayPlugin.calls)
        # Export torn down, standby closed, then reopened active with the key.
        ops = [op for op, *_ in b.spdk.calls]
        self.assertLess(ops.index("attach"), ops.index("subsystem_delete"))
        self.assertLess(ops.index("bdev_alceml_delete"), ops.index("bdev_alceml_create"))
        create = b.spdk.ops("bdev_alceml_create")
        self.assertEqual(len(create), 1)
        self.assertNotIn("standby", create[0][2])
        self.assertEqual(create[0][2]["reservation_key"], multi_attach.reservation_key(b))
        self.assertEqual(create[0][2]["pba_init_mode"], lun_takeover.ALCEML_OPEN_ONLY)
        self.assertEqual(len(b.spdk.ops("subsystem_create")), 2)
        self.assertIn(BASE, b.spdk.bdevs)
        dev = self.dev(db)
        self.assertEqual(dev.serving_node(), "node-b")
        self.assertIn("node-a", dev.fenced_node_ids)
        self.assertTrue(any("did not activate" in e["message"] for e in self.events))
        self.m_unavailable.assert_not_called()

    def test_cold_fallback_failure_sets_the_device_unavailable(self):
        db, _, b = self._with_standby()
        _role(b.spdk, _standby_role)
        b.spdk.fail.add("bdev_alceml_create")
        with patch.object(constants, "MULTI_ATTACH_ACTIVATION_WAIT_SEC", 0.2):
            self.assertEqual(self.run_takeover(db), lun_takeover.FAILED)
        self.m_unavailable.assert_called_once_with(DEV_ID)
        self.assertTrue(any("cold open failed" in e["message"] for e in self.events))

    def test_standby_whose_activation_failed_is_reopened(self):
        db, _, b = self._with_standby()

        def role(_name):
            if b.spdk.ops("bdev_alceml_create"):
                return {"role": "active", "state": "active"}
            return {"role": "standby", "state": "act_failed"}
        _role(b.spdk, role)
        self.assertEqual(self.run_takeover(db), lun_takeover.TAKEN_OVER)
        self.assertEqual(len(b.spdk.ops("bdev_alceml_delete")), 1)
        create = b.spdk.ops("bdev_alceml_create")
        self.assertEqual(len(create), 1)
        self.assertNotIn("standby", create[0][2])

    def test_unresponsive_warm_standby_moves_to_the_next(self):
        db, _, b = self._with_standby()
        c = db.nodes["node-c"]

        def b_dies(_name):
            b.spdk.down = True
            raise ConnectionError("gone")
        _role(b.spdk, b_dies)
        _role(c.spdk, lambda _n: {"role": "active", "state": "active"})
        with patch.object(constants, "MULTI_ATTACH_ACTIVATION_WAIT_SEC", 0.2):
            self.assertEqual(self.run_takeover(db), lun_takeover.TAKEN_OVER)
        self.assertEqual(self.dev(db).serving_node(), "node-c")

    def test_warm_standby_lost_during_activation_is_taken_over_from_at_once(self):
        # 2026-11-23 beds: the warm standby chosen was the second node lost.
        # ws3: a cold open on it and the cleanup calls waited out timeouts, and
        # the cluster suspended; ws4: 10 s more stall waiting for the watchdog's
        # next pass. Now: no cold open there, no device unavailable; the same
        # takeover moves the LUN on from the dead standby (cold, by reservation).
        db, _, b = self._with_standby()
        c = db.nodes["node-c"]

        def role(_name):
            if b.spdk.ops("attach"):    # dies right after the paths were switched to it
                b.spdk.down = True
                raise ConnectionError("gone")
            return _standby_role(_name)
        _role(b.spdk, role)
        _role(c.spdk, lambda _n: {"role": "active", "state": "active"})
        t0 = time.monotonic()
        self.assertEqual(self.run_takeover(db), lun_takeover.TAKEN_OVER)
        self.assertLess(time.monotonic() - t0, 5)
        self.assertEqual(b.spdk.ops("bdev_alceml_create"), [])
        self.m_unavailable.assert_not_called()
        self.assertTrue(any("no longer answers" in e["message"] for e in self.events))
        dev = self.dev(db)
        self.assertEqual(dev.serving_node(), "node-c")
        self.assertIn("node-a", dev.fenced_node_ids)
        self.assertIn("node-b", dev.fenced_node_ids)

    def test_activation_wait_stops_when_the_standby_is_declared_lost(self):
        db, _, b = self._with_standby()
        c = db.nodes["node-c"]

        def role(_name):
            if b.spdk.ops("attach"):
                # The watchdog declares it lost (its takeover request is
                # refused as running, but recorded); it still "answers".
                lun_takeover.note_node_lost("node-b")
            return _standby_role(_name)
        _role(b.spdk, role)
        _role(c.spdk, lambda _n: {"role": "active", "state": "active"})
        t0 = time.monotonic()
        # Without the verdict this would wait the whole activation wait (5 s).
        self.assertEqual(self.run_takeover(db), lun_takeover.TAKEN_OVER)
        self.assertLess(time.monotonic() - t0, 3)
        self.assertEqual(self.dev(db).serving_node(), "node-c")

    def test_without_the_flag_an_existing_standby_is_not_reused(self):
        # Flag off: the regular path (wait for the reservation before the export).
        db, _, b = self._with_standby()
        db.cluster.multi_attach_warm_standby = False
        _role(b.spdk, lambda _n: {"role": "active", "state": "active"})
        self.assertEqual(self.run_takeover(db), lun_takeover.TAKEN_OVER)
        self.assertEqual(b.spdk.ops("bdev_alceml_create"), [])
        self.assertEqual(self.dev(db).serving_node(), "node-b")


# ------------------------------------------------------------- plumbing

class TestWarmStandbyPlumbing(unittest.TestCase):

    def test_helper(self):
        db, dev = _warm_bed()
        self.assertTrue(multi_attach.warm_standby_enabled(db.cluster, dev))
        db.cluster.multi_attach_warm_standby = False
        self.assertFalse(multi_attach.warm_standby_enabled(db.cluster, dev))
        db.cluster.multi_attach_warm_standby = True
        dev.lun_reservations = False
        self.assertFalse(multi_attach.warm_standby_enabled(db.cluster, dev))
        self.assertFalse(multi_attach.warm_standby_enabled(Cluster(), dev))

    def test_cluster_flag_needs_multi_attach(self):
        self.assertFalse(Cluster().multi_attach_warm_standby)
        with self.assertRaises(ValueError):
            cluster_ops._validate_warm_standby(False, True)
        cluster_ops._validate_warm_standby(True, True)
        cluster_ops._validate_warm_standby(False, False)

    def test_warm_verify_is_opt_in(self):
        import importlib
        with patch.dict("os.environ", {}, clear=False):
            os.environ.pop("SIMPLYBLOCK_WARM_VERIFY", None)
            self.assertFalse(importlib.reload(constants).MULTI_ATTACH_WARM_VERIFY)
            os.environ["SIMPLYBLOCK_WARM_VERIFY"] = "1"
            self.assertTrue(importlib.reload(constants).MULTI_ATTACH_WARM_VERIFY)
        importlib.reload(constants)

    def test_cli_flag(self):
        import argparse

        from simplyblock_cli.cli import CLIWrapper
        cli = CLIWrapper.__new__(CLIWrapper)
        cli.developer_mode = False
        for cmd in ("create", "add"):
            parser = argparse.ArgumentParser()
            sub = parser.add_subparsers(dest="c")
            with patch.object(CLIWrapper, "add_sub_command",
                              lambda self, subp, name, help, **k: subp.add_parser(name)):
                getattr(cli, f"init_cluster__{cmd}")(sub)
            args, _ = parser.parse_known_args([cmd, "--multi-attach-warm-standby"])
            self.assertTrue(args.multi_attach_warm_standby, cmd)

    def test_rpc_sends_refresh_interval_for_a_standby_only(self):
        from simplyblock_core.rpc_client import RPCClient
        rpc = RPCClient.__new__(RPCClient)
        rpc._request = MagicMock(return_value=True)
        rpc.bdev_alceml_create("a", "b", "u", pba_init_mode=1, reservation_key=1 << 32,
                               standby=True, refresh_interval_ms=2000, warm_verify=True)
        params = rpc._request.call_args.kwargs
        self.assertEqual((params["standby"], params["refresh_interval_ms"], params["warm_verify"]),
                         (True, 2000, True))
        rpc.bdev_alceml_create("a", "b", "u", pba_init_mode=1, reservation_key=1 << 32, standby=True)
        self.assertNotIn("warm_verify", rpc._request.call_args.kwargs)
        rpc.bdev_alceml_create("a", "b", "u", pba_init_mode=1, refresh_interval_ms=2000, warm_verify=True)
        params = rpc._request.call_args.kwargs
        self.assertNotIn("standby", params)
        self.assertNotIn("refresh_interval_ms", params)
        self.assertNotIn("warm_verify", params)

    def test_monitor_drives_warm_clusters_only(self):
        db, _ = _warm_bed()
        node = db.nodes["node-b"]
        storage_node_monitor._warm_standby_last.clear()
        storage_node_monitor._warm_standby_threads.clear()
        with patch.object(storage_node_monitor, "db") as mdb, \
                patch.object(lun_takeover, "ensure_warm_standbys") as ensure:
            mdb.get_cluster_by_id.return_value = db.cluster
            storage_node_monitor._ensure_warm_standbys(node)
            storage_node_monitor._warm_standby_threads["node-b"].join(5)
            ensure.assert_called_once_with(node)
            # Throttled per node.
            storage_node_monitor._ensure_warm_standbys(node)
            self.assertEqual(ensure.call_count, 1)
            storage_node_monitor._warm_standby_last.clear()
            db.cluster.multi_attach_warm_standby = False
            storage_node_monitor._ensure_warm_standbys(node)
            self.assertEqual(ensure.call_count, 1)
        storage_node_monitor._warm_standby_last.clear()
        storage_node_monitor._warm_standby_threads.clear()


if __name__ == "__main__":
    unittest.main()


class TestSwitchPathsSkipsLostNodes(unittest.TestCase):

    def test_a_node_declared_lost_is_not_waited_for(self):
        import threading
        from types import SimpleNamespace

        from simplyblock_core.models.nvme_device import NVMeDevice as _Dev

        release = threading.Event()

        def node(nid, slow):
            rpc = MagicMock()
            if slow:
                rpc.bdev_nvme_attach_controller.side_effect = lambda *a, **k: release.wait(5) or True
            else:
                rpc.bdev_nvme_attach_controller.return_value = True
            n = MagicMock()
            n.get_id.return_value = nid
            n.status = StorageNode.STATUS_ONLINE
            n.rpc_client.return_value = rpc
            return n
        fast, slow = node("node-fast", False), node("node-slow", True)
        db = MagicMock()
        db.get_storage_nodes_by_cluster_id.return_value = [fast, slow]
        dev = _Dev()
        dev.uuid = "d1"
        dev.alceml_bdev = "alceml_d1"
        dev.nvmf_ip = "10.0.0.9"
        standby = SimpleNamespace(data_nics=[])
        t0 = time.monotonic()
        threading.Timer(0.3, lambda: lun_takeover.note_node_lost("node-slow")).start()
        with patch("simplyblock_core.distr_controller.send_dev_status_event"), \
                patch.object(lun_takeover, "_transport", return_value="TCP"):
            added = lun_takeover._switch_paths(db, "c1", dev, standby, None, ([], None, "TCP"),
                                               since=t0)
        took = time.monotonic() - t0
        release.set()
        self.assertEqual(added, ["node-fast"])
        self.assertLess(took, 2)

    def test_peers_attaching_to_a_lost_standby_are_not_waited_for(self):
        import threading
        from types import SimpleNamespace

        from simplyblock_core.models.nvme_device import NVMeDevice as _Dev

        release = threading.Event()

        def node(nid):
            rpc = MagicMock()
            # Every peer's attach waits on the dead export.
            rpc.bdev_nvme_attach_controller.side_effect = lambda *a, **k: release.wait(5) or True
            n = MagicMock()
            n.get_id.return_value = nid
            n.status = StorageNode.STATUS_ONLINE
            n.rpc_client.return_value = rpc
            return n
        db = MagicMock()
        db.get_storage_nodes_by_cluster_id.return_value = [node("p1"), node("p2")]
        dev = _Dev()
        dev.uuid = "d2"
        dev.alceml_bdev = "alceml_d2"
        dev.nvmf_ip = "10.0.0.9"
        standby = SimpleNamespace(data_nics=[], get_id=lambda: "standby-x")
        t0 = time.monotonic()
        threading.Timer(0.3, lambda: lun_takeover.note_node_lost("standby-x")).start()
        with patch("simplyblock_core.distr_controller.send_dev_status_event"), \
                patch.object(lun_takeover, "_transport", return_value="TCP"):
            added = lun_takeover._switch_paths(db, "c1", dev, standby, None, ([], None, "TCP"),
                                               since=t0)
        took = time.monotonic() - t0
        release.set()
        self.assertEqual(added, [])
        self.assertLess(took, 2)

    def test_a_standby_that_stops_answering_is_found_lost_by_the_switch(self):
        # The LUN watchdog runs in another process: the switch probes the
        # standby itself (2026-11-23 ma-ws6).
        import threading

        from simplyblock_core.models.nvme_device import NVMeDevice as _Dev

        release = threading.Event()

        def node(nid):
            rpc = MagicMock()
            rpc.bdev_nvme_attach_controller.side_effect = lambda *a, **k: release.wait(5) or True
            n = MagicMock()
            n.get_id.return_value = nid
            n.status = StorageNode.STATUS_ONLINE
            n.rpc_client.return_value = rpc
            return n
        db = MagicMock()
        db.get_storage_nodes_by_cluster_id.return_value = [node("q1"), node("q2")]
        dev = _Dev()
        dev.uuid = "d3"
        dev.alceml_bdev = "alceml_d3"
        dev.nvmf_ip = "10.0.0.9"
        standby = MagicMock()
        standby.get_id.return_value = "standby-y"
        standby.data_nics = []
        standby.rpc_client.return_value.get_version.side_effect = ConnectionError("gone")
        t0 = time.monotonic()
        with patch("simplyblock_core.distr_controller.send_dev_status_event"), \
                patch.object(lun_takeover, "_transport", return_value="TCP"):
            added = lun_takeover._switch_paths(db, "c1", dev, standby, None, ([], None, "TCP"),
                                               since=t0)
        took = time.monotonic() - t0
        release.set()
        self.assertEqual(added, [])
        self.assertLess(took, 2.5)
        self.assertTrue(lun_takeover.node_lost_since("standby-y", t0))
