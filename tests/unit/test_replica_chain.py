"""add_clone / convert reach every lvstore member that answers, whatever its
DB status (replica_chain).

bf23, 2026-11-12, LVS_16: the tertiary never got the add_clone that linked a
clone under its snapshot, so its in-memory tree had the snapshot clone-less.
After it took leadership through IO it deleted that snapshot's data. The call
sites had picked members by DB status (ONLINE only, or the secondary only).

Pure logic tests: DB and RPC clients are mocked.
"""
import unittest
from unittest.mock import MagicMock, patch

from requests.exceptions import ConnectionError as ReqConnectionError
from requests.exceptions import ReadTimeout

from simplyblock_core.controllers import replica_chain as rc
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCConnectionError, RPCRemoteError

WRITABLE = {"driver_specific": {"lvol": {"is_snapshot": False}}}
SNAPSHOT = {"driver_specific": {"lvol": {"is_snapshot": True}}}


def _conn_error(cause):
    try:
        raise RPCConnectionError("x") from cause
    except RPCConnectionError as e:
        return e


def _node(node_id, status=StorageNode.STATUS_ONLINE, bdev=WRITABLE):
    n = MagicMock()
    n.get_id.return_value = node_id
    n.status = status
    rpc = n.rpc_client.return_value
    rpc.bdev_get.return_value = bdev
    rpc.bdev_lvol_add_clone.return_value = True
    rpc.bdev_lvol_convert.return_value = True
    return n


def _db(nodes):
    db = MagicMock()
    db.get_storage_node_by_id.side_effect = lambda nid: nodes[nid]
    return db


def _primary(sec="sec", ter="ter"):
    p = MagicMock()
    p.secondary_node_id = sec
    p.tertiary_node_id = ter
    return p


class TestMembers(unittest.TestCase):

    def test_every_status_but_removed_is_a_member(self):
        nodes = {"sec": _node("sec", StorageNode.STATUS_DOWN),
                 "ter": _node("ter", StorageNode.STATUS_REMOVED)}
        got = rc.lvstore_replicas(_primary(), db=_db(nodes))
        self.assertEqual([(r, n.get_id()) for r, n in got], [("secondary", "sec")])

    def test_unknown_ids_and_exclusions_are_left_out(self):
        nodes = {"sec": _node("sec")}
        self.assertEqual(rc.lvstore_replicas(_primary(), exclude_ids={"sec"}, db=_db(nodes)), [])


class TestAddCloneAndConvert(unittest.TestCase):

    def test_tertiary_not_online_still_gets_both_calls(self):
        """The bf23 shape: a tertiary in restart answers RPCs and holds the
        tree, so it is told about the link and the convert."""
        nodes = {"sec": _node("sec"), "ter": _node("ter", StorageNode.STATUS_RESTARTING)}
        db = _db(nodes)
        self.assertIsNone(rc.add_clone_on_replicas(_primary(), "LVS/C", "LVS/S", db=db))
        self.assertIsNone(rc.convert_on_replicas(_primary(), "LVS/C", db=db))
        for n in nodes.values():
            n.rpc_client.return_value.bdev_lvol_add_clone.assert_called_once_with("LVS/C", "LVS/S")
            n.rpc_client.return_value.bdev_lvol_convert.assert_called_once_with("LVS/C")

    def test_unreachable_member_is_skipped_not_failed(self):
        ter = _node("ter", StorageNode.STATUS_OFFLINE)
        ter.rpc_client.return_value.bdev_get.side_effect = _conn_error(ReqConnectionError())
        self.assertEqual(rc.add_clone_on(ter, "tertiary", "LVS/C", "LVS/S"), (rc.SKIPPED, None))
        self.assertEqual(rc.convert_on(ter, "tertiary", "LVS/C"), (rc.SKIPPED, None))
        ter.rpc_client.return_value.bdev_lvol_add_clone.assert_not_called()

    def test_a_read_timeout_reached_the_member_and_fails(self):
        ter = _node("ter")
        ter.rpc_client.return_value.bdev_lvol_convert.side_effect = _conn_error(ReadTimeout())
        outcome, err = rc.convert_on(ter, "tertiary", "LVS/C")
        self.assertEqual(outcome, rc.FAILED)
        self.assertIn("ter", err)

    def test_an_error_answer_fails_and_the_other_member_still_runs(self):
        nodes = {"sec": _node("sec"), "ter": _node("ter")}
        nodes["sec"].rpc_client.return_value.bdev_lvol_add_clone.side_effect = \
            RPCRemoteError("File exists", -17)
        err = rc.add_clone_on_replicas(_primary(), "LVS/C", "LVS/S", db=_db(nodes))
        self.assertIn("secondary sec", err)
        nodes["ter"].rpc_client.return_value.bdev_lvol_add_clone.assert_called_once()

    def test_a_false_answer_fails(self):
        sec = _node("sec")
        sec.rpc_client.return_value.bdev_lvol_convert.return_value = False
        self.assertEqual(rc.convert_on(sec, "secondary", "LVS/C")[0], rc.FAILED)

    def test_member_without_the_bdev_is_skipped(self):
        sec = _node("sec", bdev=None)
        self.assertEqual(rc.add_clone_on(sec, "secondary", "LVS/C", "LVS/S"), (rc.SKIPPED, None))
        self.assertEqual(rc.convert_on(sec, "secondary", "LVS/C"), (rc.SKIPPED, None))

    def test_retries_are_idempotent(self):
        linked = _node("sec", bdev={"driver_specific": {"lvol": {"base_snapshot": "S"}}})
        self.assertEqual(rc.add_clone_on(linked, "secondary", "LVS/C", "LVS/S"), (rc.DONE, None))
        linked.rpc_client.return_value.bdev_lvol_add_clone.assert_not_called()
        converted = _node("sec", bdev=SNAPSHOT)
        self.assertEqual(rc.add_clone_on(converted, "secondary", "LVS/C", "LVS/S"), (rc.DONE, None))
        self.assertEqual(rc.convert_on(converted, "secondary", "LVS/C"), (rc.DONE, None))
        converted.rpc_client.return_value.bdev_lvol_convert.assert_not_called()

    def test_a_converted_lvol_with_no_clones_is_not_converted_again(self):
        """What a live SPDK answers right after bdev_lvol_convert, before
        anything is cloned from it (61 cluster, 2026-10-29): snapshot=false,
        clone=false, only write support gone. A batch cutover that fails
        leaves its intermediate snapshots in exactly this state, and SPDK
        rejects converting them again."""
        converted = _node("sec", bdev={
            "supported_io_types": {"read": True, "write": False},
            "driver_specific": {"lvol": {"snapshot": False, "clone": False}}})
        self.assertEqual(rc.convert_on(converted, "secondary", "LVS/C"), (rc.DONE, None))
        converted.rpc_client.return_value.bdev_lvol_convert.assert_not_called()

    def test_a_writable_lvol_is_still_converted(self):
        writable = _node("sec", bdev={
            "supported_io_types": {"read": True, "write": True},
            "driver_specific": {"lvol": {"snapshot": False, "clone": True}}})
        self.assertEqual(rc.convert_on(writable, "secondary", "LVS/C"), (rc.DONE, None))
        writable.rpc_client.return_value.bdev_lvol_convert.assert_called_once_with("LVS/C")


# ---------------------------------------------------------------------------
# Owed changes: recorded when a member is skipped, replayed when it answers
# ---------------------------------------------------------------------------

class _KV:
    """Dict-backed stand-in for the FDB handle (no transactions, no indices)."""

    def __init__(self):
        self.data = {}

    def set(self, key, value):
        self.data[key] = value

    def clear(self, key):
        self.data.pop(key, None)

    def get_range_startswith(self, prefix, limit=0, reverse=False):
        rows = sorted((k, v) for k, v in self.data.items() if k.startswith(prefix))
        return rows[:limit] if limit else rows


def _info(name, uuid, base=None, snapshot=False):
    lvol = {"is_snapshot": snapshot}
    if base:
        lvol["base_snapshot"] = base
    return {"name": uuid, "uuid": uuid, "aliases": [name], "driver_specific": {"lvol": lvol}}


class _Spdk:
    """One node's SPDK: bdevs by name and uuid, lvstore leadership."""

    def __init__(self, bdevs=(), leader=False, reachable=True):
        self.bdevs = list(bdevs)
        self.leader = leader
        self.reachable = reachable
        self.add_clone_calls = []
        self.convert_calls = []

    def _reach(self):
        if not self.reachable:
            raise _conn_error(ReqConnectionError())

    def bdev_get(self, ref):
        self._reach()
        for b in self.bdevs:
            if ref == b["uuid"] or ref in b["aliases"]:
                return b
        return None

    def get_lvstore(self, name):
        self._reach()
        return {"lvs leadership": self.leader}

    def get_version(self):
        self._reach()
        return {}

    def bdev_lvol_add_clone(self, clone, parent):
        self._reach()
        if self.bdev_get(parent) is None:
            raise RPCRemoteError("No such device", -19)
        self.add_clone_calls.append((clone, parent))
        self.bdev_get(clone)["driver_specific"]["lvol"]["base_snapshot"] = parent.split("/")[-1]
        return True

    def bdev_lvol_convert(self, name):
        self._reach()
        self.convert_calls.append(name)
        self.bdev_get(name)["driver_specific"]["lvol"]["is_snapshot"] = True
        return True


def _snode(node_id, spdk, status=StorageNode.STATUS_ONLINE, sec="", ter=""):
    n = MagicMock()
    n.get_id.return_value = node_id
    n.status = status
    n.secondary_node_id = sec
    n.tertiary_node_id = ter
    n.rpc_client.side_effect = lambda **kw: spdk
    return n


class _Cluster:
    """Primary P (leader) with secondary S and tertiary T, and a fake DB."""

    def __init__(self, s_spdk=None, t_spdk=None):
        self.p = _Spdk([_info("LVS/C", "u-c", base="S"), _info("LVS/S", "u-s", snapshot=True)],
                       leader=True)
        self.s = s_spdk or _Spdk([_info("LVS/C", "u-c"), _info("LVS/S", "u-s", snapshot=True)])
        self.t = t_spdk or _Spdk([_info("LVS/C", "u-c"), _info("LVS/S", "u-s", snapshot=True)])
        self.nodes = {"P": _snode("P", self.p, sec="S", ter="T"),
                      "S": _snode("S", self.s), "T": _snode("T", self.t)}
        self.db = MagicMock()
        self.db.kv_store = _KV()
        self.db.get_storage_node_by_id.side_effect = lambda nid: self.nodes[nid]

    def owed(self, node_id):
        return rc.pending_for(node_id, self.db)


class TestOwedChanges(unittest.TestCase):

    def setUp(self):
        patcher = patch.object(rc, "_restart_owned", return_value=False)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_unreachable_member_is_recorded_with_the_leaders_uuids(self):
        c = _Cluster()
        c.t.reachable = False
        self.assertIsNone(rc.add_clone_on_replicas(c.nodes["P"], "LVS/C", "LVS/S", db=c.db))
        (rec,) = c.owed("T")
        self.assertEqual((rec.op, rec.bdev_uuid, rec.parent_uuid, rec.reason, rec.lvs_name,
                          rec.primary_node_id),
                         ("add_clone", "u-c", "u-s", "unreachable", "LVS", "P"))
        self.assertEqual(c.owed("S"), [])

    def test_replay_links_the_member_once_it_answers_and_drops_the_record(self):
        """Gap 1: a member that missed the link while unreachable and never
        reloaded its lvstore is told as soon as it answers again."""
        c = _Cluster()
        c.t.reachable = False
        rc.add_clone_on_replicas(c.nodes["P"], "LVS/C", "LVS/S", db=c.db)
        c.t.reachable = True
        got = rc.replay_pending(c.nodes["T"], db=c.db)
        self.assertEqual(got, {"done": 1, "skipped": 0, "failed": 0})
        self.assertEqual(c.t.add_clone_calls, [("LVS/C", "LVS/S")])
        self.assertEqual(c.owed("T"), [])

    def test_replay_finds_the_bdev_by_uuid_after_a_rename(self):
        c = _Cluster()
        c.t.reachable = False
        rc.add_clone_on_replicas(c.nodes["P"], "LVS/C", "LVS/S", db=c.db)
        c.t.reachable = True
        for spdk in (c.p, c.t):
            spdk.bdev_get("u-c")["aliases"] = ["LVS/C_final"]
        rc.replay_pending(c.nodes["T"], db=c.db)
        self.assertEqual(c.t.add_clone_calls, [("LVS/C_final", "LVS/S")])

    def test_an_unreachable_member_stays_owed(self):
        c = _Cluster()
        c.t.reachable = False
        rc.add_clone_on_replicas(c.nodes["P"], "LVS/C", "LVS/S", db=c.db)
        self.assertEqual(rc.replay_pending(c.nodes["T"], db=c.db)["skipped"], 1)
        self.assertEqual(len(c.owed("T")), 1)

    def test_a_change_the_leader_no_longer_has_is_dropped_not_replayed(self):
        """A record outlives deletes and rollbacks: replaying it blindly
        would corrupt the tree it is meant to repair."""
        c = _Cluster()
        c.t.reachable = False
        rc.add_clone_on_replicas(c.nodes["P"], "LVS/C", "LVS/S", db=c.db)
        c.p.bdevs = [b for b in c.p.bdevs if b["uuid"] != "u-c"]   # rolled back
        c.t.reachable = True
        self.assertEqual(rc.replay_pending(c.nodes["T"], db=c.db)["done"], 1)
        self.assertEqual(c.t.add_clone_calls, [])
        self.assertEqual(c.owed("T"), [])

    def test_a_relinked_clone_is_not_linked_to_its_old_parent(self):
        c = _Cluster()
        c.t.reachable = False
        rc.add_clone_on_replicas(c.nodes["P"], "LVS/C", "LVS/S", db=c.db)
        c.p.bdev_get("u-c")["driver_specific"]["lvol"]["base_snapshot"] = "S2"
        c.t.reachable = True
        rc.replay_pending(c.nodes["T"], db=c.db)
        self.assertEqual(c.t.add_clone_calls, [])
        self.assertEqual(c.owed("T"), [])

    def test_without_a_leader_to_confirm_it_the_change_is_kept(self):
        c = _Cluster()
        c.t.reachable = False
        rc.add_clone_on_replicas(c.nodes["P"], "LVS/C", "LVS/S", db=c.db)
        c.t.reachable = True
        c.p.leader = False
        self.assertEqual(rc.replay_pending(c.nodes["T"], db=c.db)["skipped"], 1)
        self.assertEqual(c.t.add_clone_calls, [])
        # ...unless it is about to be granted the leaderless lvstore itself
        self.assertEqual(rc.replay_pending(c.nodes["T"], db=c.db, trusted=True)["done"], 1)
        self.assertEqual(c.t.add_clone_calls, [("LVS/C", "LVS/S")])

    def test_records_of_a_member_that_left_the_lvstore_are_dropped(self):
        c = _Cluster()
        c.t.reachable = False
        rc.add_clone_on_replicas(c.nodes["P"], "LVS/C", "LVS/S", db=c.db)
        c.nodes["P"].tertiary_node_id = "X"
        c.nodes["X"] = _snode("X", _Spdk())
        c.t.reachable = True
        rc.replay_pending(c.nodes["T"], db=c.db)
        self.assertEqual(c.t.add_clone_calls, [])
        self.assertEqual(c.owed("T"), [])

    def test_unregistered_member_is_recorded_and_linked_once_registered(self):
        """Gap 2: a member that answers without the landing volume owes the
        link; it gets it once the volume is registered there."""
        c = _Cluster(t_spdk=_Spdk([_info("LVS/S", "u-s", snapshot=True)]))
        rc.add_clone_on_replicas(c.nodes["P"], "LVS/C", "LVS/S", db=c.db)
        (rec,) = c.owed("T")
        self.assertEqual(rec.reason, "absent")
        self.assertEqual(rc.replay_pending(c.nodes["T"], db=c.db)["skipped"], 1)
        c.t.bdevs.append(_info("LVS/C", "u-c"))        # registered later
        self.assertEqual(rc.replay_pending(c.nodes["T"], db=c.db)["done"], 1)
        self.assertEqual(c.t.add_clone_calls, [("LVS/C", "LVS/S")])

    def test_convert_waits_for_an_owed_link(self):
        """A snapshot cannot be linked any more, and a snapshot reads as
        already linked: a member owing the link is not converted first."""
        c = _Cluster()
        c.t.reachable = False
        rc.add_clone_on_replicas(c.nodes["P"], "LVS/C", "LVS/S", db=c.db)
        c.p.bdev_get("u-c")["driver_specific"]["lvol"]["is_snapshot"] = True
        c.t.reachable = True
        # the convert step finds the owed link, replays it, then converts
        self.assertIsNone(rc.convert_on_replicas(c.nodes["P"], "LVS/C", db=c.db))
        self.assertEqual(c.t.add_clone_calls, [("LVS/C", "LVS/S")])
        self.assertEqual(c.t.convert_calls, ["LVS/C"])
        self.assertEqual(c.owed("T"), [])

    def test_convert_is_owed_too_while_the_link_cannot_be_replayed(self):
        c = _Cluster(t_spdk=_Spdk([_info("LVS/C", "u-c")]))   # no parent there
        # a non-fatal step: the -19 answer is recorded as owed
        rc.add_clone_on_replicas(c.nodes["P"], "LVS/C", "LVS/S", db=c.db, record_failed=True)
        c.p.bdev_get("u-c")["driver_specific"]["lvol"]["is_snapshot"] = True
        self.assertEqual(rc.convert_on(c.nodes["T"], "tertiary", "LVS/C",
                                       owner=c.nodes["P"], db=c.db), (rc.SKIPPED, None))
        self.assertEqual(c.t.convert_calls, [])
        self.assertEqual(sorted(r.op for r in c.owed("T")), ["add_clone", "convert"])
        # replay keeps the convert behind the owed link
        self.assertEqual(rc.replay_pending(c.nodes["T"], db=c.db)["done"], 0)
        self.assertEqual(c.t.convert_calls, [])
        # parent registered: link first, then convert
        c.t.bdevs.append(_info("LVS/S", "u-s", snapshot=True))
        self.assertEqual(rc.replay_pending(c.nodes["T"], db=c.db)["done"], 2)
        self.assertEqual(c.t.add_clone_calls, [("LVS/C", "LVS/S")])
        self.assertEqual(c.t.convert_calls, ["LVS/C"])

    def test_replay_does_not_take_an_unlinked_snapshot_as_linked(self):
        """A snapshot registered after the leader converted it was not
        converted on this member: 'a snapshot was linked first' does not
        hold for it."""
        c = _Cluster(t_spdk=_Spdk([_info("LVS/S", "u-s", snapshot=True)]))
        rc.add_clone_on_replicas(c.nodes["P"], "LVS/C", "LVS/S", db=c.db)
        c.t.bdevs.append(_info("LVS/C", "u-c", snapshot=True))
        self.assertEqual(rc.replay_pending(c.nodes["T"], db=c.db)["skipped"], 1)
        self.assertEqual(len(c.owed("T")), 1)

    def test_an_error_answer_on_a_non_fatal_step_is_recorded(self):
        c = _Cluster()
        c.t.bdev_lvol_add_clone = MagicMock(side_effect=RPCRemoteError("busy", -16))
        err = rc.add_clone_on_replicas(c.nodes["P"], "LVS/C", "LVS/S", db=c.db,
                                       record_failed=True)
        self.assertIn("tertiary T", err)
        (rec,) = c.owed("T")
        self.assertEqual(rec.reason, "failed")

    def test_an_error_answer_on_a_retried_step_is_not_recorded(self):
        c = _Cluster()
        c.t.bdev_lvol_add_clone = MagicMock(side_effect=RPCRemoteError("busy", -16))
        self.assertIsNotNone(rc.add_clone_on_replicas(c.nodes["P"], "LVS/C", "LVS/S", db=c.db))
        self.assertEqual(c.owed("T"), [])

    def test_a_restart_owned_lvstore_is_left_to_the_restart(self):
        c = _Cluster()
        c.t.reachable = False
        rc.add_clone_on_replicas(c.nodes["P"], "LVS/C", "LVS/S", db=c.db)
        c.t.reachable = True
        with patch.object(rc, "_restart_owned", return_value=True):
            self.assertEqual(rc.replay_pending(c.nodes["T"], db=c.db)["skipped"], 1)
        self.assertEqual(c.t.add_clone_calls, [])

    def test_a_member_owing_nothing_costs_no_rpc(self):
        c = _Cluster()
        self.assertEqual(rc.replay_pending(c.nodes["T"], db=c.db),
                         {"done": 0, "skipped": 0, "failed": 0})
        c.nodes["T"].rpc_client.assert_not_called()


class TestReachable(unittest.TestCase):

    def test_an_error_answer_is_an_answer(self):
        n = _node("n")
        n.rpc_client.return_value.get_version.side_effect = RPCRemoteError("x", -1)
        self.assertTrue(rc.reachable(n))

    def test_connection_refused_is_not(self):
        n = _node("n")
        n.rpc_client.return_value.get_version.side_effect = _conn_error(ReqConnectionError())
        self.assertFalse(rc.reachable(n))


if __name__ == "__main__":
    unittest.main()
