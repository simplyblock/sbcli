# coding=utf-8
"""
test_node_add_concurrency.py – unit tests for the parallel node-add machinery.

Node-add is parallelized so the slow node-local setup (SPDK boot, device prep)
runs concurrently across node-add tasks, while the two things that must NOT
race are guarded:

  1. The cross-node mesh wiring — serialized per cluster by ClusterAddNodeLock
     (db_controller acquire/refresh/release FDB transactions + the blocking
     acquire/heartbeat helpers in storage_node_ops).
  2. NVMe-oF port allocation — made atomic against concurrent adds by
     transactional PortReservation (db_controller.reserve_cluster_nvmf_port).

Plus the now-concurrent set_cluster_status, made a transactional compare-and-set.

All FDB access is faked in-memory; no live FoundationDB is required.
"""

import json
import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core import constants
from simplyblock_core.models.cluster import ClusterAddNodeLock, PortReservation
from simplyblock_core.models.storage_node import StorageNode


# ---------------------------------------------------------------------------
# In-memory FDB transaction fake
# ---------------------------------------------------------------------------

class _Val(bytes):
    """An FDB value stand-in: bytes (so json.loads works) that also answers
    .present() like a real fdb future result."""
    def present(self):
        return getattr(self, "_present", len(self) > 0)


def _mk_val(data):
    if data is None:
        v = _Val(b"")
        v._present = False
    else:
        v = _Val(json.dumps(data).encode())
        v._present = True
    return v


class _Future:
    def __init__(self, data):
        self._data = data

    def wait(self):
        return _mk_val(self._data)


class FakeTx:
    """Minimal FDB transaction: dict keyed by bytes -> decoded dict."""

    def __init__(self, store=None):
        self.store = dict(store or {})
        self.deleted = []
        self.written = {}

    def get(self, key):
        return _Future(self.store.get(key))

    def __setitem__(self, key, value):
        decoded = json.loads(value)
        self.store[key] = decoded
        self.written[key] = decoded

    def __delitem__(self, key):
        self.deleted.append(key)
        self.store.pop(key, None)

    def get_range_startswith(self, prefix, limit=0, reverse=False):
        out = []
        for k, data in self.store.items():
            if k.startswith(prefix):
                out.append((k, json.dumps(data).encode()))
        return out


def _new_db():
    from simplyblock_core.db_controller import DBController
    return DBController.__new__(DBController)


# ---------------------------------------------------------------------------
# 1. ClusterAddNodeLock acquire / refresh / release transactions
# ---------------------------------------------------------------------------

class TestClusterAddLockTx(unittest.TestCase):

    def _key(self, cluster_id):
        lock = ClusterAddNodeLock()
        lock.cluster_id = cluster_id
        return lock.get_db_id().encode()

    def test_acquire_when_absent(self):
        db = _new_db()
        tr = FakeTx()
        won, current = db._try_acquire_cluster_add_lock_tx(tr, "c1", "ownerA", now=1000)
        self.assertTrue(won)
        self.assertIsNone(current)
        written = tr.store[self._key("c1")]
        self.assertEqual(written["owner"], "ownerA")
        self.assertEqual(written["acquired_at"], 1000)
        self.assertEqual(written["heartbeat_at"], 1000)

    def test_blocked_when_held_by_live_owner(self):
        db = _new_db()
        existing = {"cluster_id": "c1", "owner": "ownerA",
                    "acquired_at": 1000, "heartbeat_at": 1000}
        tr = FakeTx({self._key("c1"): existing})
        # heartbeat 1000, now within TTL -> still alive
        now = 1000 + constants.CLUSTER_ADD_LOCK_TTL_SEC - 1
        won, current = db._try_acquire_cluster_add_lock_tx(tr, "c1", "ownerB", now=now)
        self.assertFalse(won)
        self.assertEqual(current, "ownerA")
        # must not have overwritten the lock
        self.assertEqual(tr.store[self._key("c1")]["owner"], "ownerA")

    def test_reclaims_stale_lock(self):
        db = _new_db()
        existing = {"cluster_id": "c1", "owner": "ownerA",
                    "acquired_at": 1000, "heartbeat_at": 1000}
        tr = FakeTx({self._key("c1"): existing})
        now = 1000 + constants.CLUSTER_ADD_LOCK_TTL_SEC + 1  # heartbeat went stale
        won, current = db._try_acquire_cluster_add_lock_tx(tr, "c1", "ownerB", now=now)
        self.assertTrue(won)
        self.assertIsNone(current)
        written = tr.store[self._key("c1")]
        self.assertEqual(written["owner"], "ownerB")
        self.assertEqual(written["acquired_at"], now)  # fresh acquisition

    def test_same_owner_reacquire_preserves_acquired_at(self):
        db = _new_db()
        existing = {"cluster_id": "c1", "owner": "ownerA",
                    "acquired_at": 1000, "heartbeat_at": 1005}
        tr = FakeTx({self._key("c1"): existing})
        won, current = db._try_acquire_cluster_add_lock_tx(tr, "c1", "ownerA", now=1010)
        self.assertTrue(won)
        written = tr.store[self._key("c1")]
        self.assertEqual(written["acquired_at"], 1000)   # preserved
        self.assertEqual(written["heartbeat_at"], 1010)  # refreshed

    def test_refresh_updates_heartbeat_for_owner(self):
        db = _new_db()
        existing = {"cluster_id": "c1", "owner": "ownerA",
                    "acquired_at": 1000, "heartbeat_at": 1000}
        tr = FakeTx({self._key("c1"): existing})
        ok = db._refresh_cluster_add_lock_tx(tr, "c1", "ownerA", now=1050)
        self.assertTrue(ok)
        self.assertEqual(tr.store[self._key("c1")]["heartbeat_at"], 1050)

    def test_refresh_fails_when_not_owner(self):
        db = _new_db()
        existing = {"cluster_id": "c1", "owner": "ownerA",
                    "acquired_at": 1000, "heartbeat_at": 1000}
        tr = FakeTx({self._key("c1"): existing})
        ok = db._refresh_cluster_add_lock_tx(tr, "c1", "ownerB", now=1050)
        self.assertFalse(ok)
        self.assertEqual(tr.store[self._key("c1")]["heartbeat_at"], 1000)

    def test_refresh_fails_when_absent(self):
        db = _new_db()
        tr = FakeTx()
        self.assertFalse(db._refresh_cluster_add_lock_tx(tr, "c1", "ownerA", now=1050))

    def test_release_deletes_for_owner(self):
        db = _new_db()
        existing = {"cluster_id": "c1", "owner": "ownerA",
                    "acquired_at": 1000, "heartbeat_at": 1000}
        tr = FakeTx({self._key("c1"): existing})
        db._release_cluster_add_lock_tx(tr, "c1", "ownerA")
        self.assertIn(self._key("c1"), tr.deleted)

    def test_release_noop_for_non_owner(self):
        db = _new_db()
        existing = {"cluster_id": "c1", "owner": "ownerA",
                    "acquired_at": 1000, "heartbeat_at": 1000}
        tr = FakeTx({self._key("c1"): existing})
        db._release_cluster_add_lock_tx(tr, "c1", "ownerB")
        self.assertEqual(tr.deleted, [])
        self.assertIn(self._key("c1"), tr.store)

    def test_release_noop_when_absent(self):
        db = _new_db()
        tr = FakeTx()
        db._release_cluster_add_lock_tx(tr, "c1", "ownerA")  # must not raise
        self.assertEqual(tr.deleted, [])


# ---------------------------------------------------------------------------
# 2. Port reservation transaction
# ---------------------------------------------------------------------------

class TestPortReservationTx(unittest.TestCase):

    def _res_key(self, cluster_id, port):
        r = PortReservation()
        r.cluster_id = cluster_id
        r.port = port
        return r.get_db_id().encode()

    def test_returns_base_when_nothing_used(self):
        db = _new_db()
        tr = FakeTx()
        port = db._reserve_next_nvmf_port_tx(tr, "c1", 4420, set(), "owner", now=1000)
        self.assertEqual(port, 4420)
        # reservation persisted
        self.assertIn(self._res_key("c1", 4420), tr.store)
        self.assertEqual(tr.store[self._res_key("c1", 4420)]["owner"], "owner")

    def test_skips_node_used_ports(self):
        db = _new_db()
        tr = FakeTx()
        port = db._reserve_next_nvmf_port_tx(tr, "c1", 4420, {4420, 4421}, "o", now=1000)
        self.assertEqual(port, 4422)

    def test_skips_live_reservations(self):
        db = _new_db()
        tr = FakeTx({
            self._res_key("c1", 4420): {"cluster_id": "c1", "port": 4420,
                                        "owner": "other", "created_at": 1000},
        })
        port = db._reserve_next_nvmf_port_tx(tr, "c1", 4420, set(), "o", now=1001)
        self.assertEqual(port, 4421)

    def test_drops_stale_reservation_and_reuses_port(self):
        db = _new_db()
        stale_created = 1000
        tr = FakeTx({
            self._res_key("c1", 4420): {"cluster_id": "c1", "port": 4420,
                                        "owner": "dead", "created_at": stale_created},
        })
        now = stale_created + constants.PORT_RESERVATION_TTL_SEC + 1
        port = db._reserve_next_nvmf_port_tx(tr, "c1", 4420, set(), "o", now=now)
        # stale reservation reclaimed and its port reused
        self.assertEqual(port, 4420)
        self.assertIn(self._res_key("c1", 4420), tr.deleted)

    def test_ignores_other_cluster_reservations(self):
        db = _new_db()
        tr = FakeTx({
            self._res_key("c2", 4420): {"cluster_id": "c2", "port": 4420,
                                        "owner": "x", "created_at": 1000},
        })
        # c2's reservation lives under a different key prefix; reserving for c1
        # must not see it and should hand out the base port.
        port = db._reserve_next_nvmf_port_tx(tr, "c1", 4420, set(), "o", now=1001)
        self.assertEqual(port, 4420)

    def test_concurrent_allocations_distinct(self):
        """Two reservations against the same (shared) tx state pick distinct
        ports — the second sees the first's reservation."""
        db = _new_db()
        tr = FakeTx()
        p1 = db._reserve_next_nvmf_port_tx(tr, "c1", 4420, set(), "a", now=1000)
        p2 = db._reserve_next_nvmf_port_tx(tr, "c1", 4420, set(), "b", now=1000)
        self.assertNotEqual(p1, p2)
        self.assertEqual({p1, p2}, {4420, 4421})


# ---------------------------------------------------------------------------
# 3. Blocking-acquire / heartbeat helpers in storage_node_ops
# ---------------------------------------------------------------------------

class TestBlockingAcquireHelper(unittest.TestCase):

    def test_returns_true_on_immediate_acquire(self):
        from simplyblock_core.storage_node_ops import _acquire_cluster_add_lock_blocking
        db = MagicMock()
        db.acquire_cluster_add_lock.return_value = (True, None)
        self.assertTrue(_acquire_cluster_add_lock_blocking(db, "c1", "owner"))

    def test_returns_false_on_timeout(self):
        from simplyblock_core.storage_node_ops import _acquire_cluster_add_lock_blocking
        db = MagicMock()
        db.acquire_cluster_add_lock.return_value = (False, "someone-else")
        # timeout=0 -> one failed attempt, then deadline reached, no sleep
        self.assertFalse(_acquire_cluster_add_lock_blocking(db, "c1", "owner", timeout=0))

    def test_retries_until_acquired(self):
        from simplyblock_core.storage_node_ops import _acquire_cluster_add_lock_blocking
        db = MagicMock()
        db.acquire_cluster_add_lock.side_effect = [
            (False, "other"), (False, "other"), (True, None)]
        with patch("simplyblock_core.storage_node_ops.time.sleep"):
            self.assertTrue(
                _acquire_cluster_add_lock_blocking(db, "c1", "owner", timeout=100, poll=0))
        self.assertEqual(db.acquire_cluster_add_lock.call_count, 3)

    def test_heartbeat_stops_when_lock_lost(self):
        from simplyblock_core.storage_node_ops import _cluster_add_lock_heartbeat
        import threading
        db = MagicMock()
        db.refresh_cluster_add_lock.return_value = False  # lost the lock
        stop = threading.Event()
        # wait() patched to return False once (fire) then we rely on the lost
        # refresh to break the loop.
        with patch.object(stop, "wait", side_effect=[False, True]):
            _cluster_add_lock_heartbeat(db, "c1", "owner", stop)
        db.refresh_cluster_add_lock.assert_called_once()


# ---------------------------------------------------------------------------
# 4. set_cluster_status transactional compare-and-set
# ---------------------------------------------------------------------------

class TestSetClusterStatusAtomic(unittest.TestCase):

    def _make_cluster(self, status):
        from simplyblock_core.models.cluster import Cluster
        c = Cluster()
        c.uuid = "c1"
        c.status = status
        return c

    @patch("simplyblock_core.cluster_ops.cluster_events")
    @patch("simplyblock_core.cluster_ops.db_controller")
    def test_uses_atomic_update_and_emits_event(self, mock_db, mock_events):
        from simplyblock_core import cluster_ops
        from simplyblock_core.models.cluster import Cluster

        cluster = self._make_cluster(Cluster.STATUS_ACTIVE)
        mock_db.get_cluster_by_id.return_value = cluster

        def _fake_atomic_update(obj, mutate_fn):
            mutate_fn(obj)
            return obj
        mock_db.atomic_update.side_effect = _fake_atomic_update

        cluster_ops.set_cluster_status("c1", Cluster.STATUS_IN_EXPANSION)

        mock_db.atomic_update.assert_called_once()
        # status changed on the object
        self.assertEqual(cluster.status, Cluster.STATUS_IN_EXPANSION)
        # event emitted with correct old/new
        mock_events.cluster_status_change.assert_called_once()
        args, _ = mock_events.cluster_status_change.call_args
        self.assertEqual(args[1], Cluster.STATUS_IN_EXPANSION)
        self.assertEqual(args[2], Cluster.STATUS_ACTIVE)

    @patch("simplyblock_core.cluster_ops.cluster_events")
    @patch("simplyblock_core.cluster_ops.db_controller")
    def test_noop_when_already_target_status(self, mock_db, mock_events):
        from simplyblock_core import cluster_ops
        from simplyblock_core.models.cluster import Cluster

        cluster = self._make_cluster(Cluster.STATUS_IN_EXPANSION)
        mock_db.get_cluster_by_id.return_value = cluster

        cluster_ops.set_cluster_status("c1", Cluster.STATUS_IN_EXPANSION)

        mock_db.atomic_update.assert_not_called()
        mock_events.cluster_status_change.assert_not_called()

    @patch("simplyblock_core.cluster_ops.cluster_events")
    @patch("simplyblock_core.cluster_ops.db_controller")
    def test_no_event_when_peer_won_race(self, mock_db, mock_events):
        """A concurrent caller already set the target status between our read
        and the tx: the mutator aborts the write (returns False) and we must
        not emit a spurious event."""
        from simplyblock_core import cluster_ops
        from simplyblock_core.models.cluster import Cluster

        cluster = self._make_cluster(Cluster.STATUS_ACTIVE)
        mock_db.get_cluster_by_id.return_value = cluster

        def _fake_atomic_update(obj, mutate_fn):
            # Simulate fresh row already at target: mutator returns False.
            fresh = self._make_cluster(Cluster.STATUS_IN_EXPANSION)
            mutate_fn(fresh)
            return fresh
        mock_db.atomic_update.side_effect = _fake_atomic_update

        cluster_ops.set_cluster_status("c1", Cluster.STATUS_IN_EXPANSION)
        mock_events.cluster_status_change.assert_not_called()


# ---------------------------------------------------------------------------
# 5. Public DBController lock/reservation wrappers (now computation + kv guard)
# ---------------------------------------------------------------------------

class TestLockWrappers(unittest.TestCase):

    @patch("simplyblock_core.db_controller.fdb.transactional", create=True)
    def test_acquire_wrapper_runs_tx(self, mock_transactional):
        mock_transactional.return_value = MagicMock(return_value=(True, None))
        db = _new_db()
        db.kv_store = MagicMock()
        won, current = db.acquire_cluster_add_lock("c1", "owner")
        self.assertTrue(won)
        self.assertIsNone(current)
        mock_transactional.assert_called_once()

    def test_acquire_wrapper_no_db(self):
        db = _new_db()
        db.kv_store = None
        won, reason = db.acquire_cluster_add_lock("c1", "owner")
        self.assertFalse(won)
        self.assertEqual(reason, "No DB connection")

    def test_refresh_wrapper_no_db(self):
        db = _new_db()
        db.kv_store = None
        self.assertFalse(db.refresh_cluster_add_lock("c1", "owner"))

    @patch("simplyblock_core.db_controller.fdb.transactional", create=True)
    def test_refresh_wrapper_runs_tx(self, mock_transactional):
        mock_transactional.return_value = MagicMock(return_value=True)
        db = _new_db()
        db.kv_store = MagicMock()
        self.assertTrue(db.refresh_cluster_add_lock("c1", "owner"))

    def test_release_wrapper_no_db_is_noop(self):
        db = _new_db()
        db.kv_store = None
        # Must not raise.
        db.release_cluster_add_lock("c1", "owner")

    @patch("simplyblock_core.db_controller.fdb.transactional", create=True)
    def test_release_wrapper_runs_tx(self, mock_transactional):
        inner = MagicMock()
        mock_transactional.return_value = inner
        db = _new_db()
        db.kv_store = MagicMock()
        db.release_cluster_add_lock("c1", "owner")
        inner.assert_called_once()

    @patch("simplyblock_core.utils.get_node_nvmf_ports", return_value={4420})
    @patch("simplyblock_core.utils.get_nvmf_base_port", return_value=4420)
    @patch("simplyblock_core.db_controller.fdb.transactional", create=True)
    def test_reserve_wrapper_passes_base_and_node_used(
            self, mock_transactional, mock_base, mock_node_ports):
        inner = MagicMock(return_value=4421)
        mock_transactional.return_value = inner
        db = _new_db()
        db.kv_store = MagicMock()
        port = db.reserve_cluster_nvmf_port("c1", "owner")
        self.assertEqual(port, 4421)
        # inner(self, kv_store, cluster_id, base_port, node_used, owner, now)
        args = inner.call_args[0]
        self.assertEqual(args[3], 4420)        # base_port
        self.assertEqual(args[4], {4420})      # node_used
        self.assertEqual(args[5], "owner")     # owner


# ---------------------------------------------------------------------------
# 6. utils port helpers: reservation-aware used-port set
# ---------------------------------------------------------------------------

class TestUtilsPortHelpers(unittest.TestCase):

    @patch("simplyblock_core.utils._get_cluster_port_config", return_value=(4420, 8080, 50001))
    def test_get_nvmf_base_port(self, mock_config):
        from simplyblock_core.utils import get_nvmf_base_port
        self.assertEqual(get_nvmf_base_port("c1"), 4420)

    @patch("simplyblock_core.utils._get_active_port_reservations", return_value={4500})
    @patch("simplyblock_core.utils.get_node_nvmf_ports", return_value={4420, 4421})
    def test_all_nvmf_ports_unions_reservations(self, mock_nodes, mock_res):
        from simplyblock_core.utils import _get_all_nvmf_ports
        self.assertEqual(_get_all_nvmf_ports("c1"), {4420, 4421, 4500})

    @patch("simplyblock_core.db_controller.DBController")
    def test_active_reservations_filters_stale(self, mock_db_cls):
        from simplyblock_core.utils import _get_active_port_reservations
        mock_db_cls.return_value.kv_store = MagicMock()
        # created_at far in the future -> fresh; created_at 0 -> stale.
        fresh = PortReservation()
        fresh.cluster_id = "c1"
        fresh.port = 4420
        fresh.created_at = 10 ** 12
        stale = PortReservation()
        stale.cluster_id = "c1"
        stale.port = 4421
        stale.created_at = 0
        with patch.object(PortReservation, "read_from_db", return_value=[fresh, stale]):
            res = _get_active_port_reservations("c1")
        self.assertEqual(res, {4420})

    @patch("simplyblock_core.db_controller.DBController")
    def test_active_reservations_best_effort_on_read_error(self, mock_db_cls):
        """A read failure must fall back to an empty set, never propagate."""
        from simplyblock_core.utils import _get_active_port_reservations
        mock_db_cls.return_value.kv_store = MagicMock()
        with patch.object(PortReservation, "read_from_db", side_effect=RuntimeError("boom")):
            self.assertEqual(_get_active_port_reservations("c1"), set())


# ---------------------------------------------------------------------------
# 7. Cluster-wide device-order counters under parallel add
#    (regression: parallel add produced duplicate physical_label /
#     cluster_device_order -> corrupt distr map -> lvstore I/O error at
#     activation)
# ---------------------------------------------------------------------------

class TestPhysicalLabelExcludeSelf(unittest.TestCase):

    def _node(self, uuid, mgmt_ip, label, cluster_id="c1"):
        n = StorageNode()
        n.uuid = uuid
        n.mgmt_ip = mgmt_ip
        n.physical_label = label
        n.cluster_id = cluster_id
        return n

    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_without_exclude_returns_own_stale_label(self, mock_db_cls):
        """Demonstrates why exclude is needed: a node already persisted with a
        provisional label matches its own mgmt_ip and gets that stale label
        back."""
        from simplyblock_core.storage_node_ops import get_next_physical_device_order
        me = self._node("me", "10.0.0.9", 2)
        nodes = [self._node("a", "10.0.0.1", 1), me]
        mock_db_cls.return_value.get_storage_nodes_by_cluster_id.return_value = nodes
        self.assertEqual(get_next_physical_device_order(me), 2)

    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_exclude_self_picks_next_free_label(self, mock_db_cls):
        from simplyblock_core.storage_node_ops import get_next_physical_device_order
        me = self._node("me", "10.0.0.9", 2)  # stale provisional label
        nodes = [
            self._node("a", "10.0.0.1", 1),
            self._node("b", "10.0.0.2", 2),
            me,
        ]
        mock_db_cls.return_value.get_storage_nodes_by_cluster_id.return_value = nodes
        # Self skipped; 1 and 2 used by peers -> next free is 3.
        self.assertEqual(
            get_next_physical_device_order(me, exclude_node_id=me.get_id()), 3)

    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_exclude_self_still_shares_colocated_peer_label(self, mock_db_cls):
        """A co-located peer (same mgmt_ip) still shares its label even when we
        exclude self — that intentional sharing must survive the exclude."""
        from simplyblock_core.storage_node_ops import get_next_physical_device_order
        me = self._node("me", "10.0.0.1", 0)
        peer_same_host = self._node("a", "10.0.0.1", 5)
        mock_db_cls.return_value.get_storage_nodes_by_cluster_id.return_value = [
            peer_same_host, me]
        self.assertEqual(
            get_next_physical_device_order(me, exclude_node_id=me.get_id()), 5)


if __name__ == "__main__":
    unittest.main()
