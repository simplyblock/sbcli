# coding=utf-8
"""
test_lvstore_mutation_lock.py – unit tests for the per-lvstore snapshot-mutation
lock.

Concurrent snapshot creates of the same lvstore must register their snapshots on
the secondary/tertiary in creation (blobid) order; out-of-order registration
builds the replica blob tree with a child before its parent and corrupts the
lvstore. snapshot_controller serializes the create→register section per lvstore
behind an LVStoreMutationLock (db_controller acquire/refresh/release FDB
transactions + the blocking acquire/heartbeat helpers in snapshot_controller).

All FDB access is faked in-memory; no live FoundationDB is required.
"""

import json
import unittest

from simplyblock_core import constants
from simplyblock_core.models.lvstore_lock import LVStoreMutationLock


# ---------------------------------------------------------------------------
# In-memory FDB transaction fake (mirrors test_node_add_concurrency.py)
# ---------------------------------------------------------------------------

class _Val(bytes):
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
    def __init__(self, store=None):
        self.store = dict(store or {})
        self.deleted = []

    def get(self, key):
        return _Future(self.store.get(key))

    def __setitem__(self, key, value):
        self.store[key] = json.loads(value)

    def __delitem__(self, key):
        self.deleted.append(key)
        self.store.pop(key, None)


def _new_db():
    from simplyblock_core.db_controller import DBController
    return DBController.__new__(DBController)


TTL = constants.LVSTORE_MUTATION_LOCK_TTL_SEC


class TestLVStoreMutationLockTx(unittest.TestCase):

    def _key(self, cluster_id, lvs_name):
        lock = LVStoreMutationLock()
        lock.cluster_id = cluster_id
        lock.lvs_name = lvs_name
        return lock.get_db_id().encode()

    def test_key_is_per_lvstore(self):
        # Different lvstores in the same cluster must not share a lock key.
        self.assertNotEqual(self._key("c1", "LVS_1"), self._key("c1", "LVS_2"))

    def test_acquire_when_absent(self):
        db = _new_db()
        tr = FakeTx()
        won, current = db._try_acquire_lvstore_lock_tx(
            tr, "c1", "LVS_1", "ownerA", now=1000, ttl=TTL)
        self.assertTrue(won)
        self.assertIsNone(current)
        written = tr.store[self._key("c1", "LVS_1")]
        self.assertEqual(written["owner"], "ownerA")
        self.assertEqual(written["acquired_at"], 1000)
        self.assertEqual(written["heartbeat_at"], 1000)

    def test_blocked_when_held_by_live_owner(self):
        db = _new_db()
        existing = {"cluster_id": "c1", "lvs_name": "LVS_1", "owner": "ownerA",
                    "acquired_at": 1000, "heartbeat_at": 1000}
        tr = FakeTx({self._key("c1", "LVS_1"): existing})
        now = 1000 + TTL - 1  # still within TTL
        won, current = db._try_acquire_lvstore_lock_tx(
            tr, "c1", "LVS_1", "ownerB", now=now, ttl=TTL)
        self.assertFalse(won)
        self.assertEqual(current, "ownerA")
        self.assertEqual(tr.store[self._key("c1", "LVS_1")]["owner"], "ownerA")

    def test_concurrent_other_lvstore_not_blocked(self):
        # A live holder on LVS_1 must not block a create on LVS_2.
        db = _new_db()
        held = {"cluster_id": "c1", "lvs_name": "LVS_1", "owner": "ownerA",
                "acquired_at": 1000, "heartbeat_at": 1000}
        tr = FakeTx({self._key("c1", "LVS_1"): held})
        won, current = db._try_acquire_lvstore_lock_tx(
            tr, "c1", "LVS_2", "ownerB", now=1001, ttl=TTL)
        self.assertTrue(won)
        self.assertIsNone(current)

    def test_reclaims_stale_lock(self):
        db = _new_db()
        existing = {"cluster_id": "c1", "lvs_name": "LVS_1", "owner": "ownerA",
                    "acquired_at": 1000, "heartbeat_at": 1000}
        tr = FakeTx({self._key("c1", "LVS_1"): existing})
        now = 1000 + TTL + 1  # heartbeat went stale
        won, current = db._try_acquire_lvstore_lock_tx(
            tr, "c1", "LVS_1", "ownerB", now=now, ttl=TTL)
        self.assertTrue(won)
        self.assertIsNone(current)
        written = tr.store[self._key("c1", "LVS_1")]
        self.assertEqual(written["owner"], "ownerB")
        self.assertEqual(written["acquired_at"], now)

    def test_same_owner_reacquire_preserves_acquired_at(self):
        db = _new_db()
        existing = {"cluster_id": "c1", "lvs_name": "LVS_1", "owner": "ownerA",
                    "acquired_at": 1000, "heartbeat_at": 1005}
        tr = FakeTx({self._key("c1", "LVS_1"): existing})
        won, _ = db._try_acquire_lvstore_lock_tx(
            tr, "c1", "LVS_1", "ownerA", now=1010, ttl=TTL)
        self.assertTrue(won)
        written = tr.store[self._key("c1", "LVS_1")]
        self.assertEqual(written["acquired_at"], 1000)   # preserved
        self.assertEqual(written["heartbeat_at"], 1010)  # refreshed

    def test_refresh_updates_heartbeat_for_owner(self):
        db = _new_db()
        existing = {"cluster_id": "c1", "lvs_name": "LVS_1", "owner": "ownerA",
                    "acquired_at": 1000, "heartbeat_at": 1000}
        tr = FakeTx({self._key("c1", "LVS_1"): existing})
        ok = db._refresh_lvstore_lock_tx(tr, "c1", "LVS_1", "ownerA", now=1050)
        self.assertTrue(ok)
        self.assertEqual(tr.store[self._key("c1", "LVS_1")]["heartbeat_at"], 1050)

    def test_refresh_fails_when_not_owner(self):
        db = _new_db()
        existing = {"cluster_id": "c1", "lvs_name": "LVS_1", "owner": "ownerA",
                    "acquired_at": 1000, "heartbeat_at": 1000}
        tr = FakeTx({self._key("c1", "LVS_1"): existing})
        ok = db._refresh_lvstore_lock_tx(tr, "c1", "LVS_1", "ownerB", now=1050)
        self.assertFalse(ok)
        self.assertEqual(tr.store[self._key("c1", "LVS_1")]["heartbeat_at"], 1000)

    def test_refresh_fails_when_absent(self):
        db = _new_db()
        tr = FakeTx()
        self.assertFalse(db._refresh_lvstore_lock_tx(tr, "c1", "LVS_1", "ownerA", now=1050))

    def test_release_deletes_when_owner_matches(self):
        db = _new_db()
        key = self._key("c1", "LVS_1")
        existing = {"cluster_id": "c1", "lvs_name": "LVS_1", "owner": "ownerA",
                    "acquired_at": 1000, "heartbeat_at": 1000}
        tr = FakeTx({key: existing})
        db._release_lvstore_lock_tx(tr, "c1", "LVS_1", "ownerA")
        self.assertIn(key, tr.deleted)
        self.assertNotIn(key, tr.store)

    def test_release_noop_when_owner_differs(self):
        # A late release from a holder that already lost the lock must not free
        # the lock a different holder has since reclaimed.
        db = _new_db()
        key = self._key("c1", "LVS_1")
        existing = {"cluster_id": "c1", "lvs_name": "LVS_1", "owner": "ownerB",
                    "acquired_at": 2000, "heartbeat_at": 2000}
        tr = FakeTx({key: existing})
        db._release_lvstore_lock_tx(tr, "c1", "LVS_1", "ownerA")
        self.assertNotIn(key, tr.deleted)
        self.assertEqual(tr.store[key]["owner"], "ownerB")


if __name__ == "__main__":
    unittest.main()
