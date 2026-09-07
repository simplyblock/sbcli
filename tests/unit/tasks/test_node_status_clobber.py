"""Regression test: setting one node field must not clobber node status.

2026-08-31, iteration 4 of the migration-enabled soak. Node 61a887af was
container_killed; recovering its LVS_4 requires port-blocking that lvstore's
leader, fec6e628, which was simultaneously host_rebooted. The port-block path
did:

    current_leader.lvstore_status = "in_creation"
    current_leader.write_to_db()          # whole record, from a stale copy

current_leader had been read before the peer went down, so this restored
status=online over the monitor's offline write -- and emitted no
STATUS_CHANGE event, because a blind object write never goes through the
status path. Ground truth: fec6e628's SPDK started at 12:02:31, yet the
record read "online" from 11:52 to 12:02 and sn list showed it online
throughout.

Consequence: _check_peer_disconnected could never observe offline, so the
port-block was issued to a dead node and the restart aborted. Each retry
re-resurrected "online" -- five aborts over fifteen minutes, and a second
false status (61a887af forced to OFFLINE while its own SPDK was healthy).

These tests pin the invariant on the primitive the fix uses, and demonstrate
the lost update that the old pattern suffers, using a fake store with the
same read-modify-write semantics as db_controller.atomic_update.
"""
import copy


class _Node:
    """Minimal stand-in: the two fields whose interaction caused the bug."""

    def __init__(self, status="online", lvstore_status=""):
        self.status = status
        self.lvstore_status = lvstore_status
        self.uuid = "node-1"

    def get_id(self):
        return self.uuid


class _Store:
    """Stands in for FDB: holds the authoritative record."""

    def __init__(self, node):
        self._record = node

    def read(self):
        """A caller gets its own copy, as a DB read does."""
        return copy.deepcopy(self._record)

    def write_whole_object(self, obj):
        """The old pattern: serialise the caller's whole (possibly stale) copy."""
        self._record = copy.deepcopy(obj)

    def atomic_update(self, obj, mutate_fn):
        """The fix: re-read fresh inside the transaction, mutate, write back.

        Mirrors db_controller.atomic_update, whose docstring names this exact
        hazard: a plain read/assign/write "writes the entire serialized object
        and silently clobbers concurrent updates to other fields".
        """
        fresh = self._record
        mutate_fn(fresh)
        return copy.deepcopy(fresh)


def test_old_pattern_loses_the_monitor_status_write():
    """Demonstrates the bug, so the fix below is not vacuous."""
    store = _Store(_Node(status="online"))
    stale = store.read()                      # restart flow reads the peer
    store._record.status = "offline"          # monitor observes it die

    stale.lvstore_status = "in_creation"      # restart flow sets one field
    store.write_whole_object(stale)           # ... and writes the lot

    assert store._record.status == "online", (
        "expected the old pattern to resurrect the stale status")


def test_field_scoped_update_preserves_a_concurrent_status_write():
    store = _Store(_Node(status="online"))
    stale = store.read()
    store._record.status = "offline"           # monitor observes it die

    fresh = store.atomic_update(
        stale, lambda x: setattr(x, "lvstore_status", "in_creation"))

    assert store._record.status == "offline", (
        "the monitor's status must survive an lvstore_status write")
    assert store._record.lvstore_status == "in_creation"
    assert fresh.status == "offline", (
        "the caller must continue with the fresh record, not its stale copy")


def test_caller_sees_offline_so_the_next_attempt_can_skip_the_port_block():
    """Why this breaks the retry loop rather than just tidying a write."""
    store = _Store(_Node(status="online"))
    stale = store.read()
    store._record.status = "offline"
    store.atomic_update(stale, lambda x: setattr(x, "lvstore_status", "in_creation"))
    assert store.read().status == "offline"
