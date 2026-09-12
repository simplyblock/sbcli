"""Consistency groups: membership epochs, placement pinning, generation
warnings, and the wiring contracts of the group snapshot flow."""
import inspect

import pytest

from simplyblock_core.controllers import consistency_group_controller as cgc
from simplyblock_core.models.replication import ConsistencyGroup


def _group(members=None, last_seq=0, lvs="LVS_1", node="NODE_A"):
    g = ConsistencyGroup()
    g.uuid = "g1"
    g.cluster_id = "CL"
    g.policy_id = "CL/p1"
    g.lvs_name = lvs
    g.node_id = node
    g.last_group_seq = last_seq
    g.members = members or {}
    return g


# --------------------------------------------------------------------------- #
# Epoch semantics (requirement 4)
# --------------------------------------------------------------------------- #

def test_member_included_between_join_and_removal():
    g = _group({"v1": {"joined_seq": 2, "removed_seq": 5}})
    assert not g.included_in_seq("v1", 1)
    assert g.included_in_seq("v1", 2)
    assert g.included_in_seq("v1", 5)
    assert not g.included_in_seq("v1", 6)


def test_open_epoch_member_included_from_join_onwards():
    g = _group({"v1": {"joined_seq": 3, "removed_seq": 0}})
    assert not g.included_in_seq("v1", 2)
    assert g.included_in_seq("v1", 3)
    assert g.included_in_seq("v1", 99)


def test_late_joiner_is_warned_about_for_older_generations():
    """A volume attached after generation 4 is NOT in generations 1..4:
    failing over to one of those must say so."""
    g = _group({
        "old": {"joined_seq": 1, "removed_seq": 0},
        "late": {"joined_seq": 5, "removed_seq": 0},
    }, last_seq=6)
    warnings = cgc.generation_membership_warnings(g, 4)
    assert len(warnings) == 1
    assert "late" in warnings[0]
    assert "NOT included" in warnings[0]
    assert "old" not in warnings[0].split(":")[-1]


def test_departed_member_is_warned_about_when_generation_contains_it():
    g = _group({
        "stay": {"joined_seq": 1, "removed_seq": 0},
        "gone": {"joined_seq": 1, "removed_seq": 3},
    }, last_seq=6)
    warnings = cgc.generation_membership_warnings(g, 2)
    assert len(warnings) == 1
    assert "gone" in warnings[0]
    assert "no longer" in warnings[0]


def test_matching_generation_produces_no_warnings():
    g = _group({
        "a": {"joined_seq": 1, "removed_seq": 0},
        "b": {"joined_seq": 1, "removed_seq": 0},
    }, last_seq=3)
    assert cgc.generation_membership_warnings(g, 3) == []


def test_both_warning_kinds_can_coexist():
    g = _group({
        "late": {"joined_seq": 5, "removed_seq": 0},
        "gone": {"joined_seq": 1, "removed_seq": 3},
    }, last_seq=6)
    warnings = cgc.generation_membership_warnings(g, 2)
    assert len(warnings) == 2


def test_no_group_or_no_seq_is_silent():
    assert cgc.generation_membership_warnings(None, 3) == []
    assert cgc.generation_membership_warnings(_group(), 0) == []


# --------------------------------------------------------------------------- #
# Placement / lifecycle contracts (requirement 1 + 2)
# --------------------------------------------------------------------------- #

class _FakeDB:
    def __init__(self, group):
        self._group = group

    def get_consistency_group_for_policy(self, policy_id):
        return self._group

    @property
    def kv_store(self):
        return None


class _Policy:
    policy_name = "p1"
    consistency_group = True

    def get_id(self):
        return "CL/p1"


class _Lvol:
    def __init__(self, lvol_id, node, lvs):
        self._id, self.node_id, self.lvs_name = lvol_id, node, lvs

    def get_id(self):
        return self._id


def test_attach_to_pinned_group_fails_on_wrong_lvs(monkeypatch):
    g = _group(lvs="LVS_1", node="NODE_A")
    monkeypatch.setattr(cgc, "db", _FakeDB(g))
    with pytest.raises(cgc.ConsistencyGroupError):
        cgc.add_member(_Policy(), _Lvol("v1", "NODE_B", "LVS_2"))
    assert "v1" not in (g.members or {})


def test_first_member_pins_the_group(monkeypatch):
    g = _group(lvs="", node="")
    g.write_to_db = lambda kv=None: None
    monkeypatch.setattr(cgc, "db", _FakeDB(g))
    cgc.add_member(_Policy(), _Lvol("v1", "NODE_A", "LVS_1"))
    assert g.node_id == "NODE_A" and g.lvs_name == "LVS_1"
    assert g.members["v1"]["joined_seq"] == 1


def test_late_joiner_epoch_starts_at_next_generation(monkeypatch):
    """Requirement 4: membership becomes active only with the FIRST group
    snapshot taken after the attach."""
    g = _group({"v1": {"joined_seq": 1, "removed_seq": 0}}, last_seq=7)
    g.write_to_db = lambda kv=None: None
    monkeypatch.setattr(cgc, "db", _FakeDB(g))
    cgc.add_member(_Policy(), _Lvol("v2", "NODE_A", "LVS_1"))
    assert g.members["v2"]["joined_seq"] == 8
    # ... and generation 7 correctly warns about it
    warnings = cgc.generation_membership_warnings(g, 7)
    assert warnings and "v2" in warnings[0]


def test_detach_closes_the_epoch_at_current_generation(monkeypatch):
    g = _group({"v1": {"joined_seq": 1, "removed_seq": 0}}, last_seq=4)
    g.write_to_db = lambda kv=None: None
    monkeypatch.setattr(cgc, "db", _FakeDB(g))
    cgc.remove_member("CL/p1", "v1")
    assert g.members["v1"]["removed_seq"] == 4
    assert g.included_in_seq("v1", 4)
    assert not g.included_in_seq("v1", 5)


# --------------------------------------------------------------------------- #
# Wiring contracts (source-inspection, matching the repo's test idiom)
# --------------------------------------------------------------------------- #

def test_policy_lifecycle_auto_creates_and_deletes_the_group():
    from simplyblock_core.controllers import replication_policy_controller as rpc
    src_add = inspect.getsource(rpc.add_policy)
    assert "create_group_for_policy" in src_add
    src_rm = inspect.getsource(rpc.remove_policy)
    assert "delete_group_for_policy" in src_rm


def test_attach_checks_the_group_before_any_state_is_written():
    from simplyblock_core.controllers import replication_policy_controller as rpc
    src = inspect.getsource(rpc.attach_policy)
    assert "add_member" in src
    assert src.index("add_member") < src.index("lvol.replication_policy_id = pol.get_id()")


def test_create_path_pins_cg_volumes_to_the_group_node():
    from simplyblock_core.controllers import lvol_controller as lc
    src = inspect.getsource(lc.add_lvol_ha)
    assert "pinned_node_for_policy" in src
    pin = src.index("pinned_node_for_policy")
    place = src.index("host_node = None")
    assert pin < place, "the pin must be resolved before placement"


def test_cadence_snapshots_cg_policies_as_a_group():
    from simplyblock_core.services import snapshot_monitor as sm
    src = inspect.getsource(sm.take_due_internal_snapshots)
    assert "create_group_snapshot" in src
    assert "grouped_ids" in src, "group members must leave the per-volume loop"


def test_group_snapshot_is_one_rpc_and_bumps_seq_only_on_full_success():
    src = inspect.getsource(cgc.create_group_snapshot_for_group)
    assert "bdev_lvol_snapshot_group" in src
    rpc_at = src.index("bdev_lvol_snapshot_group")
    seq_at = src.index("group.last_group_seq = group_seq")
    assert rpc_at < seq_at, "the generation counter moves only after the whole tick"
    assert "_rollback_all" in src


def test_failover_result_carries_membership_warnings():
    from simplyblock_core.controllers import lvol_controller as lc
    src = inspect.getsource(lc.replicate_lvol_on_target_cluster)
    assert "warnings_for_snapshot" in src
    assert '"warnings": warnings' in src


def test_target_snapshot_copy_inherits_group_provenance():
    from simplyblock_core.services import snapshot_replication as sr
    src = inspect.getsource(sr)
    assert 'new_snapshot.group_id = getattr(snapshot, "group_id", "")' in src
    assert 'new_snapshot.group_seq = getattr(snapshot, "group_seq", 0)' in src


# --------------------------------------------------------------------------- #
# Standalone group: ensure-by-name, membership, listings, delete, clone
# --------------------------------------------------------------------------- #

from simplyblock_core.models.lvol_model import LVol  # noqa: E402
from simplyblock_core.models.snapshot import SnapShot  # noqa: E402


class _StandaloneLvol:
    def __init__(self, lvol_id, node="NODE_A", lvs="LVS_1", status=LVol.STATUS_ONLINE):
        self._id, self.node_id, self.lvs_name, self.status = lvol_id, node, lvs, status

    def get_id(self):
        return self._id


class _StandaloneSnap:
    def __init__(self, snap_id, group_id, seq, lvol, status=SnapShot.STATUS_ONLINE, created_at=100):
        self._id, self.group_id, self.group_seq = snap_id, group_id, seq
        self.lvol, self.status, self.created_at = lvol, status, created_at

    def get_id(self):
        return self._id


class _StandaloneDB:
    """A fake DB that records writes, for the group-first controller paths."""
    kv_store = None

    def __init__(self):
        self.groups = {}
        self.lvols = {}
        self.snapshots = {}

    def register(self, group):
        self.groups[group.uuid] = group

    def get_consistency_groups(self, cluster_id=None):
        return [g for g in self.groups.values()
                if cluster_id is None or g.cluster_id == cluster_id]

    def get_consistency_group_by_name(self, cluster_id, name):
        for g in self.groups.values():
            if g.cluster_id == cluster_id and g.group_name == name:
                return g
        return None

    def get_consistency_group_by_id(self, group_id):
        wanted = group_id.split('/')[-1]
        for g in self.groups.values():
            if g.uuid == wanted:
                return g
        raise KeyError(group_id)

    def get_lvol_by_id(self, lvol_id):
        return self.lvols[lvol_id]

    def get_snapshots(self, cluster_id=None):
        return list(self.snapshots.values())


@pytest.fixture
def standalone(monkeypatch):
    db = _StandaloneDB()
    monkeypatch.setattr(cgc, "db", db)
    # Persisting a group registers it in the fake so lookups find it.
    monkeypatch.setattr(ConsistencyGroup, "write_to_db",
                        lambda self, kv=None: db.register(self))
    return db


def test_ensure_group_is_idempotent_by_name(standalone):
    a = cgc.ensure_group("CL", "db-group")
    b = cgc.ensure_group("CL", "db-group")
    assert a.uuid == b.uuid
    assert len(standalone.get_consistency_groups("CL")) == 1
    assert a.group_name == "db-group"


def test_concurrent_first_volumes_converge_on_one_group(standalone):
    g1 = cgc.ensure_group("CL", "db-group")
    cgc.add_member_to_group(g1, _StandaloneLvol("v1"))
    g2 = cgc.ensure_group("CL", "db-group")   # the racing second volume
    cgc.add_member_to_group(g2, _StandaloneLvol("v2"))
    assert g1.uuid == g2.uuid
    assert set(g1.members) == {"v1", "v2"}
    assert len(standalone.get_consistency_groups("CL")) == 1


def test_add_member_to_group_rejects_off_store(standalone):
    g = cgc.ensure_group("CL", "db-group")
    cgc.add_member_to_group(g, _StandaloneLvol("v1", node="NODE_A", lvs="LVS_1"))
    with pytest.raises(cgc.ConsistencyGroupError):
        cgc.add_member_to_group(g, _StandaloneLvol("v2", node="NODE_B", lvs="LVS_2"))
    assert "v2" not in g.members


def test_add_member_to_group_rejects_over_cap(standalone):
    from simplyblock_core import constants
    cap = constants.MAX_CONSISTENCY_GROUP_MEMBERS
    g = cgc.ensure_group("CL", "big-group")
    for i in range(cap):
        cgc.add_member_to_group(g, _StandaloneLvol(f"v{i}"))
    assert sum(1 for m in g.members.values() if m["removed_seq"] == 0) == cap
    with pytest.raises(cgc.ConsistencyGroupError):
        cgc.add_member_to_group(g, _StandaloneLvol("one-too-many"))
    assert "one-too-many" not in g.members


def test_detached_member_frees_a_cap_slot(standalone):
    from simplyblock_core import constants
    cap = constants.MAX_CONSISTENCY_GROUP_MEMBERS
    g = cgc.ensure_group("CL", "big-group")
    for i in range(cap):
        cgc.add_member_to_group(g, _StandaloneLvol(f"v{i}"))
    # A generation has been taken, so the detach closes the epoch (rather than
    # dropping the entry) and frees the slot.
    g.last_group_seq = 1
    cgc.remove_member_from_group(g, "v0")
    assert g.members["v0"]["removed_seq"] != 0
    cgc.add_member_to_group(g, _StandaloneLvol("replacement"))
    assert "replacement" in g.members
    assert sum(1 for m in g.members.values() if m["removed_seq"] == 0) == cap


def test_detach_before_first_generation_frees_a_cap_slot(standalone):
    """The generation-0 counterpart: no generation ever contained the member,
    so the detach drops the entry entirely, and the slot is free again."""
    from simplyblock_core import constants
    cap = constants.MAX_CONSISTENCY_GROUP_MEMBERS
    g = cgc.ensure_group("CL", "big-group")
    for i in range(cap):
        cgc.add_member_to_group(g, _StandaloneLvol(f"v{i}"))
    cgc.remove_member_from_group(g, "v0")
    assert "v0" not in g.members
    cgc.add_member_to_group(g, _StandaloneLvol("replacement"))
    assert "replacement" in g.members
    assert sum(1 for m in g.members.values() if m["removed_seq"] == 0) == cap


def test_precheck_names_offline_and_off_store_members(standalone):
    g = cgc.ensure_group("CL", "db-group")
    g.node_id, g.lvs_name = "NODE_A", "LVS_1"
    g.members = {"ok": {"joined_seq": 1, "removed_seq": 0},
                 "down": {"joined_seq": 1, "removed_seq": 0},
                 "moved": {"joined_seq": 1, "removed_seq": 0}}
    standalone.lvols = {
        "ok": _StandaloneLvol("ok"),
        "down": _StandaloneLvol("down", status=LVol.STATUS_OFFLINE),
        "moved": _StandaloneLvol("moved", node="NODE_B", lvs="LVS_9"),
    }
    members, err = cgc._precheck_members(g)
    assert members is None
    assert "down" in err and "moved" in err


def test_precheck_passes_when_all_healthy(standalone):
    g = cgc.ensure_group("CL", "db-group")
    g.node_id, g.lvs_name = "NODE_A", "LVS_1"
    g.members = {"a": {"joined_seq": 1, "removed_seq": 0}}
    standalone.lvols = {"a": _StandaloneLvol("a")}
    members, err = cgc._precheck_members(g)
    assert err is None
    assert [m.get_id() for m in members] == ["a"]


def test_group_for_lvol_finds_open_member_only(standalone):
    g = cgc.ensure_group("CL", "db-group")
    g.members = {"open": {"joined_seq": 1, "removed_seq": 0},
                 "left": {"joined_seq": 1, "removed_seq": 3}}
    standalone.register(g)
    assert cgc.group_for_lvol("open") is g
    assert cgc.group_for_lvol("left") is None
    assert cgc.group_for_lvol("stranger") is None


def test_list_members_reports_current_only(standalone):
    g = cgc.ensure_group("CL", "db-group")
    g.node_id, g.lvs_name = "NODE_A", "LVS_1"
    g.members = {"a": {"joined_seq": 1, "removed_seq": 0},
                 "gone": {"joined_seq": 1, "removed_seq": 4}}
    standalone.lvols = {"a": _StandaloneLvol("a")}
    rows = cgc.list_members(g)
    assert [r["lvol_id"] for r in rows] == ["a"]
    assert rows[0]["online"] is True


def test_list_generations_reports_incomplete(standalone):
    g = cgc.ensure_group("CL", "db-group")
    g.members = {"a": {"joined_seq": 1, "removed_seq": 0},
                 "b": {"joined_seq": 1, "removed_seq": 0}}
    gid = g.get_id()
    la, lb = _StandaloneLvol("a"), _StandaloneLvol("b")
    # generation 1 complete (both members), generation 2 missing member b
    standalone.snapshots = {
        "s1a": _StandaloneSnap("s1a", gid, 1, la),
        "s1b": _StandaloneSnap("s1b", gid, 1, lb),
        "s2a": _StandaloneSnap("s2a", gid, 2, la),
    }
    rows = {r["group_seq"]: r for r in cgc.list_generations(g)}
    assert rows[1]["expected"] == 2 and rows[1]["present"] == 2 and rows[1]["complete"]
    assert rows[2]["expected"] == 2 and rows[2]["present"] == 1 and not rows[2]["complete"]


def test_delete_generation_removes_every_member_snapshot(standalone, monkeypatch):
    from simplyblock_core.controllers import snapshot_controller
    g = cgc.ensure_group("CL", "db-group")
    gid = g.get_id()
    la = _StandaloneLvol("a")
    standalone.snapshots = {
        "s2a": _StandaloneSnap("s2a", gid, 2, la),
        "s2b": _StandaloneSnap("s2b", gid, 2, la),
        "s3a": _StandaloneSnap("s3a", gid, 3, la),
    }
    deleted_calls = []

    def _fake_delete(sid, *a, **k):
        deleted_calls.append(sid)
        return True

    monkeypatch.setattr(snapshot_controller, "delete", _fake_delete)
    deleted, err = cgc.delete_generation(g, 2)
    assert err is None
    assert set(deleted_calls) == {"s2a", "s2b"}   # generation 3 untouched


def test_delete_generation_unknown_seq_errors(standalone):
    g = cgc.ensure_group("CL", "db-group")
    deleted, err = cgc.delete_generation(g, 9)
    assert deleted is None and "not found" in err


def test_clone_generation_forms_new_group(standalone, monkeypatch):
    from simplyblock_core.controllers import snapshot_controller
    g = cgc.ensure_group("CL", "db-group")
    g.node_id, g.lvs_name = "NODE_A", "LVS_1"
    gid = g.get_id()
    standalone.snapshots = {
        "s1a": _StandaloneSnap("s1a", gid, 1, _StandaloneLvol("a")),
        "s1b": _StandaloneSnap("s1b", gid, 1, _StandaloneLvol("b")),
    }
    # each clone lands on the pinned node so the new group colocates
    standalone.lvols = {"clone-a": _StandaloneLvol("clone-a"),
                        "clone-b": _StandaloneLvol("clone-b")}
    seq = iter(["clone-a", "clone-b"])
    monkeypatch.setattr(snapshot_controller, "clone",
                        lambda sid, name, *a, **k: (next(seq), False))
    created, err = cgc.clone_generation(g, 1, into_name="restored")
    assert err is None
    assert set(created) == {"clone-a", "clone-b"}
    new_group = standalone.get_consistency_group_by_name("CL", "restored")
    assert new_group is not None
    assert set(new_group.members) == {"clone-a", "clone-b"}
