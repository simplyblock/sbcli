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
    src = inspect.getsource(cgc.create_group_snapshot)
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
