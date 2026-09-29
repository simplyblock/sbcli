"""Members of one consistency group must replicate to the SAME target node, so
their destination copies share one node/LVS and the group can be snapshotted and
failed over as a single crash-consistent unit (design-consistency-groups.md §4;
design-csi-addons-replication.md §14). _group_replication_node is the pure pick:
the target node an already-replicating member uses. Regression: 2026-09-26 —
ramen-e2e-cg's two members replicated to different target nodes (55aa5f76,
d9706433) because target co-location keyed on subsystem/nqn, and CG members are
distinct subsystems.
"""
from simplyblock_core.controllers.lvol_controller import _group_replication_node


class _LVol:
    def __init__(self, lvol_id, replication_node_id=""):
        self._id = lvol_id
        self.replication_node_id = replication_node_id

    def get_id(self):
        return self._id


class _Group:
    def __init__(self, members):
        self.members = {m: {"joined_seq": 1, "removed_seq": 0} for m in members}


def test_follows_a_member_that_already_has_a_target_node():
    lv1 = _LVol("LV1", replication_node_id="NODE_T1")
    lv2 = _LVol("LV2")  # the one being placed
    group = _Group(["LV1", "LV2"])
    assert _group_replication_node(lv2, group, [lv1, lv2]) == "NODE_T1"


def test_first_member_has_no_pin_yet():
    lv1 = _LVol("LV1")
    group = _Group(["LV1", "LV2"])
    assert _group_replication_node(lv1, group, [lv1]) == ""


def test_ignores_itself_even_when_it_has_a_node():
    lv1 = _LVol("LV1", replication_node_id="NODE_SELF")
    group = _Group(["LV1", "LV2"])
    # Only OTHER members pin the choice; a volume never follows its own stale id.
    assert _group_replication_node(lv1, group, [lv1]) == ""


def test_ignores_lvols_not_in_the_group():
    stranger = _LVol("OTHER", replication_node_id="NODE_X")
    lv2 = _LVol("LV2")
    group = _Group(["LV1", "LV2"])
    assert _group_replication_node(lv2, group, [stranger, lv2]) == ""


def test_no_group_returns_empty():
    lv2 = _LVol("LV2")
    assert _group_replication_node(lv2, None, [lv2]) == ""
