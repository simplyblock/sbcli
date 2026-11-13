"""Pure rule of the position of a node in the ACTIVE triplet of an LVS
(storage_node_ops.active_position): primary / secondary / tertiary of the
triplet that leads it, inactive in the other one, moving while the leadership
is being moved. Also the ids of the active triplet in slot order
(active_triplet_ids). The tests that read the nodes of that triplet from the
real database (active_triplet_nodes) are in
tests/integration/test_sync_replication_active_triplet.py.
"""
import pytest

from simplyblock_core import storage_node_ops as ops
from simplyblock_core.models.storage_node import StorageNode

PRIMARY, SECONDARY, TERTIARY = (
    ops.ActivePosition.PRIMARY, ops.ActivePosition.SECONDARY, ops.ActivePosition.TERTIARY)
INACTIVE, MOVING = ops.ActivePosition.INACTIVE, ops.ActivePosition.MOVING

HOME = ("p", "s", "t")
REMOTE = ("rp", "rs", "rt")


def _owner(**fields):
    node = StorageNode()
    node.uuid = "p"
    node.site = "site-a"
    node.lvstore = "LVS_1"
    node.secondary_node_id = "s"
    node.tertiary_node_id = "t"
    node.remote_primary_node_id = "rp"
    node.remote_secondary_node_id = "rs"
    node.remote_tertiary_node_id = "rt"
    for key, value in fields.items():
        setattr(node, key, value)
    return node


def _positions(owner, node_ids):
    return tuple(ops.active_position(owner, node_id) for node_id in node_ids)


class TestActivePosition:

    @pytest.mark.parametrize("fields, passive", [
        pytest.param({"lvs_active_site": ""}, REMOTE, id="unset"),
        pytest.param({"lvs_active_site": "site-a"}, REMOTE, id="own-site"),
        # outside sync replication there is only the home triplet
        pytest.param({"site": "", "remote_primary_node_id": "", "remote_secondary_node_id": "",
                      "remote_tertiary_node_id": ""}, (), id="non-sync"),
    ])
    def test_led_from_home(self, fields, passive):
        owner = _owner(**fields)
        assert _positions(owner, HOME) == (PRIMARY, SECONDARY, TERTIARY)
        assert _positions(owner, passive) == (INACTIVE,) * len(passive)

    def test_led_from_other_site(self):
        owner = _owner(lvs_active_site="site-b")
        assert _positions(owner, REMOTE) == (PRIMARY, SECONDARY, TERTIARY)
        assert _positions(owner, HOME) == (INACTIVE, INACTIVE, INACTIVE)

    @pytest.mark.parametrize("marker", ["moving:site-b", "moving:site-a"])
    def test_move_in_flight_gives_no_member_position(self, marker):
        owner = _owner(lvs_active_site=marker)
        assert _positions(owner, HOME + REMOTE) == (MOVING,) * 6

    @pytest.mark.parametrize("active_site", ["", "site-b", "moving:site-b"])
    @pytest.mark.parametrize("node_id", ["x", ""])
    def test_node_that_is_no_member_has_no_position(self, active_site, node_id):
        with pytest.raises(ValueError, match="not a member"):
            ops.active_position(_owner(lvs_active_site=active_site), node_id)

    def test_ftt1_home_triplet_has_no_tertiary(self):
        owner = _owner(tertiary_node_id="")
        assert _positions(owner, ("p", "s")) == (PRIMARY, SECONDARY)
        assert _positions(owner, REMOTE) == (INACTIVE, INACTIVE, INACTIVE)
        # The empty slot is not a member: "" must not come back as the tertiary.
        with pytest.raises(ValueError, match="not a member"):
            ops.active_position(owner, "")
        owner.lvs_active_site = "site-b"
        assert _positions(owner, ("p", "s")) == (INACTIVE, INACTIVE)

    def test_ftt1_remote_triplet_has_no_tertiary(self):
        owner = _owner(remote_tertiary_node_id="", lvs_active_site="site-b")
        assert _positions(owner, ("rp", "rs")) == (PRIMARY, SECONDARY)
        assert _positions(owner, HOME) == (INACTIVE, INACTIVE, INACTIVE)
        with pytest.raises(ValueError, match="not a member"):
            ops.active_position(owner, "")


class TestActiveTripletIds:

    @pytest.mark.parametrize("active_site", ["", "site-a"])
    def test_led_from_home(self, active_site):
        assert ops.active_triplet_ids(_owner(lvs_active_site=active_site)) == HOME

    def test_led_from_other_site(self):
        assert ops.active_triplet_ids(_owner(lvs_active_site="site-b")) == REMOTE

    def test_empty_slots_keep_their_place(self):
        owner = _owner(lvs_active_site="site-b", remote_primary_node_id="", remote_tertiary_node_id="")
        assert ops.active_triplet_ids(owner) == ("", "rs", "")

    def test_empty_remote_triplet(self):
        owner = _owner(lvs_active_site="site-b", remote_primary_node_id="", remote_secondary_node_id="",
                       remote_tertiary_node_id="")
        assert ops.active_triplet_ids(owner) == ("", "", "")

    @pytest.mark.parametrize("marker", ["moving:site-b", "moving:site-a"])
    def test_move_in_flight_has_no_active_triplet(self, marker):
        with pytest.raises(ValueError, match="no active triplet"):
            ops.active_triplet_ids(_owner(lvs_active_site=marker))
