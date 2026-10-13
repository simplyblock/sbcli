"""Pure rule of the position of a node in the ACTIVE triplet of an LVS
(storage_node_ops.active_position): primary / secondary / tertiary of the
triplet that leads it, inactive in the other one, moving while the leadership
is being moved. The nodes of that triplet read from the real database
(active_triplet_nodes) are in
tests/integration/test_sync_replication_active_triplet.py.
"""
import enum

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

    def test_a_non_sync_owner_has_only_the_home_triplet(self):
        owner = _owner(site="", remote_primary_node_id="", remote_secondary_node_id="",
                       remote_tertiary_node_id="")
        assert _positions(owner, HOME) == (PRIMARY, SECONDARY, TERTIARY)

    @pytest.mark.parametrize("active_site", ["", "site-a"])
    def test_led_from_home(self, active_site):
        owner = _owner(lvs_active_site=active_site)
        assert _positions(owner, HOME) == (PRIMARY, SECONDARY, TERTIARY)
        assert _positions(owner, REMOTE) == (INACTIVE, INACTIVE, INACTIVE)

    def test_led_from_the_other_site(self):
        owner = _owner(lvs_active_site="site-b")
        assert _positions(owner, REMOTE) == (PRIMARY, SECONDARY, TERTIARY)
        assert _positions(owner, HOME) == (INACTIVE, INACTIVE, INACTIVE)

    @pytest.mark.parametrize("marker", ["moving:site-b", "moving:site-a"])
    def test_a_move_in_flight_gives_no_member_a_position(self, marker):
        owner = _owner(lvs_active_site=marker)
        assert _positions(owner, HOME + REMOTE) == (MOVING,) * 6

    @pytest.mark.parametrize("active_site", ["", "site-b", "moving:site-b"])
    @pytest.mark.parametrize("node_id", ["x", ""])
    def test_a_node_that_is_no_member_has_no_position(self, active_site, node_id):
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

    def test_positions_are_an_enum_and_never_strings(self):
        assert issubclass(ops.ActivePosition, enum.Enum)
        assert {p.name for p in ops.ActivePosition} == {
            "PRIMARY", "SECONDARY", "TERTIARY", "INACTIVE", "MOVING"}
        assert not any(isinstance(p, str) or isinstance(p.value, str) for p in ops.ActivePosition)
