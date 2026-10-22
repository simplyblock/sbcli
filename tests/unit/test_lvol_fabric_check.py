"""The fabric check of a new volume asks for a secondary node only when the
volume has one. A volume of a single-node cluster is created with ha_type
"single" and its host has no secondary: looking one up raised KeyError on the
blank id, and every create in such a cluster answered 500."""

from types import SimpleNamespace

from simplyblock_core.controllers import lvol_controller


def _node(secondary="", active=True):
    return SimpleNamespace(secondary_node_id=secondary, active_tcp=active, active_rdma=False)


class _DB:
    """Answers a node lookup with the node given, or fails the test."""

    def __init__(self, node=None):
        self.node = node

    def get_storage_node_by_id(self, node_id):
        assert self.node is not None, f"looked up node {node_id!r}"
        return self.node


def test_a_single_volume_needs_no_secondary():
    assert lvol_controller._fabric_error(_DB(), _node(), "single", "tcp") is None


def test_a_single_volume_needs_its_host_fabric_active():
    assert "Primary" in lvol_controller._fabric_error(_DB(), _node(active=False), "single", "tcp")


def test_an_ha_volume_needs_an_active_secondary():
    error = lvol_controller._fabric_error(_DB(_node(active=False)), _node(secondary="s1"), "ha", "tcp")
    assert "Secondary" in error


def test_an_ha_volume_without_a_secondary_is_refused_not_raised():
    error = lvol_controller._fabric_error(_DB(), _node(), "ha", "tcp")
    assert error and "secondary" in error.lower()


def _cluster(ha_type="ha", single=False):
    return SimpleNamespace(ha_type=ha_type, is_single_node=single)


def test_a_single_node_clusters_volumes_default_to_single():
    """A single-node cluster records the API default ha_type 'ha' and activates
    as non-HA regardless; its volumes follow the activation, not the record."""
    assert lvol_controller._volume_ha_type("default", _cluster("ha", single=True)) == "single"


def test_a_clusters_volumes_default_to_its_ha_type():
    assert lvol_controller._volume_ha_type("default", _cluster("ha")) == "ha"
    assert lvol_controller._volume_ha_type("default", _cluster("single")) == "single"


def test_an_explicit_ha_type_is_kept():
    assert lvol_controller._volume_ha_type("single", _cluster("ha")) == "single"
