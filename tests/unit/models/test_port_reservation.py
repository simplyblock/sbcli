"""PortReservation: how long a reservation is honoured, and the slot name the
rpc port of a storage node is reserved under."""

from simplyblock_core import constants, storage_node_ops
from simplyblock_core.models.cluster import PortReservation


def _reservation(created_at, slot=""):
    res = PortReservation()
    res.cluster_id = "c1"
    res.port = 4420
    res.created_at = created_at
    res.slot = slot
    return res


def test_an_unnamed_reservation_expires_after_the_short_ttl():
    res = _reservation(1000)
    assert not res.expired(1000 + constants.PORT_RESERVATION_TTL_SEC)
    assert res.expired(1000 + constants.PORT_RESERVATION_TTL_SEC + 1)


def test_a_slot_reservation_outlives_the_short_ttl():
    res = _reservation(1000, slot="rpc/w2/0/0xf")
    assert not res.expired(1000 + constants.PORT_RESERVATION_TTL_SEC + 1)
    assert not res.expired(1000 + constants.PORT_RESERVATION_SLOT_TTL_SEC)
    assert res.expired(1000 + constants.PORT_RESERVATION_SLOT_TTL_SEC + 1)


def test_the_slot_survives_a_round_trip():
    res = PortReservation().from_dict(_reservation(1000, slot="rpc/w2/0/0xf").to_dict())
    assert res.slot == "rpc/w2/0/0xf"


def test_a_record_written_before_slots_reads_as_unnamed():
    res = PortReservation().from_dict({"cluster_id": "c1", "port": 4420, "owner": "o", "created_at": 1000})
    assert res.slot == ""
    assert res.expired(1000 + constants.PORT_RESERVATION_TTL_SEC + 1)


def test_the_rpc_slot_is_the_identity_of_a_storage_node_on_its_host():
    slot = storage_node_ops._rpc_port_slot("10.0.0.12:5000", 0, "0xf")
    assert slot == storage_node_ops._rpc_port_slot("10.0.0.12:5000", 0, "0xf")
    # another socket, a co-socket sibling and another host are other slots
    assert len({
        slot,
        storage_node_ops._rpc_port_slot("10.0.0.12:5000", 1, "0xf"),
        storage_node_ops._rpc_port_slot("10.0.0.12:5000", 0, "0xf0"),
        storage_node_ops._rpc_port_slot("10.0.0.13:5000", 0, "0xf"),
    }) == 4
    assert storage_node_ops._rpc_port_slot("10.0.0.12:5000", 0, None) == "rpc/10.0.0.12:5000/0/"
