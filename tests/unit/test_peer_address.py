"""utils/peer_address: a node agent named by the other site's DNS is reached
at its node address, with TLS still verified against the name."""
import socket
from unittest.mock import MagicMock

import pytest

from simplyblock_core.utils import peer_address


@pytest.fixture(autouse=True)
def clean_registry(monkeypatch):
    monkeypatch.setattr(peer_address, "_registered", {})


def test_host_of():
    assert peer_address.host_of("ip-1.svc.ns.svc:5000") == "ip-1.svc.ns.svc"
    assert peer_address.host_of("https://cp.example:5000/x") == "cp.example"
    assert peer_address.host_of("10.0.0.5") == "10.0.0.5"


def test_a_name_that_resolves_is_untouched(monkeypatch):
    calls = []
    monkeypatch.setattr(peer_address, "_original_create_connection",
                        lambda address, *a, **k: calls.append(address) or "sock")
    assert peer_address._create_connection(("node.local", 5000)) == "sock"
    assert calls == [("node.local", 5000)]


def test_an_unresolvable_name_goes_to_its_registered_address(monkeypatch):
    calls = []

    def connect(address, *a, **k):
        calls.append(address)
        if address[0] == "ip-1.sb-node-api.simplyblock.svc":
            raise socket.gaierror("no such host")
        return "sock"
    monkeypatch.setattr(peer_address, "_original_create_connection", connect)
    monkeypatch.setattr(peer_address, "_from_records", lambda name: None)
    peer_address.register("ip-1.sb-node-api.simplyblock.svc:5000", "192.168.3.60")
    assert peer_address._create_connection(("ip-1.sb-node-api.simplyblock.svc", 5000)) == "sock"
    assert calls[-1] == ("192.168.3.60", 5000)


def test_an_unresolvable_name_falls_back_to_the_node_record(monkeypatch):
    def connect(address, *a, **k):
        if not address[0][0].isdigit():
            raise socket.gaierror("no such host")
        return address
    monkeypatch.setattr(peer_address, "_original_create_connection", connect)
    monkeypatch.setattr(peer_address, "_from_records",
                        lambda name: "192.168.17.29" if name == "ip-2.svc" else None)
    assert peer_address._create_connection(("ip-2.svc", 5000)) == ("192.168.17.29", 5000)
    with pytest.raises(socket.gaierror):
        peer_address._create_connection(("unknown.svc", 5000))


def test_records_map_the_api_name_to_mgmt_ip(monkeypatch):
    from simplyblock_core.models.storage_node import StorageNode
    node = StorageNode()
    node.uuid, node.status, node.mgmt_ip = "n1", StorageNode.STATUS_ONLINE, "192.168.17.29"
    node.api_endpoint = "ip-2.sb-node-api.simplyblock.svc:5000"
    db = MagicMock()
    db.get_storage_nodes.return_value = [node]
    monkeypatch.setattr("simplyblock_core.db_controller.DBController", lambda: db)
    assert peer_address._from_records("ip-2.sb-node-api.simplyblock.svc") == "192.168.17.29"
    assert peer_address._from_records("other.svc") is None


def test_records_map_the_spdk_proxy_name_too(monkeypatch):
    from simplyblock_core.models.storage_node import StorageNode
    node = StorageNode()
    node.uuid, node.status, node.mgmt_ip = "n1", StorageNode.STATUS_ONLINE, "192.168.17.29"
    node.hostname = "ip-192-168-17-29"
    node.api_endpoint = "ip-192-168-17-29.sb-node-api.simplyblock.svc:5000"
    db = MagicMock()
    db.get_storage_nodes.return_value = [node]
    monkeypatch.setattr("simplyblock_core.db_controller.DBController", lambda: db)
    proxy_host = node._k8s_service_host(node.spdk_proxy_service)
    assert peer_address._from_records(proxy_host) == "192.168.17.29"
