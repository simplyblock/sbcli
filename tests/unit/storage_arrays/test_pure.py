"""Pure FlashArray adapter specifics beyond the shared contract."""
from __future__ import annotations

import uuid

import pytest

from simplyblock_core.storage_arrays.adapters.pure import (
    PureFlashArrayAdapter,
    pure_nguid,
    pure_wwn,
)
from simplyblock_core.storage_arrays.base import (
    ArrayConflictError,
    ArrayNotFoundError,
    ArrayRequestError,
    HostSpec,
    Initiator,
    Protocol,
)
from simplyblock_core.storage_arrays.client import ArrayClient
from tests.unit.storage_arrays.fakes import FakeTransport
from tests.unit.storage_arrays.fakes.pure import contract_setup

from .conftest import StaticProvider

GIB = 1 << 30


def make(**options):
    setup = contract_setup()
    config = setup.config.model_copy(update={"options": {**setup.config.options, **options}})
    client = ArrayClient(config, StaticProvider(setup.credential),  # type: ignore[arg-type]
                         transport=FakeTransport(setup.fake),
                         session_kwargs={"sleep": lambda s: None})
    return client, setup.fake


def test_identity_matches_vendor_examples():
    # Sample values from Ansible purestorage.flasharray purefa_volume.py RETURN docs.
    assert pure_nguid("CD6B99EF258647C5000BE684") == "eui.00cd6b99ef25864724a937c5000be684"
    assert pure_wwn("361019ECACE43DB3000120A4") == "naa.624a9370361019ecace43db3000120a4"


def test_newest_supported_rest_version_is_used():
    client, fake = make()
    client.get_info()
    # The fake offers 1.19 .. 2.40; 2.40 is above what the adapter was written for.
    assert ("POST", "/api/2.36/login") in fake.requests
    assert all(not p.startswith("/api/2.40") for _, p in fake.requests)


def test_pinned_api_version():
    client, fake = make(api_version="2.21")
    client.get_info()
    assert ("POST", "/api/2.21/login") in fake.requests
    assert ("GET", "/api/api_version") not in fake.requests


def test_no_usable_version():
    client, fake = make()
    fake.versions = ["1.19", "2.0", "2.2"]
    with pytest.raises(ArrayRequestError, match="no REST 2.x version"):
        client.get_info()


def test_nvme_connection_reports_nsid():
    client, fake = make()
    vol = client.create_volume(str(uuid.uuid4()), GIB)
    host = client.ensure_host(HostSpec(name="nv", initiators=(
        Initiator(protocol=Protocol.NVME_TCP, address="nqn.2014-08.org.nvmexpress:uuid:nv"),)))
    m = client.map_volume(vol.id, host)
    (conn,) = fake.connections
    assert "nsid" in conn and "lun" not in conn
    assert m.lun == conn["nsid"] == 1
    assert client.list_mappings(vol.id) == [m]


def test_scsi_connection_reports_lun():
    client, fake = make()
    vol = client.create_volume(str(uuid.uuid4()), GIB)
    host = client.ensure_host(HostSpec(name="sc", initiators=(
        Initiator(protocol=Protocol.ISCSI, address="iqn.2026-01.io.sb:sc"),)))
    assert client.map_volume(vol.id, host).lun == fake.connections[0]["lun"] == 1


def test_fc_wwns_are_sent_upper_case_without_colons():
    client, fake = make()
    client.ensure_host(HostSpec(name="fc", initiators=(
        Initiator(protocol=Protocol.FC, address="21:00:00:24:ff:4c:1a:2b"),)))
    assert fake.hosts["fc"]["wwns"] == ["21000024FF4C1A2B"]


def test_existing_host_gets_add_fields():
    client, fake = make()
    client.ensure_host(HostSpec(name="h", initiators=(
        Initiator(protocol=Protocol.ISCSI, address="iqn.2026-01.io.sb:a"),)))
    client.ensure_host(HostSpec(name="h", initiators=(
        Initiator(protocol=Protocol.ISCSI, address="iqn.2026-01.io.sb:a"),
        Initiator(protocol=Protocol.ISCSI, address="iqn.2026-01.io.sb:b"))))
    assert fake.hosts["h"]["iqns"] == ["iqn.2026-01.io.sb:a", "iqn.2026-01.io.sb:b"]
    assert ("PATCH", "/api/2.36/hosts") in fake.requests


def test_eradicate_option():
    client, fake = make(eradicate=True)
    vol = client.create_volume(str(uuid.uuid4()), GIB)
    client.delete_volume(vol.id)
    assert not fake.volumes

    client, fake = make()
    vol = client.create_volume(str(uuid.uuid4()), GIB)
    client.delete_volume(vol.id)
    assert fake.volumes[vol.id]["destroyed"] is True
    assert client.get_volume(vol.id) is None


@pytest.mark.parametrize(("message", "exc"), [
    ("Volume does not exist.", ArrayNotFoundError),
    ("Host already exists.", ArrayConflictError),
    ("Implicit truncation is not permitted.", ArrayRequestError),
])
def test_error_messages_are_classified(message, exc):
    client, fake = make()
    client.get_info()
    adapter = client._adapter
    assert isinstance(adapter, PureFlashArrayAdapter)
    fake.fail_next(400, {"errors": [{"message": message, "context": "x"}]})
    with pytest.raises(exc):
        adapter._items("GET", "/volumes", params={"names": "x"})
