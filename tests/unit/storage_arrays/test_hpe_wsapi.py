"""HPE WSAPI adapter specifics beyond the shared contract."""
from __future__ import annotations

import dataclasses
import uuid

import pytest

from simplyblock_core.storage_arrays.base import (
    ArrayConflictError,
    ArrayError,
    ArrayNotFoundError,
    HostSpec,
    Initiator,
    Protocol,
)
from simplyblock_core.storage_arrays.client import ArrayClient
from simplyblock_core.storage_arrays.testing import FakeTransport, make_config
from simplyblock_core.storage_arrays.testing.hpe_wsapi import contract_setup

from .conftest import StaticProvider

GIB = 1 << 30
MIB = 1 << 20


def _client(**options):
    setup = contract_setup()
    if options:
        setup = dataclasses.replace(
            setup, config=make_config("hpe-wsapi", "https://wsapi.fake.example",
                                      **{"cpg": "SSD_r6", **options}))
    client = ArrayClient(setup.config, StaticProvider(setup.credential),  # type: ignore[arg-type]
                         transport=FakeTransport(setup.fake),
                         session_kwargs={"sleep": lambda s: None})
    return client, setup.fake


def _iscsi(name, *iqns):
    return HostSpec(name=name, initiators=tuple(
        Initiator(protocol=Protocol.ISCSI, address=i) for i in iqns))


def test_session_key_header_and_logout():
    client, fake = _client()
    client.get_info()
    assert len(fake.sessions) == 1
    client.close()
    assert fake.sessions == set()
    assert any(p.startswith("/api/v1/credentials/") for m, p in fake.requests if m == "DELETE")


def test_get_info_reads_system():
    client, _ = _client()
    info = client.get_info()
    assert info.model.startswith("HPE Alletra") and info.serial == "CZ12345678"


def test_cpg_option_required():
    from simplyblock_core.storage_arrays.adapters.hpe_wsapi import HpeWsapiAdapter
    from simplyblock_core.storage_arrays.base import AdapterContext
    from simplyblock_core.storage_arrays.http import RestSession
    config = make_config("hpe-wsapi", "https://wsapi.fake.example")
    ctx = AdapterContext(config, contract_setup().credential, RestSession(config.endpoint))
    with pytest.raises(ArrayError, match="cpg"):
        HpeWsapiAdapter(ctx)


def test_invalid_provisioning_rejected():
    with pytest.raises(ArrayError, match="provisioning"):
        _client(provisioning="full")[0].get_info()


def test_create_body_tpvv_and_reduce():
    client, fake = _client()
    client.create_volume(str(uuid.uuid4()), GIB)
    body = fake.created_bodies[-1]
    assert body["tpvv"] is True and "reduce" not in body
    assert body["cpg"] == "SSD_r6" and body["sizeMiB"] == 1024

    client, fake = _client(provisioning="reduce")
    client.create_volume(str(uuid.uuid4()), GIB)
    assert fake.created_bodies[-1]["reduce"] is True and "tpvv" not in fake.created_bodies[-1]


def test_names_fit_31_chars_and_wwn_is_naa():
    client, fake = _client()
    vol = client.create_volume(str(uuid.uuid4()), GIB)
    assert len(vol.name) <= 31 and vol.id == vol.name
    assert vol.wwn == "naa." + fake.volumes[vol.name]["wwn"].lower()
    assert vol.wwn.startswith("naa.60002ac")


def test_size_rounds_to_256_mib():
    client, fake = _client()
    vol = client.create_volume(str(uuid.uuid4()), GIB + 1)
    assert vol.size_bytes == GIB + 256 * MIB
    assert fake.volumes[vol.name]["sizeMiB"] == 1280


def test_grow_sends_increment():
    client, fake = _client()
    vol = client.create_volume(str(uuid.uuid4()), 2 * GIB)
    seen = []
    orig = fake.put_volume

    def spy(req, name):
        seen.append(req.json())
        return orig(req, name)

    fake._routes = [(m, p, spy if h == orig else h) for m, p, h in fake._routes]
    grown = client.resize_volume(vol.id, 5 * GIB)
    assert seen == [{"action": 3, "sizeMiB": 3 * 1024}]
    assert grown.size_bytes == 5 * GIB


def test_delete_with_vlun_is_conflict_at_adapter_level():
    client, fake = _client()
    vol = client.create_volume(str(uuid.uuid4()), GIB)
    host = client.ensure_host(_iscsi("sbh1", "iqn.2026-01.io.sb:x"))
    client.map_volume(vol.id, host)
    adapter = client._adapter
    assert adapter is not None
    with pytest.raises(ArrayConflictError, match="code 34"):
        adapter.delete_volume(vol.id)
    # the client unmaps first
    assert client.delete_volume(vol.id) is True


def test_vlun_location_and_dedup_of_active_paths():
    client, fake = _client()
    vol = client.create_volume(str(uuid.uuid4()), GIB)
    h = client.ensure_host(_iscsi("sbh2", "iqn.2026-01.io.sb:y"))
    fake.vluns.append({"volumeName": "other", "hostname": h, "lun": 0})  # LUN 0 taken
    fake.add_volume("other", GIB)
    m = client.map_volume(vol.id, h)
    assert m.lun == 1
    members = [x for x in fake._vlun_members(vol.id)]
    assert len(members) == 1 + fake.paths_per_host
    assert client.list_mappings(vol.id) == [m]


def test_missing_host_on_map_is_not_found():
    client, _ = _client()
    vol = client.create_volume(str(uuid.uuid4()), GIB)
    with pytest.raises(ArrayNotFoundError, match="code 17"):
        client.map_volume(vol.id, "nohost")


def test_mixed_iscsi_fc_host_uses_two_requests():
    client, fake = _client()
    spec = HostSpec(name="sbmixed", initiators=(
        Initiator(protocol=Protocol.ISCSI, address="iqn.2026-01.io.sb:m"),
        Initiator(protocol=Protocol.FC, address="10:00:00:00:c9:aa:bb:cc")))
    assert client.ensure_host(spec) == "sbmixed"
    host = fake.hosts["sbmixed"]
    assert [p["name"] for p in host["iSCSIPaths"]] == ["iqn.2026-01.io.sb:m"]
    assert [p["wwn"] for p in host["FCPaths"]] == ["10000000C9AABBCC"]
    assert client.ensure_host(spec) == "sbmixed"


def test_persona_option_used():
    client, fake = _client(persona=11)
    client.ensure_host(_iscsi("sbvm", "iqn.2026-01.io.sb:v"))
    assert fake.hosts["sbvm"]["persona"] == 11


def test_path_owned_by_other_host_is_conflict():
    client, _ = _client()
    client.ensure_host(_iscsi("sba", "iqn.2026-01.io.sb:dup"))
    with pytest.raises(ArrayConflictError, match="code 73"):
        client.ensure_host(_iscsi("sbb", "iqn.2026-01.io.sb:dup"))


def test_nvme_refused():
    client, fake = _client()
    spec = HostSpec(name="sbn", initiators=(
        Initiator(protocol=Protocol.NVME_TCP, address="nqn.2014-08.org.nvmexpress:uuid:x"),))
    with pytest.raises(ArrayError, match="does not support"):
        client.ensure_host(spec)
    assert not fake.requests


def test_host_name_limit_enforced_by_client():
    client, fake = _client()
    with pytest.raises(ArrayError, match="exceeds 31"):
        client.ensure_host(_iscsi("h" * 32, "iqn.2026-01.io.sb:long"))
    assert not fake.requests
