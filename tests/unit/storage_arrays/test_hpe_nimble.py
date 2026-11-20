"""HPE Alletra 5000/6000 / Nimble adapter specifics beyond the shared contract."""
from __future__ import annotations

import dataclasses
import uuid

import pytest

from simplyblock_core.storage_arrays.base import (
    ArrayConflictError,
    ArrayError,
    HostSpec,
    Initiator,
    Protocol,
)
from simplyblock_core.storage_arrays.client import ArrayClient
from simplyblock_core.storage_arrays.testing import FakeTransport, make_config
from simplyblock_core.storage_arrays.testing.hpe_nimble import contract_setup

from .conftest import StaticProvider

GIB = 1 << 30
MIB = 1 << 20


def _client(**options):
    setup = contract_setup()
    if options:
        setup = dataclasses.replace(
            setup, config=make_config("hpe-nimble", "https://nimble.fake.example:5392",
                                      **options))
    client = ArrayClient(setup.config, StaticProvider(setup.credential),  # type: ignore[arg-type]
                         transport=FakeTransport(setup.fake),
                         session_kwargs={"sleep": lambda s: None})
    return client, setup.fake


def _iscsi(name, *iqns):
    return HostSpec(name=name, initiators=tuple(
        Initiator(protocol=Protocol.ISCSI, address=i) for i in iqns))


def _fc(name, *wwpns):
    return HostSpec(name=name, initiators=tuple(
        Initiator(protocol=Protocol.FC, address=w) for w in wwpns))


def test_token_released_by_id_on_close():
    client, fake = _client()
    client.get_info()
    assert len(fake.tokens) == 1
    (tid,) = fake.tokens
    client.close()
    assert fake.tokens == {}
    assert ("DELETE", f"/v1/tokens/{tid}") in fake.requests


def test_port_5392_is_kept():
    setup = contract_setup()
    sent: list[str] = []

    class Recording(FakeTransport):
        def send(self, request, **kw):
            sent.append(request.url)
            return super().send(request, **kw)

    client = ArrayClient(setup.config, StaticProvider(setup.credential),  # type: ignore[arg-type]
                         transport=Recording(setup.fake))
    client.get_info()
    assert sent and all(u.startswith("https://nimble.fake.example:5392/v1/") for u in sent)


def test_get_info():
    client, _ = _client()
    info = client.get_info()
    assert info.model == "AF40" and info.serial == "AF-123456"
    assert info.version.startswith("6.")


def test_sizes_in_mib_and_eui_identity():
    client, fake = _client()
    vol = client.create_volume(str(uuid.uuid4()), GIB + 1)
    assert vol.size_bytes == GIB + MIB
    raw = fake.volumes[vol.id]
    assert raw["size"] == 1025
    assert vol.nguid is None and vol.wwn == "eui." + raw["serial_number"]
    assert vol.identifier == vol.wwn


def test_pool_and_performance_policy_resolved_by_name():
    client, fake = _client(pool="fast", performance_policy="Oracle OLTP")
    vol = client.create_volume(str(uuid.uuid4()), GIB)
    raw = fake.volumes[vol.id]
    assert raw["pool_name"] == "fast" and raw["perfpolicy_name"] == "Oracle OLTP"


def test_unknown_pool_is_an_error():
    client, fake = _client(pool="nope")
    with pytest.raises(ArrayError, match="pool 'nope' not found"):
        client.create_volume(str(uuid.uuid4()), GIB)
    assert fake.volumes == {}


def test_delete_offlines_first():
    client, fake = _client()
    vol = client.create_volume(str(uuid.uuid4()), GIB)
    adapter = client._adapter
    assert adapter is not None
    # a direct DELETE of an online volume is refused by the array
    with pytest.raises(ArrayConflictError, match="SM_vol_online"):
        adapter._request("DELETE", f"/v1/volumes/{vol.id}")
    assert client.delete_volume(vol.id) is True
    calls = [(m, p) for m, p in fake.requests if p == f"/v1/volumes/{vol.id}"]
    assert ("PUT", f"/v1/volumes/{vol.id}") in calls
    assert calls.index(("PUT", f"/v1/volumes/{vol.id}")) < len(calls) - 1


def test_fc_group_uses_colon_wwpns():
    client, fake = _client()
    hid = client.ensure_host(_fc("sbfc", "2100000E1E2F3A4B"))
    assert fake.groups[hid]["access_protocol"] == "fc"
    assert [i["wwpn"] for i in fake.initiators.values()] == ["21:00:00:0e:1e:2f:3a:4b"]
    assert client.ensure_host(_fc("sbfc", "21:00:00:0e:1e:2f:3a:4b")) == hid
    assert len(fake.initiators) == 1


def test_mixed_protocol_host_refused():
    client, _ = _client()
    spec = HostSpec(name="sbmix", initiators=(
        Initiator(protocol=Protocol.ISCSI, address="iqn.2026-01.io.sb:m"),
        Initiator(protocol=Protocol.FC, address="2100000e1e2f3a4c")))
    with pytest.raises(ArrayError, match="either iSCSI or FC"):
        client.ensure_host(spec)


def test_existing_group_of_other_protocol_conflicts():
    client, _ = _client()
    client.ensure_host(_iscsi("sbsame", "iqn.2026-01.io.sb:s"))
    with pytest.raises(ArrayConflictError, match="access_protocol iscsi"):
        client.ensure_host(_fc("sbsame", "2100000e1e2f3a4d"))


def test_initiator_owned_elsewhere_conflicts_before_creating_group():
    client, fake = _client()
    client.ensure_host(_iscsi("sbown", "iqn.2026-01.io.sb:o"))
    with pytest.raises(ArrayConflictError, match="sbown"):
        client.ensure_host(_iscsi("sbthief", "iqn.2026-01.io.sb:o"))
    assert {g["name"] for g in fake.groups.values()} == {"sbown"}


def test_iscsi_luns_are_zero_with_volume_scoped_targets():
    client, fake = _client()
    hid = client.ensure_host(_iscsi("sbvst", "iqn.2026-01.io.sb:v"))
    luns = [client.map_volume(client.create_volume(str(uuid.uuid4()), GIB).id, hid).lun
            for _ in range(2)]
    assert luns == [0, 0]


def test_fc_luns_are_distinct_per_group():
    client, fake = _client()
    hid = client.ensure_host(_fc("sbfc2", "2100000e1e2f3a4e"))
    luns = [client.map_volume(client.create_volume(str(uuid.uuid4()), GIB).id, hid).lun
            for _ in range(3)]
    assert luns == [0, 1, 2]


def test_acr_is_applied_to_both_and_map_is_idempotent():
    client, fake = _client()
    vol = client.create_volume(str(uuid.uuid4()), GIB)
    hid = client.ensure_host(_iscsi("sbacr", "iqn.2026-01.io.sb:a"))
    m = client.map_volume(vol.id, hid)
    assert client.map_volume(vol.id, hid) == m
    (acr,) = fake.acrs.values()
    assert acr["apply_to"] == "both" and acr["vol_id"] == vol.id


def test_malformed_volume_id_is_none():
    client, _ = _client()
    assert client.get_volume("not-a-nimble-id") is None


def test_host_in_use_cannot_be_deleted():
    client, _ = _client()
    vol = client.create_volume(str(uuid.uuid4()), GIB)
    hid = client.ensure_host(_iscsi("sbbusy", "iqn.2026-01.io.sb:b"))
    client.map_volume(vol.id, hid)
    with pytest.raises(ArrayConflictError, match="SM_igrp_has_acl"):
        client.delete_host(hid)


def test_error_body_has_generic_then_specific_message():
    client, _ = _client()
    client.ensure_host(_iscsi("sbdup", "iqn.2026-01.io.sb:d1"))
    adapter = client._adapter
    assert adapter is not None
    with pytest.raises(ArrayConflictError, match="SM_http_conflict: .*; SM_eexist"):
        adapter._data("POST", "/v1/initiator_groups",
                      json={"data": {"name": "sbdup", "access_protocol": "iscsi"}})


def test_retried_delete_tolerates_already_offline_volume():
    # Offlining an offline volume answers "Object is already in requested
    # state" (Cinder ignores it); a delete retried after the offline step
    # must not fail on it.
    client, fake = _client()
    vol = client.create_volume(str(uuid.uuid4()), GIB)
    fake.volumes[vol.id]["online"] = False
    assert client.delete_volume(vol.id) is True
    assert vol.id not in fake.volumes


def test_shrink_is_refused_without_force():
    client, fake = _client()
    vol = client.create_volume(str(uuid.uuid4()), 2 * GIB)
    adapter = client._adapter
    assert adapter is not None
    with pytest.raises(ArrayError, match="SM_vol_size_decreased"):
        adapter.resize_volume(vol.id, GIB)
    assert fake.volumes[vol.id]["size"] == 2048


def test_async_jobs_are_awaited_and_objects_reread():
    client, fake = _client()
    fake.async_ops = {"POST volume", "PUT volume", "DELETE volume", "POST initiator_group",
                      "POST initiator", "POST acr", "DELETE acr", "DELETE initiator_group"}
    vol = client.create_volume(str(uuid.uuid4()), GIB)
    assert vol.size_bytes == GIB and vol.id in fake.volumes
    assert client.resize_volume(vol.id, 2 * GIB).size_bytes == 2 * GIB
    hid = client.ensure_host(_iscsi("sbasync", "iqn.2026-01.io.sb:as"))
    assert len(fake.initiators) == 1
    m = client.map_volume(vol.id, hid)
    assert m.lun == 0 and m.host_id == hid
    client.unmap_volume(vol.id, hid)
    assert fake.acrs == {}
    assert client.delete_volume(vol.id) is True
    client.delete_host(hid)
    assert fake.volumes == {} and fake.groups == {}
    polls = [p for m_, p in fake.requests if m_ == "GET" and p.startswith("/v1/jobs/")]
    assert len(polls) >= 3 * len(fake.jobs) and all(v == 0 for v in fake.jobs.values())


def test_async_job_timeout():
    client, fake = _client(job_timeout_s=4)
    fake.async_ops = {"POST volume"}
    fake.job_polls = 100
    with pytest.raises(ArrayError, match="did not finish within 4"):
        client.create_volume(str(uuid.uuid4()), GIB)
