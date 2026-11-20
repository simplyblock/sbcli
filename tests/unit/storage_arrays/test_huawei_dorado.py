"""Huawei OceanStor Dorado specifics beyond the shared contract suite."""
from __future__ import annotations

import uuid

import pytest

from simplyblock_core.storage_arrays.adapters.huawei_dorado import HuaweiDoradoAdapter
from simplyblock_core.storage_arrays.base import (
    ArrayAuthError,
    ArrayConflictError,
    ArrayError,
    ArrayRequestError,
    HostSpec,
    Initiator,
    Protocol,
)
from simplyblock_core.storage_arrays.client import ArrayClient
from simplyblock_core.storage_arrays.testing import FakeTransport
from simplyblock_core.storage_arrays.testing.huawei_dorado import contract_setup

GIB = 1 << 30
IQN_A = Initiator(protocol=Protocol.ISCSI, address="iqn.2026-01.io.sb:a")
IQN_B = Initiator(protocol=Protocol.ISCSI, address="iqn.2026-01.io.sb:b")


class _Provider:
    def __init__(self, credential):
        self.credential = credential
        self.invalidated = 0

    def resolve(self, ref, context):
        return self.credential

    def invalidate(self, ref=None):
        self.invalidated += 1


def _client(**options):
    setup = contract_setup()
    if options:
        setup.config = setup.config.model_copy(
            update={"options": {**setup.config.options, **options}})
    client = ArrayClient(setup.config, _Provider(setup.credential),  # type: ignore[arg-type]
                         transport=FakeTransport(setup.fake),
                         session_kwargs={"sleep": lambda s: None})
    return client, setup.fake


def _envelope(code, description="x"):
    return {"data": {}, "error": {"code": code, "description": description}}


def test_login_and_device_scoped_paths():
    client, fake = _client()
    info = client.get_info()
    assert info.serial == fake.device_id and info.version == "6.1.5"
    assert info.model == "OceanStor Dorado 5000 V6"
    paths = [p for _, p in fake.requests]
    assert paths[0] == "/deviceManager/rest/xx/sessions"
    assert paths[1] == f"/deviceManager/rest/{fake.device_id}/system/"
    client.close()
    assert fake.requests[-1] == ("DELETE", f"/deviceManager/rest/{fake.device_id}/sessions")
    assert not fake.tokens


def test_capacity_is_in_sectors_and_rounded_to_mib():
    client, fake = _client()
    vol = client.create_volume(str(uuid.uuid4()), 10 * GIB + 1)
    assert vol.size_bytes == 10 * GIB + (1 << 20)
    assert fake.luns[vol.id]["CAPACITY"] == str((10 * GIB + (1 << 20)) // 512)
    assert fake.luns[vol.id]["ALLOCTYPE"] == "1"
    assert vol.wwn == "naa." + fake.luns[vol.id]["WWN"].lower()
    assert len(vol.name) <= 31


def test_unknown_pool():
    client, _ = _client(storage_pool="nope")
    with pytest.raises(ArrayError, match="storage pool 'nope' not found"):
        client.create_volume(str(uuid.uuid4()), GIB)


def test_http_200_error_envelopes_are_mapped():
    client, fake = _client()
    client.get_info()
    fake.fail_next(200, _envelope(1077948993, "The name already exists."), path="/lun$")
    with pytest.raises(ArrayConflictError, match="1077948993"):
        client.find_volume(str(uuid.uuid4()))
    fake.fail_next(200, _envelope(1077949006, "The system is busy."), path="/lun$")
    with pytest.raises(ArrayRequestError, match="1077949006"):
        client.find_volume(str(uuid.uuid4()))
    fake.fail_next(200, _envelope(1077936859, "The LUN does not exist."), path="/lun/")
    assert client.get_volume("4242") is None


def test_minus_401_envelope_relogs_in_once():
    client, fake = _client()
    client.get_info()
    fake.expire_sessions()
    client.get_info()
    logins = [p for _, p in fake.requests if p.endswith("/xx/sessions")]
    assert len(logins) == 2


def test_wrong_password_is_auth_error():
    client, fake = _client()
    fake.set_credential(username="admin", password="other")
    with pytest.raises(ArrayAuthError, match="1077987870"):
        client.get_info()


def test_mapping_triple_and_teardown():
    client, fake = _client()
    v1 = client.create_volume(str(uuid.uuid4()), GIB)
    v2 = client.create_volume(str(uuid.uuid4()), GIB)
    host = client.ensure_host(HostSpec(name="sbtest-a", initiators=(IQN_A,)))
    m1 = client.map_volume(v1.id, host)
    m2 = client.map_volume(v2.id, host)
    assert (m1.lun, m2.lun) == (1, 2)  # HostLUNID parsed from ASSOCIATEMETADATA
    assert {g["NAME"] for g in fake.lungroups.values()} == {f"sb_lg_{host}"}
    assert {g["NAME"] for g in fake.hostgroups.values()} == {f"sb_hg_{host}"}
    assert {v["NAME"] for v in fake.views.values()} == {f"sb_mv_{host}"}
    assert client.map_volume(v1.id, host) == m1

    with pytest.raises(ArrayConflictError, match="still has LUNs"):
        client.delete_host(host)
    client.delete_volume(v1.id)
    client.delete_volume(v2.id)
    client.delete_host(host)
    assert not (fake.hosts or fake.lungroups or fake.hostgroups or fake.views)
    assert fake.initiators["iscsi_initiator"]["iqn.2026-01.io.sb:a"]["ISFREE"] == "true"
    # the freed initiator can be claimed by a new host
    client.ensure_host(HostSpec(name="sbtest-b", initiators=(IQN_A,)))


def test_initiator_bound_to_foreign_host_creates_nothing():
    client, fake = _client()
    owner = client.ensure_host(HostSpec(name="customer-host", initiators=(IQN_A,)))
    with pytest.raises(ArrayConflictError, match="customer-host"):
        client.ensure_host(HostSpec(name="sbtest-x", initiators=(IQN_B, IQN_A)))
    assert [h["NAME"] for h in fake.hosts.values()] == ["customer-host"]
    assert fake.initiators["iscsi_initiator"]["iqn.2026-01.io.sb:a"]["PARENTID"] == owner


def test_fc_initiator_ids_are_plain_lowercase_hex():
    client, fake = _client()
    host = client.ensure_host(HostSpec(name="sbtest-fc", initiators=(
        Initiator(protocol=Protocol.FC, address="21:00:00:24:FF:4C:1A:2B"),)))
    ini = fake.initiators["fc_initiator"]["21000024ff4c1a2b"]
    assert ini["PARENTID"] == host and ini["ISFREE"] == "false"


def test_host_in_foreign_hostgroup_conflicts_on_map():
    client, fake = _client()
    vol = client.create_volume(str(uuid.uuid4()), GIB)
    host = client.ensure_host(HostSpec(name="sbtest-a", initiators=(IQN_A,)))
    gid = str(fake.next_id())
    fake.hostgroups[gid] = {"ID": gid, "NAME": "customer_hg", "TYPE": "14",
                            "DESCRIPTION": "", "hosts": {host}}
    with pytest.raises(ArrayConflictError, match="customer_hg"):
        client.map_volume(vol.id, host)


def test_lun_in_customer_lungroup_is_not_listed_and_blocks_delete():
    client, fake = _client()
    vol = client.create_volume(str(uuid.uuid4()), GIB)
    gid = str(fake.next_id())
    fake.lungroups[gid] = {"ID": gid, "NAME": "customer_lg", "TYPE": "256",
                           "DESCRIPTION": "", "luns": {vol.id}}
    assert client.list_mappings(vol.id) == []
    with pytest.raises(ArrayRequestError):
        client.delete_volume(vol.id)
    assert client.get_volume(vol.id) is not None


def test_name_query_keeps_separator_literal():
    # Huawei's CSI driver and Cinder send filter=NAME::<name>&range=[0-100]
    # with a literal "::"; only the name is escaped.
    q = HuaweiDoradoAdapter._name_query("/lun", "sb-a b/c")
    assert q == "/lun?filter=NAME::sb-a%20b%2Fc&range=[0-100]"
    client, _ = _client()
    vol_uuid = str(uuid.uuid4())
    vol = client.create_volume(vol_uuid, GIB)
    assert client.find_volume(vol_uuid) == vol


def test_delete_host_never_unbinds_foreign_initiators():
    client, fake = _client()
    fake.ignore_parent_filter = True
    keep = client.ensure_host(HostSpec(name="sbtest-keep", initiators=(IQN_A,)))
    gone = client.ensure_host(HostSpec(name="sbtest-gone", initiators=(IQN_B,)))
    client.delete_host(gone)
    ini = fake.initiators["iscsi_initiator"][IQN_A.address]
    assert ini["ISFREE"] == "false" and ini["PARENTID"] == keep
    assert fake.initiators["iscsi_initiator"][IQN_B.address]["ISFREE"] == "true"
