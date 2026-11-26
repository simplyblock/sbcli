"""Hitachi VSP specifics beyond the shared contract: path layouts, async
jobs, LDEV labels and paging, per-port host groups, LUN paths, locks."""
from __future__ import annotations

import uuid

import pytest

from simplyblock_core.storage_arrays.adapters import hitachi_vsp as vsp
from simplyblock_core.storage_arrays.base import (
    ArrayConflictError,
    ArrayError,
    ArrayRequestError,
    HostSpec,
    Initiator,
    Protocol,
)
from simplyblock_core.storage_arrays.client import ArrayClient
from simplyblock_core.storage_arrays.testing import FakeTransport
from simplyblock_core.storage_arrays.testing.hitachi_vsp import contract_setup

from .conftest import StaticProvider

GIB = 1 << 30
FC_A = Initiator(protocol=Protocol.FC, address="21:00:00:24:ff:4c:1a:2b")
FC_B = Initiator(protocol=Protocol.FC, address="21:00:00:24:ff:4c:1a:2c")
IQN_A = Initiator(protocol=Protocol.ISCSI, address="iqn.2026-01.io.sb:host-a")


def _client(**options):
    setup = contract_setup(**options)
    sleeps: list[float] = []
    client = ArrayClient(setup.config, StaticProvider(setup.credential),  # type: ignore[arg-type]
                         transport=FakeTransport(setup.fake),
                         session_kwargs={"sleep": sleeps.append})
    return client, setup.fake, sleeps


@pytest.fixture()
def env():
    client, fake, sleeps = _client()
    yield client, fake, sleeps
    client.close()


def _uuid() -> str:
    return str(uuid.uuid4())


def test_embedded_paths_session_header_and_logout(env):
    client, fake, _ = env
    client.get_info()
    assert ("POST", "/ConfigurationManager/v1/objects/sessions") in fake.requests
    assert ("GET", "/ConfigurationManager/v1/objects/storages/instance") in fake.requests
    assert len(fake.sessions) == 1
    client.close()
    assert fake.sessions == {}
    assert fake.requests[-1][0] == "DELETE"
    assert fake.requests[-1][1].startswith("/ConfigurationManager/v1/objects/sessions/")


def test_info_fields(env):
    client, fake, _ = env
    info = client.get_info()
    assert info.model == "VSP E990"
    assert info.serial == str(fake.serial)
    assert info.version.startswith("93-")


def test_server_mode_uses_storage_device_prefix():
    setup_fake = contract_setup().fake
    client, fake, _ = _client(storage_device_id=setup_fake.storage_device_id)
    try:
        prefix = f"/ConfigurationManager/v1/objects/storages/{fake.storage_device_id}"
        info = client.get_info()
        assert info.serial == str(fake.serial)
        vol = client.create_volume(_uuid(), GIB)
        host = client.ensure_host(HostSpec(name="sbtest-srv", initiators=(FC_A,)))
        client.map_volume(vol.id, host)
        assert client.delete_volume(vol.id)
        paths = [p for _, p in fake.requests]
        assert all(p.startswith(prefix) for p in paths)
        assert any(p.startswith(f"{prefix}/jobs/") for p in paths)
        assert not any(p.startswith("/ConfigurationManager/v1/objects/ldevs") for p in paths)
    finally:
        client.close()


def test_wrong_storage_device_id_is_not_found():
    client, _, _ = _client(storage_device_id="938000999999")
    try:
        with pytest.raises(ArrayError):
            client.get_info()
    finally:
        client.close()


def test_create_waits_for_jobs_and_labels_ldev(env):
    client, fake, sleeps = env
    fake.job_polls = 3
    vol = client.create_volume(_uuid(), 10 * GIB + 1)
    lid = int(vol.id)
    assert fake.ldevs[lid]["label"] == vol.name
    assert vol.size_bytes == 10 * GIB + (1 << 20)          # 1 MiB granularity
    assert fake.ldevs[lid]["blocks"] * 512 == vol.size_bytes
    assert vol.wwn == "naa." + fake.naa(lid).lower()
    assert sleeps, "job polling must go through the session's sleep"
    methods = [m for m, p in fake.requests if "/jobs/" in p]
    assert len(methods) >= 2 * 4        # 2 jobs x (3 running + 1 completed)


def test_job_timeout_is_reported_without_real_sleep():
    client, fake, sleeps = _client(job_timeout_s=10)
    try:
        client.get_info()
        fake.job_polls = 10**6
        with pytest.raises(ArrayRequestError, match="not completed after 10s"):
            client.create_volume(_uuid(), GIB)
        assert 10 <= sum(sleeps) < 20
    finally:
        client.close()


def test_failed_job_maps_storage_error(env):
    client, fake, _ = env
    vol = client.create_volume(_uuid(), GIB)
    host = client.ensure_host(HostSpec(name="sbtest-a", initiators=(FC_A,)))
    client.map_volume(vol.id, host)
    # LDEV delete with an LU path left fails inside the job (KART30000-E + SSB)
    adapter = client._adapter
    assert adapter is not None
    with pytest.raises(ArrayConflictError, match="KART30000-E.*LU paths"):
        adapter.delete_volume(vol.id)
    assert int(vol.id) in fake.ldevs


def test_deleted_ldev_reads_not_defined(env):
    client, fake, _ = env
    vol = client.create_volume(_uuid(), GIB)
    client.delete_volume(vol.id)
    assert int(vol.id) not in fake.ldevs
    assert client.get_volume(vol.id) is None
    assert client.get_volume("not-a-number") is None


def test_label_failure_deletes_the_new_ldev(env):
    client, fake, _ = env
    client.get_info()
    fake.fail_next(500, {"errorSource": "x", "message": "label failed",
                         "messageId": "KART30000-E"}, path=r"/ldevs/\d+$")
    with pytest.raises(ArrayRequestError, match="label failed"):
        client.create_volume(_uuid(), GIB)
    assert fake.ldevs == {}


def test_find_pages_through_ldevs(env, monkeypatch):
    # count is a window of LDEV numbers: the head steps by count and the scan
    # ends at the first window past the last LDEV number (KART40044-E).
    client, fake, _ = env
    monkeypatch.setattr(vsp, "PAGE_SIZE", 2)
    fake.last_ldev = 9
    for i in range(5):
        fake.add_ldev(f"other-{i}", GIB)
    vol_uuid = _uuid()
    vol = client.create_volume(vol_uuid, GIB)
    assert int(vol.id) == 5
    fake.requests.clear()
    assert client.find_volume(vol_uuid) == vol
    heads = [p for m, p in fake.requests if m == "GET" and p.endswith("/ldevs")]
    assert len(heads) == 6          # windows 0, 2, 4, 6, 8, then 10 is past the end


def test_find_scans_past_sparse_windows(env):
    # An LDEV far above the first 16384 numbers is still found, although the
    # first window returns (almost) nothing.
    client, fake, _ = env
    vol_uuid = _uuid()
    name = client.volume_name(vol_uuid)
    fake.add_ldev("other", GIB, ldev_id=3)
    fake.add_ldev(name, GIB, ldev_id=40000)
    vol = client.find_volume(vol_uuid)
    assert vol is not None and vol.id == "40000"
    gets = [p for m, p in fake.requests if m == "GET" and p.endswith("/ldevs")]
    assert len(gets) == 4           # 0, 16384, 32768, 49152 up to LDEV 65535


def test_find_first_window_error_is_not_swallowed(env):
    client, fake, _ = env
    client.get_info()
    fake.fail_next(400, {"errorSource": "x", "message": "bad range",
                         "messageId": "KART40044-E"}, path=r"/ldevs$")
    with pytest.raises(ArrayError, match="KART40044-E"):
        client.find_volume(_uuid())


def test_duplicate_labels_conflict(env):
    client, fake, _ = env
    vol_uuid = _uuid()
    name = client.volume_name(vol_uuid)
    fake.add_ldev(name, GIB)
    fake.add_ldev(name, GIB)
    with pytest.raises(ArrayConflictError, match="Several LDEVs"):
        client.find_volume(vol_uuid)


def test_ldev_range_option():
    client, fake, _ = _client(ldev_range="100-101")
    try:
        fake.add_ldev("outside", GIB, ldev_id=5)
        a = client.create_volume(_uuid(), GIB)
        b = client.create_volume(_uuid(), GIB)
        assert {a.id, b.id} == {"100", "101"}
        with pytest.raises(ArrayError, match="No free LDEV"):
            client.create_volume(_uuid(), GIB)
    finally:
        client.close()


@pytest.mark.parametrize("options, match", [
    ({"ports": []}, "ports"),
    ({"ports": ["eth0"]}, "ports"),
    ({"ldev_range": "9-3"}, "ldev_range"),
    ({"pool_id": "x"}, "pool_id"),
    ({"bogus": 1}, "Unknown"),
])
def test_bad_options(options, match):
    client, _, _ = _client(**options)
    with pytest.raises(ArrayError, match=match):
        client.get_info()


def test_host_is_a_group_per_port_of_its_protocol(env):
    client, fake, _ = env
    host = client.ensure_host(HostSpec(name="sbtest-fc", initiators=(FC_A,)))
    groups = {k: g for k, g in fake.groups.items() if g["name"] == host}
    assert sorted(p for p, _ in groups) == ["CL1-A", "CL2-A"]
    assert all(g["wwns"] == ["21000024ff4c1a2b"] for g in groups.values())
    assert all(g["mode"] == "LINUX/IRIX" for g in groups.values())

    iscsi = client.ensure_host(HostSpec(name="sbtest-is", initiators=(IQN_A,)))
    assert sorted(p for (p, _), g in fake.groups.items() if g["name"] == iscsi) == \
        ["CL1-B", "CL2-B"]

    client.delete_host(host)
    assert not any(g["name"] == host for g in fake.groups.values())
    assert ("CL1-A", 0) in fake.groups  # default group untouched


def test_host_mode_options():
    client, fake, _ = _client(host_mode="VMWARE_EX", host_mode_options=[54, 63])
    try:
        client.ensure_host(HostSpec(name="sbtest-vm", initiators=(FC_A,)))
        g = next(g for g in fake.groups.values() if g["name"] == "sbtest-vm")
        assert g["mode"] == "VMWARE_EX" and g["modeOptions"] == [54, 63]
    finally:
        client.close()


def test_no_port_for_protocol():
    client, _, _ = _client(ports=["CL1-B"])
    try:
        with pytest.raises(ArrayError, match="no configured fc port"):
            client.ensure_host(HostSpec(name="sbtest-fc", initiators=(FC_A,)))
    finally:
        client.close()


def test_iscsi_ownership_is_enforced_by_scan(env):
    """The array lets one IQN into several targets of a port; the adapter must not."""
    client, fake, _ = env
    client.ensure_host(HostSpec(name="sbtest-a", initiators=(IQN_A,)))
    with pytest.raises(ArrayConflictError, match="sbtest-a"):
        client.ensure_host(HostSpec(name="sbtest-b", initiators=(IQN_A,)))
    assert not any(g["name"] == "sbtest-b" for g in fake.groups.values())


def test_fc_wwn_in_unmanaged_group_conflicts(env):
    client, fake, _ = env
    client.get_info()
    fake.groups[("CL2-A", 0)]["wwns"].append("21000024ff4c1a2c")
    with pytest.raises(ArrayConflictError, match="CL2-A"):
        client.ensure_host(HostSpec(name="sbtest-b", initiators=(FC_B,)))


def test_same_lun_on_every_port(env):
    client, fake, _ = env
    other = client.create_volume(_uuid(), GIB)
    vol = client.create_volume(_uuid(), GIB)
    host = client.ensure_host(HostSpec(name="sbtest-a", initiators=(FC_A,)))
    # occupy LUN 0 on the second port only
    num = next(n for (p, n), g in fake.groups.items() if p == "CL2-A" and g["name"] == host)
    fake.groups[("CL2-A", num)]["luns"][0] = int(other.id)
    m = client.map_volume(vol.id, host)
    assert m.lun == 1
    luns = {p: lun for (p, n), g in fake.groups.items() if g["name"] == host
            for lun, lid in g["luns"].items() if lid == int(vol.id)}
    assert luns == {"CL1-A": 1, "CL2-A": 1}
    assert client.list_mappings(vol.id) == [m]


def test_map_repairs_missing_port_path(env):
    client, fake, _ = env
    vol = client.create_volume(_uuid(), GIB)
    host = client.ensure_host(HostSpec(name="sbtest-a", initiators=(FC_A,)))
    m = client.map_volume(vol.id, host)
    num = next(n for (p, n), g in fake.groups.items() if p == "CL2-A" and g["name"] == host)
    fake.groups[("CL2-A", num)]["luns"].clear()
    assert client.map_volume(vol.id, host) == m
    assert fake.groups[("CL2-A", num)]["luns"] == {m.lun: int(vol.id)}


def test_unmap_removes_every_port_path(env):
    client, fake, _ = env
    vol = client.create_volume(_uuid(), GIB)
    host = client.ensure_host(HostSpec(name="sbtest-a", initiators=(FC_A,)))
    client.map_volume(vol.id, host)
    client.unmap_volume(vol.id, host)
    assert not any(g["luns"] for g in fake.groups.values())
    client.unmap_volume(vol.id, host)


def test_map_to_unknown_host_is_not_found(env):
    client, _, _ = env
    vol = client.create_volume(_uuid(), GIB)
    with pytest.raises(ArrayError, match="no host group"):
        client.map_volume(vol.id, "sbtest-missing")


def test_lock_required_without_use_lock_fails():
    client, fake, _ = _client()
    try:
        fake.require_lock = True
        with pytest.raises(ArrayError, match="not locked"):
            client.create_volume(_uuid(), GIB)
    finally:
        client.close()


def test_use_lock_wraps_changes():
    client, fake, _ = _client(use_lock=True)
    try:
        fake.require_lock = True
        vol = client.create_volume(_uuid(), GIB)
        host = client.ensure_host(HostSpec(name="sbtest-a", initiators=(FC_A,)))
        client.map_volume(vol.id, host)
        client.resize_volume(vol.id, 2 * GIB)
        assert client.delete_volume(vol.id)
        client.delete_host(host)
        assert fake.lock_owner is None
        locks = [p for _, p in fake.requests if p.endswith("/lock/invoke")]
        unlocks = [p for _, p in fake.requests if p.endswith("/unlock/invoke")]
        assert len(locks) == len(unlocks) >= 6
    finally:
        client.close()


def test_lock_released_when_change_fails():
    client, fake, _ = _client(use_lock=True)
    try:
        client.get_info()
        fake.require_lock = True
        fake.pools.clear()
        with pytest.raises(ArrayError):
            client.create_volume(_uuid(), GIB)
        assert fake.lock_owner is None
    finally:
        client.close()


def test_resize_expands_by_difference(env):
    client, fake, _ = env
    vol = client.create_volume(_uuid(), 2 * GIB)
    grown = client.resize_volume(vol.id, 3 * GIB)
    assert grown.size_bytes == 3 * GIB
    assert fake.ldevs[int(vol.id)]["blocks"] == 3 * GIB // 512


def test_classify_error_bodies():
    nf = vsp._classify("GET x", 400, {"message": "The LDEV is not defined.",
                                      "messageId": "KART30000-E"})
    assert type(nf).__name__ == "ArrayNotFoundError"
    cf = vsp._classify("POST x", None, {"message": "already registered",
                                        "messageId": "KART30000-E",
                                        "errorCode": {"SSB1": "B957", "SSB2": "4184"}})
    assert isinstance(cf, ArrayConflictError) and "SSB1=B957" in str(cf)
    other = vsp._classify("POST x", 503, {"message": "busy"})
    assert isinstance(other, ArrayRequestError) and other.status == 503


@pytest.mark.parametrize("body, kind", [
    # codes from Hitachi's Cinder driver (hbsd_rest_api.py / hbsd_rest.py)
    ({"message": "x", "messageId": "KART30013-E"}, "ArrayNotFoundError"),
    ({"message": "x", "messageId": "KART30000-E", "errorCode": {"errorCode": "EX_ENOOBJ"}},
     "ArrayNotFoundError"),
    ({"message": "x", "messageId": "KART40050-E"}, "ArrayConflictError"),
    ({"message": "x", "messageId": "KART30000-E",
      "errorCode": {"SSB1": "2E22", "SSB2": "0001"}}, "ArrayConflictError"),
    ({"message": "x", "messageId": "KART30000-E",
      "errorCode": {"SSB1": "b958", "SSB2": "0947"}}, "ArrayConflictError"),
    ({"message": "x", "messageId": "KART30000-E",
      "errorCode": {"SSB1": "2E11", "SSB2": "2209"}}, "ArrayRequestError"),
])
def test_classify_structured_codes(body, kind):
    assert type(vsp._classify("POST x", 400, body)).__name__ == kind


def test_lock_uses_services_root():
    client, fake, _ = _client(use_lock=True)
    try:
        client.create_volume(_uuid(), GIB)
        locks = [p for _, p in fake.requests if p.endswith("/lock/invoke")]
        assert locks and all(
            p == "/ConfigurationManager/v1/services/resource-group-service/actions/lock/invoke"
            for p in locks)
    finally:
        client.close()
    sdid = contract_setup().fake.storage_device_id
    client, fake, _ = _client(use_lock=True, storage_device_id=sdid)
    try:
        client.create_volume(_uuid(), GIB)
        locks = [p for _, p in fake.requests if p.endswith("/lock/invoke")]
        assert locks and all(p.startswith(f"/ConfigurationManager/v1/{sdid}/services/")
                             for p in locks)
    finally:
        client.close()
