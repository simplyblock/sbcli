"""Dell PowerMax specifics beyond the shared contract: cylinder sizing,
async jobs, one storage group + masking view per volume/host, hex LUNs,
non-unique volume identifiers."""
from __future__ import annotations

import uuid

import pytest

from simplyblock_core.storage_arrays.adapters.dell_powermax import (
    CYLINDER_BYTES,
    DellPowerMaxAdapter,
)
from simplyblock_core.storage_arrays.base import (
    AdapterContext,
    ArrayConflictError,
    ArrayError,
    ArrayNotFoundError,
    ArrayRequestError,
    ArrayUnsupportedError,
    HostSpec,
    Initiator,
    Protocol,
)
from simplyblock_core.storage_arrays.client import ArrayClient
from simplyblock_core.storage_arrays.http import RestSession
from simplyblock_core.storage_arrays.testing import FakeTransport
from simplyblock_core.storage_arrays.testing.dell_powermax import (
    SYMMETRIX_ID,
    contract_setup,
)

GIB = 1 << 30


class _Provider:
    def __init__(self, credential):
        self.credential = credential

    def resolve(self, ref, context):
        return self.credential

    def invalidate(self, ref=None):
        pass


def _client(setup) -> ArrayClient:
    return ArrayClient(setup.config, _Provider(setup.credential),  # type: ignore[arg-type]
                       transport=FakeTransport(setup.fake),
                       session_kwargs={"sleep": lambda s: None})


def _adapter(setup, **options) -> DellPowerMaxAdapter:
    opts = dict(setup.config.options)
    opts.update(options)
    opts = {k: v for k, v in opts.items() if v is not None}
    config = setup.config.model_copy(update={"options": opts})
    session = RestSession.for_config(config, transport=FakeTransport(setup.fake),
                                     sleep=lambda s: None)
    adapter = DellPowerMaxAdapter(AdapterContext(config, setup.credential, session))
    adapter.login()
    return adapter


def _iscsi(name: str) -> HostSpec:
    return HostSpec(name=name, initiators=(
        Initiator(protocol=Protocol.ISCSI, address=f"iqn.2026-01.io.sb:{name}"),))


@pytest.fixture()
def setup():
    return contract_setup()


def test_cylinder_granularity(setup):
    client = _client(setup)
    vol = client.create_volume(str(uuid.uuid4()), 10 * GIB)
    assert CYLINDER_BYTES == 1_966_080
    assert vol.size_bytes % CYLINDER_BYTES == 0
    assert vol.size_bytes == 5462 * CYLINDER_BYTES  # ceil(10 GiB / 1920 KiB)
    assert setup.fake.volumes[vol.id]["cap_cyl"] == 5462


def test_one_storage_group_per_volume(setup):
    client = _client(setup)
    vol = client.create_volume(str(uuid.uuid4()), GIB)
    sg = setup.fake.storage_groups[vol.name]
    assert sg["volumes"] == [vol.id] and sg["srp"] == "SRP_1" and sg["slo"] == "NONE"
    assert vol.wwn == "naa." + setup.fake.volumes[vol.id]["effective_wwn"].lower()
    assert client.delete_volume(vol.id)
    assert vol.name not in setup.fake.storage_groups
    assert setup.fake.volumes == {}


def test_service_level_is_applied(setup):
    adapter = _adapter(setup, service_level="Diamond")
    vol = adapter.create_volume("sb-diamond", GIB)
    assert setup.fake.storage_groups[vol.name]["slo"] == "Diamond"


def test_async_jobs_are_polled(setup):
    adapter = _adapter(setup, async_jobs=True)
    vol = adapter.create_volume("sb-async", 2 * GIB)
    vol = adapter.resize_volume(vol.id, 4 * GIB)
    assert vol.size_bytes >= 4 * GIB
    host = adapter.ensure_host(_iscsi("sbtest-async"))
    m = adapter.map_volume(vol.id, host)
    assert m.lun == 1
    adapter.unmap_volume(vol.id, host)
    adapter.delete_volume(vol.id)
    jobs = setup.fake.jobs.values()
    assert len(jobs) >= 5 and all(j["status"] == "SUCCEEDED" for j in jobs)
    assert all(j["_polls"] == 2 for j in jobs)
    assert any("/system/job/" in path for _, path in setup.fake.requests)


def test_failed_job_is_classified(setup):
    adapter = _adapter(setup, async_jobs=True)
    vol = adapter.create_volume("sb-jobfail", 4 * GIB)
    with pytest.raises(ArrayRequestError, match="FAILED.*larger"):
        adapter.resize_volume(vol.id, 2 * GIB)
    adapter.ensure_host(_iscsi("sbtest-j1"))
    with pytest.raises(ArrayConflictError, match="already in use"):
        adapter.ensure_host(HostSpec(name="sbtest-j2",
                                     initiators=_iscsi("sbtest-j1").initiators))


def test_job_timeout(setup):
    adapter = _adapter(setup, async_jobs=True, job_timeout_s=2)
    with pytest.raises(ArrayRequestError, match="did not finish"):
        adapter.create_volume("sb-slow", GIB)


def test_hex_lun_addresses(setup):
    setup.fake.first_lun = 0x1A
    adapter = _adapter(setup)
    vol = adapter.create_volume("sb-hexlun", GIB)
    host = adapter.ensure_host(_iscsi("sbtest-hex"))
    assert adapter.map_volume(vol.id, host).lun == 26
    assert adapter.list_mappings(vol.id)[0].lun == 26


def test_port_group_by_protocol(setup):
    adapter = _adapter(setup)
    vol = adapter.create_volume("sb-pg", GIB)
    fc = adapter.ensure_host(HostSpec(name="sbtest-fc", initiators=(
        Initiator(protocol=Protocol.FC, address="21:00:00:24:FF:4C:1A:2B"),)))
    assert setup.fake.hosts[fc]["initiator"] == ["21000024ff4c1a2b"]
    adapter.map_volume(vol.id, fc)
    view = setup.fake.views[DellPowerMaxAdapter.view_name(vol.name, fc)]
    assert view["pg"] == "sb_pg_fc"


def test_single_port_group_name(setup):
    adapter = _adapter(setup, port_group="sb_pg_iscsi")
    vol = adapter.create_volume("sb-pg1", GIB)
    host = adapter.ensure_host(_iscsi("sbtest-pg1"))
    adapter.map_volume(vol.id, host)
    assert setup.fake.views[f"{vol.name}_{host}"]["pg"] == "sb_pg_iscsi"


def test_map_without_port_group_is_refused(setup):
    adapter = _adapter(setup, port_group=None)
    vol = adapter.create_volume("sb-nopg", GIB)
    host = adapter.ensure_host(_iscsi("sbtest-nopg"))
    with pytest.raises(ArrayError, match="port_group"):
        adapter.map_volume(vol.id, host)
    with pytest.raises(ArrayNotFoundError):
        adapter.map_volume(vol.id, "sbtest-missing")


def test_long_host_name_gets_hashed_view_name(setup):
    adapter = _adapter(setup)
    vol = adapter.create_volume("sb-" + "a" * 24, GIB)
    name = "sbtest-" + "h" * 57
    host = adapter.ensure_host(_iscsi(name))
    adapter.map_volume(vol.id, host)
    (view,) = setup.fake.views
    assert len(view) <= 64 and view.startswith(vol.name + "_")
    assert adapter.map_volume(vol.id, host).host_id == host  # idempotent


def test_duplicate_identifiers(setup):
    adapter = _adapter(setup)
    vol = adapter.create_volume("sb-dup", GIB)
    setup.fake.add_volume("sb-dup", GIB)  # same identifier, outside our SG
    assert adapter.find_volume("sb-dup") == vol
    setup.fake.add_volume("sb-dup2", GIB)
    setup.fake.add_volume("sb-dup2", GIB)
    with pytest.raises(ArrayConflictError, match="2 PowerMax devices"):
        adapter.find_volume("sb-dup2")


def test_leftover_empty_storage_group_is_reused(setup):
    adapter = _adapter(setup)
    setup.fake.storage_groups["sb-left"] = setup.fake._sg_record("sb-left", "SRP_1", None)
    vol = adapter.create_volume("sb-left", GIB)
    assert setup.fake.storage_groups["sb-left"]["volumes"] == [vol.id]
    with pytest.raises(ArrayConflictError, match="already exists"):
        adapter.create_volume("sb-left", GIB)


def test_shared_storage_group_mapping_is_not_unmapped(setup):
    adapter = _adapter(setup)
    vol = adapter.create_volume("sb-shared", GIB)
    host = adapter.ensure_host(_iscsi("sbtest-shared"))
    fake = setup.fake
    fake.storage_groups["customer_sg"] = fake._sg_record("customer_sg", "SRP_1", None)
    fake.storage_groups["customer_sg"]["volumes"].append(vol.id)
    fake.volumes[vol.id]["storageGroupId"].append("customer_sg")
    adapter._call("POST", f"{adapter._slo}/maskingview", json={
        "maskingViewId": "customer_mv",
        "hostOrHostGroupSelection": {"useExistingHostParam": {"hostId": host}},
        "portGroupSelection": {"useExistingPortGroupParam": {"portGroupId": "sb_pg_iscsi"}},
        "storageGroupSelection": {"useExistingStorageGroupParam": {
            "storageGroupId": "customer_sg"}}})
    assert [m.host_id for m in adapter.list_mappings(vol.id)] == [host]
    with pytest.raises(ArrayConflictError, match="shared storage group"):
        adapter.unmap_volume(vol.id, host)


def test_errors_and_lookups(setup):
    adapter = _adapter(setup)
    assert adapter.get_volume("0FFFF") is None
    assert adapter.list_mappings("0FFFF") == []
    adapter.unmap_volume("0FFFF", "nobody")
    adapter.delete_host("nobody")
    with pytest.raises(ArrayNotFoundError):
        adapter.delete_volume("0FFFF")


def test_get_info_and_version_check(setup):
    info = _adapter(setup).get_info()
    assert info.model == "PowerMax_8500" and info.serial == SYMMETRIX_ID
    assert "6079" in info.version
    with pytest.raises(ArrayUnsupportedError, match="92"):
        _adapter(setup, api_version="92").get_info()


def test_unknown_symmetrix(setup):
    with pytest.raises(ArrayNotFoundError):
        _adapter(setup, symmetrix_id="000197900999").get_info()


def test_symmetrix_id_is_required(setup):
    config = setup.config.model_copy(update={"options": {}})
    session = RestSession.for_config(config, transport=FakeTransport(setup.fake))
    with pytest.raises(ArrayError, match="symmetrix_id"):
        DellPowerMaxAdapter(AdapterContext(config, setup.credential, session))


def test_basic_auth_on_every_request(setup):
    adapter = _adapter(setup)
    assert adapter.session.http.auth == ("smc", "pmax-pass")
    adapter.close()
    assert adapter.session.http.auth is None
