"""NetApp ONTAP adapter specifics beyond the shared contract."""
from __future__ import annotations

import uuid

import pytest

from simplyblock_core.storage_arrays.adapters.netapp_ontap import (
    VOLUME_SUBSYSTEM_COMMENT,
    NetAppOntapAdapter,
    flexvol_name,
    ontap_wwn,
    parse_nsid,
)
from simplyblock_core.storage_arrays.base import (
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
from simplyblock_core.storage_arrays.testing import FakeError, FakeTransport
from simplyblock_core.storage_arrays.testing.netapp_ontap import (
    FakeNetAppOntap,
    contract_setup,
)

from .conftest import StaticProvider

GIB = 1 << 30
NQN_A = "nqn.2014-08.org.nvmexpress:uuid:host-a"
NQN_B = "nqn.2014-08.org.nvmexpress:uuid:host-b"


def make(**options):
    setup = contract_setup(**options)
    client = ArrayClient(setup.config, StaticProvider(setup.credential),  # type: ignore[arg-type]
                         transport=FakeTransport(setup.fake),
                         session_kwargs={"sleep": lambda s: None})
    return client, setup.fake


def iscsi(name, *iqns):
    return HostSpec(name=name, initiators=tuple(
        Initiator(protocol=Protocol.ISCSI, address=a) for a in iqns))


def nvme(name, *nqns):
    return HostSpec(name=name, initiators=tuple(
        Initiator(protocol=Protocol.NVME_TCP, address=a) for a in nqns))


def calls(fake: FakeNetAppOntap, method: str, fragment: str) -> list[int]:
    return [i for i, (m, p) in enumerate(fake.requests) if m == method and fragment in p]


# -- identity helpers -----------------------------------------------------------


def test_wwn_from_serial():
    # 12 ASCII chars -> 24 hex digits behind NetApp's NAA-6 prefix
    assert ontap_wwn("wCVt1]IlvQWv") == "naa.600a0980" + "7743567431" + "5d496c76515776"
    assert len(ontap_wwn("wCVt1]IlvQWv")) == len("naa.") + 32


def test_volume_wwn_matches_fake_serial():
    client, fake = make()
    vol = client.create_volume(str(uuid.uuid4()), GIB)
    serial = fake.luns[vol.id]["serial_number"]
    assert vol.wwn == ontap_wwn(serial) == vol.identifier
    assert vol.nguid is None


def test_parse_nsid_and_flexvol_name():
    assert parse_nsid("00000001h") == 1
    assert parse_nsid("0000001Ah") == 26
    assert parse_nsid(3) == 3
    assert parse_nsid(None) is None
    assert flexvol_name("sb-0123abcd") == "sb_0123abcd"


# -- layout ------------------------------------------------------------------------


def test_create_makes_thin_flexvol_then_lun():
    client, fake = make()
    vol = client.create_volume(str(uuid.uuid4()), 10 * GIB)
    lun = fake.luns[vol.id]
    flex = fake.volumes[lun["location"]["volume"]["uuid"]]
    assert flex["name"] == flexvol_name(vol.name)
    assert lun["name"] == f"/vol/{flex['name']}/{vol.name}"
    assert lun["os_type"] == "linux"
    assert lun["space"]["guarantee"]["requested"] is False
    assert flex["guarantee"]["type"] == "none"
    assert flex["space"]["snapshot"]["reserve_percent"] == 0
    assert flex["snapshot_policy"]["name"] == "none"
    assert flex["size"] >= 10 * GIB * 105 // 100
    assert calls(fake, "POST", "/api/storage/volumes")[0] < calls(fake, "POST",
                                                                  "/api/storage/luns")[0]


def test_resize_grows_flexvol_before_lun():
    client, fake = make()
    vol = client.create_volume(str(uuid.uuid4()), 4 * GIB)
    fake.requests.clear()
    grown = client.resize_volume(vol.id, 40 * GIB)
    assert grown.size_bytes == 40 * GIB
    vol_patch = calls(fake, "PATCH", "/api/storage/volumes/")
    lun_patch = calls(fake, "PATCH", "/api/storage/luns/")
    assert vol_patch and lun_patch and vol_patch[0] < lun_patch[0]
    flex_uuid = fake.luns[vol.id]["location"]["volume"]["uuid"]
    assert fake.volumes[flex_uuid]["size"] >= 40 * GIB


def test_fake_refuses_lun_larger_than_flexvol():
    """Guards the ordering above: the fake rejects a LUN that outgrows its FlexVol."""
    client, fake = make()
    vol = client.create_volume(str(uuid.uuid4()), 4 * GIB)
    adapter = client._adapter
    assert adapter is not None
    with pytest.raises(ArrayRequestError, match="exceeds"):
        adapter._call("PATCH", f"/api/storage/luns/{vol.id}",
                      json={"space": {"size": 64 * GIB}})


def test_delete_removes_lun_then_flexvol():
    client, fake = make()
    vol = client.create_volume(str(uuid.uuid4()), GIB)
    fake.requests.clear()
    assert client.delete_volume(vol.id)
    assert calls(fake, "DELETE", "/api/storage/luns/")[0] < calls(
        fake, "DELETE", "/api/storage/volumes/")[0]
    assert not fake.luns and not fake.volumes


def test_shared_flexvol_mode():
    client, fake = make(volume_per_lun=False, flexvol="sb_shared")
    fake.add_flexvol("sb_shared", 100 * GIB)
    vol = client.create_volume(str(uuid.uuid4()), 4 * GIB)
    assert fake.luns[vol.id]["name"] == f"/vol/sb_shared/{vol.name}"
    assert len(fake.volumes) == 1
    client.resize_volume(vol.id, 8 * GIB)
    assert not calls(fake, "PATCH", "/api/storage/volumes/")
    client.delete_volume(vol.id)
    assert not fake.luns and [v["name"] for v in fake.volumes.values()] == ["sb_shared"]


def test_rerun_reuses_flexvol_of_interrupted_create():
    client, fake = make()
    vol_uuid = str(uuid.uuid4())
    name = client.volume_name(vol_uuid)
    fake.add_flexvol(flexvol_name(name), 2 * GIB)
    vol = client.create_volume(vol_uuid, 8 * GIB)
    assert len(fake.volumes) == 1
    assert fake.volumes[fake.luns[vol.id]["location"]["volume"]["uuid"]]["size"] >= 8 * GIB


def test_multiple_aggregates_pick_most_free_space():
    client, fake = make(aggregates="aggr1, aggr2")
    vol = client.create_volume(str(uuid.uuid4()), GIB)
    flex = fake.volumes[fake.luns[vol.id]["location"]["volume"]["uuid"]]
    assert [a["name"] for a in flex["aggregates"]] == ["aggr2"]


# -- jobs ----------------------------------------------------------------------------


def test_job_is_polled_until_success():
    client, fake = make()
    fake.job_polls = 4
    client.create_volume(str(uuid.uuid4()), GIB)
    assert len(calls(fake, "GET", "/api/cluster/jobs/")) == 5


def test_job_failure_raises_and_rolls_nothing_back():
    client, fake = make()
    fake.fail_next_job("Insufficient space in aggregate aggr1.", "917830")
    with pytest.raises(ArrayRequestError, match="Insufficient space"):
        client.create_volume(str(uuid.uuid4()), GIB)
    assert not fake.volumes and not fake.luns


def test_duplicate_flexvol_job_failure_is_conflict():
    client, fake = make()
    adapter_vol = "sb_dup"
    fake.add_flexvol(adapter_vol, GIB)
    client.get_info()
    adapter = client._adapter
    assert adapter is not None
    with pytest.raises(ArrayConflictError, match="Duplicate"):
        adapter._create_flexvol(adapter_vol, GIB)


def test_job_timeout():
    client, fake = make(job_timeout_s=10)
    fake.jobs_stuck = True
    with pytest.raises(ArrayRequestError, match="still running"):
        client.create_volume(str(uuid.uuid4()), GIB)


def test_lun_create_failure_removes_new_flexvol(monkeypatch):
    client, fake = make()

    def refuse(*a, **kw):
        raise FakeError(400, {"error": {"message": "LUN creation refused", "code": "1"}})

    monkeypatch.setattr(fake, "_check_size", refuse)
    with pytest.raises(ArrayRequestError, match="refused"):
        client.create_volume(str(uuid.uuid4()), GIB)
    assert not fake.volumes


# -- hosts -------------------------------------------------------------------------------


def test_igroup_is_mixed_linux_and_fc_wwpn_is_colon_lowercase():
    client, fake = make()
    spec = HostSpec(name="h1", initiators=(
        Initiator(protocol=Protocol.ISCSI, address="IQN.2026-01.io.sb:h1"),
        Initiator(protocol=Protocol.FC, address="0x21000024FF4C1A2B")))
    assert client.ensure_host(spec) == "h1"
    (group,) = fake.igroups.values()
    assert group["protocol"] == "mixed" and group["os_type"] == "linux"
    assert {i["name"] for i in group["initiators"]} == {
        "iqn.2026-01.io.sb:h1", "21:00:00:24:ff:4c:1a:2b"}


def test_initiator_in_foreign_igroup_conflicts_although_ontap_allows_it():
    client, fake = make()
    client.get_info()
    adapter = client._adapter
    assert adapter is not None
    # ONTAP itself accepts the same IQN in a second igroup ...
    for name in ("customer", "other"):
        adapter._call("POST", "/api/protocols/san/igroups", json={
            "svm": {"name": "svm1"}, "name": name, "os_type": "linux", "protocol": "iscsi",
            "initiators": [{"name": "iqn.2026-01.io.sb:x"}]})
    # ... the adapter refuses
    with pytest.raises(ArrayConflictError, match="customer"):
        client.ensure_host(iscsi("mine", "iqn.2026-01.io.sb:x"))
    assert "mine" not in {g["name"] for g in fake.igroups.values()}


def test_existing_iscsi_igroup_is_widened_for_fc():
    client, fake = make()
    client.get_info()
    adapter = client._adapter
    assert adapter is not None
    adapter._call("POST", "/api/protocols/san/igroups", json={
        "svm": {"name": "svm1"}, "name": "h2", "os_type": "linux", "protocol": "iscsi",
        "initiators": [{"name": "iqn.2026-01.io.sb:h2"}]})
    client.ensure_host(HostSpec(name="h2", initiators=(
        Initiator(protocol=Protocol.ISCSI, address="iqn.2026-01.io.sb:h2"),
        Initiator(protocol=Protocol.FC, address="21:00:00:24:ff:4c:1a:99"))))
    (group,) = fake.igroups.values()
    assert group["protocol"] == "mixed" and len(group["initiators"]) == 2


def test_nvme_fc_wwpn_address_is_refused():
    client, _ = make()
    with pytest.raises(ArrayUnsupportedError, match="NQN"):
        client.ensure_host(HostSpec(name="fc", initiators=(
            Initiator(protocol=Protocol.NVME_FC, address="21:00:00:24:ff:4c:1a:2b"),)))


def test_nvme_rdma_is_not_supported():
    client, _ = make()
    with pytest.raises(ArrayError, match="does not support"):
        client.ensure_host(HostSpec(name="r", initiators=(
            Initiator(protocol=Protocol.NVME_RDMA, address=NQN_A),)))


# -- options -------------------------------------------------------------------------------


@pytest.mark.parametrize(("options", "match"), [
    ({"svm": None}, "needs options: svm"),
    ({"protocol_family": "fcoe"}, "protocol_family"),
    ({"volume_per_lun": False}, "flexvol is required"),
    ({"space_guarantee": "file"}, "space_guarantee"),
    ({"snapshot_reserve_percent": 95}, "snapshot_reserve_percent"),
    ({"namespace_block_size": 1024}, "namespace_block_size"),
    ({"job_timeout_s": "soon"}, "job_timeout_s"),
    ({"pool": "x"}, "Unknown NetApp ONTAP options"),
])
def test_option_validation(options, match):
    setup = contract_setup()
    opts = {**setup.config.options, **options}
    opts = {k: v for k, v in opts.items() if v is not None}
    from simplyblock_core.storage_arrays.base import AdapterContext
    from simplyblock_core.storage_arrays.http import RestSession
    config = setup.config.model_copy(update={"options": opts})
    with pytest.raises(ArrayError, match=match):
        NetAppOntapAdapter(AdapterContext(config, setup.credential,
                                          RestSession(config.endpoint)))


def test_unknown_svm_fails_login():
    setup = contract_setup()
    config = setup.config.model_copy(update={"options": {"svm": "nope"}})
    client = ArrayClient(config, StaticProvider(setup.credential),  # type: ignore[arg-type]
                         transport=FakeTransport(setup.fake))
    with pytest.raises(ArrayNotFoundError, match="nope"):
        client.get_info()


def test_get_info():
    client, _ = make()
    info = client.get_info()
    assert info.model == "AFF-A400"
    assert info.version.startswith("NetApp Release 9.14.1")
    assert info.serial


# -- NVMe family and protocol routing ------------------------------------------------------


def test_nvme_family_creates_namespace():
    client, fake = make(protocol_family="nvme")
    vol = client.create_volume(str(uuid.uuid4()), 8 * GIB)
    assert vol.id in fake.namespaces and not fake.luns
    ns = fake.namespaces[vol.id]
    assert ns["space"]["block_size"] == 4096 and ns["os_type"] == "linux"
    assert vol.wwn is None and vol.identifier == f"id.{vol.id}"
    assert client.get_volume(vol.id) == vol
    grown = client.resize_volume(vol.id, 16 * GIB, expected_identifier=vol.identifier)
    assert grown.size_bytes == 16 * GIB


def test_nvme_multi_attach_through_volume_subsystem():
    client, fake = make(protocol_family="nvme")
    vol = client.create_volume(str(uuid.uuid4()), 8 * GIB)
    ha = client.ensure_host(nvme("node-a", NQN_A))
    hb = client.ensure_host(nvme("node-b", NQN_B))
    ma = client.map_volume(vol.id, ha)
    assert ma.lun == 1 and ma.host_id == "node-a"
    assert client.map_volume(vol.id, ha) == ma
    client.map_volume(vol.id, hb)
    assert {m.host_id for m in client.list_mappings(vol.id)} == {"node-a", "node-b"}
    (smap,) = fake.subsystem_maps
    vsub = fake.subsystems[smap["subsystem"]["uuid"]]
    assert vsub["comment"] == VOLUME_SUBSYSTEM_COMMENT
    assert vsub["name"] == flexvol_name(vol.name)
    assert {h["nqn"] for h in vsub["hosts"]} == {NQN_A, NQN_B}

    client.unmap_volume(vol.id, ha)
    client.unmap_volume(vol.id, ha)
    assert [m.host_id for m in client.list_mappings(vol.id)] == ["node-b"]

    assert client.delete_volume(vol.id)
    assert not fake.namespaces and not fake.subsystem_maps and not fake.volumes
    assert {s["name"] for s in fake.subsystems.values()} == {"node-a", "node-b"}
    client.delete_host(ha)
    client.delete_host(hb)
    assert not fake.subsystems


def test_volume_subsystem_does_not_count_as_host_owner():
    client, _ = make(protocol_family="nvme")
    vol = client.create_volume(str(uuid.uuid4()), GIB)
    client.map_volume(vol.id, client.ensure_host(nvme("node-a", NQN_A)))
    # NQN_A is now in node-a and in the volume subsystem: re-ensuring is fine,
    assert client.ensure_host(nvme("node-a", NQN_A)) == "node-a"
    # but another host claiming it conflicts.
    with pytest.raises(ArrayConflictError, match="node-a"):
        client.ensure_host(nvme("node-x", NQN_A))


def test_scsi_volume_to_nvme_host_is_unsupported_and_vice_versa():
    client, _ = make()
    lun = client.create_volume(str(uuid.uuid4()), GIB)
    host = client.ensure_host(nvme("nv", NQN_A))
    with pytest.raises(ArrayUnsupportedError, match="NVMe host"):
        client.map_volume(lun.id, host)

    nclient, nfake = make(protocol_family="nvme")
    ns = nclient.create_volume(str(uuid.uuid4()), GIB)
    nclient.ensure_host(iscsi("sc", "iqn.2026-01.io.sb:sc"))
    with pytest.raises(ArrayUnsupportedError, match="SCSI host"):
        nclient.map_volume(ns.id, "sc")
    assert not nfake.subsystem_maps


def test_volumes_of_the_other_family_are_still_found():
    client, fake = make(protocol_family="nvme")
    ns = client.create_volume(str(uuid.uuid4()), GIB)
    scsi_setup_config = client.config.model_copy(
        update={"options": {"svm": "svm1", "protocol_family": "scsi"}})
    provider = StaticProvider(contract_setup().credential)
    scsi_client = ArrayClient(scsi_setup_config, provider,  # type: ignore[arg-type]
                              transport=FakeTransport(fake))
    assert scsi_client.get_volume(ns.id) == ns
    assert scsi_client.delete_volume(ns.id)
    assert not fake.namespaces


def test_namespace_mapped_to_foreign_subsystem_is_refused():
    client, fake = make(protocol_family="nvme")
    vol = client.create_volume(str(uuid.uuid4()), GIB)
    adapter = client._adapter
    assert adapter is not None
    adapter._call("POST", "/api/protocols/nvme/subsystems", json={
        "svm": {"name": "svm1"}, "name": "customer_ss", "os_type": "linux"})
    adapter._call("POST", "/api/protocols/nvme/subsystem-maps", json={
        "svm": {"name": "svm1"}, "namespace": {"uuid": vol.id},
        "subsystem": {"name": "customer_ss"}})
    client.ensure_host(nvme("node-a", NQN_A))
    with pytest.raises(ArrayConflictError, match="does not manage"):
        client.map_volume(vol.id, "node-a")


def test_lun_numbers_come_from_lun_maps():
    client, fake = make()
    host = client.ensure_host(iscsi("h", "iqn.2026-01.io.sb:h"))
    v1 = client.create_volume(str(uuid.uuid4()), GIB)
    v2 = client.create_volume(str(uuid.uuid4()), GIB)
    assert client.map_volume(v1.id, host).lun == 0
    assert client.map_volume(v2.id, host).lun == 1
    assert {m["logical_unit_number"] for m in fake.lun_maps} == {0, 1}


def test_map_to_missing_host_is_not_found():
    client, _ = make()
    vol = client.create_volume(str(uuid.uuid4()), GIB)
    with pytest.raises(ArrayNotFoundError):
        client.map_volume(vol.id, "ghost")
