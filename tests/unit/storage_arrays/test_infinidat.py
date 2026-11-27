"""Infinidat InfiniBox specifics beyond the shared contract."""
from __future__ import annotations

import uuid

import pytest

from simplyblock_core.storage_arrays.adapters.infinidat import infinibox_wwn
from simplyblock_core.storage_arrays.base import (
    ArrayConflictError,
    ArrayError,
    ArrayNotFoundError,
    ArrayRequestError,
    HostSpec,
    Initiator,
    Protocol,
)
from simplyblock_core.storage_arrays.client import ArrayClient
from tests.unit.storage_arrays.fakes import FakeTransport, make_config
from tests.unit.storage_arrays.fakes.infinidat import contract_setup

from .conftest import StaticProvider

GIB = 1 << 30


@pytest.fixture()
def setup():
    return contract_setup()


def _client(setup, **options):
    config = setup.config
    if options:
        config = make_config("infinidat-infinibox", config.endpoint,
                             **{**config.options, **options})
    return ArrayClient(config, StaticProvider(setup.credential),  # type: ignore[arg-type]
                       transport=FakeTransport(setup.fake),
                       session_kwargs={"sleep": lambda s: None})


def _iqn(n: str) -> Initiator:
    return Initiator(protocol=Protocol.ISCSI, address=f"iqn.2026-01.io.sb:{n}")


def test_wwn_from_serial():
    assert infinibox_wwn("742B0F000004E2B000000000000F3A3") == "naa.6742b0f000004e2b000000000000f3a3"
    assert infinibox_wwn("6742b0f000004e2b000000000000f3a3") == "naa.6742b0f000004e2b000000000000f3a3"


def test_volume_identity_and_pool(setup):
    with _client(setup) as c:
        vol = c.create_volume(str(uuid.uuid4()), 10 * GIB + 1)
        raw = setup.fake.volumes[int(vol.id)]
        assert vol.wwn == "naa.6" + raw["serial"]
        assert len(vol.wwn) == len("naa.") + 32
        assert raw["pool_id"] == next(iter(setup.fake.pools))
        assert raw["provtype"] == "THIN"
        assert vol.size_bytes == 10 * GIB + (1 << 20)


def test_thick_option_and_bad_option(setup):
    with _client(setup, provtype="thick") as c:
        vol = c.create_volume(str(uuid.uuid4()), GIB)
        assert setup.fake.volumes[int(vol.id)]["provtype"] == "THICK"
    with _client(setup, provtype="sparse") as c, pytest.raises(ArrayError, match="provtype"):
        c.get_info()


def test_missing_pool(setup):
    with _client(setup, pool="nope") as c, pytest.raises(ArrayNotFoundError, match="nope"):
        c.create_volume(str(uuid.uuid4()), GIB)


def test_destructive_requests_send_approval(setup):
    with _client(setup) as c:
        vol = c.create_volume(str(uuid.uuid4()), GIB)
        host = c.ensure_host(HostSpec(name="h1", initiators=(_iqn("a"),)))
        c.map_volume(vol.id, host)
        assert c.delete_volume(vol.id)  # unmap + delete, both need approved=true
        c.delete_host(host)
    assert not setup.fake.volumes and not setup.fake.hosts


def test_every_mutation_is_preapproved(setup):
    # infinisdk sends approved=true on every non-GET request; so does the adapter.
    setup.fake.approval_for_all = True
    fc = Initiator(protocol=Protocol.FC, address="21:00:00:24:ff:4c:1a:2b")
    with _client(setup) as c:
        vol = c.create_volume(str(uuid.uuid4()), GIB)
        vol = c.resize_volume(vol.id, 2 * GIB)
        host = c.ensure_host(HostSpec(name="h1", initiators=(_iqn("a"), fc)))
        c.map_volume(vol.id, host)
        c.unmap_volume(vol.id, host)
        assert c.delete_volume(vol.id)
        c.delete_host(host)
    assert not setup.fake.volumes and not setup.fake.hosts


@pytest.mark.parametrize("code", ["APPROVAL_REQUIRED", "APPROVAL_REQUIRED_VOLUME_HAS_CHILDREN"])
def test_specific_approval_codes_are_not_auth_errors(setup, code):
    with _client(setup) as c:
        vol = c.create_volume(str(uuid.uuid4()), GIB)
        setup.fake.fail_next(403, {"result": None, "error": {
            "code": code, "message": "needs approval", "reasons": ["has children"]},
            "metadata": {"ready": True}}, path=r"/volumes/\d+$")
        adapter = c._adapter
        assert adapter is not None
        with pytest.raises(ArrayRequestError, match=code):
            adapter.delete_volume(vol.id)


def test_get_info_reads_model_field(setup):
    with _client(setup) as c:
        info = c.get_info()
    assert info.model == "F4304" and info.serial == "1250" and info.version == "7.3.10.0"


def test_approval_required_is_not_an_auth_error(setup):
    with _client(setup) as c:
        vol = c.create_volume(str(uuid.uuid4()), GIB)
        setup.fake.fail_next(403, {"result": None, "error": {
            "code": "APPROVAL_REQUIRED", "message": "needs approval"},
            "metadata": {"ready": True}}, path=r"/volumes/\d+$")
        adapter = c._adapter
        assert adapter is not None
        with pytest.raises(ArrayRequestError, match="APPROVAL_REQUIRED"):
            adapter.delete_volume(vol.id)


def test_conflicting_port_rolls_back_new_host(setup):
    with _client(setup) as c:
        c.ensure_host(HostSpec(name="h1", initiators=(_iqn("a"),)))
        with pytest.raises(ArrayConflictError):
            c.ensure_host(HostSpec(name="h2", initiators=(_iqn("b"), _iqn("a"))))
    assert [h["name"] for h in setup.fake.hosts.values()] == ["h1"]


def test_fc_and_iscsi_ports_normalised(setup):
    fc = Initiator(protocol=Protocol.FC, address="21:00:00:24:FF:4C:1A:2B")
    with _client(setup) as c:
        host = c.ensure_host(HostSpec(name="h1", initiators=(fc, _iqn("A"))))
        ports = setup.fake.hosts[int(host)]["ports"]
        assert {(p["type"], p["address"]) for p in ports} == {
            ("FC", "21000024ff4c1a2b"), ("ISCSI", "iqn.2026-01.io.sb:a")}
        # colon-formatted ports returned by the array still count as present
        ports[0]["address"] = "21:00:00:24:ff:4c:1a:2b"
        assert c.ensure_host(HostSpec(name="h1", initiators=(fc,))) == host
        assert len(setup.fake.hosts[int(host)]["ports"]) == 2


def test_luns_start_at_one_per_host(setup):
    with _client(setup) as c:
        host = c.ensure_host(HostSpec(name="h1", initiators=(_iqn("a"),)))
        luns = [c.map_volume(c.create_volume(str(uuid.uuid4()), GIB).id, host).lun
                for _ in range(3)]
    assert luns == [1, 2, 3]


def test_pagination_walks_all_pages(setup, monkeypatch):
    import simplyblock_core.storage_arrays.adapters.infinidat as mod
    monkeypatch.setattr(mod, "_PAGE_SIZE", 2)
    for i in range(5):
        setup.fake.add_volume(f"sb-same-{i}", GIB)
    with _client(setup) as c:
        c.get_info()
        adapter = c._adapter
        assert adapter is not None
        assert len(adapter._list("/volumes")) == 5


def test_non_numeric_ids_are_absent(setup):
    with _client(setup) as c:
        assert c.get_volume("sb-abc") is None
        c.delete_host("not-a-number")


def test_bad_credential_is_auth_error(setup):
    setup.fake.set_credential(password="other")
    with _client(setup) as c, pytest.raises(ArrayError):
        c.get_info()
