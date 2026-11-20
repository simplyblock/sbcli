"""Dell PowerFlex specifics beyond the shared contract."""
from __future__ import annotations

import uuid

import pytest

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
from simplyblock_core.storage_arrays.testing import FakeTransport, make_config
from simplyblock_core.storage_arrays.testing.dell_powerflex import (
    FakePowerFlex,
    contract_setup,
)

from .conftest import StaticProvider

GIB = 1 << 30


def _client(setup, **options):
    config = setup.config
    if options:
        config = make_config("dell-powerflex", config.endpoint,
                             **{**config.options, **options})
    return ArrayClient(config, StaticProvider(setup.credential),  # type: ignore[arg-type]
                       transport=FakeTransport(setup.fake),
                       session_kwargs={"sleep": lambda s: None})


def _sdc(setup, which=0) -> Initiator:
    return setup.initiators[Protocol.SDC][which][0]


@pytest.fixture(params=[3, 4], ids=["v3-token", "v4-oauth"])
def setup(request):
    return contract_setup(generation=request.param)


def test_auto_auth_uses_generation_specific_login(setup):
    with _client(setup) as c:
        info = c.get_info()
    fake: FakePowerFlex = setup.fake
    paths = [p for _, p in fake.requests]
    assert paths[0] == "/rest/auth/login"
    if fake.generation == 3:
        assert "/api/login" in paths and "R3_6" in info.version
    else:
        assert "/api/login" not in paths and "R4_5" in info.version
    # close() logged out
    assert not fake.tokens and not fake.access_tokens


def test_pinned_token_auth_skips_oauth_probe():
    setup = contract_setup(generation=3)
    with _client(setup, auth="token") as c:
        c.get_info()
    assert ("POST", "/rest/auth/login") not in setup.fake.requests


def test_pinned_oauth_on_3x_fails():
    setup = contract_setup(generation=3)
    with _client(setup, auth="oauth") as c, pytest.raises(ArrayNotFoundError):
        c.get_info()


def test_bad_options_rejected():
    setup = contract_setup()
    for opts in ({"auth": "kerberos"}, {"sdc_naming": "maybe"}, {"bogus": 1}):
        with _client(setup, **opts) as c, pytest.raises(ArrayError):
            c.get_info()
    config = make_config("dell-powerflex", "https://pf.example")
    with ArrayClient(config, StaticProvider(setup.credential),  # type: ignore[arg-type]
                     transport=FakeTransport(setup.fake)) as c, \
            pytest.raises(ArrayError, match="storage_pool_id"):
        c.get_info()


def test_sizes_round_to_8_gib(setup):
    with _client(setup) as c:
        vol = c.create_volume(str(uuid.uuid4()), 10 * GIB)
        assert vol.size_bytes == 16 * GIB
        raw = setup.fake.volumes[vol.id]
        assert raw["sizeInKb"] == 16 * 1024 * 1024
        assert raw["volumeType"] == "ThinProvisioned"
        grown = c.resize_volume(vol.id, 17 * GIB)
        assert grown.size_bytes == 24 * GIB
        assert vol.wwn is None and vol.nguid is None
        assert vol.identifier == f"id.{vol.id}"


def test_not_found_500_is_none_but_other_500_raises(setup):
    with _client(setup) as c:
        assert c.get_volume("deadbeef00000001") is None
        c.get_info()
        setup.fake.fail_next(500, {"message": "MDM is busy", "httpStatusCode": 500,
                                   "errorCode": 3}, path="Volume::")
        with pytest.raises(ArrayRequestError, match="MDM is busy"):
            c.get_volume("deadbeef00000001")


def test_duplicate_name_conflicts(setup):
    setup.fake.add_volume("sb-taken", 8 * GIB)
    with _client(setup) as c:
        c.get_info()
        adapter = c._adapter
        assert adapter is not None
        with pytest.raises(ArrayConflictError):
            adapter.create_volume("sb-taken", 8 * GIB)


def test_ensure_host_finds_preregistered_sdc_and_claims_name(setup):
    with _client(setup) as c:
        host = c.ensure_host(HostSpec(name="worker-1", initiators=(_sdc(setup),)))
        assert setup.fake.sdcs[host]["name"] == "worker-1"
        # GUID match is case-insensitive
        lower = Initiator(protocol=Protocol.SDC, address=_sdc(setup).address.lower())
        assert c.ensure_host(HostSpec(name="worker-1", initiators=(lower,))) == host
        with pytest.raises(ArrayConflictError, match="worker-1"):
            c.ensure_host(HostSpec(name="worker-2", initiators=(_sdc(setup),)))
        # delete_host leaves the SDC alone
        c.delete_host(host)
        assert host in setup.fake.sdcs


def test_unknown_sdc_guid_is_not_found(setup):
    ghost = Initiator(protocol=Protocol.SDC, address=str(uuid.uuid4()))
    with _client(setup) as c, pytest.raises(ArrayNotFoundError, match="install the SDC"):
        c.ensure_host(HostSpec(name="worker-9", initiators=(ghost,)))


def test_multi_sdc_host_is_refused_by_the_client(setup):
    spec = HostSpec(name="worker-1", initiators=(_sdc(setup, 0), _sdc(setup, 1)))
    with _client(setup) as c, pytest.raises(ArrayError, match="at most 1"):
        c.ensure_host(spec)


def test_admin_named_sdc_and_ignore_mode(setup):
    sid = setup.fake.add_sdc("ABCDEF01-0000-0000-0000-000000000001", name="esx-prod-7")
    spec = HostSpec(name="worker-1", initiators=(
        Initiator(protocol=Protocol.SDC, address="ABCDEF01-0000-0000-0000-000000000001"),))
    with _client(setup) as c, pytest.raises(ArrayConflictError):
        c.ensure_host(spec)
    with _client(setup, sdc_naming="ignore") as c:
        assert c.ensure_host(spec) == sid
    assert setup.fake.sdcs[sid]["name"] == "esx-prod-7"


def test_mapping_multi_sdc_and_null_mapped_info(setup):
    with _client(setup) as c:
        vol = c.create_volume(str(uuid.uuid4()), 8 * GIB)
        assert setup.fake.volumes[vol.id]["mappedSdcInfo"] is None
        assert c.list_mappings(vol.id) == []
        a = c.ensure_host(HostSpec(name="w-a", initiators=(_sdc(setup, 0),)))
        b = c.ensure_host(HostSpec(name="w-b", initiators=(_sdc(setup, 1),)))
        m = c.map_volume(vol.id, a)
        assert m.lun is None
        c.map_volume(vol.id, b)  # needs allowMultipleMappings
        assert {x.host_id for x in c.list_mappings(vol.id)} == {a, b}
        c.unmap_volume(vol.id, a)
        c.unmap_volume(vol.id, b)
        assert setup.fake.volumes[vol.id]["mappedSdcInfo"] is None
        assert c.delete_volume(vol.id)


def test_remove_mapped_volume_refused_by_array(setup):
    with _client(setup) as c:
        vol = c.create_volume(str(uuid.uuid4()), 8 * GIB)
        host = c.ensure_host(HostSpec(name="w-a", initiators=(_sdc(setup, 0),)))
        c.map_volume(vol.id, host)
        adapter = c._adapter
        assert adapter is not None
        with pytest.raises(ArrayConflictError, match="mapped"):
            adapter.delete_volume(vol.id)


def test_session_expiry_relogs_in(setup):
    with _client(setup) as c:
        c.get_info()
        setup.fake.expire_sessions()
        assert c.find_volume(str(uuid.uuid4())) is None
