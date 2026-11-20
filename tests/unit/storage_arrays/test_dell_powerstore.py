"""Dell PowerStore specifics beyond the shared contract: CSRF token and
session cookie, PostgREST paging, 8 KiB granularity, 422 classification."""
from __future__ import annotations

import uuid

import pytest
from pydantic import SecretStr

from simplyblock_core.credentials import Credential
from simplyblock_core.storage_arrays.adapters.dell_powerstore import (
    TOKEN_HEADER,
    DellPowerStoreAdapter,
    powerstore_nguid,
    powerstore_wwn,
)
from simplyblock_core.storage_arrays.base import (
    AdapterContext,
    ArrayConflictError,
    ArrayError,
    HostSpec,
    Initiator,
    Protocol,
)
from simplyblock_core.storage_arrays.client import ArrayClient
from simplyblock_core.storage_arrays.http import RestSession
from simplyblock_core.storage_arrays.testing import FakeTransport, make_config
from simplyblock_core.storage_arrays.testing.dell_powerstore import contract_setup

GIB = 1 << 30


class _Provider:
    def __init__(self, credential):
        self.credential = credential

    def resolve(self, ref, context):
        return self.credential

    def invalidate(self, ref=None):
        pass


@pytest.fixture()
def setup():
    return contract_setup()


@pytest.fixture()
def client(setup):
    c = ArrayClient(setup.config, _Provider(setup.credential),  # type: ignore[arg-type]
                    transport=FakeTransport(setup.fake),
                    session_kwargs={"sleep": lambda s: None})
    yield c
    c.close()


def _adapter(setup, **options) -> DellPowerStoreAdapter:
    config = setup.config.model_copy(update={"options": options})
    session = RestSession.for_config(config, transport=FakeTransport(setup.fake))
    adapter = DellPowerStoreAdapter(AdapterContext(config, setup.credential, session))
    adapter.login()
    return adapter


def _iscsi(name: str) -> HostSpec:
    return HostSpec(name=name, initiators=(
        Initiator(protocol=Protocol.ISCSI, address=f"iqn.2026-01.io.sb:{name}"),))


def test_login_keeps_cookie_and_csrf_token_not_basic_auth(setup):
    adapter = _adapter(setup)
    assert adapter.session.http.auth is None
    assert adapter.session.headers[TOKEN_HEADER] in setup.fake.sessions.values()
    assert "auth_cookie" in adapter.session.http.cookies
    vol = adapter.create_volume("sb-csrf", GIB)
    assert vol.size_bytes == GIB


def test_mutation_without_csrf_token_is_forbidden(setup):
    adapter = _adapter(setup)
    adapter.session.headers.pop(TOKEN_HEADER)
    assert adapter.find_volume("sb-none") is None  # GET needs only the cookie
    with pytest.raises(ArrayError, match="403"):
        adapter.create_volume("sb-nocsrf", GIB)


def test_close_logs_out(setup):
    adapter = _adapter(setup)
    assert len(setup.fake.sessions) == 1
    adapter.close()
    assert setup.fake.sessions == {}
    assert ("POST", "/api/rest/logout") in setup.fake.requests


def test_size_is_rounded_to_8k(client, setup):
    vol = client.create_volume(str(uuid.uuid4()), GIB + 1)
    assert vol.size_bytes == GIB + 8192
    assert setup.fake.volumes[vol.id]["size"] % 8192 == 0


def test_fake_rejects_unaligned_size(setup):
    adapter = _adapter(setup)
    with pytest.raises(ArrayError, match="multiple of 8192"):
        adapter.create_volume("sb-odd", GIB + 512)


def test_identity_format(client):
    vol = client.create_volume(str(uuid.uuid4()), GIB)
    assert vol.wwn and vol.wwn.startswith("naa.68ccf098") and vol.wwn == vol.wwn.lower()
    assert vol.nguid and vol.nguid.startswith("eui.") and len(vol.nguid) == 4 + 32
    assert powerstore_wwn("NAA.68CCF098ABC") == "naa.68ccf098abc"
    assert powerstore_nguid("nguid.ABCD") == "eui.abcd"
    assert powerstore_wwn(None) is None and powerstore_nguid("") is None


def test_select_and_filter_are_sent(client, setup):
    client.find_volume(str(uuid.uuid4()))
    adapter = client._adapter
    assert adapter is not None
    # unknown select fields are refused by the array (400), not silently ignored
    with pytest.raises(ArrayError, match="400"):
        adapter._list("/volume", "id,bogus")


def test_paged_host_lookup(setup):
    setup.fake.page_limit = 2
    adapter = _adapter(setup)
    ids = [adapter.ensure_host(_iscsi(f"sbtest-page-{i}")) for i in range(5)]
    assert len(set(ids)) == 5
    # the last host sits on the third page of an unfiltered listing
    assert len(adapter._list("/host", "id,name")) == 5
    assert adapter.ensure_host(_iscsi("sbtest-page-4")) == ids[4]


def test_fc_initiators_are_colon_separated_and_deduplicated(setup):
    adapter = _adapter(setup)
    spec = HostSpec(name="sbtest-fc", initiators=(
        Initiator(protocol=Protocol.FC, address="0x21000024FF4C1A2B"),))
    host_id = adapter.ensure_host(spec)
    ports = setup.fake.hosts[host_id]["host_initiators"]
    assert [(p["port_name"], p["port_type"]) for p in ports] == [
        ("21:00:00:24:ff:4c:1a:2b", "FC")]
    grown = HostSpec(name="sbtest-fc", initiators=spec.initiators + (
        Initiator(protocol=Protocol.FC, address="21:00:00:24:ff:4c:1a:2c"),))
    assert adapter.ensure_host(grown) == host_id
    assert adapter.ensure_host(grown) == host_id
    assert len(setup.fake.hosts[host_id]["host_initiators"]) == 2


def test_nvme_host_uses_nvme_port_type(setup):
    adapter = _adapter(setup)
    spec = HostSpec(name="sbtest-nvme", initiators=(
        Initiator(protocol=Protocol.NVME_TCP, address="nqn.2014-08.org.nvmexpress:uuid:x"),))
    host_id = adapter.ensure_host(spec)
    assert setup.fake.hosts[host_id]["host_initiators"][0]["port_type"] == "NVMe"
    assert setup.fake.hosts[host_id]["os_type"] == "Linux"


def test_initiator_conflict_is_classified(setup):
    adapter = _adapter(setup)
    adapter.ensure_host(_iscsi("sbtest-one"))
    other = HostSpec(name="sbtest-two", initiators=_iscsi("sbtest-one").initiators)
    with pytest.raises(ArrayConflictError, match="already registered"):
        adapter.ensure_host(other)


def test_delete_attached_volume_is_conflict(setup):
    adapter = _adapter(setup)
    vol = adapter.create_volume("sb-attached", GIB)
    host = adapter.ensure_host(_iscsi("sbtest-att"))
    m = adapter.map_volume(vol.id, host)
    assert m.lun == 1
    with pytest.raises(ArrayConflictError):
        adapter.delete_volume(vol.id)
    with pytest.raises(ArrayConflictError):
        adapter.delete_host(host)
    adapter.unmap_volume(vol.id, host)
    adapter.delete_volume(vol.id)
    assert adapter.get_volume(vol.id) is None


def test_luns_are_per_host(setup):
    adapter = _adapter(setup)
    host = adapter.ensure_host(_iscsi("sbtest-luns"))
    luns = [adapter.map_volume(adapter.create_volume(f"sb-lun{i}", GIB).id, host).lun
            for i in range(3)]
    assert luns == [1, 2, 3]


def test_appliance_option_and_unknown_option(setup):
    adapter = _adapter(setup, appliance_id="A2")
    vol = adapter.create_volume("sb-a2", GIB)
    assert setup.fake.volumes[vol.id]["appliance_id"] == "A2"
    with pytest.raises(ArrayError, match="Unknown"):
        _adapter(setup, pool="x")


def test_get_info(client):
    info = client.get_info()
    assert info.model == "PowerStore 1200T"
    assert info.version == "PowerStoreOS 4.0.0.0"
    assert info.serial == "PS4a7b5c3e9f01"


def test_api_token_credential_is_refused():
    setup = contract_setup()
    config = make_config("dell-powerstore", "https://ps.example")
    session = RestSession.for_config(config, transport=FakeTransport(setup.fake))
    with pytest.raises(ArrayError, match="username"):
        DellPowerStoreAdapter(AdapterContext(
            config, Credential({"api_token": SecretStr("t")}), session))
