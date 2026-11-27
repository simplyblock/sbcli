"""Vendor-neutral pieces: registry, naming/sizing, config validation,
RestSession error mapping and retries, HTTPS mock server end to end."""
from __future__ import annotations

import threading
import uuid

import pytest
import requests
from pydantic import SecretStr, ValidationError

from simplyblock_core.credentials import Credential
from simplyblock_core.storage_arrays.base import (
    ArrayAuthError,
    ArrayConfig,
    ArrayConflictError,
    ArrayNotFoundError,
    ArrayRequestError,
    ArrayUnsupportedError,
    HostSpec,
    Initiator,
    Protocol,
    colon_wwpn,
    normalize_wwpn,
)
from simplyblock_core.storage_arrays.client import (
    ArrayClient,
    credential_context,
    managed_name,
    round_size,
)
from simplyblock_core.storage_arrays.http import RestSession
from simplyblock_core.storage_arrays.registry import ADAPTERS, ALIASES, canonical_vendor
from tests.unit.storage_arrays.fakes import (
    FakeArray,
    FakeTransport,
    Reply,
    route,
    serve,
)
from tests.unit.storage_arrays.fakes.pure import FakePureFlashArray

from .conftest import StaticProvider


def _config(**kw):
    base = dict(id="a1", vendor="pure", endpoint="https://pure01.example.com",
                credential_ref={"kind": "vault-kv", "mount": "m", "path": "p"})
    base.update(kw)
    return ArrayConfig(**base)


class TestRegistry:

    def test_all_eleven_systems_registered(self):
        assert len(ADAPTERS) == 11

    def test_aliases_resolve(self):
        for alias, target in ALIASES.items():
            assert canonical_vendor(alias) == target
        assert canonical_vendor(" Pure ") == "pure-flasharray"

    def test_unknown_vendor(self):
        with pytest.raises(ArrayUnsupportedError, match="supported"):
            canonical_vendor("acme-array")


class TestNamingAndSizing:

    def test_managed_name_fits_31_chars(self):
        u = str(uuid.uuid4())
        assert managed_name(u, 31) == "sb-" + uuid.UUID(u).hex[:24]
        assert len(managed_name(u, 20)) == 20

    def test_managed_name_requires_uuid(self):
        with pytest.raises(ValueError):
            managed_name("not-a-uuid", 63)

    @pytest.mark.parametrize("size, gran, minimum, expected", [
        (1, 512, 1 << 20, 1 << 20),
        ((1 << 30) + 1, 512, 1 << 20, (1 << 30) + 512),
        (10 << 30, 8 << 30, 8 << 30, 16 << 30),
        (8 << 30, 8 << 30, 8 << 30, 8 << 30),
    ])
    def test_round_size(self, size, gran, minimum, expected):
        assert round_size(size, gran, minimum) == expected

    def test_credential_context_normalizes_endpoint(self):
        assert credential_context(_config(endpoint="https://PURE01.example.com/")) == \
            "array/a1/https://pure01.example.com:443"
        assert credential_context(_config(endpoint="https://u.example:8443/univmax")) == \
            "array/a1/https://u.example:8443"


class TestConfigAndModel:

    def test_https_only(self):
        with pytest.raises(ValidationError):
            _config(endpoint="http://pure01.example.com")

    def test_wwpn_forms(self):
        assert normalize_wwpn("21:00:00:24:FF:4C:1A:2B") == "21000024ff4c1a2b"
        assert normalize_wwpn("0x21000024ff4c1a2b") == "21000024ff4c1a2b"
        assert colon_wwpn("21000024FF4C1A2B") == "21:00:00:24:ff:4c:1a:2b"
        with pytest.raises(ValueError):
            normalize_wwpn("2100")

    def test_host_cannot_mix_scsi_and_nvme(self):
        with pytest.raises(ValidationError):
            HostSpec(name="h", initiators=(
                Initiator(protocol=Protocol.ISCSI, address="iqn.x"),
                Initiator(protocol=Protocol.NVME_TCP, address="nqn.x")))
        HostSpec(name="h", initiators=(
            Initiator(protocol=Protocol.ISCSI, address="iqn.x"),
            Initiator(protocol=Protocol.FC, address="2100000000000001")))

    def test_unknown_option_is_refused(self):
        provider = StaticProvider(Credential({"api_token": SecretStr("pure-token")}))
        client = ArrayClient(_config(options={"bogus": 1}), provider,  # type: ignore[arg-type]
                             transport=FakeTransport(FakePureFlashArray()))
        with pytest.raises(Exception, match="Unknown .* options: bogus"):
            client.get_info()

    def test_wrong_credential_shape_is_refused(self):
        provider = StaticProvider(Credential({"username": SecretStr("u"),
                                              "password": SecretStr("p")}))
        client = ArrayClient(_config(), provider,  # type: ignore[arg-type]
                             transport=FakeTransport(FakePureFlashArray()))
        with pytest.raises(ArrayAuthError, match="needs credential fields api_token"):
            client.get_info()


class _Flaky(FakeArray):
    def __init__(self):
        super().__init__()
        self.calls = 0

    @route("GET", "/flaky")
    def flaky(self, req):
        self.calls += 1
        return Reply(503 if self.calls < 3 else 200, {"ok": self.calls})

    @route("POST", "/flaky")
    def flaky_post(self, req):
        self.calls += 1
        return Reply(503, {})

    @route("GET", "/status/(?P<code>\\d+)")
    def status(self, req, code):
        return Reply(int(code), {"error": "x"})


class TestRestSession:

    def _session(self, fake):
        return RestSession("https://x.example", transport=FakeTransport(fake),
                           sleep=lambda s: None)

    def test_idempotent_requests_are_retried(self):
        fake = _Flaky()
        assert self._session(fake).get("/flaky").json() == {"ok": 3}

    def test_non_idempotent_requests_are_not_retried(self):
        fake = _Flaky()
        with pytest.raises(ArrayRequestError):
            self._session(fake).post("/flaky")
        assert fake.calls == 1

    @pytest.mark.parametrize("code, exc", [
        (401, ArrayAuthError), (403, ArrayAuthError), (404, ArrayNotFoundError),
        (409, ArrayConflictError), (400, ArrayRequestError), (500, ArrayRequestError),
    ])
    def test_status_mapping(self, code, exc):
        with pytest.raises(exc):
            self._session(_Flaky()).get(f"/status/{code}")

    def test_ok_statuses_are_returned(self):
        resp = self._session(_Flaky()).get("/status/400", ok=(400,))
        assert resp.status_code == 400

    def test_ca_bundle_file_is_removed_on_close(self):
        import os
        session = RestSession.for_config(_config(ca_bundle="-----BEGIN CERTIFICATE-----\n"))
        path = session.http.verify
        assert isinstance(path, str) and os.path.exists(path)
        session.close()
        assert not os.path.exists(path)

    def test_verify_false_only_when_configured(self):
        assert RestSession.for_config(_config()).http.verify is True
        assert RestSession.for_config(_config(verify_tls=False)).http.verify is False


class TestHttpsMockServer:

    def test_end_to_end_over_tls_with_pinned_ca(self):
        fake = FakePureFlashArray(api_token="t0k")
        server = serve(fake, "127.0.0.1", 0)
        port = server.server_address[1]
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with open(server.certfile) as f:
                ca = f.read()
            provider = StaticProvider(Credential({"api_token": SecretStr("t0k")}))
            config = _config(endpoint=f"https://127.0.0.1:{port}", ca_bundle=ca)
            with ArrayClient(config, provider) as client:  # type: ignore[arg-type]
                vol = client.create_volume(str(uuid.uuid4()), 1 << 30)
                assert client.get_volume(vol.id) == vol
                assert client.delete_volume(vol.id)

            # Without the pinned CA the self-signed server is refused.
            with ArrayClient(_config(endpoint=f"https://127.0.0.1:{port}"),
                             provider) as client:  # type: ignore[arg-type]
                with pytest.raises(ArrayRequestError, match="SSLError"):
                    client.get_info()
        finally:
            server.shutdown()
            server.server_close()

    def test_requests_session_is_plain(self):
        # FakeTransport is mounted per session, never globally.
        assert not isinstance(requests.Session().get_adapter("https://x"), FakeTransport)


# Options set through Kubernetes arrive as strings. Each is read as the type of
# its default, so "false" is false rather than a truthy string.
def test_string_options_are_read_as_the_type_of_their_default():
    from unittest.mock import MagicMock

    from pydantic import SecretStr

    from simplyblock_core.credentials import Credential
    from simplyblock_core.storage_arrays.adapters.dell_powermax import (
        DellPowerMaxAdapter,
    )
    from simplyblock_core.storage_arrays.adapters.hitachi_vsp import HitachiVspAdapter
    from simplyblock_core.storage_arrays.adapters.pure import PureFlashArrayAdapter
    from simplyblock_core.storage_arrays.base import AdapterContext, ArrayError
    from tests.unit.storage_arrays.fakes import make_config

    def adapter(cls, vendor, creds, **options):
        return cls(AdapterContext(make_config(vendor, "https://a.example", **options),
                                  Credential({k: SecretStr(v) for k, v in creds.items()}), MagicMock()))

    pure = adapter(PureFlashArrayAdapter, "pure-flasharray", {"api_token": "t"}, eradicate="false")
    assert pure.option("eradicate") is False
    assert adapter(PureFlashArrayAdapter, "pure-flasharray", {"api_token": "t"},
                   eradicate="True").option("eradicate") is True
    with pytest.raises(ArrayError, match="eradicate"):
        adapter(PureFlashArrayAdapter, "pure-flasharray", {"api_token": "t"}, eradicate="maybe")

    up = {"username": "u", "password": "p"}
    vsp = adapter(HitachiVspAdapter, "hitachi-vsp", up, pool_id="1", ports="CL1-A",
                  host_mode_options="2, 13", job_timeout_s="60", use_lock="0")
    assert vsp.option("host_mode_options") == ["2", "13"]
    assert vsp.option("job_timeout_s") == 60
    assert vsp.option("use_lock") is False

    pmax = adapter(DellPowerMaxAdapter, "dell-powermax", up, symmetrix_id="000197900123",
                   port_group='{"iscsi": "pg_iscsi", "fc": "pg_fc"}', async_jobs="no")
    assert pmax.option("port_group") == {"iscsi": "pg_iscsi", "fc": "pg_fc"}
    assert pmax.option("async_jobs") is False
    plain = adapter(DellPowerMaxAdapter, "dell-powermax", up, symmetrix_id="000197900123",
                    port_group="pg_all")
    assert plain.option("port_group") == "pg_all"
