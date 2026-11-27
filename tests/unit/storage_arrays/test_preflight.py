"""The array pre-check (storage_arrays.preflight) against every vendor fake:
all checks pass, the recorded trace holds no secret, and replaying the trace
gives the same results without the array."""
from __future__ import annotations

import json
import tarfile

import pytest

from simplyblock_core import array
from simplyblock_core.storage_arrays.preflight import __main__ as cli
from simplyblock_core.storage_arrays.preflight import checks as C
from simplyblock_core.storage_arrays.preflight.trace import (
    REDACTED,
    RecordingAdapter,
    Redactor,
    ReplayAdapter,
)
from tests.unit.storage_arrays.fakes import FakeTransport

from .conftest import VENDORS, fake_module
from .test_san_plugin import Bed


def _config(bed: Bed, **kw) -> C.Config:
    return C.Config(
        vendor=bed.vendor, endpoint=bed.setup.config.endpoint, protocol=bed.protocol.value,
        options={**bed.setup.config.options, "protocol": bed.protocol.value},
        credentials={k: getattr(v, "get_secret_value", lambda v=v: v)() for k, v in bed.creds.items()},
        run_id="check-test-0001", host_name="check-host",
        write=True, initiators={k: v for k, v in bed.initiators(0).items() if k != "hostname"}, **kw)


def _factory(cfg: C.Config, transport):
    def make(options):
        p = array.plugin_class(cfg.vendor)(endpoint=cfg.endpoint, options=options, cluster_id=cfg.run_id)
        p.transport = transport
        p.session_kwargs = {"sleep": lambda s: None}
        return p
    return make


def _run(cfg: C.Config, transport) -> C.Runner:
    runner = C.Runner(echo=lambda s: None)
    make = _factory(cfg, transport)
    st = C.run_checks(cfg, make, None, runner, live=False)
    C.cleanup(st, cfg, make, echo=lambda s: None)
    return runner


@pytest.fixture(params=VENDORS)
def bed(request) -> Bed:
    return Bed(request.param)


def test_every_check_passes_against_the_fake(bed):
    cfg = _config(bed)
    runner = _run(cfg, FakeTransport(bed.setup.fake))
    bad = [(r.id, r.status, r.detail) for r in runner.results if r.status != "PASS"]
    assert not bad
    ids = [r.id for r in runner.results]
    assert ids == ["api.login", "api.capabilities", "lun.host_object", "lun.create", "lun.attach", "lun.grow",
                   "lun.resize_edges", "lun.fence", "lun.unfence", "lun.detach", "lun.delete",
                   "lun.host_removed"]


def test_the_trace_has_no_secret_and_replays_to_the_same_results(bed):
    cfg = _config(bed)
    redactor = Redactor(list(cfg.credentials.values()))
    recorder = RecordingAdapter(redactor, inner=FakeTransport(bed.setup.fake))
    first = _run(cfg, recorder)
    entries = recorder.export()
    assert entries
    text = json.dumps(entries)
    for secret in cfg.credentials.values():
        assert secret not in text
    for secret in redactor.secrets:
        assert secret not in text

    replayed = ReplayAdapter(json.loads(text))
    second = _run(cfg, replayed)
    assert [(r.id, r.status) for r in second.results] == [(r.id, r.status) for r in first.results]
    assert replayed.unmatched == []


def test_a_failure_is_reported_and_cleaned_up():
    bed = Bed("pure-flasharray")
    cfg = _config(bed)
    fake = bed.setup.fake

    class Breaking(FakeTransport):
        """The array goes away for the growth (Pure resizes with a PATCH),
        and is back for the cleanup."""
        broken = 1

        def send(self, request, **kw):
            if request.method == "PATCH" and self.broken:
                self.broken -= 1
                raise ConnectionError("array went away")
            return super().send(request, **kw)

    runner = _run(cfg, Breaking(fake))
    status = {r.id: r.status for r in runner.results}
    assert status["lun.grow"] == "FAIL"
    assert status["lun.resize_edges"] == "SKIP"
    assert status["lun.create"] == "PASS"
    # The cleanup removed the LUN and the host object the run created.
    p = _factory(cfg, FakeTransport(fake))(cfg.options)
    p.connect(cfg.endpoint, cfg.credentials, cfg.options)
    from simplyblock_core.array.san import lun_uuid
    assert p._run(lambda c: c.find_volume(lun_uuid(cfg.run_id, C.DATA_LUN))) is None


def test_redactor_learns_secrets_by_name_and_removes_them_everywhere():
    r = Redactor(["hunter22"])
    body = r.body({"username": "admin", "password": "hunter22", "auth": {"token": "tok-12345"},
                   "items": [{"api_token": "abcd-9999"}]})
    assert body["password"] == REDACTED and body["auth"]["token"] == REDACTED
    assert body["items"][0]["api_token"] == REDACTED and body["username"] == "admin"
    headers = r.headers({"Content-Type": "application/json", "Authorization": "Basic YWRtaW46aHVudGVyMjI=",
                         "X-Auth-Token": "sess-777777", "Set-Cookie": "session=cookie-88888; Path=/"})
    assert headers["Content-Type"] == "application/json"
    assert headers["Authorization"] == REDACTED and headers["X-Auth-Token"] == REDACTED
    url = r.url("https://a/api/v1/luns?token=qwerty12&name=x")
    assert "qwerty12" not in url and "name=x" in url
    scrubbed = r.scrub({"later": "/api/session/sess-777777/luns", "c": "cookie-88888", "b": "YWRtaW46aHVudGVyMjI="})
    assert "sess-777777" not in json.dumps(scrubbed)
    assert "cookie-88888" not in json.dumps(scrubbed)
    assert "YWRtaW46aHVudGVyMjI=" not in json.dumps(scrubbed)


def test_bundle_round_trip(tmp_path, monkeypatch):
    """cmd_check's bundle replays through the replay command."""
    bed = Bed("netapp-ontap")
    cfg = _config(bed)
    redactor = Redactor(list(cfg.credentials.values()))
    recorder = RecordingAdapter(redactor, inner=FakeTransport(bed.setup.fake))
    runner = _run(cfg, recorder)
    name = "array-check-netapp-ontap-check-test-0001"
    root = tmp_path / name
    root.mkdir()
    meta = {"vendor": cfg.vendor, "endpoint": cfg.endpoint, "protocol": cfg.protocol,
            "options": cfg.options, "journal_options": {}, "credential_fields": sorted(cfg.credentials),
            "run_id": cfg.run_id, "host_name": cfg.host_name, "write": True, "size_gib": 1,
            "initiators": cfg.initiators}
    (root / "report.json").write_text(json.dumps({"meta": meta, "results": runner.as_dicts()}))
    (root / "trace.jsonl").write_text("\n".join(json.dumps(e) for e in recorder.export()))
    bundle = tmp_path / f"{name}.tar.gz"
    with tarfile.open(bundle, "w:gz") as tar:
        tar.add(root, arcname=name)
    replayed, adapter, _ = cli.replay(str(bundle), echo=lambda s: None)
    assert [r.status for r in replayed.results] == [r.status for r in runner.results]
    assert adapter.unmatched == []
    assert cli.cmd_replay(type("A", (), {"bundle": str(bundle)})()) == 0


def test_fake_modules_exist_for_every_vendor():
    for vendor in VENDORS:
        assert fake_module(vendor) is not None


def test_replay_skips_requests_it_does_not_make():
    """The live run read the LUN more often (host checks) than a replay
    does; the replay still sees the state of its own point in the trace."""
    import requests

    def e(method, url, body):
        return {"method": method, "url": url, "status": 200, "response_body": body}
    trace = [e("GET", "/lun/1", {"size": 1}), e("GET", "/lun/1", {"size": 1}),   # 2nd: a host check
             e("PATCH", "/lun/1", {}), e("GET", "/lun/1", {"size": 2}),
             e("GET", "/lun/1", {"size": 2})]                                      # a poll
    s = requests.Session()
    s.mount("https://", ReplayAdapter(trace))
    sizes = [s.get("https://a/lun/1").json()["size"]]
    s.patch("https://a/lun/1")
    sizes += [s.get("https://a/lun/1").json()["size"] for _ in range(3)]
    assert sizes == [1, 2, 2, 2]
