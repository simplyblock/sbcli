"""Stateful fakes of each vendor's management REST API.

Each ``fakes/<vendor>.py`` module provides

* a :class:`FakeArray` subclass that keeps volumes/hosts/mappings in memory
  and answers the subset of the vendor API its adapter uses, with the
  vendor's status codes, error bodies and quirks (async jobs, session
  tokens, size units); and
* ``contract_setup()`` returning a :class:`ContractSetup` for the shared
  contract suite.

Fakes plug into ``requests`` in-process through :class:`FakeTransport` (no
sockets), or run as a real HTTPS server with :func:`serve` / ``python -m
tests.unit.storage_arrays.fakes`` for end-to-end and UI testing.

A fake encodes our reading of the vendor API. Passing the contract suite
proves the adapter logic; it does not prove API fidelity -- that needs the
same suite against a vendor simulator or a real array (``live`` tier).
"""
from __future__ import annotations

import base64
import http.client
import io
import json
import logging
import re
import threading
import types
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import parse_qs, urlsplit

import requests
from requests.adapters import BaseAdapter
from requests.structures import CaseInsensitiveDict

from simplyblock_core.credentials import Credential
from simplyblock_core.storage_arrays.base import ArrayConfig, Initiator, Protocol

Handler = Callable[..., "Reply"]


@dataclass
class Request:
    method: str
    path: str
    query: dict[str, list[str]]
    headers: CaseInsensitiveDict
    body: bytes

    def json(self) -> Any:
        return json.loads(self.body) if self.body else None

    def q(self, name: str, default: str | None = None) -> str | None:
        values = self.query.get(name)
        return values[0] if values else default

    def basic_auth(self) -> tuple[str, str] | None:
        header = self.headers.get("Authorization", "")
        if not header.lower().startswith("basic "):
            return None
        user, _, pw = base64.b64decode(header[6:]).decode().partition(":")
        return user, pw


@dataclass
class Reply:
    status: int = 200
    body: Any = None
    headers: dict[str, str] = field(default_factory=dict)


def route(method: str, pattern: str):
    """Decorate a FakeArray method as the handler of ``method`` + path regex.
    Named groups are passed as keyword arguments."""
    def deco(fn):
        fn._route = (method.upper(), re.compile(f"^{pattern}$"))
        return fn
    return deco


class FakeArray:
    """Base class: routing, fault injection, request log, thread safety."""

    vendor: str = ""

    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.requests: list[tuple[str, str]] = []
        self._faults: list[tuple[re.Pattern[str] | None, Reply, int]] = []
        self._routes: list[tuple[str, re.Pattern[str], Handler]] = []
        for name in dir(type(self)):
            fn = getattr(type(self), name)
            r = getattr(fn, "_route", None)
            if r:
                self._routes.append((r[0], r[1], getattr(self, name)))
        self._id = 0

    # -- helpers for subclasses ----------------------------------------------

    def next_id(self) -> int:
        self._id += 1
        return self._id

    # -- fault injection --------------------------------------------------------

    def fail_next(self, status: int, body: Any = None, *, path: str | None = None,
                  times: int = 1) -> None:
        """Answer the next ``times`` requests (optionally only those whose path
        matches ``path`` regex) with ``status`` and ``body``."""
        with self.lock:
            self._faults.append((re.compile(path) if path else None, Reply(status, body), times))

    def expire_sessions(self) -> None:
        """Invalidate every session token, as an array restart or timeout would.
        Basic-auth fakes have nothing to expire."""

    def set_credential(self, **fields: str) -> None:
        """Change the credential the fake accepts (simulates rotation)."""
        raise NotImplementedError

    # -- dispatch ---------------------------------------------------------------

    def handle(self, req: Request) -> Reply:
        with self.lock:
            self.requests.append((req.method, req.path))
            for i, (pattern, reply, times) in enumerate(self._faults):
                if pattern is None or pattern.search(req.path):
                    if times <= 1:
                        del self._faults[i]
                    else:
                        self._faults[i] = (pattern, reply, times - 1)
                    return reply
            for method, pattern, handler in self._routes:
                if method != req.method:
                    continue
                m = pattern.match(req.path)
                if m:
                    try:
                        return handler(req, **m.groupdict())
                    except FakeError as e:
                        return e.reply
            return self.not_routed(req)

    def not_routed(self, req: Request) -> Reply:
        return Reply(404, {"error": f"fake: no route for {req.method} {req.path}"})


# The LIO data path (fakes.lio) reads a fake through two accessors, which
# every vendor fake implements:
#
# * ``lio_volume_ids()``: the ids of the live volumes, as the vendor's adapter
#   reports them in ``Volume.id``;
# * ``lio_host_iqns(host_id)``: the iSCSI initiator names of the host the
#   adapter knows as ``host_id`` (the id ``ensure_host`` returned or the name
#   ``list_mappings`` reports), lower case;
# * ``lio_all_iqns()``: the iSCSI initiator names of every host, so that a
#   registered host can log in before anything is mapped to it.

class FakeError(Exception):
    """Raise inside a handler to answer with ``reply``."""

    def __init__(self, status: int, body: Any = None, headers: dict[str, str] | None = None):
        super().__init__(status)
        self.reply = Reply(status, body, headers or {})


class FakeTransport(BaseAdapter):
    """A ``requests`` transport adapter answering from a :class:`FakeArray`."""

    def __init__(self, fake: FakeArray):
        super().__init__()
        self.fake = fake

    def send(self, request, stream=False, timeout=None, verify=True, cert=None, proxies=None):
        parts = urlsplit(request.url)
        body = request.body or b""
        if isinstance(body, str):
            body = body.encode()
        req = Request(
            method=request.method.upper(),
            path=parts.path,
            query=parse_qs(parts.query, keep_blank_values=True),
            headers=CaseInsensitiveDict(request.headers),
            body=body,
        )
        reply = self.fake.handle(req)
        return _to_response(request, reply)

    def close(self):
        pass


def _encode(reply: Reply) -> tuple[bytes, dict[str, str]]:
    headers = dict(reply.headers)
    if reply.body is None:
        return b"", headers
    if isinstance(reply.body, bytes):
        return reply.body, headers
    if isinstance(reply.body, str):
        headers.setdefault("Content-Type", "text/plain")
        return reply.body.encode(), headers
    headers.setdefault("Content-Type", "application/json")
    return json.dumps(reply.body).encode(), headers


class _Raw(io.BytesIO):
    """Body stream that also carries the headers the way urllib3 does, so
    ``requests`` stores Set-Cookie values in the session's cookie jar exactly
    as it would for a real response."""

    def __init__(self, content: bytes, headers: dict[str, str]):
        super().__init__(content)
        msg = http.client.HTTPMessage()
        for k, v in headers.items():
            msg[k] = v
        self._original_response = types.SimpleNamespace(msg=msg)


def _to_response(request, reply: Reply) -> requests.Response:
    content, headers = _encode(reply)
    resp = requests.Response()
    resp.status_code = reply.status
    resp.headers = CaseInsensitiveDict(headers)
    resp.raw = _Raw(content, headers)
    resp._content = content
    resp.url = request.url
    resp.request = request
    resp.encoding = "utf-8"
    resp.reason = ""
    return resp


@dataclass
class ContractSetup:
    """What the shared contract suite needs from a vendor fake."""

    fake: FakeArray
    config: ArrayConfig
    credential: Credential
    #: Credential the fake rejects.
    bad_credential: Credential
    #: Two distinct initiator sets per supported protocol, for two hosts.
    initiators: dict[Protocol, tuple[tuple[Initiator, ...], tuple[Initiator, ...]]]
    #: Create a volume directly on the fake (bypassing the client), for the
    #: "not simplyblock-owned" checks. Returns the array's volume id.
    create_foreign_volume: Callable[[str, int], str]
    #: Whether mapping yields a LUN/NSID number.
    has_lun_numbers: bool = True


def make_config(vendor: str, endpoint: str, **options: Any) -> ArrayConfig:
    return ArrayConfig(
        id=f"fake-{vendor}",
        vendor=vendor,
        endpoint=endpoint,
        credential_ref={"kind": "vault-kv", "mount": "m", "path": f"arrays/{vendor}"},
        options=options,
    )


# -- standalone HTTPS mock server ------------------------------------------------


def serve(fake: FakeArray, host: str = "127.0.0.1", port: int = 8443,
          certfile: str | None = None, keyfile: str | None = None,
          after_request: Callable[[], None] | None = None):
    """Serve ``fake`` over HTTPS (a throwaway self-signed certificate is
    generated unless ``certfile``/``keyfile`` are given). Returns the server;
    call ``serve_forever()`` on it. ``after_request`` runs after every
    request, before the answer is sent: the LIO data path (``fakes.lio``)
    catches up there, so a mapping the caller was told about is already
    presented. GETs included: on arrays with asynchronous jobs, a change
    takes effect while its job is polled."""
    import ssl
    import tempfile
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def _do(self):
            parts = urlsplit(self.path)
            length = int(self.headers.get("Content-Length") or 0)
            body = self.rfile.read(length) if length else b""
            reply = fake.handle(Request(
                method=self.command, path=parts.path,
                query=parse_qs(parts.query, keep_blank_values=True),
                headers=CaseInsensitiveDict(dict(self.headers.items())), body=body))
            if after_request is not None:
                try:
                    after_request()
                except Exception:  # noqa: BLE001 - the API answer still goes out
                    logging.getLogger(__name__).exception("after_request failed")
            content, headers = _encode(reply)
            self.send_response(reply.status)
            for k, v in headers.items():
                self.send_header(k, v)
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            self.wfile.write(content)

        do_GET = do_POST = do_PUT = do_PATCH = do_DELETE = _do

        def log_message(self, fmt, *args):  # quieter default logging
            pass

    server = ThreadingHTTPServer((host, port), Handler)
    if certfile is None:
        tmp = tempfile.mkdtemp(prefix="sb-array-mock-")
        certfile, keyfile = f"{tmp}/cert.pem", f"{tmp}/key.pem"
        _self_signed(host, certfile, keyfile)
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    ctx.load_cert_chain(certfile, keyfile)
    server.socket = ctx.wrap_socket(server.socket, server_side=True)
    server.certfile = certfile  # type: ignore[attr-defined]
    return server


def _self_signed(host: str, certfile: str, keyfile: str) -> None:
    import datetime
    import ipaddress

    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import NameOID

    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, host)])
    try:
        san: x509.GeneralName = x509.IPAddress(ipaddress.ip_address(host))
    except ValueError:
        san = x509.DNSName(host)
    now = datetime.datetime.now(datetime.UTC)
    cert = (x509.CertificateBuilder().subject_name(name).issuer_name(name)
            .public_key(key.public_key()).serial_number(x509.random_serial_number())
            .not_valid_before(now - datetime.timedelta(minutes=5))
            .not_valid_after(now + datetime.timedelta(days=30))
            .add_extension(x509.SubjectAlternativeName([san]), critical=False)
            .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
            .sign(key, hashes.SHA256()))
    with open(certfile, "wb") as f:
        f.write(cert.public_bytes(serialization.Encoding.PEM))
    with open(keyfile, "wb") as f:
        f.write(key.private_bytes(serialization.Encoding.PEM,
                                  serialization.PrivateFormat.PKCS8,
                                  serialization.NoEncryption()))
