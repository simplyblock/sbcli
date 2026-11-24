"""Recording and replaying an array's REST traffic.

:class:`RecordingAdapter` is a ``requests`` transport that sends every request
for real and keeps a copy of the request and the response. The trace is
written out with secrets removed (:class:`Redactor`): credentials, tokens,
cookies and every value of a field or header that looks like one, wherever
the value turns up later (a session token reused in a URL, say).

:class:`ReplayAdapter` serves a trace back to the plug-in without an array:
the responses to the same requests come back in the recorded order. A
customer's bundle so reproduces, offline, what the plug-in saw at the
customer.
"""
from __future__ import annotations

import json
import re
import threading
import time
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit

import requests
from requests.adapters import BaseAdapter, HTTPAdapter
from requests.structures import CaseInsensitiveDict

REDACTED = "<redacted>"
MAX_BODY = 1 << 20

#: Field and header names whose values are secrets.
_SECRET_NAME = re.compile(
    r"passw|secret|token|cookie|credential|api[-_]?key|private[-_]?key|authoriz|"
    r"session[-_]?(id|key|token)|^sessionid$|csrf|xsrf|^key$|^auth$|ibasetoken|^jsessionid$",
    re.IGNORECASE)

#: Headers kept as they are; every other header's value is redacted.
_SAFE_HEADERS = frozenset({
    "accept", "accept-encoding", "connection", "content-encoding", "content-length",
    "content-type", "date", "location", "retry-after", "server", "transfer-encoding",
    "user-agent", "vary", "x-request-id", "x-correlation-id",
})


class Redactor:
    """Removes secrets from trace entries. Values it redacts by name are
    learned and removed wherever else they appear."""

    def __init__(self, secrets: Any = ()):
        self.secrets: set[str] = set()
        for s in secrets:
            self.learn(s.get_secret_value() if hasattr(s, "get_secret_value") else s)

    def learn(self, value: Any) -> None:
        if isinstance(value, str) and len(value) >= 4:
            self.secrets.add(value)
            # A basic-auth or bearer header carries the secret after a scheme.
            if " " in value:
                self.secrets.add(value.split(" ", 1)[1])

    def headers(self, headers: Any) -> dict[str, str]:
        out = {}
        for name, value in (headers or {}).items():
            if name.lower() in _SAFE_HEADERS:
                out[name] = str(value)
            else:
                self.learn(str(value))
                # Cookies: the name of each cookie stays readable.
                if name.lower() in ("cookie", "set-cookie"):
                    for part in re.split(r"[;,]\s*", str(value)):
                        if "=" in part:
                            self.learn(part.split("=", 1)[1])
                out[name] = REDACTED
        return out

    def body(self, obj: Any) -> Any:
        if isinstance(obj, dict):
            out = {}
            for k, v in obj.items():
                if _SECRET_NAME.search(str(k)) and isinstance(v, (str, int, float)) and v != "":
                    self.learn(str(v))
                    out[k] = REDACTED
                else:
                    out[k] = self.body(v)
            return out
        if isinstance(obj, list):
            return [self.body(v) for v in obj]
        return obj

    def url(self, url: str) -> str:
        parts = urlsplit(url)
        query = []
        for k, v in parse_qsl(parts.query, keep_blank_values=True):
            if _SECRET_NAME.search(k):
                self.learn(v)
                v = REDACTED
            query.append((k, v))
        path = parts.path + (("?" + urlencode(query)) if query else "")
        return path

    def scrub(self, obj: Any) -> Any:
        """Every learned secret, wherever it is in ``obj``."""
        if isinstance(obj, str):
            for s in sorted(self.secrets, key=len, reverse=True):
                if s in obj:
                    obj = obj.replace(s, REDACTED)
            return obj
        if isinstance(obj, dict):
            return {self.scrub(k): self.scrub(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [self.scrub(v) for v in obj]
        return obj


def _decode(raw: bytes | str | None) -> Any:
    if raw is None or raw == b"" or raw == "":
        return None
    if isinstance(raw, bytes):
        if len(raw) > MAX_BODY:
            return f"<{len(raw)} bytes, not kept>"
        try:
            raw = raw.decode("utf-8")
        except UnicodeDecodeError:
            return f"<{len(raw)} binary bytes>"
    try:
        return json.loads(raw)
    except ValueError:
        return raw


class RecordingAdapter(HTTPAdapter):
    """Sends for real and records each exchange (redacted on the way in)."""

    def __init__(self, redactor: Redactor, inner: BaseAdapter | None = None, **kw: Any):
        super().__init__(**kw)
        self.redactor = redactor
        #: Sends instead of the network when set (a vendor fake, in tests).
        self.inner = inner
        self.entries: list[dict[str, Any]] = []
        self._t0 = time.monotonic()
        self._lock = threading.Lock()

    def send(self, request, **kw):  # type: ignore[override]
        r = self.redactor
        entry: dict[str, Any] = {
            "t": round(time.monotonic() - self._t0, 3),
            "method": request.method,
            "url": r.url(request.url),
            "request_headers": r.headers(request.headers),
            "request_body": r.body(_decode(request.body)),
        }
        start = time.monotonic()
        try:
            resp = self.inner.send(request, **kw) if self.inner is not None else super().send(request, **kw)
        except Exception as e:
            entry.update(error=f"{type(e).__name__}: {e}", elapsed_ms=round((time.monotonic() - start) * 1000))
            self._add(entry)
            raise
        entry.update(
            status=resp.status_code,
            elapsed_ms=round((time.monotonic() - start) * 1000),
            response_headers=r.headers(resp.headers),
            response_body=r.body(_decode(resp.content)),
        )
        self._add(entry)
        return resp

    def _add(self, entry: dict[str, Any]) -> None:
        with self._lock:
            entry["seq"] = len(self.entries)
            self.entries.append(entry)

    def export(self) -> list[dict[str, Any]]:
        """The entries with every learned secret removed (call at the end:
        secrets learned late are removed from earlier entries too)."""
        with self._lock:
            return [self.redactor.scrub(e) for e in self.entries]


def _key(method: str, url: str) -> tuple[str, str]:
    parts = urlsplit(url)
    query = sorted(parse_qsl(parts.query, keep_blank_values=True))
    return method.upper(), parts.path + ("?" + urlencode(query) if query else "")


class ReplayAdapter(BaseAdapter):
    """Answers requests from a trace, following its timeline: a request gets
    the next recorded exchange with the same method and URL after the one
    answered last, so requests the replay does not make (those of the host
    checks) are skipped and the array's state stays in step. Without one
    ahead, the latest one before it answers (a poll repeated more often). A
    request the trace does not have fails as a connection error, naming it."""

    def __init__(self, entries: list[dict[str, Any]]):
        super().__init__()
        self.entries = entries
        self.keys = [_key(e["method"], e["url"]) for e in entries]
        self.cursor = 0
        self.unmatched: list[str] = []

    def _find(self, match) -> dict | None:
        for i in range(self.cursor, len(self.entries)):
            if match(self.keys[i]):
                self.cursor = i + 1
                return self.entries[i]
        for i in range(self.cursor - 1, -1, -1):
            if match(self.keys[i]):
                return self.entries[i]
        return None

    def send(self, request, **kw):  # type: ignore[override]
        k = _key(request.method, request.url)
        path = k[1].split("?", 1)[0]
        e = self._find(lambda key: key == k) or \
            self._find(lambda key: key[0] == k[0] and key[1].split("?", 1)[0] == path)
        if e is None:
            self.unmatched.append(f"{k[0]} {k[1]}")
            raise requests.ConnectionError(f"not in the trace: {k[0]} {k[1]}")
        if "error" in e:
            raise requests.ConnectionError(f"recorded: {e['error']}")
        resp = requests.Response()
        resp.status_code = int(e["status"])
        resp.headers = CaseInsensitiveDict(e.get("response_headers") or {})
        body = e.get("response_body")
        resp._content = (json.dumps(body) if isinstance(body, (dict, list)) else (body or "")).encode()
        resp.url = request.url
        resp.request = request
        resp.encoding = "utf-8"
        return resp

    def close(self) -> None:
        pass
