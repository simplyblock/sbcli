"""Small HTTPS/JSON session shared by all array adapters.

Adapters never build ``requests`` sessions themselves, so TLS verification,
timeouts, retries and error mapping are uniform, and tests can swap the
transport for an in-process fake (see ``tests/unit/storage_arrays/fakes``).
"""
from __future__ import annotations

import logging
import os
import tempfile
import time
from collections.abc import Callable, Iterable
from typing import Any

import requests
from requests.adapters import BaseAdapter

from .base import (
    ArrayAuthError,
    ArrayConfig,
    ArrayConflictError,
    ArrayNotFoundError,
    ArrayRequestError,
)

logger = logging.getLogger(__name__)

_IDEMPOTENT = frozenset({"GET", "HEAD", "OPTIONS"})
_RETRY_STATUSES = frozenset({502, 503, 504})
_MAX_ERROR_BODY = 600


class RestSession:
    """``requests`` wrapper bound to one array endpoint.

    * ``verify``: ``True`` (system CAs), a CA bundle path, or ``False``.
    * Non-2xx responses raise: 401/403 -> :class:`ArrayAuthError`, 404 ->
      :class:`ArrayNotFoundError`, 409 -> :class:`ArrayConflictError`, other ->
      :class:`ArrayRequestError`. Pass ``ok=`` to accept extra statuses.
    * Idempotent requests are retried on connection errors and 502/503/504.
    """

    def __init__(
        self,
        base_url: str,
        *,
        verify: bool | str = True,
        timeout_s: float = 60.0,
        retries: int = 2,
        backoff_s: float = 0.5,
        transport: BaseAdapter | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ):
        self.base_url = base_url.rstrip("/")
        self.timeout_s = timeout_s
        self.retries = retries
        self.backoff_s = backoff_s
        #: Used for retry backoff and by adapters that poll async jobs, so
        #: tests can make all waiting instant.
        self.sleep = sleep
        self.http = requests.Session()
        self.http.verify = verify
        self.http.headers.update({"Accept": "application/json"})
        if transport is not None:
            self.http.mount("https://", transport)
            self.http.mount("http://", transport)
        self._ca_file: str | None = None

    @classmethod
    def for_config(cls, config: ArrayConfig, *, transport: BaseAdapter | None = None,
                   base_url: str | None = None, **kw: Any) -> RestSession:
        verify: bool | str = config.verify_tls
        ca_file = None
        if config.verify_tls and config.ca_bundle:
            fd, ca_file = tempfile.mkstemp(prefix="sb-array-ca-", suffix=".pem")
            with os.fdopen(fd, "w") as f:
                f.write(config.ca_bundle)
            verify = ca_file
        session = cls(base_url or config.endpoint, verify=verify, timeout_s=config.timeout_s,
                      transport=transport, **kw)
        session._ca_file = ca_file
        return session

    # -- auth helpers ---------------------------------------------------------

    @property
    def headers(self) -> Any:
        return self.http.headers

    def set_basic_auth(self, username: str, password: str) -> None:
        self.http.auth = (username, password)

    def clear_auth(self) -> None:
        self.http.auth = None

    # -- requests -------------------------------------------------------------

    def url(self, path: str) -> str:
        if path.startswith(("http://", "https://")):
            return path
        return f"{self.base_url}/{path.lstrip('/')}"

    def request(
        self,
        method: str,
        path: str,
        *,
        params: Any = None,
        json: Any = None,
        data: Any = None,
        headers: dict[str, str] | None = None,
        ok: Iterable[int] = (),
        auth: Any = None,
    ) -> requests.Response:
        method = method.upper()
        attempts = 1 + (self.retries if method in _IDEMPOTENT else 0)
        ok_extra = frozenset(ok)
        last_exc: Exception | None = None
        for attempt in range(attempts):
            try:
                kwargs: dict[str, Any] = dict(params=params, json=json, data=data,
                                              headers=headers, timeout=self.timeout_s)
                if auth is not None:
                    kwargs["auth"] = auth
                resp = self.http.request(method, self.url(path), **kwargs)
            except requests.RequestException as e:
                last_exc = e
                if attempt + 1 < attempts:
                    self.sleep(self.backoff_s * (attempt + 1))
                    continue
                raise ArrayRequestError(
                    f"{method} {path}: {type(e).__name__}: {_safe(str(e))}") from e
            if resp.status_code in _RETRY_STATUSES and attempt + 1 < attempts:
                self.sleep(self.backoff_s * (attempt + 1))
                continue
            if 200 <= resp.status_code < 300 or resp.status_code in ok_extra:
                return resp
            raise error_for(method, path, resp)
        raise ArrayRequestError(f"{method} {path}: giving up") from last_exc  # pragma: no cover

    def get(self, path: str, **kw: Any) -> requests.Response:
        return self.request("GET", path, **kw)

    def post(self, path: str, **kw: Any) -> requests.Response:
        return self.request("POST", path, **kw)

    def put(self, path: str, **kw: Any) -> requests.Response:
        return self.request("PUT", path, **kw)

    def patch(self, path: str, **kw: Any) -> requests.Response:
        return self.request("PATCH", path, **kw)

    def delete(self, path: str, **kw: Any) -> requests.Response:
        return self.request("DELETE", path, **kw)

    def close(self) -> None:
        try:
            self.http.close()
        finally:
            if self._ca_file:
                try:
                    os.unlink(self._ca_file)
                except OSError:
                    pass
                self._ca_file = None


def _safe(text: str) -> str:
    return text[:_MAX_ERROR_BODY]


def body_text(resp: requests.Response) -> str:
    try:
        return _safe(resp.text or "")
    except Exception:  # pragma: no cover - decoding issues
        return ""


def error_for(method: str, path: str, resp: requests.Response) -> Exception:
    msg = f"{method} {path} -> HTTP {resp.status_code}: {body_text(resp)}"
    if resp.status_code in (401, 403):
        return ArrayAuthError(msg)
    if resp.status_code == 404:
        return ArrayNotFoundError(msg)
    if resp.status_code == 409:
        return ArrayConflictError(msg)
    return ArrayRequestError(msg, status=resp.status_code)


def json_of(resp: requests.Response) -> Any:
    """Response body as JSON (``None`` for an empty body)."""
    if not resp.content:
        return None
    try:
        return resp.json()
    except ValueError as e:
        raise ArrayRequestError(
            f"Expected JSON from {resp.request.method} {resp.url}, got: {body_text(resp)}",
            status=resp.status_code) from e
