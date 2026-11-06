"""Clients for a peer control plane's internal API.

``peer_for_target`` is the one way the replication code reaches a target
cluster's control plane: over HTTP when the target names a peer endpoint, and
in-process otherwise. Both implement ``PeerClient``, so a caller never needs to
know which kind of target it has.
"""
import hashlib
import logging
import os
import tempfile
from typing import Any, Protocol
from urllib.parse import quote

import requests
from pydantic import SecretStr, ValidationError
from requests.adapters import HTTPAdapter
from urllib3 import Retry

from simplyblock_core.models.replication import ReplicationTarget
from simplyblock_core.peer import service
from simplyblock_core.peer.protocol import PeerInfo

logger = logging.getLogger(__name__)

#: Path of the peer API under a control plane's root URL.
PEER_API_PREFIX = "/api/v2/_peer/clusters"


class PeerError(Exception):
    """A call to a peer control plane failed."""


class PeerUnreachableError(PeerError):
    """The peer did not answer: connection refused, timed out, or kept failing with 5xx."""


class PeerAuthError(PeerError):
    """The peer refused the credential for the addressed cluster (HTTP 401/403)."""


class PeerNotFoundError(PeerError):
    """The peer does not manage the addressed cluster, or the addressed object (HTTP 404)."""


class PeerProtocolError(PeerError):
    """The peer answered with a status or body this control plane does not understand."""


class PeerClient(Protocol):
    """The calls a source control plane makes to the control plane of its target."""

    @property
    def cluster_id(self) -> str:
        """The target cluster every call addresses."""
        ...

    def info(self) -> PeerInfo:
        ...


class LocalPeerClient:
    """A target cluster managed by this same control plane: calls the service directly."""

    def __init__(self, cluster_id: str):
        self._cluster_id = cluster_id

    @property
    def cluster_id(self) -> str:
        return self._cluster_id

    def info(self) -> PeerInfo:
        try:
            return service.info(self._cluster_id)
        except KeyError as e:
            raise PeerNotFoundError(str(e)) from e


def _ca_bundle_path(ca_cert: str) -> str:
    """A file holding *ca_cert*, for ``requests``' ``verify=``, which takes a path.

    Named by the bundle's hash, so every process that needs the same bundle
    writes identical content to the same name. A per-process cache of a public
    value: nothing depends on processes agreeing about it.
    """
    digest = hashlib.sha256(ca_cert.encode()).hexdigest()
    path = os.path.join(tempfile.gettempdir(), f"sb-peer-ca-{digest}.pem")
    if not os.path.exists(path):
        # Write under a unique name and rename, so a concurrent reader never
        # sees a half-written bundle.
        fd, tmp = tempfile.mkstemp(dir=os.path.dirname(path), suffix=".pem")
        with os.fdopen(fd, "w") as f:
            f.write(ca_cert)
        os.replace(tmp, path)
    return path


class HttpPeerClient:
    """A target cluster managed by another control plane, reached over its peer API."""

    def __init__(self, endpoint: str, cluster_id: str, secret: SecretStr,
                 ca_cert: str = "", timeout: float = 30, retries: int = 3):
        if not endpoint:
            raise ValueError("A peer endpoint is required")
        self._cluster_id = cluster_id
        self._base = f"{endpoint.rstrip('/')}{PEER_API_PREFIX}/{quote(cluster_id, safe='')}"
        self._secret = secret
        self._timeout = timeout
        self._session = requests.Session()
        if ca_cert:
            self._session.verify = _ca_bundle_path(ca_cert)
        # Retries cover a peer that is restarting or briefly overloaded. Only
        # idempotent methods are retried; each write endpoint carries its own
        # idempotency key, so the caller decides when to send it again.
        retry = Retry(total=retries, connect=retries, read=retries, backoff_factor=0.5,
                      status_forcelist=(502, 503, 504), allowed_methods=frozenset({"GET"}),
                      raise_on_status=False)
        self._session.mount("http://", HTTPAdapter(max_retries=retry))
        self._session.mount("https://", HTTPAdapter(max_retries=retry))

    @property
    def cluster_id(self) -> str:
        return self._cluster_id

    def _request(self, method: str, path: str) -> Any:
        url = f"{self._base}/{path}"
        logger.debug("Peer request: %s %s", method, url)
        try:
            response = self._session.request(
                method, url, timeout=self._timeout,
                # Unwrapped here, at the send, and nowhere earlier.
                headers={"Authorization": f"Bearer {self._secret.get_secret_value()}"})
        except (requests.ConnectionError, requests.Timeout) as e:
            raise PeerUnreachableError(f"{method} {url}: {e}") from e
        except requests.RequestException as e:
            raise PeerProtocolError(f"{method} {url}: {e}") from e

        logger.debug("Peer response: %s %s -> %s", method, url, response.status_code)
        status = response.status_code
        if status in (401, 403):
            raise PeerAuthError(f"{method} {url}: refused with {status}")
        if status == 404:
            raise PeerNotFoundError(f"{method} {url}: {self._detail(response)}")
        if status >= 500:
            raise PeerUnreachableError(f"{method} {url}: {status} {self._detail(response)}")
        if not 200 <= status < 300:
            raise PeerProtocolError(f"{method} {url}: {status} {self._detail(response)}")
        try:
            return response.json()
        except ValueError as e:
            raise PeerProtocolError(f"{method} {url}: response is not JSON") from e

    @staticmethod
    def _detail(response: requests.Response) -> str:
        try:
            return str(response.json().get("detail", ""))
        except (ValueError, AttributeError):
            return ""

    def info(self) -> PeerInfo:
        try:
            return PeerInfo.model_validate(self._request("GET", "info"))
        except ValidationError as e:
            raise PeerProtocolError(f"Unexpected info response from {self._base}: {e}") from e


def peer_for_target(target: ReplicationTarget) -> PeerClient:
    """The client for *target*'s cluster: over HTTP when it names a peer, else in-process."""
    if target.has_peer():
        return HttpPeerClient(target.peer_endpoint, target.target_cluster_id,
                              target.peer_secret, target.peer_ca_cert)
    return LocalPeerClient(target.target_cluster_id)
