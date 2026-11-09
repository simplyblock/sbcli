"""Clients for a peer control plane's internal API.

``peer_for_target`` is the one way the replication code reaches a target
cluster's control plane: over HTTP when the target names a peer endpoint, and
in-process otherwise. Both implement ``PeerClient``, so a caller never needs to
know which kind of target it has.
"""
import contextlib
import hashlib
import logging
import os
import tempfile
import threading
from typing import Any, Protocol
from urllib.parse import quote

import requests
from pydantic import BaseModel, SecretStr, ValidationError
from requests.adapters import HTTPAdapter
from urllib3 import Retry

from simplyblock_core.exceptions import PreconditionError
from simplyblock_core.models.replication import ReplicationTarget
from simplyblock_core.peer import service
from simplyblock_core.peer.protocol import (
    DescribeRequest,
    DescribeResponse,
    EnsureHubRequest,
    EnsureHubResponse,
    LandingRequest,
    LandingResponse,
    PeerInfo,
)

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


class PeerRefusedError(PeerError):
    """The peer refused the step on its side: a precondition does not hold (HTTP 409)."""


class PeerProtocolError(PeerError):
    """The peer answered with a status or body this control plane does not understand."""


class PeerClient(Protocol):
    """The calls a source control plane makes to the control plane of its target."""

    @property
    def cluster_id(self) -> str:
        """The target cluster every call addresses."""

    def info(self) -> PeerInfo:
        """Identity, peer API version and capabilities of the target cluster.

        Raises:
            PeerError: the target's control plane cannot answer for the cluster.
        """

    def discard_landing(self, lvol_id: str) -> None:
        """Delete the replication landing volume *lvol_id*; one already gone is success.

        Raises:
            PeerNotFoundError: the cluster is unknown, or the volume is on another cluster.
            PeerRefusedError: the volume is not a landing volume, or the delete is refused.
            PeerError: the target's control plane cannot be reached or understood.
        """

    def describe(self, request: DescribeRequest) -> DescribeResponse:
        """Read-only answers about the target side: lvstore leadership of a landing
        volume, whether copies exist or have dependent clones, chain links.

        Raises:
            PeerNotFoundError: the cluster is unknown.
            PeerError: the target's control plane cannot be reached or understood.
        """

    def prepare_landing(self, request: LandingRequest) -> LandingResponse:
        """Make a replicated snapshot's landing volume ready on the target: adopt
        one a previous request made, report a copy of the same data, or create it.

        Raises:
            PeerNotFoundError: the cluster is unknown, or the node hint is not on it.
            PeerRefusedError: no member or pool can take the volume, or the create failed.
            PeerError: the target's control plane cannot be reached or understood.
        """

    def ensure_hub(self, request: EnsureHubRequest) -> EnsureHubResponse:
        """Get the target ready to receive a transfer into the landing volume: its
        settled leader, the receiving hub's attach address and the map id.

        Raises:
            PeerNotFoundError: the cluster or the landing volume is unknown.
            PeerError: the target's control plane cannot be reached or understood.
        """


class LocalPeerClient:
    """A target cluster managed by this same control plane: calls the service directly."""

    def __init__(self, cluster_id: str):
        self._cluster_id = cluster_id

    @property
    def cluster_id(self) -> str:
        return self._cluster_id

    def info(self) -> PeerInfo:
        """Identity and peer API version of the target cluster.

        Raises:
            PeerNotFoundError: this control plane does not manage the cluster.
        """
        try:
            return service.info(self._cluster_id)
        except KeyError as e:
            raise PeerNotFoundError(str(e)) from e

    def discard_landing(self, lvol_id: str) -> None:
        """Delete the replication landing volume *lvol_id*; one already gone is success.

        Raises:
            PeerNotFoundError: the cluster is unknown, or the volume is on another cluster.
            PeerRefusedError: the volume is not a landing volume, or the delete is refused.
        """
        try:
            service.discard_landing(self._cluster_id, lvol_id)
        except KeyError as e:
            raise PeerNotFoundError(str(e)) from e
        except PreconditionError as e:
            raise PeerRefusedError(str(e)) from e

    def describe(self, request: DescribeRequest) -> DescribeResponse:
        """Read-only answers about the target side (see ``PeerClient.describe``).

        Raises:
            PeerNotFoundError: this control plane does not manage the cluster.
        """
        try:
            return service.describe(self._cluster_id, request)
        except KeyError as e:
            raise PeerNotFoundError(str(e)) from e

    def prepare_landing(self, request: LandingRequest) -> LandingResponse:
        """Make the landing volume ready (see ``PeerClient.prepare_landing``).

        Raises:
            PeerNotFoundError: the cluster is unknown, or the node hint is not on it.
            PeerRefusedError: no member or pool can take the volume, or the create failed.
        """
        try:
            return service.prepare_landing(self._cluster_id, request)
        except KeyError as e:
            raise PeerNotFoundError(str(e)) from e
        except PreconditionError as e:
            raise PeerRefusedError(str(e)) from e

    def ensure_hub(self, request: EnsureHubRequest) -> EnsureHubResponse:
        """Get the target ready to receive (see ``PeerClient.ensure_hub``).

        Raises:
            PeerNotFoundError: the cluster or the landing volume is unknown.
        """
        try:
            return service.ensure_hub(self._cluster_id, request)
        except KeyError as e:
            raise PeerNotFoundError(str(e)) from e


# Process-local on purpose: each process writes the bundles it uses into its own
# directory, and nothing depends on two processes (API replicas, services, the
# CLI) agreeing about it. The lock only keeps two threads of one process from
# creating two directories.
_ca_dir: str | None = None
_ca_dir_lock = threading.Lock()


def _ca_bundle_path(ca_cert: str) -> str:
    """A file holding *ca_cert*, for ``requests``' ``verify=``, which takes a path.

    The file decides which certificates the peer connection trusts, so it lives
    in a directory this process created with ``mkdtemp`` (mode 0700): a shared,
    predictable name in the temp directory could be planted by another user
    before this process writes it. Named by the bundle's hash, so clients of
    the same bundle in this process share one file.

    Raises:
        OSError: the directory or the file cannot be created.
    """
    global _ca_dir
    with _ca_dir_lock:
        if _ca_dir is None:
            _ca_dir = tempfile.mkdtemp(prefix="sb-peer-ca-")
        directory = _ca_dir
    digest = hashlib.sha256(ca_cert.encode()).hexdigest()
    path = os.path.join(directory, f"{digest}.pem")
    if not os.path.exists(path):
        # Write under a unique name and rename, so a concurrent reader never
        # sees a half-written bundle.
        fd, tmp = tempfile.mkstemp(dir=directory, suffix=".pem")
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
        # idempotent methods are retried (a DELETE of something already gone
        # succeeds); each other write endpoint carries its own idempotency key,
        # so the caller decides when to send it again.
        retry = Retry(total=retries, connect=retries, read=retries, backoff_factor=0.5,
                      status_forcelist=(502, 503, 504), allowed_methods=frozenset({"GET", "DELETE"}),
                      raise_on_status=False)
        self._session.mount("http://", HTTPAdapter(max_retries=retry))
        self._session.mount("https://", HTTPAdapter(max_retries=retry))

    @property
    def cluster_id(self) -> str:
        return self._cluster_id

    def _request(self, method: str, path: str, body: BaseModel | None = None) -> Any:
        """Send one call to the peer, with *body* as its JSON, and return its JSON body.

        Raises:
            PeerUnreachableError: no answer (connection refused, timeout) or a 5xx
                after the retries.
            PeerAuthError: the peer refused the credential (401/403).
            PeerNotFoundError: the peer does not know the cluster or object (404).
            PeerRefusedError: the peer refused the step (409).
            PeerProtocolError: any other status, or a body that is not JSON.
        """
        url = f"{self._base}/{path}"
        logger.debug("Peer request: %s %s", method, url)
        try:
            response = self._session.request(
                method, url, timeout=self._timeout,
                json=body.model_dump(mode="json") if body is not None else None,
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
        if status == 409:
            raise PeerRefusedError(f"{method} {url}: {self._detail(response)}")
        if status >= 500:
            raise PeerUnreachableError(f"{method} {url}: {status} {self._detail(response)}")
        if not 200 <= status < 300:
            raise PeerProtocolError(f"{method} {url}: {status} {self._detail(response)}")
        if status == 204:
            return None
        try:
            return response.json()
        except ValueError as e:
            raise PeerProtocolError(f"{method} {url}: response is not JSON") from e

    @staticmethod
    def _detail(response: requests.Response) -> str:
        """The ``detail`` of an error body, for the message of the error raised for it."""
        # Only decorates an error already being raised: a body that is not JSON,
        # or not an object, leaves the message without a detail.
        with contextlib.suppress(ValueError, AttributeError):
            return str(response.json().get("detail", ""))
        return ""

    def info(self) -> PeerInfo:
        """Identity and peer API version of the target cluster.

        Raises:
            PeerError: as for every peer call (see ``_request``), and
                PeerProtocolError when the answer is not a PeerInfo.
        """
        try:
            return PeerInfo.model_validate(self._request("GET", "info"))
        except ValidationError as e:
            raise PeerProtocolError(f"Unexpected info response from {self._base}: {e}") from e

    def discard_landing(self, lvol_id: str) -> None:
        """Delete the replication landing volume *lvol_id*; one already gone is success.

        Raises:
            PeerError: as for every peer call (see ``_request``).
        """
        self._request("DELETE", f"landing-volumes/{quote(lvol_id, safe='')}")

    def describe(self, request: DescribeRequest) -> DescribeResponse:
        """Read-only answers about the target side (see ``PeerClient.describe``).

        Raises:
            PeerError: as for every peer call (see ``_request``), and
                PeerProtocolError when the answer is not a DescribeResponse.
        """
        try:
            return DescribeResponse.model_validate(self._request("POST", "describe", request))
        except ValidationError as e:
            raise PeerProtocolError(f"Unexpected describe response from {self._base}: {e}") from e

    def prepare_landing(self, request: LandingRequest) -> LandingResponse:
        """Make the landing volume ready (see ``PeerClient.prepare_landing``).

        Not retried by the transport: a lost answer is resent by the runner's
        next pass with the same request, which adopts what the first one made.

        Raises:
            PeerError: as for every peer call (see ``_request``), and
                PeerProtocolError when the answer is not a LandingResponse.
        """
        try:
            return LandingResponse.model_validate(self._request("POST", "landing-volumes", request))
        except ValidationError as e:
            raise PeerProtocolError(f"Unexpected landing response from {self._base}: {e}") from e

    def ensure_hub(self, request: EnsureHubRequest) -> EnsureHubResponse:
        """Get the target ready to receive (see ``PeerClient.ensure_hub``).

        Raises:
            PeerError: as for every peer call (see ``_request``), and
                PeerProtocolError when the answer is not an EnsureHubResponse.
        """
        path = f"landing-volumes/{quote(request.landing_lvol_id, safe='')}/hub"
        try:
            return EnsureHubResponse.model_validate(self._request("POST", path))
        except ValidationError as e:
            raise PeerProtocolError(f"Unexpected hub response from {self._base}: {e}") from e


def peer_for_target(target: ReplicationTarget) -> PeerClient:
    """The client for *target*'s cluster: over HTTP when it names a peer, else in-process."""
    if target.has_peer():
        return HttpPeerClient(target.peer_endpoint, target.target_cluster_id,
                              target.peer_secret, target.peer_ca_cert)
    return LocalPeerClient(target.target_cluster_id)
