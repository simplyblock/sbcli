import base64
import hmac
import json
import logging
from typing import Annotated
from json.decoder import JSONDecodeError
from uuid import UUID

import kubernetes.client
import kubernetes.config
from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from simplyblock_core.db_controller import DBController
from simplyblock_web.settings import Settings as WebSettings

_db = DBController()
security = HTTPBearer()
_web_settings = WebSettings()
_k8s_auth_api: kubernetes.client.AuthenticationV1Api | None = None
_logger = logging.getLogger(__name__)


def _get_k8s_auth_api() -> kubernetes.client.AuthenticationV1Api | None:
    global _k8s_auth_api
    if _k8s_auth_api is not None:
        return _k8s_auth_api
    try:
        kubernetes.config.load_incluster_config()
    except kubernetes.config.ConfigException:
        try:
            kubernetes.config.load_kube_config()
        except Exception:
            _logger.warning("Could not load Kubernetes config; k8s service account auth disabled")
            return None
    _k8s_auth_api = kubernetes.client.AuthenticationV1Api()
    return _k8s_auth_api


def _urlsafe_b64decode_unpadded(encoded: str) -> bytes:
    """Urlsafe base 64 decode with implicit padding
    """
    padding = "=" * (-len(encoded) % 4)
    return base64.urlsafe_b64decode(encoded + padding)


def _insecure_decode_jwt(token: str) -> tuple[dict, dict, bytes]:
    """Decodes the given JWT without verifying its authenticty

    Attempts to decode the given JWT. *Not* meant for validation, the signature is not verified.
    If the input is not a valid encoded JWT, the function fails with a ValueError.
    """
    try: 
        header, payload, signature = [_urlsafe_b64decode_unpadded(part) for part in token.split('.')]
        return json.loads(header.decode(encoding="ascii")), json.loads(payload.decode(encoding="ascii")), signature
    except (ValueError, JSONDecodeError) as e:
        raise ValueError("Ill-formatted JWT") from e


def _is_kubernetes_jwt(token: str) -> bool:
    """Heuristic to check whether the given string may be a Kubernetes-issued JWT

    Return `True`, if the given string is a JWT that looks like it has been issued
    by Kubernetes, `False` otherwise.
    """
    try:
        header, payload, signature = _insecure_decode_jwt(token)
        return payload.get("sub", "").startswith("system:serviceaccount:")
    except ValueError:
        return False


def authenticated_service_account(
    credentials: Annotated[HTTPAuthorizationCredentials, Depends(security)],
) -> str | None:
    """FastAPI dependency: identify which k8s service account a bearer token authenticates as.

    Returns the service account username when the token has JWT structure, the
    k8s API is reachable, and the TokenReview confirms it is authenticated.
    Returns `None` otherwise. Authorization (whether the SA is permitted) is
    handled by the caller.
    """
    token = credentials.credentials
    if not _is_kubernetes_jwt(token):
        return None

    auth_api = _get_k8s_auth_api()
    if auth_api is None:
        return None

    try:
        review = auth_api.create_token_review(
            body=kubernetes.client.V1TokenReview(
                spec=kubernetes.client.V1TokenReviewSpec(token=token)
            )
        )
    except Exception:
        _logger.warning("k8s TokenReview failed", exc_info=True)
        return None

    if not review.status.authenticated:
        return None

    return review.status.user.username


def authorized_cluster(
    credentials: Annotated[HTTPAuthorizationCredentials, Depends(security)],
) -> UUID | None:
    """FastAPI dependency: identify which cluster a bearer token authenticates as.

    Returns `None` immediately when cluster-secret authentication is disabled via
    ``SB_ENABLE_CLUSTER_SECRET_AUTH=false``.  Otherwise returns the UUID of the
    cluster whose secret matches the bearer token, or `None` if no cluster
    matches (or the matched ID isn't a valid UUID).
    """
    if not _web_settings.enable_cluster_secret_auth:
        return None

    token = credentials.credentials
    matched_id = next((
        cluster.id
        for cluster in _db.get_clusters()
        if hmac.compare_digest(cluster.secret.get_secret_value(), token)
    ), None)

    if matched_id is None:
        return None
    try:
        return UUID(matched_id)
    except ValueError:
        return None


def verify_api_token(
    sa_name: Annotated[str | None, Depends(authenticated_service_account)],
    authorized_cluster_id: Annotated[UUID | None, Depends(authorized_cluster)],
    cluster_id: UUID | None = None,
) -> None:
    """FastAPI dependency: enforce authentication and per-cluster authorization.

    Succeeds when either:
    - the token authenticates as an admin service account, or
    - the token authenticates as a cluster, and that cluster matches *cluster_id*
      (or no specific cluster is being addressed).

    Raises 401 otherwise.
    """
    if sa_name is not None and sa_name in _web_settings.k8s_admin_service_accounts:
        return

    if authorized_cluster_id is None:
        raise HTTPException(401, 'Invalid token')

    if cluster_id is not None and authorized_cluster_id != cluster_id:
        raise HTTPException(401, 'Invalid token')
