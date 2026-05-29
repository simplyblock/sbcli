import base64
import hmac
import json
import logging
from typing import Annotated
from json.decoder import JSONDecodeError

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


def check_k8s_token(
    credentials: Annotated[HTTPAuthorizationCredentials, Depends(security)],
) -> bool:
    """FastAPI dependency: authenticate via Kubernetes service account JWT.

    Returns `True` only when:
    - the token has JWT structure,
    - the k8s API is reachable, and
    - the token belongs to a configured admin service account.

    Returns `False` in all other cases without raising.
    """
    token = credentials.credentials
    if not _is_kubernetes_jwt(token):
        return False

    if not _web_settings.k8s_admin_service_accounts:
        return False

    auth_api = _get_k8s_auth_api()
    if auth_api is None:
        return False

    try:
        review = auth_api.create_token_review(
            body=kubernetes.client.V1TokenReview(
                spec=kubernetes.client.V1TokenReviewSpec(token=token)
            )
        )
        return (
            review.status.authenticated
            and review.status.user.username in _web_settings.k8s_admin_service_accounts
        )
    except Exception:
        _logger.warning("k8s TokenReview failed", exc_info=True)
        return False


def check_cluster_secret(
    credentials: Annotated[HTTPAuthorizationCredentials, Depends(security)],
    cluster_id: str | None = None,
) -> bool:
    """FastAPI dependency: authenticate via cluster secret.

    Returns `True` when the bearer token matches a cluster secret and, if
    *cluster_id* is present in the path, belongs to that specific cluster.

    Returns `False` in all other cases without raising.
    """
    token = credentials.credentials
    authorized_cluster_id = next((
        cluster.id
        for cluster in _db.get_clusters()
        if hmac.compare_digest(cluster.secret, token)
    ), None)

    return (authorized_cluster_id is not None) and (cluster_id is None or cluster_id == authorized_cluster_id)


def verify_api_token(
    k8s_ok: Annotated[bool, Depends(check_k8s_token)],
    secret_ok: Annotated[bool, Depends(check_cluster_secret)],
) -> None:
    """FastAPI dependency: succeed if any auth check passes, raise 401 otherwise."""
    if not (k8s_ok or secret_ok):
        raise HTTPException(401, 'Invalid token')
