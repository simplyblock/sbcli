"""
test_auth.py – unit tests for simplyblock_web.api.v2._auth.
"""

import base64
import json
from unittest.mock import MagicMock, patch
from uuid import UUID, uuid4

import pytest
from fastapi import HTTPException
from fastapi.security import HTTPAuthorizationCredentials
from pydantic import SecretStr


def _make_credentials(token: str) -> HTTPAuthorizationCredentials:
    return HTTPAuthorizationCredentials(scheme="Bearer", credentials=token)


def _make_review(authenticated: bool, username: str = ""):
    review = MagicMock()
    review.status.authenticated = authenticated
    review.status.user.username = username
    return review


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _make_k8s_jwt(sub: str = "system:serviceaccount:default:op") -> str:
    """Produce a syntactically valid JWT whose `sub` looks like a k8s SA."""
    header = _b64url(json.dumps({"alg": "RS256", "typ": "JWT"}).encode())
    payload = _b64url(json.dumps({"sub": sub}).encode())
    signature = _b64url(b"signature")
    return f"{header}.{payload}.{signature}"


# ---------------------------------------------------------------------------
# authenticated_service_account
# ---------------------------------------------------------------------------

class TestAuthenticatedServiceAccount:

    def setup_method(self):
        import simplyblock_web.api.v2._auth as auth
        auth._k8s_auth_api = None

    def test_returns_none_for_non_jwt_token(self):
        import simplyblock_web.api.v2._auth as auth
        assert auth.authenticated_service_account(_make_credentials("not-a-jwt")) is None

    def test_returns_none_when_k8s_api_unavailable(self):
        import simplyblock_web.api.v2._auth as auth

        with patch.object(auth, "_get_k8s_auth_api", return_value=None):
            assert auth.authenticated_service_account(_make_credentials(_make_k8s_jwt())) is None

    def test_returns_username_from_token_review(self):
        import simplyblock_web.api.v2._auth as auth

        username = "system:serviceaccount:default:stranger"
        mock_api = MagicMock()
        mock_api.create_token_review.return_value = _make_review(authenticated=True, username=username)

        with patch.object(auth, "_get_k8s_auth_api", return_value=mock_api):
            assert auth.authenticated_service_account(_make_credentials(_make_k8s_jwt(username))) == username

    def test_returns_none_when_token_not_authenticated(self):
        import simplyblock_web.api.v2._auth as auth

        mock_api = MagicMock()
        mock_api.create_token_review.return_value = _make_review(authenticated=False, username="anyone")

        with patch.object(auth, "_get_k8s_auth_api", return_value=mock_api):
            assert auth.authenticated_service_account(_make_credentials(_make_k8s_jwt())) is None

    def test_returns_none_on_token_review_exception(self):
        import simplyblock_web.api.v2._auth as auth

        mock_api = MagicMock()
        mock_api.create_token_review.side_effect = RuntimeError("network error")

        with patch.object(auth, "_get_k8s_auth_api", return_value=mock_api):
            assert auth.authenticated_service_account(_make_credentials(_make_k8s_jwt())) is None


# ---------------------------------------------------------------------------
# authorized_cluster
# ---------------------------------------------------------------------------

class TestAuthorizedCluster:

    _CLUSTER_UUID = "11111111-1111-1111-1111-111111111111"

    def _cluster(self, id: str, secret: str) -> MagicMock:
        c = MagicMock()
        c.id = id
        c.secret = SecretStr(secret)
        return c

    def test_returns_uuid_for_matching_secret(self):
        import simplyblock_web.api.v2._auth as auth

        mock_db = MagicMock()
        mock_db.get_clusters.return_value = [self._cluster(self._CLUSTER_UUID, "s3cr3t")]

        with patch.object(auth, "_db", mock_db):
            assert auth.authorized_cluster(_make_credentials("s3cr3t")) == UUID(self._CLUSTER_UUID)

    def test_returns_none_for_unknown_secret(self):
        import simplyblock_web.api.v2._auth as auth

        mock_db = MagicMock()
        mock_db.get_clusters.return_value = [self._cluster(self._CLUSTER_UUID, "correct")]

        with patch.object(auth, "_db", mock_db):
            assert auth.authorized_cluster(_make_credentials("wrong")) is None

    def test_returns_none_when_no_clusters(self):
        import simplyblock_web.api.v2._auth as auth

        mock_db = MagicMock()
        mock_db.get_clusters.return_value = []

        with patch.object(auth, "_db", mock_db):
            assert auth.authorized_cluster(_make_credentials("any-token")) is None

    def test_returns_none_when_cluster_id_is_not_a_uuid(self):
        import simplyblock_web.api.v2._auth as auth

        mock_db = MagicMock()
        mock_db.get_clusters.return_value = [self._cluster("not-a-uuid", "s3cr3t")]

        with patch.object(auth, "_db", mock_db):
            assert auth.authorized_cluster(_make_credentials("s3cr3t")) is None

    def test_returns_none_without_db_lookup_when_cluster_secret_auth_disabled(self):
        import simplyblock_web.api.v2._auth as auth

        mock_db = MagicMock()
        mock_db.get_clusters.return_value = [self._cluster(self._CLUSTER_UUID, "s3cr3t")]
        mock_settings = MagicMock()
        mock_settings.enable_cluster_secret_auth = False

        with patch.object(auth, "_db", mock_db), patch.object(auth, "_web_settings", mock_settings):
            result = auth.authorized_cluster(_make_credentials("s3cr3t"))

        assert result is None
        mock_db.get_clusters.assert_not_called()


# ---------------------------------------------------------------------------
# verify_api_token (orchestration)
# ---------------------------------------------------------------------------

class TestVerifyApiToken:

    _ADMIN_SA = "system:serviceaccount:default:op"

    def _settings(self, admins: list[str]) -> MagicMock:
        s = MagicMock()
        s.k8s_admin_service_accounts = admins
        return s

    def test_passes_for_admin_sa_without_cluster_id(self):
        import simplyblock_web.api.v2._auth as auth

        with patch.object(auth, "_web_settings", self._settings([self._ADMIN_SA])):
            auth.verify_api_token(sa_name=self._ADMIN_SA, authorized_cluster_id=None, cluster_id=None)

    def test_passes_for_admin_sa_regardless_of_cluster_id(self):
        import simplyblock_web.api.v2._auth as auth

        with patch.object(auth, "_web_settings", self._settings([self._ADMIN_SA])):
            auth.verify_api_token(
                sa_name=self._ADMIN_SA,
                authorized_cluster_id=None,
                cluster_id=uuid4(),
            )

    def test_raises_401_for_non_admin_sa_without_cluster_auth(self):
        import simplyblock_web.api.v2._auth as auth

        with patch.object(auth, "_web_settings", self._settings([self._ADMIN_SA])):
            with pytest.raises(HTTPException) as exc_info:
                auth.verify_api_token(
                    sa_name="system:serviceaccount:default:stranger",
                    authorized_cluster_id=None,
                    cluster_id=None,
                )
        assert exc_info.value.status_code == 401

    def test_passes_for_cluster_auth_without_path_cluster_id(self):
        import simplyblock_web.api.v2._auth as auth

        with patch.object(auth, "_web_settings", self._settings([self._ADMIN_SA])):
            auth.verify_api_token(sa_name=None, authorized_cluster_id=uuid4(), cluster_id=None)

    def test_passes_when_authorized_cluster_matches_path(self):
        import simplyblock_web.api.v2._auth as auth

        cluster = uuid4()
        with patch.object(auth, "_web_settings", self._settings([self._ADMIN_SA])):
            auth.verify_api_token(sa_name=None, authorized_cluster_id=cluster, cluster_id=cluster)

    def test_raises_401_when_authorized_cluster_does_not_match_path(self):
        import simplyblock_web.api.v2._auth as auth

        with patch.object(auth, "_web_settings", self._settings([self._ADMIN_SA])):
            with pytest.raises(HTTPException) as exc_info:
                auth.verify_api_token(
                    sa_name=None,
                    authorized_cluster_id=uuid4(),
                    cluster_id=uuid4(),
                )
        assert exc_info.value.status_code == 401

    def test_raises_401_when_neither_check_succeeds(self):
        import simplyblock_web.api.v2._auth as auth

        with patch.object(auth, "_web_settings", self._settings([self._ADMIN_SA])):
            with pytest.raises(HTTPException) as exc_info:
                auth.verify_api_token(sa_name=None, authorized_cluster_id=None, cluster_id=None)
        assert exc_info.value.status_code == 401


# ---------------------------------------------------------------------------
# verify_metrics_token
# ---------------------------------------------------------------------------

class TestVerifyMetricsToken:
    """The metrics gate is wider than the general one, and only wider there.

    Every test that admits the metrics service account is paired with the same
    principal being refused by `verify_api_token`, since the split is only
    worth anything if the scrape credential stays confined to the exporter.
    """

    _ADMIN_SA = "system:serviceaccount:default:op"
    _METRICS_SA = "system:serviceaccount:monitoring:prometheus"

    def _settings(self, admins: list[str], metrics: list[str]) -> MagicMock:
        s = MagicMock()
        s.k8s_admin_service_accounts = admins
        s.k8s_metrics_service_accounts = metrics
        return s

    def _both_lists(self) -> MagicMock:
        return self._settings([self._ADMIN_SA], [self._METRICS_SA])

    def test_passes_for_metrics_sa(self):
        import simplyblock_web.api.v2._auth as auth

        with patch.object(auth, "_web_settings", self._both_lists()):
            auth.verify_metrics_token(sa_name=self._METRICS_SA, authorized_cluster_id=None)

    def test_metrics_sa_is_refused_by_the_general_gate(self):
        import simplyblock_web.api.v2._auth as auth

        with patch.object(auth, "_web_settings", self._both_lists()):
            with pytest.raises(HTTPException) as exc_info:
                auth.verify_api_token(
                    sa_name=self._METRICS_SA,
                    authorized_cluster_id=None,
                    cluster_id=None,
                )
        assert exc_info.value.status_code == 401

    def test_passes_for_admin_sa(self):
        import simplyblock_web.api.v2._auth as auth

        with patch.object(auth, "_web_settings", self._both_lists()):
            auth.verify_metrics_token(sa_name=self._ADMIN_SA, authorized_cluster_id=None)

    def test_passes_for_cluster_auth(self):
        import simplyblock_web.api.v2._auth as auth

        with patch.object(auth, "_web_settings", self._both_lists()):
            auth.verify_metrics_token(sa_name=None, authorized_cluster_id=uuid4())

    def test_raises_401_for_unlisted_sa(self):
        import simplyblock_web.api.v2._auth as auth

        with patch.object(auth, "_web_settings", self._both_lists()):
            with pytest.raises(HTTPException) as exc_info:
                auth.verify_metrics_token(
                    sa_name="system:serviceaccount:default:stranger",
                    authorized_cluster_id=None,
                )
        assert exc_info.value.status_code == 401

    def test_raises_401_when_metrics_list_is_empty(self):
        import simplyblock_web.api.v2._auth as auth

        with patch.object(auth, "_web_settings", self._settings([self._ADMIN_SA], [])):
            with pytest.raises(HTTPException) as exc_info:
                auth.verify_metrics_token(sa_name=self._METRICS_SA, authorized_cluster_id=None)
        assert exc_info.value.status_code == 401
