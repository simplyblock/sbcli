# coding=utf-8
"""
test_auth.py – unit tests for simplyblock_web.api.v2._auth.
"""

from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException
from fastapi.security import HTTPAuthorizationCredentials


def _make_credentials(token: str) -> HTTPAuthorizationCredentials:
    return HTTPAuthorizationCredentials(scheme="Bearer", credentials=token)


def _make_review(authenticated: bool, username: str = ""):
    review = MagicMock()
    review.status.authenticated = authenticated
    review.status.user.username = username
    return review


# ---------------------------------------------------------------------------
# _authenticate_k8s_service_account
# ---------------------------------------------------------------------------

class TestAuthenticateK8sServiceAccount:

    def setup_method(self):
        import simplyblock_web.api.v2._auth as auth
        auth._k8s_auth_api = None

    def test_returns_false_when_list_empty(self):
        import simplyblock_web.api.v2._auth as auth

        mock_settings = MagicMock()
        mock_settings.k8s_admin_service_accounts = []

        with patch.object(auth, "_web_settings", mock_settings):
            assert auth._authenticate_k8s_service_account("any-token") is False

    def test_returns_false_when_k8s_api_unavailable(self):
        import simplyblock_web.api.v2._auth as auth

        mock_settings = MagicMock()
        mock_settings.k8s_admin_service_accounts = ["system:serviceaccount:default:op"]

        with (
            patch.object(auth, "_web_settings", mock_settings),
            patch.object(auth, "_get_k8s_auth_api", return_value=None),
        ):
            assert auth._authenticate_k8s_service_account("any-token") is False

    def test_returns_true_for_admin_service_account(self):
        import simplyblock_web.api.v2._auth as auth

        mock_api = MagicMock()
        mock_api.create_token_review.return_value = _make_review(
            authenticated=True, username="system:serviceaccount:default:op"
        )
        mock_settings = MagicMock()
        mock_settings.k8s_admin_service_accounts = ["system:serviceaccount:default:op"]

        with (
            patch.object(auth, "_web_settings", mock_settings),
            patch.object(auth, "_get_k8s_auth_api", return_value=mock_api),
        ):
            assert auth._authenticate_k8s_service_account("sa-token") is True

    def test_returns_false_for_unknown_service_account(self):
        import simplyblock_web.api.v2._auth as auth

        mock_api = MagicMock()
        mock_api.create_token_review.return_value = _make_review(
            authenticated=True, username="system:serviceaccount:default:stranger"
        )
        mock_settings = MagicMock()
        mock_settings.k8s_admin_service_accounts = ["system:serviceaccount:default:op"]

        with (
            patch.object(auth, "_web_settings", mock_settings),
            patch.object(auth, "_get_k8s_auth_api", return_value=mock_api),
        ):
            assert auth._authenticate_k8s_service_account("sa-token") is False

    def test_returns_false_on_token_review_exception(self):
        import simplyblock_web.api.v2._auth as auth

        mock_api = MagicMock()
        mock_api.create_token_review.side_effect = RuntimeError("network error")
        mock_settings = MagicMock()
        mock_settings.k8s_admin_service_accounts = ["system:serviceaccount:default:op"]

        with (
            patch.object(auth, "_web_settings", mock_settings),
            patch.object(auth, "_get_k8s_auth_api", return_value=mock_api),
        ):
            assert auth._authenticate_k8s_service_account("sa-token") is False


# ---------------------------------------------------------------------------
# _authenticate_cluster_secret
# ---------------------------------------------------------------------------

class TestAuthenticateClusterSecret:

    def _cluster(self, id: str, secret: str) -> MagicMock:
        c = MagicMock()
        c.id = id
        c.secret = secret
        return c

    def test_passes_with_valid_secret(self):
        import simplyblock_web.api.v2._auth as auth

        mock_db = MagicMock()
        mock_db.get_clusters.return_value = [self._cluster("c1", "s3cr3t")]

        with patch.object(auth, "_db", mock_db):
            auth._authenticate_cluster_secret("s3cr3t", None)  # must not raise

    def test_raises_401_for_wrong_secret(self):
        import simplyblock_web.api.v2._auth as auth

        mock_db = MagicMock()
        mock_db.get_clusters.return_value = [self._cluster("c1", "correct")]

        with patch.object(auth, "_db", mock_db):
            with pytest.raises(HTTPException) as exc_info:
                auth._authenticate_cluster_secret("wrong", None)
        assert exc_info.value.status_code == 401

    def test_passes_with_matching_cluster_id(self):
        import simplyblock_web.api.v2._auth as auth

        mock_db = MagicMock()
        mock_db.get_clusters.return_value = [self._cluster("c1", "s3cr3t")]

        with patch.object(auth, "_db", mock_db):
            auth._authenticate_cluster_secret("s3cr3t", "c1")  # must not raise

    def test_raises_401_for_mismatched_cluster_id(self):
        import simplyblock_web.api.v2._auth as auth

        mock_db = MagicMock()
        mock_db.get_clusters.return_value = [self._cluster("c1", "s3cr3t")]

        with patch.object(auth, "_db", mock_db):
            with pytest.raises(HTTPException) as exc_info:
                auth._authenticate_cluster_secret("s3cr3t", "c2")
        assert exc_info.value.status_code == 401


# ---------------------------------------------------------------------------
# verify_api_token (orchestration)
# ---------------------------------------------------------------------------

class TestVerifyApiToken:

    def test_returns_early_when_k8s_auth_succeeds(self):
        import simplyblock_web.api.v2._auth as auth

        mock_cluster_auth = MagicMock()

        with (
            patch.object(auth, "_authenticate_k8s_service_account", return_value=True),
            patch.object(auth, "_authenticate_cluster_secret", mock_cluster_auth),
        ):
            auth.verify_api_token(_make_credentials("sa-token"))

        mock_cluster_auth.assert_not_called()

    def test_falls_through_to_cluster_secret_when_k8s_fails(self):
        import simplyblock_web.api.v2._auth as auth

        mock_cluster_auth = MagicMock()

        with (
            patch.object(auth, "_authenticate_k8s_service_account", return_value=False),
            patch.object(auth, "_authenticate_cluster_secret", mock_cluster_auth),
        ):
            auth.verify_api_token(_make_credentials("cluster-token"), cluster_id="c1")

        mock_cluster_auth.assert_called_once_with("cluster-token", "c1")
