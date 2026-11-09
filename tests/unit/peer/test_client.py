"""HttpPeerClient against a stubbed HTTP session, and peer_for_target's choice.

Nothing here touches the database: the HTTP client never does, and
LocalPeerClient is only constructed. Its calls into the service run against
real FDB in tests/integration/test_peer_service.py.
"""
import logging
import os
import stat
import tempfile
from unittest.mock import MagicMock, patch
from uuid import UUID

import pytest
import requests
from pydantic import SecretStr

from simplyblock_core.models.replication import ReplicationTarget
from simplyblock_core.peer.client import (
    HttpPeerClient,
    LocalPeerClient,
    PeerAuthError,
    PeerNotFoundError,
    PeerProtocolError,
    PeerRefusedError,
    PeerUnreachableError,
    peer_for_target,
)
from simplyblock_core.peer.protocol import PEER_API_VERSION

CLUSTER_ID = "6f9ad3a2-5b0e-4d55-9a84-0d3c3b1f2e10"
SECRET = "peer-plaintext"
INFO_BODY = {"cluster_id": CLUSTER_ID, "api_version": PEER_API_VERSION, "capabilities": ["info"]}


def _response(status=200, body=None, json_error=False):
    response = MagicMock(spec=requests.Response)
    response.status_code = status
    if json_error:
        response.json.side_effect = ValueError("not json")
    else:
        response.json.return_value = body if body is not None else {}
    return response


@pytest.fixture()
def session_request():
    with patch.object(requests.Session, "request") as request:
        yield request


def _client(**kwargs):
    return HttpPeerClient("https://cp-b.example:8443/", CLUSTER_ID, SecretStr(SECRET), **kwargs)


class TestInfo:

    def test_addresses_the_cluster_under_the_peer_prefix(self, session_request):
        session_request.return_value = _response(body=INFO_BODY)

        info = _client().info()

        assert info.cluster_id == UUID(CLUSTER_ID)
        assert info.api_version == PEER_API_VERSION
        method, url = session_request.call_args.args
        assert method == "GET"
        assert url == f"https://cp-b.example:8443/api/v2/_peer/clusters/{CLUSTER_ID}/info"

    def test_sends_the_secret_as_bearer_token(self, session_request):
        session_request.return_value = _response(body=INFO_BODY)

        _client().info()

        assert session_request.call_args.kwargs["headers"] == {"Authorization": f"Bearer {SECRET}"}

    def test_never_logs_the_secret(self, session_request, caplog):
        session_request.return_value = _response(body=INFO_BODY)

        with caplog.at_level(logging.DEBUG):
            _client().info()

        assert caplog.records, "the request should be logged"
        assert SECRET not in caplog.text

    def test_an_unexpected_body_is_a_protocol_error(self, session_request):
        session_request.return_value = _response(body={"cluster_id": "not-a-uuid"})

        with pytest.raises(PeerProtocolError):
            _client().info()


class TestErrors:

    @pytest.mark.parametrize(("status", "error"), [
        (401, PeerAuthError),
        (403, PeerAuthError),
        (404, PeerNotFoundError),
        (422, PeerProtocolError),
        (500, PeerUnreachableError),
        (503, PeerUnreachableError),
    ])
    def test_status_maps_to_a_specific_error(self, session_request, status, error):
        session_request.return_value = _response(status=status, body={"detail": "x"})

        with pytest.raises(error):
            _client().info()

    @pytest.mark.parametrize("exc", [requests.ConnectionError("refused"), requests.Timeout("slow")])
    def test_no_answer_is_unreachable(self, session_request, exc):
        session_request.side_effect = exc

        with pytest.raises(PeerUnreachableError):
            _client().info()

    def test_a_body_that_is_not_json_is_a_protocol_error(self, session_request):
        session_request.return_value = _response(json_error=True)

        with pytest.raises(PeerProtocolError):
            _client().info()

    def test_an_error_message_never_carries_the_secret(self, session_request):
        session_request.return_value = _response(status=401)

        with pytest.raises(PeerAuthError) as excinfo:
            _client().info()

        assert SECRET not in str(excinfo.value)


class TestTls:

    def test_a_ca_bundle_becomes_the_verify_path(self):
        pem = "-----BEGIN CERTIFICATE-----\nunit-test\n-----END CERTIFICATE-----\n"

        client = _client(ca_cert=pem)

        path = client._session.verify
        assert isinstance(path, str)
        with open(path) as f:
            assert f.read() == pem

    def test_the_same_bundle_reuses_one_file(self):
        pem = "-----BEGIN CERTIFICATE-----\nunit-test-reuse\n-----END CERTIFICATE-----\n"

        first = _client(ca_cert=pem)._session.verify
        second = _client(ca_cert=pem)._session.verify

        assert first == second
        assert os.path.exists(first)

    def test_the_bundle_lives_in_a_private_directory(self):
        """Another user must not be able to plant the bundle before it is written."""
        pem = "-----BEGIN CERTIFICATE-----\nunit-test-private\n-----END CERTIFICATE-----\n"

        path = _client(ca_cert=pem)._session.verify

        directory = os.path.dirname(path)
        assert directory != tempfile.gettempdir()
        if os.name == "posix":
            assert stat.S_IMODE(os.stat(directory).st_mode) == 0o700

    def test_no_bundle_keeps_the_system_trust_store(self):
        assert _client()._session.verify is True


def test_an_empty_endpoint_is_refused():
    with pytest.raises(ValueError):
        HttpPeerClient("", CLUSTER_ID, SecretStr(SECRET))


class TestPeerForTarget:

    def _target(self, endpoint=""):
        target = ReplicationTarget()
        target.target_cluster_id = CLUSTER_ID
        target.peer_endpoint = endpoint
        target.peer_secret = SecretStr(SECRET if endpoint else "")
        return target

    def test_a_local_target_is_called_in_process(self):
        client = peer_for_target(self._target())

        assert isinstance(client, LocalPeerClient)
        assert client.cluster_id == CLUSTER_ID

    def test_a_target_with_a_peer_endpoint_goes_over_http(self):
        client = peer_for_target(self._target("https://cp-b.example"))

        assert isinstance(client, HttpPeerClient)
        assert client.cluster_id == CLUSTER_ID


class TestDiscardLanding:

    LVOL_ID = "0b6f4a8e-1c2d-4e3f-9a0b-1c2d3e4f5a99"

    def test_deletes_the_landing_volume_under_the_cluster(self, session_request):
        session_request.return_value = _response(status=204)

        assert _client().discard_landing(self.LVOL_ID) is None

        method, url = session_request.call_args.args
        assert method == "DELETE"
        assert url == (f"https://cp-b.example:8443/api/v2/_peer/clusters/{CLUSTER_ID}"
                       f"/landing-volumes/{self.LVOL_ID}")

    def test_an_empty_answer_is_not_read_as_json(self, session_request):
        session_request.return_value = _response(status=204, json_error=True)

        _client().discard_landing(self.LVOL_ID)

    def test_a_volume_on_another_cluster_is_not_found(self, session_request):
        session_request.return_value = _response(status=404, body={"detail": "not on cluster"})

        with pytest.raises(PeerNotFoundError, match="not on cluster"):
            _client().discard_landing(self.LVOL_ID)

    def test_a_refusal_is_its_own_error(self, session_request):
        session_request.return_value = _response(status=409, body={"detail": "not a landing volume"})

        with pytest.raises(PeerRefusedError, match="not a landing volume"):
            _client().discard_landing(self.LVOL_ID)
