"""The peer API's target side, and replication targets on a peer control plane.

``service.info`` and ``LocalPeerClient`` answer from the cluster records, and
``add_target`` with a peer stores the connection in the target record, so these
run against real FDB. The peer control plane on the other end of the HTTP call
is above the database and is mocked at ``HttpPeerClient``.
"""
from unittest.mock import patch
from uuid import UUID

import pytest
from pydantic import SecretStr

from simplyblock_core.controllers import replication_policy_controller as rpc
from simplyblock_core.controllers.replication_policy_controller import (
    PeerConnection,
    ReplicationConfigError,
)
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.peer import service
from simplyblock_core.peer.client import (
    LocalPeerClient,
    PeerNotFoundError,
    PeerUnreachableError,
    peer_for_target,
)
from simplyblock_core.peer.protocol import PEER_API_VERSION, PeerInfo

SOURCE_CLUSTER_ID = "0b6f4a8e-1c2d-4e3f-9a0b-1c2d3e4f5a01"
PEER_CLUSTER_ID = "0b6f4a8e-1c2d-4e3f-9a0b-1c2d3e4f5a02"
PEER_POOL_ID = "0b6f4a8e-1c2d-4e3f-9a0b-1c2d3e4f5a03"
PEER = PeerConnection(endpoint="https://cp-b.example:8443", secret=SecretStr("peer-plaintext"),
                      ca_cert="-----BEGIN CERTIFICATE-----\n...\n-----END CERTIFICATE-----\n")


@pytest.fixture
def db():
    db = DBController()
    if db.kv_store is None:
        pytest.skip("FoundationDB is not available")
    return db


@pytest.fixture
def source_cluster(db):
    cluster = Cluster()
    cluster.uuid = SOURCE_CLUSTER_ID
    cluster.write_to_db(db.kv_store)
    return cluster


@pytest.fixture
def peer_answers():
    """The peer control plane, answering ``info`` for the cluster it manages."""
    with patch.object(rpc, "HttpPeerClient") as client_cls:
        client_cls.return_value.info.return_value = PeerInfo(
            cluster_id=UUID(PEER_CLUSTER_ID), api_version=PEER_API_VERSION, capabilities=["info"])
        yield client_cls


class TestInfo:

    def test_answers_for_a_cluster_of_this_control_plane(self, db, source_cluster):
        info = service.info(SOURCE_CLUSTER_ID)

        assert info.cluster_id == UUID(SOURCE_CLUSTER_ID)
        assert info.api_version == PEER_API_VERSION
        assert "info" in info.capabilities

    def test_an_unknown_cluster_raises(self, db):
        with pytest.raises(KeyError):
            service.info(PEER_CLUSTER_ID)

    def test_the_local_client_runs_the_same_service(self, db, source_cluster):
        assert LocalPeerClient(SOURCE_CLUSTER_ID).info() == service.info(SOURCE_CLUSTER_ID)

    def test_the_local_client_reports_an_unknown_cluster_as_not_found(self, db):
        with pytest.raises(PeerNotFoundError):
            LocalPeerClient(PEER_CLUSTER_ID).info()


class TestAddPeerTarget:

    def test_stores_the_peer_connection(self, db, source_cluster, peer_answers):
        target_id = rpc.add_target(SOURCE_CLUSTER_ID, "site-b", PEER_CLUSTER_ID,
                                   target_pool=PEER_POOL_ID, peer=PEER)

        stored = DBController().get_replication_target_by_id(target_id)
        assert stored.has_peer()
        assert stored.peer_endpoint == PEER.endpoint
        assert stored.peer_secret.get_secret_value() == "peer-plaintext"
        assert stored.peer_ca_cert == PEER.ca_cert
        assert stored.target_pool_uuid == PEER_POOL_ID
        peer_answers.assert_called_once_with(
            PEER.endpoint, PEER_CLUSTER_ID, PEER.secret, PEER.ca_cert)

    def test_the_stored_target_goes_through_the_http_client(self, db, source_cluster, peer_answers):
        target_id = rpc.add_target(SOURCE_CLUSTER_ID, "site-b", PEER_CLUSTER_ID, peer=PEER)

        stored = DBController().get_replication_target_by_id(target_id)
        assert not isinstance(peer_for_target(stored), LocalPeerClient)

    def test_needs_no_local_record_of_the_target_cluster(self, db, source_cluster, peer_answers):
        with pytest.raises(KeyError):
            db.get_cluster_by_id(PEER_CLUSTER_ID)

        rpc.add_target(SOURCE_CLUSTER_ID, "site-b", PEER_CLUSTER_ID, peer=PEER)

    def test_a_peer_answering_for_another_cluster_is_refused(self, db, source_cluster, peer_answers):
        peer_answers.return_value.info.return_value = PeerInfo(
            cluster_id=UUID(SOURCE_CLUSTER_ID), api_version=PEER_API_VERSION, capabilities=[])

        with pytest.raises(ReplicationConfigError, match="answered for cluster"):
            rpc.add_target(SOURCE_CLUSTER_ID, "site-b", PEER_CLUSTER_ID, peer=PEER)

        assert DBController().get_replication_targets(SOURCE_CLUSTER_ID) == []

    def test_a_peer_on_another_api_version_is_refused(self, db, source_cluster, peer_answers):
        peer_answers.return_value.info.return_value = PeerInfo(
            cluster_id=UUID(PEER_CLUSTER_ID), api_version=PEER_API_VERSION + 1, capabilities=[])

        with pytest.raises(ReplicationConfigError, match="API version"):
            rpc.add_target(SOURCE_CLUSTER_ID, "site-b", PEER_CLUSTER_ID, peer=PEER)

    def test_an_unreachable_peer_creates_nothing(self, db, source_cluster, peer_answers):
        peer_answers.return_value.info.side_effect = PeerUnreachableError("refused")

        with pytest.raises(PeerUnreachableError):
            rpc.add_target(SOURCE_CLUSTER_ID, "site-b", PEER_CLUSTER_ID, peer=PEER)

        assert DBController().get_replication_targets(SOURCE_CLUSTER_ID) == []

    def test_a_pool_name_is_refused_for_a_peer_target(self, db, source_cluster, peer_answers):
        with pytest.raises(ReplicationConfigError, match="pool ID"):
            rpc.add_target(SOURCE_CLUSTER_ID, "site-b", PEER_CLUSTER_ID,
                           target_pool="pool-by-name", peer=PEER)


class TestPolicyOnPeerTarget:
    """A peer target takes no policy until replication runs through the peer
    client: every replication path starts from a policy and would look the
    target cluster up in this control plane's database, where it has no record."""

    def test_a_policy_is_refused(self, db, source_cluster, peer_answers):
        target_id = rpc.add_target(SOURCE_CLUSTER_ID, "site-b", PEER_CLUSTER_ID, peer=PEER)

        with pytest.raises(ReplicationConfigError, match="peer control plane"):
            rpc.add_policy(SOURCE_CLUSTER_ID, "every-5m", target_id, interval_min=5)

        assert DBController().get_replication_policies(SOURCE_CLUSTER_ID) == []

    def test_a_policy_by_target_name_is_refused_too(self, db, source_cluster, peer_answers):
        rpc.add_target(SOURCE_CLUSTER_ID, "site-b", PEER_CLUSTER_ID, peer=PEER)

        with pytest.raises(ReplicationConfigError, match="peer control plane"):
            rpc.add_policy(SOURCE_CLUSTER_ID, "every-5m", "site-b", interval_min=5)


def test_a_local_target_still_needs_the_target_cluster_here(db, source_cluster):
    with pytest.raises(KeyError):
        rpc.add_target(SOURCE_CLUSTER_ID, "site-b", PEER_CLUSTER_ID)

    assert DBController().get_replication_targets(SOURCE_CLUSTER_ID) == []
