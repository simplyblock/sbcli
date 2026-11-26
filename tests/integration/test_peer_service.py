"""The peer API's target side, and replication targets on a peer control plane.

``service.info`` and ``LocalPeerClient`` answer from the cluster records, and
``add_target`` with a peer stores the connection in the target record, so these
run against real FDB. The peer control plane on the other end of the HTTP call
is above the database and is mocked at ``HttpPeerClient``.
"""
from unittest.mock import MagicMock, patch
from uuid import UUID

import pytest
from pydantic import SecretStr

from simplyblock_core.controllers import lvol_controller, snapshot_controller
from simplyblock_core.controllers import replication_policy_controller as rpc
from simplyblock_core.controllers.replication_policy_controller import (
    PeerConnection,
    ReplicationConfigError,
)
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.pool import Pool
from simplyblock_core.models.replication import ReplicationPolicy, ReplicationTarget
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.peer import service
from simplyblock_core.peer.client import (
    LocalPeerClient,
    PeerNotFoundError,
    PeerRefusedError,
    PeerUnreachableError,
    peer_for_target,
)
from simplyblock_core.peer.protocol import (
    CAPABILITIES,
    PEER_API_VERSION,
    ChainCheck,
    DescribeRequest,
    PeerInfo,
)

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


def _peer_serving(capabilities):
    """The peer control plane, answering info with *capabilities*."""
    peer = MagicMock()
    peer.info.return_value = PeerInfo(cluster_id=UUID(PEER_CLUSTER_ID), api_version=PEER_API_VERSION,
                                      capabilities=sorted(capabilities))
    return patch.object(rpc, "peer_for_target", return_value=peer)


class TestPolicyOnPeerTarget:
    """A peer target takes a policy once the peer serves every call replication
    drives on it; one on an older version would fail mid-replication instead."""

    def test_a_peer_serving_every_call_takes_a_policy(self, db, source_cluster, peer_answers):
        target_id = rpc.add_target(SOURCE_CLUSTER_ID, "site-b", PEER_CLUSTER_ID, peer=PEER)

        with _peer_serving(CAPABILITIES):
            rpc.add_policy(SOURCE_CLUSTER_ID, "every-5m", target_id, interval_min=5)

        assert [p.policy_name for p in DBController().get_replication_policies(SOURCE_CLUSTER_ID)] == [
            "every-5m"]

    def test_a_peer_lacking_a_call_is_refused(self, db, source_cluster, peer_answers):
        target_id = rpc.add_target(SOURCE_CLUSTER_ID, "site-b", PEER_CLUSTER_ID, peer=PEER)

        with _peer_serving({"info", "describe"}), \
                pytest.raises(ReplicationConfigError, match="does not serve .*finish"):
            rpc.add_policy(SOURCE_CLUSTER_ID, "every-5m", target_id, interval_min=5)

        assert DBController().get_replication_policies(SOURCE_CLUSTER_ID) == []

    def test_a_peer_that_cannot_be_asked_is_refused(self, db, source_cluster, peer_answers):
        rpc.add_target(SOURCE_CLUSTER_ID, "site-b", PEER_CLUSTER_ID, peer=PEER)

        with patch.object(rpc, "peer_for_target") as peer_for:
            peer_for.return_value.info.side_effect = PeerUnreachableError("refused")
            with pytest.raises(ReplicationConfigError, match="cannot be asked"):
                rpc.add_policy(SOURCE_CLUSTER_ID, "every-5m", "site-b", interval_min=5)


def test_a_local_target_still_needs_the_target_cluster_here(db, source_cluster):
    with pytest.raises(KeyError):
        rpc.add_target(SOURCE_CLUSTER_ID, "site-b", PEER_CLUSTER_ID)

    assert DBController().get_replication_targets(SOURCE_CLUSTER_ID) == []


SOURCE_POOL_ID = "0b6f4a8e-1c2d-4e3f-9a0b-1c2d3e4f5a04"
LANDING_ID = "0b6f4a8e-1c2d-4e3f-9a0b-1c2d3e4f5a05"


def _pool(db, cluster_id):
    pool = Pool()
    pool.uuid = SOURCE_POOL_ID
    pool.cluster_id = cluster_id
    pool.write_to_db(db.kv_store)
    return pool


def _lvol(db, name):
    lvol = LVol()
    lvol.uuid = LANDING_ID
    lvol.lvol_name = name
    lvol.pool_uuid = SOURCE_POOL_ID
    lvol.write_to_db(db.kv_store)
    return lvol


@pytest.fixture
def delete_lvol():
    """The delete itself talks to storage nodes, which this tier never runs."""
    with patch.object(lvol_controller, "delete_lvol") as delete:
        yield delete


class TestDiscardLanding:

    def test_deletes_a_landing_volume(self, db, source_cluster, delete_lvol):
        _pool(db, SOURCE_CLUSTER_ID)
        _lvol(db, "REP_snap-1")

        service.discard_landing(SOURCE_CLUSTER_ID, LANDING_ID)

        (deleted,), kwargs = delete_lvol.call_args
        assert deleted.get_id() == LANDING_ID
        assert kwargs == {"force_delete": True}

    def test_a_volume_already_gone_is_success(self, db, source_cluster, delete_lvol):
        service.discard_landing(SOURCE_CLUSTER_ID, LANDING_ID)

        delete_lvol.assert_not_called()

    def test_refuses_a_volume_that_is_not_a_landing_volume(self, db, source_cluster, delete_lvol):
        _pool(db, SOURCE_CLUSTER_ID)
        _lvol(db, "pvc-data")

        with pytest.raises(service.NotALandingVolumeError):
            service.discard_landing(SOURCE_CLUSTER_ID, LANDING_ID)

        delete_lvol.assert_not_called()

    def test_a_volume_of_another_cluster_is_not_found(self, db, source_cluster, delete_lvol):
        _pool(db, PEER_CLUSTER_ID)
        _lvol(db, "REP_snap-1")

        with pytest.raises(KeyError):
            service.discard_landing(SOURCE_CLUSTER_ID, LANDING_ID)

        delete_lvol.assert_not_called()

    def test_the_local_client_reports_a_refusal_as_refused(self, db, source_cluster, delete_lvol):
        _pool(db, SOURCE_CLUSTER_ID)
        _lvol(db, "pvc-data")

        with pytest.raises(PeerRefusedError):
            LocalPeerClient(SOURCE_CLUSTER_ID).discard_landing(LANDING_ID)

    def test_the_local_client_reports_an_unknown_cluster_as_not_found(self, db, delete_lvol):
        with pytest.raises(PeerNotFoundError):
            LocalPeerClient(PEER_CLUSTER_ID).discard_landing(LANDING_ID)


class TestRunnerPeerChoice:
    """The replication runner reaches a destination through the client its
    volume's policy target calls for: HTTP for a peer, in-process otherwise."""

    def test_a_volume_without_a_policy_stays_in_process(self, db, source_cluster):
        from simplyblock_core.services import snapshot_replication as sr

        client = sr._peer_for(LVol(), PEER_CLUSTER_ID)

        assert isinstance(client, LocalPeerClient)
        assert client.cluster_id == PEER_CLUSTER_ID

    def test_a_policy_on_a_peer_target_goes_over_http(self, db, source_cluster, peer_answers):
        from simplyblock_core.services import snapshot_replication as sr

        target_id = rpc.add_target(SOURCE_CLUSTER_ID, "site-b", PEER_CLUSTER_ID, peer=PEER)
        # add_policy refuses a peer target until every step goes through the
        # peer client, so the policy record is written directly here.
        policy = ReplicationPolicy()
        policy.uuid = "0b6f4a8e-1c2d-4e3f-9a0b-1c2d3e4f5a06"
        policy.cluster_id = SOURCE_CLUSTER_ID
        policy.policy_name = "every-5m"
        policy.target_id = target_id
        policy.write_to_db(db.kv_store)
        source_lvol = LVol()
        source_lvol.replication_policy_id = policy.get_id()

        client = sr._peer_for(source_lvol, PEER_CLUSTER_ID)

        assert not isinstance(client, LocalPeerClient)
        assert client.cluster_id == PEER_CLUSTER_ID


def _copy(db, uuid, cluster_id=SOURCE_CLUSTER_ID, prev=""):
    snapshot = SnapShot()
    snapshot.uuid = uuid
    snapshot.cluster_id = cluster_id
    snapshot.prev_snap_uuid = prev
    snapshot.snap_bdev = f"LVS/{uuid}"
    snapshot.lvol = LVol()
    snapshot.write_to_db(db.kv_store)
    return snapshot


def _node(db, node_id, status=StorageNode.STATUS_ONLINE):
    node = StorageNode()
    node.uuid = node_id
    node.cluster_id = SOURCE_CLUSTER_ID
    node.status = status
    node.write_to_db(db.kv_store)
    return node


class TestDescribe:

    def test_reports_the_online_members_leading_the_landing_volumes_lvstore(self, db, source_cluster):
        _pool(db, SOURCE_CLUSTER_ID)
        landing = _lvol(db, "REP_snap-1")
        landing.node_id, landing.nodes, landing.lvs_name = "N1", ["N1", "N2", "N3"], "LVS_1"
        landing.write_to_db(db.kv_store)
        _node(db, "N1")
        _node(db, "N2")
        _node(db, "N3", status=StorageNode.STATUS_OFFLINE)

        # SPDK's leadership flag is an RPC to the node: N2 and the offline N3 claim it.
        with patch.object(lvol_controller, "is_node_leader",
                          side_effect=lambda node, lvs: node.get_id() in ("N2", "N3")):
            answer = service.describe(SOURCE_CLUSTER_ID, DescribeRequest(landing_lvol_id=LANDING_ID))

        assert answer.landing_leaders == ["N2"]

    def test_a_landing_volume_that_is_gone_has_no_leaders(self, db, source_cluster):
        answer = service.describe(SOURCE_CLUSTER_ID, DescribeRequest(landing_lvol_id=LANDING_ID))

        assert answer.landing_leaders is None

    def test_reports_whether_copies_exist_and_have_dependent_clones(self, db, source_cluster):
        _copy(db, "T1")
        _copy(db, "T2")
        _copy(db, "T9", cluster_id=PEER_CLUSTER_ID)
        clone = LVol()
        clone.uuid = "0b6f4a8e-1c2d-4e3f-9a0b-1c2d3e4f5a07"
        clone.lvol_name = "failed-over"
        clone.cloned_from_snap = "T1"
        clone.write_to_db(db.kv_store)

        answer = service.describe(SOURCE_CLUSTER_ID, DescribeRequest(snapshot_ids=["T1", "T2", "T9", "T0"]))

        states = {s.snapshot_id: (s.exists, s.has_dependent_clone) for s in answer.snapshots}
        assert states == {"T1": (True, True), "T2": (True, False),
                          "T9": (False, False), "T0": (False, False)}

    def test_a_chain_link_in_the_database_counts_without_asking_spdk(self, db, source_cluster):
        _copy(db, "T1")
        _copy(db, "T2", prev="T1")

        answer = service.describe(SOURCE_CLUSTER_ID, DescribeRequest(
            chain_checks=[ChainCheck(successor_id="T2", predecessor_id="T1")]))

        assert answer.chains[0].chained

    def test_a_missing_successor_is_not_chained(self, db, source_cluster):
        _copy(db, "T1")

        answer = service.describe(SOURCE_CLUSTER_ID, DescribeRequest(
            chain_checks=[ChainCheck(successor_id="T2", predecessor_id="T1")]))

        assert not answer.chains[0].chained

    def test_the_local_client_reports_an_unknown_cluster_as_not_found(self, db):
        with pytest.raises(PeerNotFoundError):
            LocalPeerClient(PEER_CLUSTER_ID).describe(DescribeRequest())


def _transfer(db, task_id, target_node_id=None, status=JobSchedule.STATUS_RUNNING):
    task = JobSchedule()
    task.uuid = task_id
    task.cluster_id = SOURCE_CLUSTER_ID
    task.function_name = JobSchedule.FN_SNAPSHOT_REPLICATION
    task.status = status
    task.function_params = {"remote_lvol_id": LANDING_ID}
    if target_node_id:
        task.function_params["target_node_id"] = target_node_id
    task.write_to_db(db.kv_store)
    return task


class TestHubSessionSharing:
    """The hub session to a target member is shared by every transfer sent to
    it; the runner tells from its own task records, not the target's volumes."""

    def test_another_running_transfer_to_the_member_keeps_the_session(self, db, source_cluster):
        from simplyblock_core.services import snapshot_replication as sr

        mine = _transfer(db, "0b6f4a8e-1c2d-4e3f-9a0b-1c2d3e4f5a10", "N1")
        _transfer(db, "0b6f4a8e-1c2d-4e3f-9a0b-1c2d3e4f5a11", "N1")

        assert sr._other_active_transfers_to_node(mine, "N1")

    def test_transfers_to_other_members_do_not(self, db, source_cluster):
        from simplyblock_core.services import snapshot_replication as sr

        mine = _transfer(db, "0b6f4a8e-1c2d-4e3f-9a0b-1c2d3e4f5a10", "N1")
        _transfer(db, "0b6f4a8e-1c2d-4e3f-9a0b-1c2d3e4f5a11", "N2")
        _transfer(db, "0b6f4a8e-1c2d-4e3f-9a0b-1c2d3e4f5a12", "N1", status=JobSchedule.STATUS_DONE)

        assert not sr._other_active_transfers_to_node(mine, "N1")

    def test_a_running_transfer_naming_no_member_keeps_the_session(self, db, source_cluster):
        from simplyblock_core.services import snapshot_replication as sr

        mine = _transfer(db, "0b6f4a8e-1c2d-4e3f-9a0b-1c2d3e4f5a10", "N1")
        _transfer(db, "0b6f4a8e-1c2d-4e3f-9a0b-1c2d3e4f5a11")

        assert sr._other_active_transfers_to_node(mine, "N1")


class TestDeleteSnapshot:

    def test_deletes_a_replicated_copy(self, db, source_cluster):
        copy = _copy(db, "T1")
        copy.source_replicated_snap_uuid = "S1"
        copy.write_to_db(db.kv_store)

        with patch.object(snapshot_controller, "delete", return_value=True) as delete:
            answer = service.delete_snapshot(SOURCE_CLUSTER_ID, "T1")

        assert answer.deleted
        delete.assert_called_once_with("T1")

    def test_a_copy_already_gone_counts_as_deleted(self, db, source_cluster):
        with patch.object(snapshot_controller, "delete") as delete:
            assert service.delete_snapshot(SOURCE_CLUSTER_ID, "T1").deleted
        delete.assert_not_called()

    def test_a_copy_a_live_volume_is_cloned_from_stays(self, db, source_cluster):
        copy = _copy(db, "T1")
        copy.source_replicated_snap_uuid = "S1"
        copy.write_to_db(db.kv_store)
        clone = LVol()
        clone.uuid = "0b6f4a8e-1c2d-4e3f-9a0b-1c2d3e4f5a08"
        clone.lvol_name = "failed-over"
        clone.cloned_from_snap = "T1"
        clone.write_to_db(db.kv_store)

        with patch.object(snapshot_controller, "delete") as delete:
            answer = service.delete_snapshot(SOURCE_CLUSTER_ID, "T1")

        assert not answer.deleted and "cloned" in answer.reason
        delete.assert_not_called()

    def test_refuses_a_snapshot_replication_did_not_make(self, db, source_cluster):
        _copy(db, "U1")

        with patch.object(snapshot_controller, "delete") as delete, \
                pytest.raises(PeerRefusedError):
            LocalPeerClient(SOURCE_CLUSTER_ID).delete_snapshot("U1")
        delete.assert_not_called()


SOURCE_NODE_ID = "0b6f4a8e-1c2d-4e3f-9a0b-1c2d3e4f5a30"
VOLUME_ID = "0b6f4a8e-1c2d-4e3f-9a0b-1c2d3e4f5a31"


def _volume_on_peer_policy(db):
    """A source volume whose policy targets the peer control plane."""
    node = StorageNode()
    node.uuid = SOURCE_NODE_ID
    node.cluster_id = SOURCE_CLUSTER_ID
    node.write_to_db(db.kv_store)
    target = ReplicationTarget()
    target.uuid = "0b6f4a8e-1c2d-4e3f-9a0b-1c2d3e4f5a32"
    target.cluster_id = SOURCE_CLUSTER_ID
    target.target_name = "site-b"
    target.target_cluster_id = PEER_CLUSTER_ID
    target.peer_endpoint = PEER.endpoint
    target.peer_secret = PEER.secret
    target.write_to_db(db.kv_store)
    policy = ReplicationPolicy()
    policy.uuid = "0b6f4a8e-1c2d-4e3f-9a0b-1c2d3e4f5a33"
    policy.cluster_id = SOURCE_CLUSTER_ID
    policy.policy_name = "every-5m"
    policy.target_id = target.get_id()
    policy.write_to_db(db.kv_store)
    volume = LVol()
    volume.uuid = VOLUME_ID
    volume.lvol_name = "pvc-1"
    volume.node_id = SOURCE_NODE_ID
    volume.replication_policy_id = policy.get_id()
    volume.write_to_db(db.kv_store)
    return volume


class TestDestinationOnAPeer:
    """replication_start reaches a policy's peer destination through its peer
    client; a destination in this control plane is checked here, as before."""

    def test_a_peer_destination_is_reached_through_its_client(self, db, source_cluster):
        volume = _volume_on_peer_policy(db)

        peer = lvol_controller._destination_peer(DBController(), volume, PEER_CLUSTER_ID)

        assert not isinstance(peer, LocalPeerClient) and peer.cluster_id == PEER_CLUSTER_ID
        assert lvol_controller._destination_peer(DBController(), volume, SOURCE_CLUSTER_ID) is None

    def test_a_peer_cluster_unknown_here_is_not_refused(self, db, source_cluster):
        """The peer's cluster is not in this database: add_target confirmed it
        through the peer's /info."""
        _volume_on_peer_policy(db)

        assert lvol_controller.replication_start_error(VOLUME_ID, PEER_CLUSTER_ID) is None

    def test_a_local_destination_unknown_here_still_is(self, db, source_cluster):
        volume = _volume_on_peer_policy(db)
        volume.replication_policy_id = ""
        volume.write_to_db(db.kv_store)

        assert "does not exist" in lvol_controller.replication_start_error(VOLUME_ID, PEER_CLUSTER_ID)
