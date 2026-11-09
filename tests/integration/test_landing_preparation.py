"""Preparing a replicated snapshot's landing volume on the destination.

The source decides the pool hints from its own records (``snapshot_replication.
_pool_hints``); the destination takes the first that is a pool of its own and
otherwise its first active pool, finds a copy of the same data, adopts or clears
a volume an earlier attempt left, and creates the volume
(``peer.service.prepare_landing``). Both halves read the records they own, so
they run against real FDB here. Creating and deleting the volume talks to
storage nodes and is mocked at ``lvol_controller``.

The pool rules were pinned against the former ``_destination_pool_uuid`` by
simplyblock_core/test/test_replication_destination_resolution.py,
test_replication_failback_pool.py and test_replication_start_destination.py
(lab 2026-08-17/18/19); they hold here for the two halves together.
"""
from unittest.mock import MagicMock, patch

import pytest

from simplyblock_core.controllers import lvol_controller
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.pool import Pool
from simplyblock_core.models.replication import ReplicationPolicy, ReplicationTarget
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.peer import service
from simplyblock_core.peer.protocol import (
    ChainRequest,
    EnsureHubRequest,
    FinishRequest,
    HubAddress,
    HubPath,
    LandingRequest,
    SourceSnapshot,
)

SRC = "1b6f4a8e-1c2d-4e3f-9a0b-1c2d3e4f5a01"
TGT = "1b6f4a8e-1c2d-4e3f-9a0b-1c2d3e4f5a02"
OTHER = "1b6f4a8e-1c2d-4e3f-9a0b-1c2d3e4f5a03"
NODE = "1b6f4a8e-1c2d-4e3f-9a0b-1c2d3e4f5a10"


@pytest.fixture
def db():
    db = DBController()
    if db.kv_store is None:
        pytest.skip("FoundationDB is not available")
    return db


def _cluster(db, uuid, target_cluster="", target_pool=""):
    cluster = Cluster()
    cluster.uuid = uuid
    cluster.snapshot_replication_target_cluster = target_cluster
    cluster.snapshot_replication_target_pool = target_pool
    cluster.write_to_db(db.kv_store)


def _pool(db, uuid, cluster_id, status=Pool.STATUS_ACTIVE):
    pool = Pool()
    pool.uuid = uuid
    pool.pool_name = uuid
    pool.cluster_id = cluster_id
    pool.status = status
    pool.write_to_db(db.kv_store)


def _policy(db, target_cluster, target_pool):
    target = ReplicationTarget()
    target.uuid = "1b6f4a8e-1c2d-4e3f-9a0b-1c2d3e4f5a20"
    target.cluster_id = SRC
    target.target_name = "site-b"
    target.target_cluster_id = target_cluster
    target.target_pool_uuid = target_pool
    target.write_to_db(db.kv_store)
    policy = ReplicationPolicy()
    policy.uuid = "1b6f4a8e-1c2d-4e3f-9a0b-1c2d3e4f5a21"
    policy.cluster_id = SRC
    policy.policy_name = "every-5m"
    policy.target_id = target.get_id()
    policy.write_to_db(db.kv_store)
    return policy.get_id()


def _source_lvol(policy_id="", requested_pool=""):
    lvol = LVol()
    lvol.uuid = "1b6f4a8e-1c2d-4e3f-9a0b-1c2d3e4f5a30"
    lvol.replication_policy_id = policy_id
    lvol.replication_pool_uuid = requested_pool
    return lvol


def _request(**overrides):
    fields = dict(source_cluster_id=SRC, snapshot_id="1b6f4a8e-1c2d-4e3f-9a0b-1c2d3e4f5a40",
                  landing_name="REP_snap-1", size=1 << 30, ha_type="ha", node_hint=NODE)
    fields.update(overrides)
    return LandingRequest(**fields)


def _land_pool(db, lvol, source, destination, counterpart_hint=""):
    """The pool a copy of *lvol* coming from *source* lands in on *destination*."""
    from simplyblock_core.services import snapshot_replication as sr

    request = _request(pool_hints=sr._pool_hints(lvol, source, destination),
                       counterpart_hint=counterpart_hint)
    return service._pick_pool(db, destination, db.get_pools(destination), request)


class TestPoolChoice:

    def test_an_incoming_copy_ignores_the_destinations_outgoing_pool(self, db):
        """The 2026-08-19 regression: failing BACK into src must not use src's
        own outgoing pool, which belongs to the tgt cluster."""
        _cluster(db, SRC, target_cluster=TGT, target_pool="POOL_TGT")
        _cluster(db, TGT)
        _pool(db, "POOL_SRC", SRC)
        _pool(db, "POOL_TGT", TGT)

        assert _land_pool(db, _source_lvol(), source=TGT, destination=SRC) == "POOL_SRC"

    def test_the_policy_targets_pool_wins(self, db):
        _cluster(db, SRC)
        _cluster(db, TGT)
        _pool(db, "POOL_TGT", TGT)
        _pool(db, "POOL_FROM_TARGET", TGT)
        policy_id = _policy(db, TGT, "POOL_FROM_TARGET")

        assert _land_pool(db, _source_lvol(policy_id), SRC, TGT) == "POOL_FROM_TARGET"

    def test_a_policy_target_for_another_cluster_is_not_used(self, db):
        _cluster(db, SRC)
        _cluster(db, TGT)
        _pool(db, "POOL_TGT", TGT)
        _pool(db, "POOL_ELSEWHERE", OTHER)
        policy_id = _policy(db, OTHER, "POOL_ELSEWHERE")

        assert _land_pool(db, _source_lvol(policy_id), SRC, TGT) == "POOL_TGT"

    def test_the_sources_config_holds_for_the_direction_it_describes(self, db):
        _cluster(db, SRC, target_cluster=TGT, target_pool="POOL_CONFIGURED")
        _cluster(db, TGT)
        _pool(db, "POOL_TGT", TGT)
        _pool(db, "POOL_CONFIGURED", TGT)

        assert _land_pool(db, _source_lvol(), SRC, TGT) == "POOL_CONFIGURED"

    def test_the_requested_pool_receives_the_copies(self, db):
        _cluster(db, SRC)
        _cluster(db, TGT)
        _pool(db, "POOL_TGT", TGT)
        _pool(db, "P_TGT_B", TGT)

        assert _land_pool(db, _source_lvol(requested_pool="P_TGT_B"), SRC, TGT) == "P_TGT_B"

    def test_a_requested_pool_of_another_cluster_is_not_used(self, db):
        """A replication that later lands elsewhere (a fail-back) does not carry
        the forward move's pool into a cluster it does not belong to."""
        _cluster(db, SRC)
        _cluster(db, OTHER)
        _pool(db, "P_TGT_B", TGT)
        _pool(db, "P_OTHER", OTHER)

        assert _land_pool(db, _source_lvol(requested_pool="P_TGT_B"), SRC, OTHER) == "P_OTHER"

    def test_a_policy_overrides_a_pool_named_before_it(self, db):
        _cluster(db, SRC)
        _cluster(db, TGT)
        _pool(db, "P_TGT_B", TGT)
        _pool(db, "POOL_FROM_TARGET", TGT)
        policy_id = _policy(db, TGT, "POOL_FROM_TARGET")

        lvol = _source_lvol(policy_id, requested_pool="P_TGT_B")
        assert _land_pool(db, lvol, SRC, TGT) == "POOL_FROM_TARGET"

    def test_falls_back_to_the_first_active_pool(self, db):
        _cluster(db, SRC)
        _cluster(db, TGT)
        _pool(db, "POOL_DEAD", TGT, status=Pool.STATUS_INACTIVE)
        _pool(db, "POOL_TGT", TGT)

        assert _land_pool(db, _source_lvol(), SRC, TGT) == "POOL_TGT"

    def test_no_pool_on_the_destination_is_refused(self, db):
        _cluster(db, SRC)
        _cluster(db, TGT)

        with pytest.raises(service.LandingVolumeError, match="Unable to find pool"):
            _land_pool(db, _source_lvol(), SRC, TGT)

    def test_a_fail_back_lands_in_its_counterparts_pool(self, db):
        _cluster(db, SRC)
        _cluster(db, TGT)
        _pool(db, "POOL_SRC", SRC)
        _pool(db, "POOL_SRC_B", SRC)
        counterpart = SnapShot()
        counterpart.uuid = "1b6f4a8e-1c2d-4e3f-9a0b-1c2d3e4f5a41"
        counterpart.cluster_id = SRC
        counterpart.lvol = LVol()
        counterpart.lvol.pool_uuid = "POOL_SRC_B"
        counterpart.write_to_db(db.kv_store)

        pool = _land_pool(db, _source_lvol(), TGT, SRC, counterpart_hint=counterpart.get_id())

        assert pool == "POOL_SRC_B"

    def test_a_snapshot_without_a_counterpart_resolves_like_the_forward_direction(self, db):
        """Lab 2026-08-17/18: a blank counterpart id must never be looked up (it
        degenerated into a whole-table scan); the pool resolves as forward."""
        _cluster(db, TGT, target_cluster=SRC, target_pool="POOL_DEST")
        _cluster(db, SRC)
        _pool(db, "POOL_OTHER", SRC)
        _pool(db, "POOL_DEST", SRC)

        assert _land_pool(db, _source_lvol(), TGT, SRC, counterpart_hint="") == "POOL_DEST"


@pytest.fixture
def target(db):
    _cluster(db, TGT)
    _pool(db, "POOL_TGT", TGT)
    node = StorageNode()
    node.uuid = NODE
    node.cluster_id = TGT
    node.lvstore = "LVS_1"
    node.status = StorageNode.STATUS_ONLINE
    node.write_to_db(db.kv_store)
    return node


@pytest.fixture
def add_lvol_ha():
    with patch.object(lvol_controller, "add_lvol_ha", return_value=("NEW_LANDING", None)) as add:
        yield add


@pytest.fixture
def delete_lvol():
    with patch.object(lvol_controller, "delete_lvol") as delete:
        yield delete


def _landing(db, name="REP_snap-1", status=LVol.STATUS_ONLINE, size=1 << 30):
    lvol = LVol()
    lvol.uuid = "1b6f4a8e-1c2d-4e3f-9a0b-1c2d3e4f5a50"
    lvol.lvol_name = name
    lvol.pool_uuid = "POOL_TGT"
    lvol.status = status
    lvol.size = size
    lvol.lvs_name = "LVS_1"
    lvol.write_to_db(db.kv_store)
    return lvol


class TestPrepareLanding:

    def test_creates_the_landing_volume_with_the_sources_subsystem_packing(self, db, target,
                                                                           add_lvol_ha):
        """Regression 2026-09-30: a landing copy created without the source's
        packing defaulted to a one-namespace subsystem, so every copy and every
        clone of it got a subsystem of its own."""
        answer = service.prepare_landing(TGT, _request(max_namespace_per_subsys=10))

        assert answer.state == "ready" and answer.lvol_id == "NEW_LANDING" and not answer.adopted
        args, kwargs = add_lvol_ha.call_args
        assert args == ("REP_snap-1", 1 << 30, NODE, "ha", "POOL_TGT")
        assert kwargs["internal"] is True
        assert kwargs["namespaced"] is True and kwargs["max_namespace_per_subsys"] == 10

    def test_a_failed_create_is_refused_with_its_cause(self, db, target, add_lvol_ha):
        add_lvol_ha.return_value = (None, "Storage node is not online")

        with pytest.raises(service.LandingVolumeError, match="Storage node is not online"):
            service.prepare_landing(TGT, _request())

    def test_adopts_a_usable_volume_an_earlier_attempt_left(self, db, target, add_lvol_ha):
        """Case 6 (run 20260824_144226): a landing volume whose id never reached
        the task made every retry fail on "LVol name must be unique"."""
        left = _landing(db)

        answer = service.prepare_landing(TGT, _request())

        assert (answer.state, answer.lvol_id, answer.adopted) == ("ready", left.get_id(), True)
        add_lvol_ha.assert_not_called()

    def test_waits_for_a_leftover_still_deleting(self, db, target, add_lvol_ha, delete_lvol):
        _landing(db, status=LVol.STATUS_IN_DELETION)

        answer = service.prepare_landing(TGT, _request())

        assert answer.state == "retry" and "still deleting" in answer.reason
        delete_lvol.assert_not_called()
        add_lvol_ha.assert_not_called()

    @pytest.mark.parametrize("status,size", [(LVol.STATUS_IN_CREATION, 1 << 30),
                                             (LVol.STATUS_ONLINE, 1 << 20)])
    def test_clears_a_half_created_or_resized_leftover(self, db, target, add_lvol_ha, delete_lvol,
                                                       status, size):
        left = _landing(db, status=status, size=size)

        answer = service.prepare_landing(TGT, _request())

        assert answer.state == "retry" and "cleared" in answer.reason
        (deleted,), kwargs = delete_lvol.call_args
        assert deleted.get_id() == left.get_id() and kwargs == {"force_delete": True}
        add_lvol_ha.assert_not_called()

    def test_links_a_copy_of_the_same_data_on_the_lvstore(self, db, target, add_lvol_ha):
        """Incident 2026-09-29: after a relocate the chain came FROM the
        destination; its originals are linked, not shipped back in full."""
        def copy(uuid, lvs, status=SnapShot.STATUS_ONLINE):
            snapshot = SnapShot()
            snapshot.uuid = uuid
            snapshot.cluster_id = TGT
            snapshot.data_uuid = "D-1"
            snapshot.status = status
            snapshot.lvol = LVol()
            snapshot.lvol.lvs_name = lvs
            snapshot.write_to_db(db.kv_store)
            return snapshot

        copy("1b6f4a8e-1c2d-4e3f-9a0b-1c2d3e4f5a61", "LVS_7")
        copy("1b6f4a8e-1c2d-4e3f-9a0b-1c2d3e4f5a62", "LVS_1", status=SnapShot.STATUS_IN_DELETION)
        same = copy("1b6f4a8e-1c2d-4e3f-9a0b-1c2d3e4f5a63", "LVS_1")

        answer = service.prepare_landing(TGT, _request(data_uuid="D-1"))

        assert (answer.state, answer.counterpart_snapshot_id, answer.lvs_name) == (
            "counterpart", same.get_id(), "LVS_1")
        add_lvol_ha.assert_not_called()

    def test_create_only_leaves_a_taken_name_alone(self, db, target, add_lvol_ha, delete_lvol):
        left = _landing(db, status=LVol.STATUS_IN_CREATION)

        answer = service.prepare_landing(TGT, _request(create_only=True))

        assert (answer.state, answer.lvol_id) == ("exists", left.get_id())
        delete_lvol.assert_not_called()
        add_lvol_ha.assert_not_called()

    def test_a_node_hint_of_another_cluster_is_not_found(self, db, target):
        _cluster(db, OTHER)

        with pytest.raises(KeyError):
            service.prepare_landing(OTHER, _request())

    def test_without_a_node_hint_the_destination_picks_one(self, db, target, add_lvol_ha):
        """A source on another control plane cannot choose a node here."""
        with patch.object(lvol_controller, "_pick_replication_node", return_value=target) as pick:
            service.prepare_landing(TGT, _request(node_hint=""))

        pick.assert_called_once()
        assert add_lvol_ha.call_args.args[2] == NODE


TP, TS, TT = (f"1b6f4a8e-1c2d-4e3f-9a0b-1c2d3e4f5a7{i}" for i in (1, 2, 3))
HUB = HubAddress(bdev_name="LVS_T1/transferhub_TS", remote_bdev_name="LVS_T1/transferhub_TSn1",
                 nqn="nqn:transferhub:LVS_T1:TS", port=4430,
                 paths=[HubPath(ip="10.0.0.2", trtype="TCP")])


@pytest.fixture
def lvstore(db):
    """A target lvstore with primary TP, secondary TS, tertiary TT, and a
    landing volume on it."""
    _cluster(db, TGT)
    _pool(db, "POOL_TGT", TGT)
    for node_id, secondary in ((TP, TS), (TS, ""), (TT, "")):
        node = StorageNode()
        node.uuid = node_id
        node.cluster_id = TGT
        node.status = StorageNode.STATUS_ONLINE
        node.secondary_node_id = secondary
        node.write_to_db(db.kv_store)
    landing = _landing(db)
    landing.node_id, landing.nodes, landing.top_bdev = TP, [TP, TS, TT], "LVS_1/REP_snap-1"
    landing.write_to_db(db.kv_store)
    service._SETTLED_LEADER.clear()
    return landing


@pytest.fixture
def spdk():
    """What the members' SPDK reports about the landing volume (an RPC each)."""
    rpc = MagicMock()
    rpc.bdev_get.return_value = {"driver_specific": {"lvol": {"map_id": 7, "snapshot": False}}}
    with patch.object(StorageNode, "rpc_client", return_value=rpc), \
            patch.object(service.time, "sleep"):
        yield rpc


@pytest.fixture
def receiving_hub():
    with patch("simplyblock_core.services.replication_final_step.prepare_receiving_hub",
               return_value=(HUB, None)) as prepare:
        yield prepare


def _leads(*node_ids):
    """SPDK's "lvs leadership" flag (an RPC to the node) set on *node_ids*."""
    return patch.object(lvol_controller, "is_node_leader",
                        side_effect=lambda node, lvs: node.get_id() in node_ids)


def _ensure(landing):
    return service.ensure_hub(TGT, EnsureHubRequest(landing_lvol_id=landing.get_id()))


class TestEnsureHub:

    def test_a_secondary_leader_receives_through_its_hub_on_the_target_lvstore(
            self, db, lvstore, spdk, receiving_hub):
        with _leads(TS):
            answer = _ensure(lvstore)

        assert (answer.state, answer.leader_node_id, answer.map_id) == ("ready", TS, 7)
        assert answer.hub == HUB
        leader, lvs_primary = receiving_hub.call_args.args
        assert (leader.get_id(), lvs_primary.get_id()) == (TS, TP)

    def test_the_primary_receives_through_its_own_hub(self, db, lvstore, spdk, receiving_hub):
        with _leads(TP):
            answer = _ensure(lvstore)

        assert answer.state == "ready"
        leader, lvs_primary = receiving_hub.call_args.args
        assert (leader.get_id(), lvs_primary.get_id()) == (TP, TP)

    def test_a_tertiary_leader_waits(self, db, lvstore, spdk, receiving_hub):
        """SPDK never demuxes hub IO on a tertiary (node_role check in
        vbdev_lvol_submit_request); until the fork allows it, the start waits."""
        with _leads(TT):
            answer = _ensure(lvstore)

        assert answer.state == "unsettled" and "tertiary" in answer.reason
        receiving_hub.assert_not_called()

    @pytest.mark.parametrize("leaders", [(), (TP, TS)])
    def test_no_single_leader_is_waited_out_never_recovered(self, db, lvstore, spdk,
                                                            receiving_hub, leaders):
        """Replication only connects to an existing leader; it never claims,
        grants or forces leadership (bf8 2026-11-02)."""
        with _leads(*leaders), \
                patch("simplyblock_core.storage_node_ops.find_leader_with_failover") as recover:
            answer = _ensure(lvstore)

        assert answer.state == "unsettled" and "not established" in answer.reason
        recover.assert_not_called()
        receiving_hub.assert_not_called()

    def test_a_landing_volume_already_converted_is_reported(self, db, lvstore, spdk, receiving_hub):
        spdk.bdev_get.return_value = {"driver_specific": {"lvol": {"map_id": 7, "snapshot": True}}}

        with _leads(TP):
            answer = _ensure(lvstore)

        assert answer.state == "converted" and set(answer.converted_on) == {TP, TS, TT}

    def test_a_missing_map_id_is_a_failure_retried_without_backoff(self, db, lvstore, spdk,
                                                                    receiving_hub):
        spdk.bdev_get.return_value = {"driver_specific": {"lvol": {}}}

        with _leads(TP):
            answer = _ensure(lvstore)

        assert (answer.state, answer.backoff) == ("failed", False)
        assert "map_id" in answer.reason

    def test_a_hub_that_cannot_be_made_is_a_failure(self, db, lvstore, spdk, receiving_hub):
        receiving_hub.return_value = (None, "Failed to create transfer hub")

        with _leads(TP):
            answer = _ensure(lvstore)

        assert answer.state == "failed" and "Failed to create transfer hub" in answer.reason


def _predecessor_copy(db, nodes):
    copy = SnapShot()
    copy.uuid = "1b6f4a8e-1c2d-4e3f-9a0b-1c2d3e4f5a80"
    copy.cluster_id = TGT
    copy.snap_bdev = "LVS_1/SNAP_1"
    copy.lvol = LVol()
    copy.lvol.node_id, copy.lvol.nodes = nodes[0], list(nodes)
    copy.write_to_db(db.kv_store)
    return copy


def _chain(db, landing, copy, chained_on=()):
    return service.chain(TGT, ChainRequest(landing_lvol_id=landing.get_id(), leader_node_id=TP,
                                           predecessor_id=copy.get_id(), chained_on=list(chained_on)))


class TestChain:

    def test_chains_the_leader_and_its_secondary(self, db, lvstore, spdk):
        spdk.bdev_get.return_value = None
        spdk.bdev_lvol_add_clone.return_value = True
        copy = _predecessor_copy(db, [TP, TS, TT])

        answer = _chain(db, lvstore, copy)

        assert (answer.state, answer.chained_on) == ("chained", [TP, TS])
        assert spdk.bdev_lvol_add_clone.call_count == 2
        spdk.bdev_lvol_add_clone.assert_called_with("LVS_1/REP_snap-1", "LVS_1/SNAP_1")

    def test_a_member_already_chained_is_not_chained_again(self, db, lvstore, spdk):
        """add_clone is not idempotent in SPDK: the source's record, and SPDK's
        own base snapshot when an answer was lost, both count."""
        spdk.bdev_get.return_value = {"driver_specific": {"lvol": {"base_snapshot": "SNAP_1"}}}
        copy = _predecessor_copy(db, [TP, TS, TT])

        answer = _chain(db, lvstore, copy, chained_on=[TP])

        assert (answer.state, answer.chained_on) == ("chained", [TP, TS])
        spdk.bdev_lvol_add_clone.assert_not_called()

    def test_an_offline_secondary_declines_before_chaining_anything(self, db, lvstore, spdk):
        """The lvstore mirrors the transfer to the secondary; an unchained one
        would read zeros wherever a delta did not write."""
        secondary = db.get_storage_node_by_id(TS)
        secondary.status = StorageNode.STATUS_OFFLINE
        secondary.write_to_db(db.kv_store)
        copy = _predecessor_copy(db, [TP, TS, TT])

        answer = _chain(db, lvstore, copy)

        assert (answer.state, answer.chained_on) == ("declined", [])
        spdk.bdev_lvol_add_clone.assert_not_called()

    def test_a_failed_secondary_chain_still_reports_the_primary(self, db, lvstore, spdk):
        spdk.bdev_get.return_value = None
        spdk.bdev_lvol_add_clone.side_effect = [True, False]
        copy = _predecessor_copy(db, [TP, TS, TT])

        answer = _chain(db, lvstore, copy)

        assert (answer.state, answer.chained_on) == ("declined", [TP])

    def test_a_copy_in_another_lvstore_cannot_be_chained_onto(self, db, lvstore, spdk):
        copy = _predecessor_copy(db, ["1b6f4a8e-1c2d-4e3f-9a0b-1c2d3e4f5a99"])

        answer = _chain(db, lvstore, copy)

        assert answer.state == "unresolvable" and "across lvstores" in answer.reason
        spdk.bdev_lvol_add_clone.assert_not_called()

    def test_a_copy_that_is_gone_cannot_be_chained_onto(self, db, lvstore, spdk):
        missing = SnapShot()
        missing.uuid = "1b6f4a8e-1c2d-4e3f-9a0b-1c2d3e4f5a81"

        answer = _chain(db, lvstore, missing)

        assert answer.state == "unresolvable"


def _finish_request(landing, copy=None, **overrides):
    fields = dict(source_cluster_id=SRC, landing_lvol_id=landing.get_id(), link="source",
                  predecessor_id=copy.get_id() if copy is not None else "",
                  snapshot=SourceSnapshot(snapshot_id="1b6f4a8e-1c2d-4e3f-9a0b-1c2d3e4f5a90",
                                          snap_name="snap-2", size=1 << 30, data_uuid="D-2",
                                          group_id="G", group_seq=4))
    fields.update(overrides)
    return FinishRequest(**fields)


class TestFinish:

    def test_makes_the_landing_volume_the_linked_copy_and_retires_it(self, db, lvstore, spdk):
        spdk.bdev_get.return_value = None
        spdk.bdev_lvol_add_clone.return_value = True
        spdk.bdev_lvol_convert.return_value = True
        copy = _predecessor_copy(db, [TP, TS, TT])
        request = _finish_request(lvstore, copy)

        with _leads(TP), patch.object(lvol_controller, "delete_lvol_from_node") as teardown:
            answer = service.finish(TGT, request)

        assert answer.state == "done"
        assert answer.copy_id == service.copy_id_for(SRC, request.snapshot.snapshot_id,
                                                     lvstore.get_id())
        assert (answer.chained_on, answer.converted_on) == ([TP, TS], [TP, TS])
        recorded = db.get_snapshot_by_id(answer.copy_id)
        assert recorded.source_replicated_snap_uuid == request.snapshot.snapshot_id
        assert (recorded.prev_snap_uuid, recorded.cluster_id) == (copy.get_id(), TGT)
        assert (recorded.group_id, recorded.group_seq) == ("G", 4)
        assert answer.record["uuid"] == answer.copy_id
        assert db.get_snapshot_by_id(copy.get_id()).next_snap_uuid == answer.copy_id
        with pytest.raises(KeyError):
            db.get_lvol_by_id(lvstore.get_id())     # the landing volume is retired
        assert teardown.call_count == 3

    def test_a_repeated_finish_answers_with_the_recorded_copy(self, db, lvstore, spdk):
        """A lost answer, or a crash before the source recorded it: the copy's
        id is derived from the request, so nothing is written twice."""
        spdk.bdev_get.return_value = None
        spdk.bdev_lvol_convert.return_value = True
        request = _finish_request(lvstore)
        with _leads(TP), patch.object(lvol_controller, "delete_lvol_from_node"):
            first = service.finish(TGT, request)
        spdk.reset_mock()

        again = service.finish(TGT, request)

        assert (again.state, again.copy_id) == ("done", first.copy_id)
        spdk.bdev_lvol_convert.assert_not_called()

    def test_a_landing_volume_that_is_gone_is_reported(self, db, lvstore, spdk):
        lvstore.remove(db.kv_store)

        answer = service.finish(TGT, _finish_request(lvstore))

        assert answer.state == "gone"

    def test_an_instance_copy_carries_no_links(self, db, lvstore, spdk):
        spdk.bdev_get.return_value = None
        spdk.bdev_lvol_add_clone.return_value = True
        spdk.bdev_lvol_convert.return_value = True
        copy = _predecessor_copy(db, [TP, TS, TT])
        with _leads(TP), patch.object(lvol_controller, "delete_lvol_from_node"):
            answer = service.finish(TGT, _finish_request(lvstore, copy, link="none"))

        recorded = db.get_snapshot_by_id(answer.copy_id)
        assert not recorded.source_replicated_snap_uuid and not recorded.prev_snap_uuid
