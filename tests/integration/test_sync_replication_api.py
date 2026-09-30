"""The sync-replication API surface against the real FoundationDB: the async
snapshot-replication operations a sync-replication cluster refuses (the
direct controller calls, and the v2 routes of the production app), the
consistency-group status, and the status routes end to end.

The live status query (``cluster_sync_status``, tested in
test_sync_replication_status.py) is mocked at its boundary; the HTTP contract
of every route is pinned by tests/unit/web/api/v2/test_sync_replication_routes.py.
"""
import uuid
from datetime import UTC, datetime
from unittest.mock import patch

import pytest

from simplyblock_core import constants
from simplyblock_core.controllers import lvol_controller, replication_policy_controller
from simplyblock_core.controllers import sync_replication_controller as src
from simplyblock_core.exceptions import (
    SyncGroupMemberError, SyncReplicationSiteError, SyncReplicationUnsupportedError,
)
from simplyblock_core.models.replication import ConsistencyGroup
from tests.integration import test_sync_replication_lvol_publish as publish
from tests.integration import test_sync_replication_lvs_stack as stack

SITE_A, SITE_B = stack.SITE_A, stack.SITE_B
NOW = datetime(2026, 9, 30, 12, 0, tzinfo=UTC)

db = stack.db
no_rpc = publish.no_rpc
web_client = publish.web_client
_layout, _pool, _volume = publish._layout, publish._pool, publish._volume


# ---------------------------------------------------------------------------
# seeding
# ---------------------------------------------------------------------------

def _sync_volume(db, **fields):
    cluster, a, b, owner = _layout(db)
    vol = _volume(db, cluster, owner, _pool(db, cluster))
    return cluster, owner, _set(db, vol, **fields)


def _async_volume(db, **fields):
    cluster = stack._seed_cluster(db)
    cluster.sync_replication = False
    cluster.write_to_db(db.kv_store)
    node = stack._seed_node(db, cluster, "n0", "")
    vol = _volume(db, cluster, node, _pool(db, cluster), active_site="", nodes=[node.get_id()])
    return cluster, node, _set(db, vol, **fields)


def _set(db, vol, **fields):
    fresh = db.get_lvol_by_id(vol.get_id())
    for key, value in fields.items():
        setattr(fresh, key, value)
    fresh.write_to_db(db.kv_store)
    return fresh


def _record(db, vol):
    return db.get_lvol_by_id(vol.get_id()).to_dict()


def _group(db, cluster, members, *, removed=(), extra_ids=()):
    group = ConsistencyGroup()
    group.uuid = str(uuid.uuid4())
    group.cluster_id = cluster.get_id()
    group.group_name = f"cg-{group.uuid[:6]}"
    group.members = {m.get_id(): {"joined_seq": 1, "removed_seq": 0} for m in members}
    group.members.update({m.get_id(): {"joined_seq": 1, "removed_seq": 2} for m in removed})
    group.members.update({i: {"joined_seq": 1, "removed_seq": 0} for i in extra_ids})
    group.write_to_db(db.kv_store)
    return group


def _cluster_status():
    return src.ClusterSyncStatus(
        state=src.STATE_RESYNCING, degraded=False, resyncing=True, completed=True, peer_ready=False,
        diverged=True, last_replicated_at=NOW, lag_seconds=30, bytes_behind=8192, computed_at=NOW, lvs=())


@pytest.fixture()
def live_status():
    """The live query of every instance: answers a fixed cluster status."""
    with patch.object(src, "cluster_sync_status", return_value=_cluster_status()) as status:
        yield status


# The direct async operations, each as (name, call(volume id)).
_DIRECT = [
    ("start", lambda vid: lvol_controller.replication_start(vid, replication_cluster_id=str(uuid.uuid4()))),
    ("stop", lambda vid: lvol_controller.replication_stop(vid)),
    ("trigger", lambda vid: lvol_controller.replication_trigger(vid)),
    ("commit", lambda vid: lvol_controller.replication_commit(vid)),
    ("cutover-proceed", lambda vid: replication_policy_controller.set_cutover_proceed(vid)),
]


# ---------------------------------------------------------------------------
# the direct async operations on a sync cluster
# ---------------------------------------------------------------------------

class TestDirectAsyncOperationsRefused:

    @pytest.mark.parametrize("name, call", _DIRECT, ids=[n for n, _ in _DIRECT])
    def test_refused_before_anything_changes(self, db, no_rpc, name, call):
        # A replication node set: commit's own precondition would pass.
        cluster, owner, vol = _sync_volume(db, do_replicate=True, replication_node_id=str(uuid.uuid4()))
        before = _record(db, vol)

        with pytest.raises(SyncReplicationUnsupportedError, match="sync-replication"):
            call(vol.get_id())

        assert _record(db, vol) == before
        assert db.get_snapshots_by_lvol_id(vol.get_id()) == []
        assert db.get_job_tasks(cluster.get_id()) == []

    def test_policy_calls_are_not_guarded(self, db, no_rpc):
        """A policy's own start / stop (from_policy) runs as before: the guard
        never fires inside a policy chain that wrote before calling it."""
        cluster, owner, vol = _sync_volume(db, do_replicate=True)

        assert lvol_controller.replication_stop(vol.get_id(), from_policy=True) is True
        assert db.get_lvol_by_id(vol.get_id()).do_replicate is False
        # No replication target configured: its own precondition answers.
        assert lvol_controller.replication_start(vol.get_id(), from_policy=True) is False
        assert db.get_lvol_by_id(vol.get_id()).do_replicate is True

    @pytest.mark.parametrize("call", [
        lambda: lvol_controller.replication_start("no-such-volume"),
        lambda: lvol_controller.replication_stop("no-such-volume"),
        lambda: lvol_controller.replication_commit("no-such-volume"),
    ])
    def test_a_missing_volume_is_still_false(self, db, call):
        assert call() is False


class TestDirectAsyncOperationsOnAnAsyncCluster:
    """The guard lets a cluster without sync replication through."""

    def test_stop(self, db, no_rpc):
        cluster, node, vol = _async_volume(db, do_replicate=True)

        assert lvol_controller.replication_stop(vol.get_id()) is True
        assert db.get_lvol_by_id(vol.get_id()).do_replicate is False

    def test_policy_managed_volume_still_refuses_the_raw_verbs(self, db, no_rpc):
        """Attaching a policy IS the way replication is started; calling the
        raw verb would let a volume run on settings that diverge from its
        policy."""
        cluster, node, vol = _async_volume(db, replication_policy_id="CL_SRC/P1", do_replicate=True)
        before = _record(db, vol)

        assert lvol_controller.replication_start(vol.get_id(), replication_cluster_id=str(uuid.uuid4())) is False
        assert lvol_controller.replication_stop(vol.get_id()) is False
        assert _record(db, vol) == before

    def test_commit_reaches_its_own_preconditions(self, db, no_rpc):
        cluster, node, vol = _async_volume(db)

        assert lvol_controller.replication_commit(vol.get_id()) is False  # no replication node

    def test_trigger_takes_its_snapshot(self, db, no_rpc):
        cluster, node, vol = _async_volume(db)

        class _Reached(Exception):
            pass

        with patch.object(lvol_controller.snapshot_controller, "add", side_effect=_Reached) as add, \
                pytest.raises(_Reached):
            lvol_controller.replication_trigger(vol.get_id())
        add.assert_called_once()

    def test_cutover_proceed_looks_for_its_relationship(self, db):
        cluster, node, vol = _async_volume(db)

        with pytest.raises(KeyError, match="No cutover_pending replication"):
            replication_policy_controller.set_cutover_proceed(vol.get_id())


# ---------------------------------------------------------------------------
# the production app
# ---------------------------------------------------------------------------

def _volume_url(cluster, vol):
    return f"/api/v2/clusters/{cluster.get_id()}/storage-pools/{vol.pool_uuid}/volumes/{vol.get_id()}/"


class TestRoutes:

    @pytest.mark.parametrize("path", ["start", "stop", "trigger", "commit", "cutover-proceed"])
    def test_async_operation_is_400(self, db, no_rpc, web_client, path):
        cluster, owner, vol = _sync_volume(db, do_replicate=True)
        before = _record(db, vol)

        response = web_client.post(_volume_url(cluster, vol) + "replication/" + path)

        assert response.status_code == 400, response.text
        assert "sync-replication" in response.json()["detail"]["message"]
        assert _record(db, vol) == before
        assert db.get_job_tasks(cluster.get_id()) == []

    @pytest.mark.parametrize("site, role", [(SITE_A, "primary"), (SITE_B, "secondary")])
    def test_volume_sync_status(self, db, no_rpc, web_client, live_status, site, role):
        cluster, owner, vol = _sync_volume(db)

        response = web_client.get(_volume_url(cluster, vol) + f"replication/sync-status?site={site}")

        assert response.status_code == 200, response.text
        body = response.json()
        assert (body["site"], body["role"], body["state"], body["bytes_behind"], body["peer_ready"]) == \
            (site, role, "resyncing", 8192, False)
        live_status.assert_called_once_with(cluster.get_id(), max_age=constants.SYNC_STATUS_CACHE_SEC)

    def test_volume_status_unknown_site_is_400(self, db, no_rpc, web_client, live_status):
        cluster, owner, vol = _sync_volume(db)

        response = web_client.get(_volume_url(cluster, vol) + "replication/status?site=site-x")

        assert response.status_code == 400, response.text
        live_status.assert_not_called()

    def test_group_status(self, db, no_rpc, web_client, live_status):
        cluster, owner, vol = _sync_volume(db)
        other = _volume(db, cluster, owner, _pool(db, cluster))
        group = _group(db, cluster, [vol, other])

        response = web_client.get(
            f"/api/v2/clusters/{cluster.get_id()}/consistency-groups/{group.uuid}/replication/status"
            f"?site={SITE_A}")

        assert response.status_code == 200, response.text
        body = response.json()
        assert (body["role"], body["state"], body["resyncing"], body["member_count"],
                body["outstanding_bytes"]) == ("source", "degraded", True, 2, 8192)


# ---------------------------------------------------------------------------
# group_sync_status
# ---------------------------------------------------------------------------

class TestGroupSyncStatus:

    def test_empty_group_is_secondary(self, db, live_status):
        cluster, owner, vol = _sync_volume(db)
        group = _group(db, cluster, [], removed=[vol])

        status = src.group_sync_status(group.get_id(), SITE_A, max_age=3)

        assert (status.role, status.site, status.member_count) == (src.ROLE_SECONDARY, SITE_A, 0)
        assert status.cluster == _cluster_status()
        live_status.assert_called_once_with(cluster.get_id(), max_age=3)

    @pytest.mark.parametrize("site, role", [(SITE_A, src.ROLE_PRIMARY), (SITE_B, src.ROLE_SECONDARY)])
    def test_every_member_served_on_the_site(self, db, live_status, site, role):
        cluster, owner, vol = _sync_volume(db)
        other = _volume(db, cluster, owner, _pool(db, cluster))

        status = src.group_sync_status(_group(db, cluster, [vol, other]).get_id(), site)

        assert (status.role, status.member_count) == (role, 2)
        live_status.assert_called_once_with(cluster.get_id(), max_age=0.0)

    def test_mixed_roles_are_secondary_on_both_sites(self, db, live_status):
        cluster, owner, vol = _sync_volume(db)
        moved = _volume(db, cluster, owner, _pool(db, cluster), active_site=SITE_B)
        group = _group(db, cluster, [vol, moved])

        assert src.group_sync_status(group.get_id(), SITE_A).role == src.ROLE_SECONDARY
        assert src.group_sync_status(group.get_id(), SITE_B).role == src.ROLE_SECONDARY

    def test_a_removed_member_does_not_count(self, db, live_status):
        cluster, owner, vol = _sync_volume(db)
        gone = _volume(db, cluster, owner, _pool(db, cluster), active_site=SITE_B)

        status = src.group_sync_status(_group(db, cluster, [vol], removed=[gone]).get_id(), SITE_A)

        assert (status.role, status.member_count) == (src.ROLE_PRIMARY, 1)

    def test_a_missing_member_refuses(self, db, live_status):
        cluster, owner, vol = _sync_volume(db)
        missing = str(uuid.uuid4())
        group = _group(db, cluster, [vol], extra_ids=[missing])

        with pytest.raises(SyncGroupMemberError) as raised:
            src.group_sync_status(group.get_id(), SITE_A)
        assert raised.value.volumes == [missing]
        live_status.assert_not_called()

    def test_unknown_site(self, db, live_status):
        cluster, owner, vol = _sync_volume(db)

        with pytest.raises(SyncReplicationSiteError):
            src.group_sync_status(_group(db, cluster, [vol]).get_id(), "site-x")

    def test_not_a_sync_cluster(self, db, live_status):
        cluster, node, vol = _async_volume(db)

        with pytest.raises(SyncReplicationUnsupportedError):
            src.group_sync_status(_group(db, cluster, [vol]).get_id(), SITE_A)
        live_status.assert_not_called()


class TestVolumeSyncStatusById:

    def test_reads_the_stored_volume(self, db, live_status):
        cluster, owner, vol = _sync_volume(db)

        status = src.volume_sync_status_by_id(vol.get_id(), SITE_B)

        assert (status.role, status.site) == (src.ROLE_SECONDARY, SITE_B)

    def test_unknown_volume(self, db, live_status):
        with pytest.raises(KeyError):
            src.volume_sync_status_by_id(str(uuid.uuid4()), SITE_A)
