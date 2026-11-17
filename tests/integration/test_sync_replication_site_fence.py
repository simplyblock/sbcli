"""The site-level fence of a sync-replication cluster (design-stretched §10.2,
P0-2), against the real FoundationDB: ``fence_site`` queues one
FN_SYNC_SITE_FENCE task per site, whose runner (the promote runner) runs the
disaster site steps once for the whole site and moves no leadership; the
answers while the proof refuses, while the task runs and once the site is
done; the unfence confirmation; the site record; and the v2 routes of the
production app.

The layout, the liveness evidence of the lost site and the stateful nvmf
fake are the disaster suite's (test_sync_replication_disaster.py): site A
(a0 a1 a2) lost, site B (b0 b1 b2) surviving, LVS_1 and LVS_2 led from A with
two volumes each. The HTTP mapping alone is pinned by
tests/unit/web/api/v2/test_site_endpoints.py.
"""
from unittest.mock import patch

import pytest

from simplyblock_core.controllers import sync_replication_controller as src
from simplyblock_core.exceptions import (
    SyncReplicationSiteError,
    SyncReplicationUnsupportedError,
    SyncSiteFenceRefusedError,
)
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.services import tasks_runner_sync_promote as runner
from tests.integration import test_sync_replication_demote_promote as dp
from tests.integration import test_sync_replication_disaster as disaster
from tests.integration import test_sync_replication_lvol_publish as publish
from tests.integration import test_sync_replication_lvs_stack as stack

SITE_A, SITE_B = stack.SITE_A, stack.SITE_B

db = stack.db
spdk = publish.spdk
evidence = disaster.evidence
moves = dp.moves
web_client = publish.web_client
_clean_leader_caches = publish._clean_leader_caches
_Lost = disaster._Lost
_record = disaster._record
_fresh, _update = stack._fresh, stack._update


def _fence(lost):
    return src.fence_site(lost.cluster.get_id(), SITE_A)


def _fenced(lost):
    """Queue the fence of A and run its task to the end; the task."""
    result = _fence(lost)
    assert not result.done
    assert lost.run(result.task_id)
    return lost.task(result.task_id)


# ---------------------------------------------------------------------------
# the call
# ---------------------------------------------------------------------------

class TestFenceCall:

    def test_queues_one_fence_task_and_changes_nothing(self, db, spdk, evidence):
        lost = _Lost(db, spdk)
        result = _fence(lost)
        assert not result.done and result.task_id
        task = lost.task(result.task_id)
        assert task.function_name == JobSchedule.FN_SYNC_SITE_FENCE
        assert task.function_params["site"] == SITE_A
        assert _record(db, lost.cluster.get_id()) == ("", "")
        assert evidence.calls == []                      # the proof is the runner's

    def test_a_call_while_the_task_runs_answers_the_same_task(self, db, spdk, evidence):
        lost = _Lost(db, spdk)
        first = _fence(lost)
        assert _fence(lost) == first
        assert len(db.get_job_tasks_by_function(lost.cluster.get_id(),
                                                JobSchedule.FN_SYNC_SITE_FENCE)) == 1

    @pytest.mark.parametrize("status", [StorageNode.STATUS_ONLINE, StorageNode.STATUS_DOWN,
                                        StorageNode.STATUS_RESTARTING])
    def test_a_node_of_the_site_that_may_run_refuses(self, db, spdk, evidence, status):
        lost = _Lost(db, spdk)
        node = _update(db, lost.a[2], status=status)
        with pytest.raises(SyncSiteFenceRefusedError, match=f"{node.get_id()} \\({status}\\)"):
            _fence(lost)
        assert db.get_job_tasks_by_function(lost.cluster.get_id(), JobSchedule.FN_SYNC_SITE_FENCE) == []

    def test_an_unknown_site_is_refused(self, db, spdk, evidence):
        lost = _Lost(db, spdk)
        with pytest.raises(SyncReplicationSiteError):
            src.fence_site(lost.cluster.get_id(), "site-x")

    def test_a_cluster_without_sync_replication_is_refused(self, db):
        cluster = stack._seed_cluster(db)
        cluster.sync_replication = False
        cluster.write_to_db(db.kv_store)
        with pytest.raises(SyncReplicationUnsupportedError):
            src.fence_site(cluster.get_id(), SITE_A)

    def test_another_site_recorded_lost_refuses(self, db, spdk, evidence):
        lost = _Lost(db, spdk)
        assert src.set_lost_site_state(lost.cluster.get_id(), SITE_B, Cluster.LOST_SITE_DOWN, ("",))
        with pytest.raises(SyncSiteFenceRefusedError, match=f"site {SITE_B} is already lost"):
            _fence(lost)

    def test_the_monitors_drain_refuses(self, db, spdk, evidence):
        lost = _Lost(db, spdk)
        lost.record(Cluster.LOST_SITE_DRAINING)
        with pytest.raises(SyncSiteFenceRefusedError, match="being shut down by the storage-node monitor"):
            _fence(lost)

    def test_a_site_being_activated_refuses(self, db, spdk, evidence):
        lost = _Lost(db, spdk)
        lost.record(Cluster.LOST_SITE_DOWN)
        assert src.begin_site_activation(lost.cluster.get_id(), SITE_A)
        with pytest.raises(SyncSiteFenceRefusedError, match=f"site {SITE_A} is being activated"):
            _fence(lost)

    def test_no_surviving_site_refuses(self, db, spdk, evidence):
        lost = _Lost(db, spdk)
        for node in lost.b:
            _update(db, node, status=StorageNode.STATUS_OFFLINE)
        with pytest.raises(SyncSiteFenceRefusedError, match="no other site has an online node"):
            _fence(lost)


# ---------------------------------------------------------------------------
# the task
# ---------------------------------------------------------------------------

class TestFenceTask:

    def test_runs_the_site_steps_once_and_moves_nothing(self, db, spdk, evidence, moves):
        lost = _Lost(db, spdk)
        task = _fenced(lost)
        assert task.status == JobSchedule.STATUS_DONE
        assert task.function_result == f"fenced site {SITE_A}"
        assert _record(db, lost.cluster.get_id()) == (SITE_A, Cluster.LOST_SITE_DONE)
        # every device of A unavailable, in the DB and in every distrib of B
        devices = lost.site_devices(lost.a)
        assert {d.status for d in devices} == {NVMeDevice.STATUS_UNAVAILABLE}
        for node in lost.b:
            events = lost.events_to(spdk, node)
            assert {events[d.cluster_device_order] for d in devices} == {NVMeDevice.STATUS_UNAVAILABLE}
        assert {_fresh(db, n).status for n in lost.a} == {StorageNode.STATUS_OFFLINE}
        # every volume of every store led from A fenced there - none excepted
        assert {n: lost.vol(n).sync_demoted_sites for n in lost.vols} == {
            n: [SITE_A] for n in lost.vols}
        assert {n: lost.vol(n).sync_active_site for n in lost.vols} == {n: SITE_A for n in lost.vols}
        # no leadership moved, nothing marked
        assert moves.calls == []
        assert _fresh(db, lost.owner1).lvs_active_site == ""
        assert _fresh(db, lost.owner2).lvs_active_site == ""
        assert db.get_sync_promote_task(lost.cluster.get_id(), "LVS_1") is None

    def test_done_answers_done_and_queues_nothing(self, db, spdk, evidence, moves):
        lost = _Lost(db, spdk)
        _fenced(lost)
        assert _fence(lost) == src.SiteFenceResult(done=True)
        assert len(db.get_job_tasks_by_function(lost.cluster.get_id(),
                                                JobSchedule.FN_SYNC_SITE_FENCE)) == 1

    def test_a_retry_from_fencing_completes(self, db, spdk, evidence, moves):
        lost = _Lost(db, spdk)
        assert src.set_lost_site_state(lost.cluster.get_id(), SITE_A, Cluster.LOST_SITE_FENCING, ("",))
        assert _fenced(lost).function_result == f"fenced site {SITE_A}"
        assert _record(db, lost.cluster.get_id()) == (SITE_A, Cluster.LOST_SITE_DONE)

    def test_a_monitor_record_down_is_fenced(self, db, spdk, evidence, moves):
        lost = _Lost(db, spdk)
        lost.record(Cluster.LOST_SITE_DOWN)
        _fenced(lost)
        assert _record(db, lost.cluster.get_id()) == (SITE_A, Cluster.LOST_SITE_DONE)

    def test_a_refused_proof_is_reported_once_then_retried(self, db, spdk, evidence, moves):
        lost = _Lost(db, spdk)
        node = _update(db, lost.a[2], status=StorageNode.STATUS_UNREACHABLE)
        evidence.alive.add(node.get_id())                 # its node API reports SPDK up
        failed = _fenced(lost)
        assert f"node {node.get_id()}: the node API reports the SPDK process up" in failed.function_result
        assert _record(db, lost.cluster.get_id()) == ("", "")
        assert {d.status for d in lost.site_devices(lost.a)} == {NVMeDevice.STATUS_ONLINE}
        with pytest.raises(SyncSiteFenceRefusedError) as refused:
            _fence(lost)
        assert refused.value.task_id == failed.uuid
        assert "the node API reports the SPDK process up" in str(refused.value)
        # reported once: the next call queues a new attempt
        evidence.alive.discard(node.get_id())
        again = _fence(lost)
        assert not again.done and again.task_id != failed.uuid
        assert lost.run(again.task_id)
        assert _record(db, lost.cluster.get_id()) == (SITE_A, Cluster.LOST_SITE_DONE)

    def test_the_drain_starting_under_the_task_fails_it(self, db, spdk, evidence, moves):
        lost = _Lost(db, spdk)
        result = _fence(lost)
        lost.record(Cluster.LOST_SITE_DRAINING)
        lost.run(result.task_id)
        assert "being shut down by the monitor" in lost.task(result.task_id).function_result
        assert _record(db, lost.cluster.get_id()) == (SITE_A, Cluster.LOST_SITE_DRAINING)

    def test_a_forced_promote_after_the_fence_moves_without_fencing_again(
            self, db, spdk, evidence, moves):
        lost = _Lost(db, spdk)
        _fenced(lost)
        sent = {n.get_id(): spdk[n.get_id()].distr_status_events_update.call_count for n in lost.b}
        result = lost.promote("v1a")
        with patch.object(runner, "_site_loss_problems", wraps=runner._site_loss_problems) as proof:
            assert lost.run(result.task_id)
        proof.assert_not_called()
        assert {n.get_id(): spdk[n.get_id()].distr_status_events_update.call_count
                for n in lost.b} == sent
        assert lost.vol("v1a").sync_active_site == SITE_B
        assert moves.calls == [("LVS_1", None, lost.b[0].get_id(), "moving:" + SITE_B, SITE_B)]

    def test_a_promote_task_does_not_take_a_fence_task(self, db, spdk, evidence, moves):
        lost = _Lost(db, spdk)
        result = _fence(lost)
        assert db.get_active_sync_promote_tasks(lost.cluster.get_id()) == []
        assert lost.task(result.task_id).function_name in runner.SPEC.function_names


# ---------------------------------------------------------------------------
# unfence and the record
# ---------------------------------------------------------------------------

class TestUnfenceAndRecord:

    @pytest.mark.parametrize("state", [Cluster.LOST_SITE_DRAINING, Cluster.LOST_SITE_DOWN,
                                       Cluster.LOST_SITE_FENCING, Cluster.LOST_SITE_DONE])
    def test_unfence_refuses_while_the_site_is_recorded(self, db, spdk, evidence, state):
        lost = _Lost(db, spdk)
        disaster._seed_record(db, lost.cluster, SITE_A, state)
        with pytest.raises(SyncSiteFenceRefusedError, match=f"still records site {SITE_A}"):
            src.confirm_site_unfenced(lost.cluster.get_id(), SITE_A)
        assert _record(db, lost.cluster.get_id()) == (SITE_A, state)       # nothing done

    def test_unfence_confirms_a_site_not_recorded(self, db, spdk, evidence):
        lost = _Lost(db, spdk)
        disaster._seed_record(db, lost.cluster, SITE_B, Cluster.LOST_SITE_DONE)
        record = src.confirm_site_unfenced(lost.cluster.get_id(), SITE_A)
        assert (record.site, record.lost, record.state) == (SITE_A, False, "")

    def test_unfence_of_an_unknown_site_is_refused(self, db, spdk, evidence):
        lost = _Lost(db, spdk)
        with pytest.raises(SyncReplicationSiteError):
            src.confirm_site_unfenced(lost.cluster.get_id(), "site-x")

    def test_the_record_of_a_fenced_site(self, db, spdk, evidence, moves):
        lost = _Lost(db, spdk)
        task = _fenced(lost)
        record = src.site_record(lost.cluster.get_id(), SITE_A)
        assert (record.site, record.lost, record.state) == (SITE_A, True, Cluster.LOST_SITE_DONE)
        assert record.fence_task_id == task.uuid
        assert {(n.node_id, n.status) for n in record.nodes} == {
            (n.get_id(), StorageNode.STATUS_OFFLINE) for n in lost.a}
        other = src.site_record(lost.cluster.get_id(), SITE_B)
        assert (other.lost, other.state, other.fence_task_id) == (False, "", "")
        assert {n.node_id for n in other.nodes} == {n.get_id() for n in lost.b}


# ---------------------------------------------------------------------------
# the v2 routes of the production app
# ---------------------------------------------------------------------------

def _url(lost, site, action):
    return f"/api/v2/clusters/{lost.cluster.get_id()}/sites/{site}/{action}"


class TestRoutes:

    def test_fence_202_then_200(self, db, spdk, evidence, moves, web_client):
        lost = _Lost(db, spdk)
        response = web_client.post(_url(lost, SITE_A, "fence"))
        assert response.status_code == 202, response.text
        task_id = response.json()["task_id"]
        assert response.headers["Location"].endswith(f"/tasks/{task_id}/")
        assert web_client.post(_url(lost, SITE_A, "fence")).json()["task_id"] == task_id
        assert lost.run(task_id)
        response = web_client.post(_url(lost, SITE_A, "fence"))
        assert response.status_code == 200, response.text
        body = response.json()
        assert (body["site"], body["lost"], body["state"]) == (SITE_A, True, "done")
        assert body["fence_task_id"] == task_id
        # the task is readable through the task route
        response = web_client.get(f"/api/v2/clusters/{lost.cluster.get_id()}/tasks/{task_id}/")
        assert response.status_code == 200, response.text
        assert response.json()["function_name"] == JobSchedule.FN_SYNC_SITE_FENCE

    def test_fence_409_while_a_node_may_run(self, db, spdk, evidence, web_client):
        lost = _Lost(db, spdk)
        node = _update(db, lost.a[0], status=StorageNode.STATUS_ONLINE)
        response = web_client.post(_url(lost, SITE_A, "fence"))
        assert response.status_code == 409, response.text
        assert node.get_id() in response.json()["detail"]["message"]

    def test_fence_409_with_the_failed_task_once(self, db, spdk, evidence, moves, web_client):
        lost = _Lost(db, spdk)
        node = _update(db, lost.a[2], status=StorageNode.STATUS_UNREACHABLE)
        evidence.alive.add(node.get_id())
        task_id = web_client.post(_url(lost, SITE_A, "fence")).json()["task_id"]
        lost.run(task_id)
        response = web_client.post(_url(lost, SITE_A, "fence"))
        assert response.status_code == 409, response.text
        assert response.json()["detail"]["task_id"] == task_id
        assert web_client.post(_url(lost, SITE_A, "fence")).status_code == 202

    def test_fence_400_for_an_unknown_site(self, db, spdk, evidence, web_client):
        lost = _Lost(db, spdk)
        assert web_client.post(_url(lost, "site-x", "fence")).status_code == 400

    def test_unfence_409_while_recorded_then_200(self, db, spdk, evidence, moves, web_client):
        lost = _Lost(db, spdk)
        lost.run(web_client.post(_url(lost, SITE_A, "fence")).json()["task_id"])
        response = web_client.post(_url(lost, SITE_A, "unfence"))
        assert response.status_code == 409, response.text
        assert SITE_A in response.json()["detail"]["message"]
        disaster._seed_record(db, lost.cluster, "", "")            # the site returned
        response = web_client.post(_url(lost, SITE_A, "unfence"))
        assert response.status_code == 200, response.text
        assert (response.json()["lost"], response.json()["state"]) == (False, "")

    def test_status(self, db, spdk, evidence, web_client):
        lost = _Lost(db, spdk)
        lost.record(Cluster.LOST_SITE_DOWN)
        response = web_client.get(_url(lost, SITE_A, "status"))
        assert response.status_code == 200, response.text
        body = response.json()
        assert (body["site"], body["lost"], body["state"], body["fence_task_id"]) == (
            SITE_A, True, "down", None)
        assert {n["id"] for n in body["nodes"]} == {n.get_id() for n in lost.a}
        assert {n["mgmt_ip"] for n in body["nodes"]} == {n.mgmt_ip for n in lost.a}

    def test_a_cluster_without_sync_replication_is_400(self, db, web_client):
        cluster = stack._seed_cluster(db)
        cluster.sync_replication = False
        cluster.write_to_db(db.kv_store)
        for action, method in (("fence", "post"), ("unfence", "post"), ("status", "get")):
            response = getattr(web_client, method)(
                f"/api/v2/clusters/{cluster.get_id()}/sites/{SITE_A}/{action}")
            assert response.status_code == 400, (action, response.text)


class TestSiteReturnWaitsForTheFence:
    """The monitor's site gate holds a site's return while a fence task of
    the cluster is open, as it does for a promote task: the fence sets the
    site's nodes offline after its proof, which would race the activation
    rebuilding them."""

    def test_an_open_fence_task_holds_the_return(self, db, spdk, evidence):
        from simplyblock_core.services import storage_node_monitor as monitor
        lost = _Lost(db, spdk)
        lost.record(Cluster.LOST_SITE_DOWN)
        _fence(lost)
        cluster = lost.cluster_now()
        problem = monitor._site_return_problem(cluster, db.get_storage_nodes_by_cluster_id(cluster.get_id()))
        assert problem == "a sync site fence task is not done"
