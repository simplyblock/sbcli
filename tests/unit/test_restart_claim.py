# coding=utf-8
"""Per-node restart claim: cross-ACTOR mutual exclusion for one node's restart.

Regression suite for the 2026-08-06 soak iter-50 incident: a manual CLI
`sn restart` and the restart task runner drove the SAME node's restart
concurrently — the task runner (force=True) sailed past every status guard,
`try_set_node_restarting`'s FDB tx only excluded PEERS (it skipped the target
node), and both actors' spdk_process_start calls replaced each other's SPDK
container mid-restart. The task lease could not discriminate them: both
actors share the one NODE_RESTART task, and the lease owner id (hostname)
can collide for a CLI and a runner service on the same mgmt host.

The claim is an (owner-token, timestamp) pair on the StorageNode row:
- acquired atomically inside `_try_set_node_restarting_tx` (refuses when the
  target is mid-transition under a FRESH claim held by anyone else, in both
  tx modes — force never bypasses it);
- heartbeated by the restart_storage_node wrapper, released on every exit;
- a stale claim (driver died) is takeover-able — the transferable-ownership
  resume path stays alive;
- `check_node_shutdown_preconditions` refuses (even with force) to shut down
  a node mid-transition under a live foreign claim;
- the wrapper's failure cleanup (_kill_spdk_until_dead + OFFLINE flip) runs
  only when THIS call actually holds the claim — a refused attempt must not
  destroy the rightful owner's in-flight restart;
- task_runner_node defers (no retry consumed) on a live foreign claim.
"""
import datetime
import json
from unittest.mock import MagicMock

import pytest

from simplyblock_core import constants, storage_node_ops
from simplyblock_core import db_controller as db_module
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.storage_node import StorageNode

import simplyblock_core.services.tasks_runner_restart as restart_runner


def _now():
    return datetime.datetime.now(datetime.UTC)


def _node(node_id="node-1", cluster_id="cl-1", status=StorageNode.STATUS_OFFLINE,
          claim_owner="", claim_age_sec=None):
    node = StorageNode()
    node.uuid = node_id
    node.cluster_id = cluster_id
    node.status = status
    node.restart_claim_owner = claim_owner
    if claim_age_sec is not None:
        node.restart_claim_ts = str(_now() - datetime.timedelta(seconds=claim_age_sec))
    return node


# --------------------------------------------------------------------------
# restart_claim_active
# --------------------------------------------------------------------------

def test_claim_active_empty_owner_is_no_conflict():
    assert db_module.restart_claim_active(_node()) is None


def test_claim_active_fresh_foreign_claim_returns_holder():
    node = _node(claim_owner="host-a:1:aa", claim_age_sec=5)
    assert db_module.restart_claim_active(node, "host-b:2:bb") == "host-a:1:aa"


def test_claim_active_own_token_is_no_conflict():
    node = _node(claim_owner="host-a:1:aa", claim_age_sec=5)
    assert db_module.restart_claim_active(node, "host-a:1:aa") is None


def test_claim_active_stale_claim_is_no_conflict():
    node = _node(claim_owner="host-a:1:aa",
                 claim_age_sec=constants.RESTART_CLAIM_TTL_SEC + 10)
    assert db_module.restart_claim_active(node) is None


def test_claim_active_unparseable_ts_is_no_conflict():
    node = _node(claim_owner="host-a:1:aa")
    node.restart_claim_ts = "not-a-timestamp"
    assert db_module.restart_claim_active(node) is None


def test_claim_active_naive_ts_treated_as_utc():
    node = _node(claim_owner="host-a:1:aa")
    node.restart_claim_ts = str(_now().replace(tzinfo=None))  # naive, fresh
    assert db_module.restart_claim_active(node) == "host-a:1:aa"


# --------------------------------------------------------------------------
# _try_set_node_restarting_tx — target-node mutual exclusion
# --------------------------------------------------------------------------

class _FakeTr(dict):
    def __setitem__(self, key, value):
        dict.__setitem__(self, key, value)


def _run_tx(monkeypatch, nodes, node_id="node-1", cluster_id="cl-1",
            allow_concurrent_peers=False, claim_owner="tok-new"):
    """Drive the raw tx function against an in-memory node set; returns
    ((acquired, reason), written_rows)."""
    tr = _FakeTr()

    def fake_read(self_model, _tr, id=None):
        if id is not None:
            return [n for n in nodes if n.get_id() == id]
        return list(nodes)

    monkeypatch.setattr(StorageNode, "read_from_db", fake_read)
    out = DBController._try_set_node_restarting_tx(
        MagicMock(), tr, cluster_id, node_id,
        allow_concurrent_peers=allow_concurrent_peers, claim_owner=claim_owner)
    written = {k: json.loads(v) for k, v in tr.items()}
    return out, written


def test_tx_refuses_fresh_foreign_claim_on_target(monkeypatch):
    target = _node(status=StorageNode.STATUS_RESTARTING,
                   claim_owner="cli:100:aa", claim_age_sec=3)
    (acquired, reason), written = _run_tx(monkeypatch, [target])
    assert acquired is False
    assert "cli:100:aa" in reason
    assert written == {}  # nothing written on refusal


def test_tx_refuses_fresh_foreign_claim_in_shutdown_state(monkeypatch):
    target = _node(status=StorageNode.STATUS_IN_SHUTDOWN,
                   claim_owner="cli:100:aa", claim_age_sec=3)
    (acquired, _), written = _run_tx(monkeypatch, [target])
    assert acquired is False
    assert written == {}


def test_tx_allows_takeover_of_stale_claim(monkeypatch):
    target = _node(status=StorageNode.STATUS_RESTARTING,
                   claim_owner="cli:100:aa",
                   claim_age_sec=constants.RESTART_CLAIM_TTL_SEC + 30)
    (acquired, reason), written = _run_tx(monkeypatch, [target])
    assert acquired is True, reason
    row = next(iter(written.values()))
    assert row["restart_claim_owner"] == "tok-new"
    assert row["status"] == StorageNode.STATUS_RESTARTING


def test_tx_allows_reacquire_by_same_owner(monkeypatch):
    target = _node(status=StorageNode.STATUS_RESTARTING,
                   claim_owner="tok-new", claim_age_sec=3)
    (acquired, _), written = _run_tx(monkeypatch, [target])
    assert acquired is True
    assert next(iter(written.values()))["restart_claim_owner"] == "tok-new"


def test_tx_ignores_leftover_claim_when_target_not_mid_transition(monkeypatch):
    # OFFLINE + leftover fresh claim: the claim only defends an ACTIVE
    # transition; a released/reset node is acquirable and the claim is
    # overwritten by the new owner.
    target = _node(status=StorageNode.STATUS_OFFLINE,
                   claim_owner="cli:100:aa", claim_age_sec=3)
    (acquired, _), written = _run_tx(monkeypatch, [target])
    assert acquired is True
    assert next(iter(written.values()))["restart_claim_owner"] == "tok-new"


def test_tx_peer_exclusion_still_enforced(monkeypatch):
    target = _node("node-1")
    peer = _node("node-2", status=StorageNode.STATUS_RESTARTING)
    (acquired, reason), written = _run_tx(monkeypatch, [target, peer])
    assert acquired is False
    assert "node-2" in reason
    assert written == {}


def test_tx_concurrent_peers_mode_still_enforces_target_claim(monkeypatch):
    # allow_concurrent_peers relaxes PEER exclusion (drained-suspension
    # parallel recovery) — never same-node exclusion.
    target = _node(status=StorageNode.STATUS_RESTARTING,
                   claim_owner="cli:100:aa", claim_age_sec=3)
    (acquired, _), written = _run_tx(monkeypatch, [target],
                                     allow_concurrent_peers=True)
    assert acquired is False
    assert written == {}


def test_tx_acquisition_stamps_claim_owner_and_fresh_ts(monkeypatch):
    target = _node(status=StorageNode.STATUS_OFFLINE)
    (acquired, _), written = _run_tx(monkeypatch, [target])
    assert acquired is True
    row = next(iter(written.values()))
    assert row["restart_claim_owner"] == "tok-new"
    age = (_now() - datetime.datetime.fromisoformat(row["restart_claim_ts"])).total_seconds()
    assert age < 10


# --------------------------------------------------------------------------
# refresh_node_restart_claim / release_node_restart_claim
# --------------------------------------------------------------------------

@pytest.fixture()
def db_with_node(monkeypatch):
    """A DBController whose get_storage_node_by_id / atomic_update operate on
    one in-memory node (atomic_update contract: mutate fresh object, return
    it; mutator returning False leaves the object unwritten — the CAS-refused
    branch — which for these owner-matched mutators is equivalent)."""
    ctrl = DBController()
    node = _node(status=StorageNode.STATUS_RESTARTING,
                 claim_owner="tok-a", claim_age_sec=100)
    monkeypatch.setattr(ctrl, "get_storage_node_by_id", lambda _id: node)
    monkeypatch.setattr(ctrl, "atomic_update", lambda obj, fn: (fn(obj), obj)[1])
    return ctrl, node


def test_refresh_claim_matching_owner_bumps_ts(db_with_node):
    ctrl, node = db_with_node
    old_ts = node.restart_claim_ts
    assert ctrl.refresh_node_restart_claim("node-1", "tok-a") is True
    assert node.restart_claim_ts != old_ts
    assert db_module.restart_claim_active(node, "tok-b") == "tok-a"


def test_refresh_claim_foreign_owner_refused(db_with_node):
    ctrl, node = db_with_node
    old_ts = node.restart_claim_ts
    assert ctrl.refresh_node_restart_claim("node-1", "tok-b") is False
    assert node.restart_claim_ts == old_ts


def test_refresh_claim_empty_owner_refused(db_with_node):
    ctrl, _ = db_with_node
    assert ctrl.refresh_node_restart_claim("node-1", "") is False


def test_release_claim_matching_owner_clears(db_with_node):
    ctrl, node = db_with_node
    assert ctrl.release_node_restart_claim("node-1", "tok-a") is True
    assert node.restart_claim_owner == ""
    assert node.restart_claim_ts == ""


def test_release_claim_foreign_owner_noop(db_with_node):
    ctrl, node = db_with_node
    assert ctrl.release_node_restart_claim("node-1", "tok-b") is False
    assert node.restart_claim_owner == "tok-a"


# --------------------------------------------------------------------------
# check_node_shutdown_preconditions — claim guard (force must NOT bypass)
# --------------------------------------------------------------------------

def _preconditions_env(monkeypatch, snode):
    fake_db = MagicMock()
    fake_db.get_storage_node_by_id.return_value = snode
    fake_db.get_storage_nodes_by_cluster_id.return_value = []
    fake_db.get_cluster_by_id.return_value = MagicMock(status="active")
    monkeypatch.setattr(storage_node_ops, "DBController", lambda: fake_db)
    tasks_ctrl = MagicMock()
    tasks_ctrl.get_active_node_restart_task.return_value = None
    tasks_ctrl.get_active_node_tasks.return_value = []
    monkeypatch.setattr(storage_node_ops, "tasks_controller", tasks_ctrl)
    return fake_db


def test_shutdown_refused_on_live_foreign_claim_even_with_force(monkeypatch):
    snode = _node(status=StorageNode.STATUS_RESTARTING,
                  claim_owner="cli:100:aa", claim_age_sec=3)
    _preconditions_env(monkeypatch, snode)
    allowed, reason = storage_node_ops.check_node_shutdown_preconditions(
        "node-1", force=True)
    assert allowed is False
    assert "claim" in reason


def test_shutdown_allowed_when_claim_stale(monkeypatch):
    snode = _node(status=StorageNode.STATUS_RESTARTING,
                  claim_owner="cli:100:aa",
                  claim_age_sec=constants.RESTART_CLAIM_TTL_SEC + 30)
    _preconditions_env(monkeypatch, snode)
    allowed, reason = storage_node_ops.check_node_shutdown_preconditions(
        "node-1", force=True)
    assert allowed is True, reason


def test_shutdown_ignores_leftover_claim_on_online_node(monkeypatch):
    snode = _node(status=StorageNode.STATUS_ONLINE,
                  claim_owner="cli:100:aa", claim_age_sec=3)
    _preconditions_env(monkeypatch, snode)
    allowed, reason = storage_node_ops.check_node_shutdown_preconditions(
        "node-1", force=False)
    assert allowed is True, reason


# --------------------------------------------------------------------------
# task_runner_node — defer (no retry consumed) on live foreign claim
# --------------------------------------------------------------------------

def _runner_task(retry=0, max_retry=5, status=JobSchedule.STATUS_RUNNING):
    task = JobSchedule()
    task.uuid = "task-1"
    task.function_name = JobSchedule.FN_NODE_RESTART
    task.node_id = "node-1"
    task.cluster_id = "cl-1"
    task.status = status
    task.retry = retry
    task.max_retry = max_retry
    task.canceled = False
    task.function_params = {}
    return task


def test_runner_defers_without_retry_on_live_foreign_claim(monkeypatch):
    claimed = _node(status=StorageNode.STATUS_RESTARTING,
                    claim_owner="cli:100:aa", claim_age_sec=3)
    task = _runner_task()

    claimed.data_nics = [MagicMock(ip4_address="10.0.0.2", if_name="eth0")]
    fake_db = MagicMock()
    fake_db.get_storage_node_by_id.return_value = claimed
    fake_db.get_task_by_id.return_value = task
    fake_db.get_cluster_by_id.return_value = MagicMock(
        status="active", suspend_drain_complete=False)
    fake_db.get_storage_nodes_by_cluster_id.return_value = []
    fake_db.atomic_update.side_effect = lambda obj, fn: (fn(obj), obj)[1]
    monkeypatch.setattr(restart_runner, "db", fake_db)
    monkeypatch.setattr(JobSchedule, "write_to_db", MagicMock())

    sops = MagicMock()
    sops.fd_dead_recovery_allowed.return_value = False
    monkeypatch.setattr(restart_runner, "storage_node_ops", sops)
    hc = MagicMock()
    hc._check_node_ping.return_value = True
    hc._check_node_api.return_value = True
    hc._check_ping_from_node.return_value = True
    monkeypatch.setattr(restart_runner, "health_controller", hc)

    res = restart_runner.task_runner_node(task)

    assert res is False  # defer: outer loop backoff, retried later
    assert task.retry == 0  # no retry budget consumed
    assert "claim held by cli:100:aa" in task.function_result
    sops.shutdown_storage_node.assert_not_called()
    sops.restart_storage_node.assert_not_called()


def test_runner_proceeds_when_claim_stale(monkeypatch):
    stale = _node(status=StorageNode.STATUS_RESTARTING,
                  claim_owner="cli:100:aa",
                  claim_age_sec=constants.RESTART_CLAIM_TTL_SEC + 30)
    task = _runner_task()

    stale.data_nics = [MagicMock(ip4_address="10.0.0.2", if_name="eth0")]
    fake_db = MagicMock()
    fake_db.get_storage_node_by_id.return_value = stale
    fake_db.get_task_by_id.return_value = task
    fake_db.get_cluster_by_id.return_value = MagicMock(
        status="active", suspend_drain_complete=False)
    fake_db.get_storage_nodes_by_cluster_id.return_value = []
    fake_db.atomic_update.side_effect = lambda obj, fn: (fn(obj), obj)[1]
    monkeypatch.setattr(restart_runner, "db", fake_db)
    monkeypatch.setattr(JobSchedule, "write_to_db", MagicMock())

    sops = MagicMock()
    sops.fd_dead_recovery_allowed.return_value = False
    sops.shutdown_storage_node.return_value = True
    sops.restart_storage_node.return_value = True
    monkeypatch.setattr(restart_runner, "storage_node_ops", sops)
    hc = MagicMock()
    hc._check_node_ping.return_value = True
    hc._check_node_api.return_value = True
    hc._check_ping_from_node.return_value = True
    monkeypatch.setattr(restart_runner, "health_controller", hc)

    restart_runner.task_runner_node(task)

    # The stale claim (dead driver) did not block the resume path: the
    # runner reached its cleanup shutdown step.
    sops.shutdown_storage_node.assert_called_once()


# --------------------------------------------------------------------------
# restart_storage_node wrapper — claim-gated failure cleanup + release
# --------------------------------------------------------------------------

def _wrapper_env(monkeypatch, impl_result, post_claim_owner):
    """Run restart_storage_node with a stubbed impl. post_claim_owner:
    'SELF' → the post-read node carries THIS call's token (impl acquired);
    anything else is stored verbatim (impl refused / foreign owner)."""
    captured = {}

    def fake_impl(node_id, **kwargs):
        captured["token"] = kwargs.get("restart_claim_token")
        return impl_result

    monkeypatch.setattr(storage_node_ops, "_restart_storage_node_impl", fake_impl)

    pre_node = _node(status=StorageNode.STATUS_OFFLINE)

    def make_post_node():
        post = _node(status=StorageNode.STATUS_RESTARTING)
        post.restart_claim_owner = (captured.get("token", "")
                                    if post_claim_owner == "SELF"
                                    else post_claim_owner)
        post.write_to_db = MagicMock()
        return post

    fake_db = MagicMock()
    # First read (pre-status snapshot) happens before the impl runs; the
    # wrapper's finally-block re-read happens after — keyed on whether the
    # stubbed impl has captured its token yet.
    fake_db.get_storage_node_by_id.side_effect = \
        lambda _id: make_post_node() if "token" in captured else pre_node

    monkeypatch.setattr(storage_node_ops, "DBController", lambda: fake_db)

    from simplyblock_core.controllers import tasks_controller as real_tasks
    monkeypatch.setattr(real_tasks, "ensure_node_restart_task", lambda n: None)

    kill = MagicMock()
    monkeypatch.setattr(storage_node_ops, "_kill_spdk_until_dead", kill)
    monkeypatch.setattr(storage_node_ops, "trigger_ana_failover_for_node", MagicMock())
    monkeypatch.setattr(storage_node_ops, "storage_events", MagicMock())
    monkeypatch.setattr(storage_node_ops, "distr_controller", MagicMock())

    result = storage_node_ops.restart_storage_node("node-1")
    return result, captured, kill, fake_db


def test_wrapper_cleanup_runs_only_when_claim_held(monkeypatch):
    """Impl failed AFTER acquiring (post node carries our token): the abort
    contract applies — SPDK killed, claim released with our token."""
    result, captured, kill, fake_db = _wrapper_env(
        monkeypatch, impl_result=False, post_claim_owner="SELF")
    assert result is False
    kill.assert_called_once()
    fake_db.release_node_restart_claim.assert_called_with("node-1", captured["token"])


def test_wrapper_skips_cleanup_when_claim_foreign(monkeypatch):
    """Impl refused (another actor's fresh claim, peer gate, ...): nothing is
    ours to clean — the 2026-08-06 iter-50 refused-CLI cleanup killed the
    task runner's in-flight SPDK container. Claim release (owner-matched CAS
    with OUR token) is still attempted and cannot touch the foreign claim."""
    result, captured, kill, fake_db = _wrapper_env(
        monkeypatch, impl_result=False, post_claim_owner="task-runner:9:zz")
    assert result is False
    kill.assert_not_called()
    fake_db.release_node_restart_claim.assert_called_with("node-1", captured["token"])


def test_wrapper_generates_unique_tokens():
    t1 = storage_node_ops._new_restart_claim_token()
    t2 = storage_node_ops._new_restart_claim_token()
    assert t1 != t2
    assert len(t1.split(":")) == 3
