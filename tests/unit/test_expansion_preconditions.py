# coding=utf-8
"""
test_expansion_preconditions.py — unit tests for the expansion pre-flight
checks in ``simplyblock_core/controllers/cluster_expansion/preconditions.py``.

Single-node expansion re-wires the secondary LVS at the newcomer's direct
successor and the tertiary LVS at the second-in-line successor, tearing down
the donors' existing stacks. The preconditions refuse to start unless the
cluster is ACTIVE, every node is ONLINE, no migration / node-restart /
backup task is open anywhere, and no delete is in flight on the impacted
donors.
"""

import unittest
from unittest.mock import MagicMock

from simplyblock_core.controllers.cluster_expansion.preconditions import (
    EXPANSION_BLOCKING_TASK_FNS,
    affected_primary_node_ids,
    check_expansion_preconditions,
    impacted_donor_node_ids,
)
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.storage_node import StorageNode


def _cluster(status=Cluster.STATUS_ACTIVE):
    c = MagicMock(spec=Cluster)
    c.status = status
    c.get_id = MagicMock(return_value="cl-1")
    return c


def _node(uuid, status=StorageNode.STATUS_ONLINE):
    n = MagicMock(spec=StorageNode)
    n.status = status
    n.get_id = MagicMock(return_value=uuid)
    return n


def _task(fn, status=JobSchedule.STATUS_RUNNING, node_id="", canceled=False):
    t = MagicMock(spec=JobSchedule)
    t.function_name = fn
    t.status = status
    t.node_id = node_id
    t.canceled = canceled
    t.uuid = f"task-{fn}-{node_id or 'x'}"
    return t


def _lvol(uuid, node_id, status=LVol.STATUS_ONLINE):
    lv = MagicMock(spec=LVol)
    lv.status = status
    lv.node_id = node_id
    lv.get_id = MagicMock(return_value=uuid)
    return lv


def _db(nodes=None, tasks=None, lvols=None):
    db = MagicMock()
    db.get_storage_nodes_by_cluster_id.return_value = nodes or []
    db.get_job_tasks.return_value = tasks or []
    db.get_lvols.return_value = lvols or []
    return db


def _move(from_node_id, to_node_id, primary_id, is_create=False):
    m = MagicMock()
    m.from_node_id = from_node_id
    m.to_node_id = to_node_id
    m.lvs_primary_node_id = primary_id
    m.is_create = is_create
    return m


class TestMoveDerivation(unittest.TestCase):
    def test_impacted_donors_are_rehome_sources_only(self):
        moves = [
            _move(None, "new", "new", is_create=True),      # newcomer create
            _move("donor-sec", "new", "pri-1"),             # re-home secondary
            _move("donor-tert", "new", "pri-2"),            # re-home tertiary
        ]
        self.assertEqual(impacted_donor_node_ids(moves),
                         {"donor-sec", "donor-tert"})
        self.assertEqual(affected_primary_node_ids(moves),
                         {"pri-1", "pri-2"})


class TestPreconditions(unittest.TestCase):
    def test_ok_on_quiescent_active_cluster(self):
        db = _db(nodes=[_node("n1"), _node("n2")],
                 tasks=[_task(JobSchedule.FN_DEV_MIG,
                              status=JobSchedule.STATUS_DONE)])
        ok, reason = check_expansion_preconditions(_cluster(), db)
        self.assertTrue(ok, reason)

    def test_rejects_non_active_cluster(self):
        for status in (Cluster.STATUS_DEGRADED, Cluster.STATUS_SUSPENDED,
                       Cluster.STATUS_IN_EXPANSION, Cluster.STATUS_IN_ACTIVATION):
            ok, reason = check_expansion_preconditions(_cluster(status), _db())
            self.assertFalse(ok)
            self.assertIn(status, reason)

    def test_rejects_offline_node(self):
        db = _db(nodes=[_node("n1"), _node("n2", StorageNode.STATUS_OFFLINE)])
        ok, reason = check_expansion_preconditions(_cluster(), db)
        self.assertFalse(ok)
        self.assertIn("n2", reason)

    def test_removed_nodes_are_ignored(self):
        db = _db(nodes=[_node("n1"), _node("n2", StorageNode.STATUS_REMOVED)])
        ok, reason = check_expansion_preconditions(_cluster(), db)
        self.assertTrue(ok, reason)

    def test_rejects_every_blocking_task_family_in_every_open_state(self):
        for fn in EXPANSION_BLOCKING_TASK_FNS:
            for status in (JobSchedule.STATUS_NEW, JobSchedule.STATUS_RUNNING,
                           JobSchedule.STATUS_SUSPENDED):
                db = _db(nodes=[_node("n1")], tasks=[_task(fn, status=status)])
                ok, reason = check_expansion_preconditions(_cluster(), db)
                self.assertFalse(ok, f"{fn}/{status} should block")
                self.assertIn(fn, reason)

    def test_done_or_canceled_tasks_do_not_block(self):
        db = _db(nodes=[_node("n1")],
                 tasks=[_task(JobSchedule.FN_LVOL_MIG,
                              status=JobSchedule.STATUS_DONE),
                        _task(JobSchedule.FN_NODE_RESTART, canceled=True)])
        ok, reason = check_expansion_preconditions(_cluster(), db)
        self.assertTrue(ok, reason)

    def test_sync_delete_on_impacted_node_blocks(self):
        db = _db(nodes=[_node("n1")],
                 tasks=[_task(JobSchedule.FN_LVOL_SYNC_DEL, node_id="donor-1")])
        ok, reason = check_expansion_preconditions(
            _cluster(), db, impacted_node_ids={"donor-1"})
        self.assertFalse(ok)
        self.assertIn("donor-1", reason)

    def test_sync_delete_elsewhere_does_not_block(self):
        db = _db(nodes=[_node("n1")],
                 tasks=[_task(JobSchedule.FN_LVOL_SYNC_DEL, node_id="other")])
        ok, reason = check_expansion_preconditions(
            _cluster(), db, impacted_node_ids={"donor-1"})
        self.assertTrue(ok, reason)

    def test_lvol_in_deletion_on_affected_primary_blocks(self):
        db = _db(nodes=[_node("n1")],
                 lvols=[_lvol("lv1", "pri-1", LVol.STATUS_IN_DELETION)])
        ok, reason = check_expansion_preconditions(
            _cluster(), db, affected_primary_ids={"pri-1"})
        self.assertFalse(ok)
        self.assertIn("pri-1", reason)

    def test_lvol_in_deletion_elsewhere_does_not_block(self):
        db = _db(nodes=[_node("n1")],
                 lvols=[_lvol("lv1", "other", LVol.STATUS_IN_DELETION)])
        ok, reason = check_expansion_preconditions(
            _cluster(), db, affected_primary_ids={"pri-1"})
        self.assertTrue(ok, reason)


class TestResumeSemantics(unittest.TestCase):
    """An in-progress plan interrupted by a node outage must be resumable:
    the outage's recovery migrations are queued (suspended, deferring on the
    expansion) and must not block the resume — but node restarts and
    backups still do."""

    def test_resume_tolerates_open_deferring_migrations(self):
        db = _db(nodes=[_node("n1")],
                 tasks=[_task(JobSchedule.FN_DEV_MIG,
                              status=JobSchedule.STATUS_SUSPENDED),
                        _task(JobSchedule.FN_FAILED_DEV_MIG,
                              status=JobSchedule.STATUS_NEW),
                        _task(JobSchedule.FN_LVOL_MIG,
                              status=JobSchedule.STATUS_SUSPENDED)])
        ok, reason = check_expansion_preconditions(_cluster(), db, resume=True)
        self.assertTrue(ok, reason)
        # The same tasks block a FRESH expansion.
        ok, _ = check_expansion_preconditions(_cluster(), db, resume=False)
        self.assertFalse(ok)

    def test_resume_still_blocks_on_node_restart_and_backup(self):
        for fn in (JobSchedule.FN_NODE_RESTART, JobSchedule.FN_BACKUP,
                   JobSchedule.FN_BACKUP_RESTORE):
            db = _db(nodes=[_node("n1")], tasks=[_task(fn)])
            ok, reason = check_expansion_preconditions(
                _cluster(), db, resume=True)
            self.assertFalse(ok, f"{fn} must still block a resume")
            self.assertIn(fn, reason)

    def test_resume_still_requires_all_nodes_online(self):
        db = _db(nodes=[_node("n1"), _node("n2", StorageNode.STATUS_OFFLINE)])
        ok, reason = check_expansion_preconditions(_cluster(), db, resume=True)
        self.assertFalse(ok)
        self.assertIn("n2", reason)


if __name__ == "__main__":
    unittest.main()
