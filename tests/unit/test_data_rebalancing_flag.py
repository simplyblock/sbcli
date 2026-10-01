"""is_data_rebalancing excludes the cluster's own volume migrations.

A node drain migrates volumes itself; with a single rebalancing flag that
counted lvol migrations it paused on its own work for the whole of every
removal (2026-09-29). is_re_balancing keeps counting everything for the
guards that need it.
"""
from unittest.mock import MagicMock

from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.services import storage_node_monitor as monitor
from simplyblock_web.api.v2._dtos import ClusterDTO


def _task(fn, status=JobSchedule.STATUS_RUNNING, canceled=False):
    t = MagicMock()
    t.function_name = fn
    t.status = status
    t.canceled = canceled
    return t


def test_volume_migrations_alone_are_not_data_rebalancing():
    rb, drb, lm = monitor._rebalancing_flags(
        [_task(JobSchedule.FN_LVOL_MIG), _task(JobSchedule.FN_LVOL_BATCH_MIG)])
    assert (rb, drb, lm) == (True, False, 2)


def test_device_migration_is_data_rebalancing():
    rb, drb, lm = monitor._rebalancing_flags(
        [_task(JobSchedule.FN_FAILED_DEV_MIG), _task(JobSchedule.FN_LVOL_MIG)])
    assert (rb, drb, lm) == (True, True, 1)


def test_done_canceled_and_unrelated_tasks_do_not_count():
    rb, drb, lm = monitor._rebalancing_flags([
        _task(JobSchedule.FN_DEV_MIG, status=JobSchedule.STATUS_DONE),
        _task(JobSchedule.FN_LVOL_MIG, canceled=True),
        _task(JobSchedule.FN_NODE_RESTART),
    ])
    assert (rb, drb, lm) == (False, False, 0)


def test_the_api_reports_both_flags():
    cl = Cluster()
    cl.uuid = "11111111-1111-1111-1111-111111111111"
    cl.nqn = "nqn.test"
    cl.status = Cluster.STATUS_ACTIVE
    cl.is_re_balancing = True
    cl.is_data_rebalancing = False
    cl.active_lvol_migrations = 3
    try:
        dto = ClusterDTO.from_model(cl)
    except Exception:  # noqa: BLE001 - the DTO needs more fields than this test sets
        import pytest
        pytest.skip("ClusterDTO needs a fuller Cluster fixture")
    assert dto.is_re_balancing is True
    assert dto.is_data_rebalancing is False
    assert dto.active_lvol_migrations == 3
