"""A cluster that does not allow operations is a deferral, never a retry.

2026-10-14 run 63: a node removal left the cluster suspended for ~35 minutes.
Every ``failed_device_migration`` task polled ``require_active_cluster`` the
whole time and each poll raised ``TaskRetry``, so by the time the cluster was
active again the tasks sat at retry 12 with a driver backoff of
min(3s * 2**11, 3600s) = one hour. The node removal that waited on those
migrations stayed in MigratingDevices with "0 of 2 devices migrated" until the
tasks pod was deleted by hand. ``backoff_reset_key`` would not have helped: it
returns None on every cluster that is not single-node or two-node.

The driver contract (task_runner_base.py) is explicit: blocked on external
state -> ``TaskDefer``, no retry consumed, no backoff. These pin the gate to
that contract using real ``Cluster`` models and no database.
"""
import pytest

from simplyblock_core.models.cluster import Cluster
from simplyblock_core.services import migration_task_common as mig
from simplyblock_core.services.task_runner_base import TaskDefer, TaskRetry


def _cluster(status):
    cluster = Cluster()
    cluster.uuid = "cl-1"
    cluster.status = status
    return cluster


@pytest.mark.parametrize("status", sorted(Cluster.OPERABLE_STATUSES))
def test_operable_statuses_pass(status):
    mig.defer_unless_operable(_cluster(status))


@pytest.mark.parametrize("status", [
    Cluster.STATUS_SUSPENDED,
    Cluster.STATUS_IN_ACTIVATION,
    Cluster.STATUS_UNREADY,
])
def test_non_operable_status_defers_without_consuming_a_retry(status):
    assert status not in Cluster.OPERABLE_STATUSES, "fixture drifted: pick a non-operable status"
    with pytest.raises(TaskDefer) as excinfo:
        mig.defer_unless_operable(_cluster(status))
    assert not isinstance(excinfo.value, TaskRetry)
    assert status in str(excinfo.value)


def test_shrinking_cluster_is_operable():
    """Draining the departing node's devices is the removal's own work; the
    gate must not deadlock the removal against the status it set itself."""
    mig.defer_unless_operable(_cluster(Cluster.STATUS_IN_SHRINK))


def test_the_task_facing_wrapper_uses_the_gate():
    """``require_active_cluster`` must not grow a second verdict of its own."""
    import inspect
    src = inspect.getsource(mig.require_active_cluster)
    assert "defer_unless_operable(cluster)" in src
    assert "TaskRetry" not in src


def test_a_running_sync_site_activation_defers_on_an_active_cluster():
    """A sync-replication site activation holds every migration while the
    cluster itself stays operable: a deferral, no retry consumed."""
    cluster = _cluster(Cluster.STATUS_ACTIVE)
    cluster.sync_replication = True
    cluster.site_activation_site = "site-a"
    with pytest.raises(TaskDefer) as excinfo:
        mig.defer_unless_operable(cluster)
    assert not isinstance(excinfo.value, TaskRetry)
    assert "site site-a is being activated" in str(excinfo.value)
    cluster.site_activation_site = ""
    mig.defer_unless_operable(cluster)
