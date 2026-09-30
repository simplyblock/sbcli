"""Pure rules of the sync-replication resync runner: how a
``distr_migration_status`` element of a catch-up is judged and how long a
non-converging run backs off. The runner's flow (waits, start, poll, finish,
re-runs, alert) runs against the real database in
tests/integration/test_tasks_runner_sync_resync.py.
"""
import pytest

from simplyblock_core import constants
from simplyblock_core.services import tasks_runner_sync_resync as runner


@pytest.mark.parametrize("element, outcome", [
    ({"status": "pending"}, runner.OUTCOME_RUNNING),
    ({"status": "running", "error": 0, "progress": 40}, runner.OUTCOME_RUNNING),
    ({"status": "stopping", "error": 0}, runner.OUTCOME_RUNNING),
    ({"status": "completed", "error": 0}, runner.OUTCOME_OK),
    # every error bit ends the run: 11 (started while the zone was lost),
    # 8 (SHUTDOWN: the zone got lost during it), IO / destination errors
    ({"status": "completed", "error": 1 << 11}, runner.OUTCOME_FAILED),
    ({"status": "completed", "error": 1 << 8}, runner.OUTCOME_FAILED),
    ({"status": "completed", "error": 1}, runner.OUTCOME_FAILED),
    ({"status": "completed"}, runner.OUTCOME_FAILED),           # no error field: not a success
    ({"status": "none"}, runner.OUTCOME_FAILED),                # lost with a restart
    ({"status": "failed"}, runner.OUTCOME_FAILED),
    (None, runner.OUTCOME_FAILED),
    ("garbage", runner.OUTCOME_FAILED),
])
def test_migration_outcome(element, outcome):
    assert runner.migration_outcome(element) == outcome


def test_retry_delay_doubles_from_the_base_up_to_the_cap():
    base, cap = constants.SYNC_RESYNC_RETRY_BASE_SEC, constants.SYNC_RESYNC_RETRY_MAX_SEC
    assert [runner.retry_delay(n) for n in (1, 2, 3)] == [base, 2 * base, 4 * base]
    assert runner.retry_delay(0) == base
    assert runner.retry_delay(50) == cap
    assert all(runner.retry_delay(n) <= runner.retry_delay(n + 1) for n in range(1, 20))
