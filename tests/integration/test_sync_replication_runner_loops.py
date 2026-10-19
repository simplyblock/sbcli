"""The own poll loops of the two sync-replication task runners against the
real FoundationDB: a failing read of the cluster list ends the loop with that
error instead of being logged and retried, as for the runners on the driver -
the process exits and the orchestrator restarts it with a fresh FDB client.

The failing read is a Cluster record whose value does not decode: the real
``get_clusters`` range read raises on it.
"""
import json
import time
import types
import uuid
from unittest.mock import MagicMock, patch

import pytest

from simplyblock_core.db_controller import DBController
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.services import tasks_runner_sync_promote, tasks_runner_sync_resync


class _StopLoop(BaseException):
    """Raised by the loop's sleep: the pass got to the end."""


@pytest.fixture()
def corrupt_cluster():
    db = DBController()
    key = Cluster().get_db_id(str(uuid.uuid4())).encode()
    db.kv_store[key] = b"not json"
    try:
        yield
    finally:
        db.kv_store.clear(key)


@pytest.mark.parametrize("runner", [tasks_runner_sync_promote, tasks_runner_sync_resync])
def test_a_cluster_read_error_ends_the_loop(corrupt_cluster, runner):
    clock = types.SimpleNamespace(sleep=MagicMock(side_effect=_StopLoop), time=time.time)
    with patch.object(runner, "time", clock), pytest.raises(json.JSONDecodeError):
        runner.main()
    clock.sleep.assert_not_called()
