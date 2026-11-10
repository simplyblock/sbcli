"""An outage restart of a replication transfer backs off by its node switches,
not by task.retry.

bf8 (2026-11-01, round 3, tgt-mid): the target primary crashed mid-transfer.
The task had collected retry=4 from "waiting for chain ancestor ... to
replicate first" passes, which count as retries but are no failure, and
_restart_elsewhere took its delay from task.retry: 15 s * 2**4 = 240 s. The
crashed node was back after ~85 s, so the transfer was never repeated on the
new target leader while the old one was down.
"""
import time
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from simplyblock_core.peer import service as peer_service
from simplyblock_core.peer.protocol import ChainRequest
from simplyblock_core.services import snapshot_replication as sr


class _Task:
    def __init__(self, retry=0, switches=0):
        self.retry = retry
        self.status = "running"
        self.function_result = ""
        self.function_params = {"snapshot_id": "S", "offset": 123}
        if switches:
            self.function_params["node_switches"] = switches
        self.uuid = "T"
        self.writes = 0

    def write_to_db(self):
        self.writes += 1


def _delay(task):
    return task.function_params["not_before"] - int(time.time())


@pytest.mark.parametrize("retry", [0, 4, 9])
def test_first_outage_restart_waits_one_base_step_whatever_the_retry_count(retry):
    t = _Task(retry=retry)
    sr._restart_elsewhere(t, "target leadership of LVS_13 moved from A to B")
    assert t.status == sr.JobSchedule.STATUS_SUSPENDED
    assert abs(_delay(t) - sr._backoff_seconds(0)) <= 1
    assert t.function_params["node_switches"] == 1
    assert "offset" not in t.function_params
    assert t.retry == retry  # an outage is not counted as a retry


def test_repeated_switches_still_back_off():
    t = _Task(retry=4, switches=2)
    sr._restart_elsewhere(t, "source member X is offline mid-transfer")
    assert abs(_delay(t) - sr._backoff_seconds(2)) <= 1


def test_a_transfer_failure_keeps_backing_off_by_retry():
    t = _Task(retry=3)
    sr._suspend_for_retry(t, "transfer failed at offset 0, retrying", backoff=True)
    assert t.retry == 4
    assert abs(_delay(t) - sr._backoff_seconds(4)) <= 1


# --- chaining onto the predecessor's copy while another member leads -------

class _Lvol:
    def __init__(self, node_id, nodes):
        self.node_id, self.nodes = node_id, nodes


class _Snap:
    def __init__(self, uuid, lvol):
        self._id, self.lvol, self.snap_bdev = uuid, lvol, f"LVS_16/SNAP_{uuid}"
        self.target_replicated_snap_uuid = "COPY"
        self.source_replicated_snap_uuid = ""

    def get_id(self):
        return self._id


class _Node:
    def __init__(self, nid):
        self._id = nid

    def get_id(self):
        return self._id


def _chain_on(leader_id):
    """The destination chaining a landing volume onto COPY with *leader_id*
    leading (peer.service.chain_landing); the leader has no secondary here."""
    copy = _Snap("COPY", _Lvol("PRIMARY", ["PRIMARY", "SECONDARY", "TERTIARY"]))
    leader = MagicMock()
    leader.get_id.return_value = leader_id
    leader.secondary_node_id = ""
    leader.rpc_client.return_value.bdev_get.return_value = None
    leader.rpc_client.return_value.bdev_lvol_add_clone.return_value = True
    db = MagicMock()
    db.get_lvol_by_id.return_value = SimpleNamespace(top_bdev="LVS_16/REP_NEW")
    db.get_snapshot_by_id.return_value = copy
    db.get_storage_node_by_id.return_value = leader
    return peer_service.chain_landing(db, ChainRequest(
        landing_lvol_id="LANDING", leader_node_id=leader_id, predecessor_id="COPY")), leader


@pytest.mark.parametrize("leader", ["PRIMARY", "SECONDARY", "TERTIARY"])
def test_any_member_of_the_copys_lvstore_can_chain(leader):
    answer, node = _chain_on(leader)
    assert answer.state == "chained"
    node.rpc_client.return_value.bdev_lvol_add_clone.assert_called_once_with(
        "LVS_16/REP_NEW", "LVS_16/SNAP_COPY")


def test_a_node_outside_the_lvstore_still_cannot():
    answer, node = _chain_on("OTHER")
    assert answer.state == "unresolvable"
    node.rpc_client.return_value.bdev_lvol_add_clone.assert_not_called()
