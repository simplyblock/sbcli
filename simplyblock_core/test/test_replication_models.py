"""D1 unit tests for cross-cluster replication model + RPC additions."""
from simplyblock_core.models.snapshot import SnapShot, SnapShotMini
from simplyblock_core.models.lvol_model import LVol, LVolReplication
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.rpc_client import RPCClient

from pydantic import SecretStr


def _make_client():
    # __init__ only builds a requests.Session; it does not open a connection.
    return RPCClient("127.0.0.1", 8080, "user", SecretStr("pass"))


def test_snapshot_type_default_and_constants():
    assert SnapShot.TYPE_USER == "user"
    assert SnapShot.TYPE_INTERNAL == "internal"
    assert SnapShot().snap_type == "user"


def test_snapshot_type_roundtrip():
    snap = SnapShot({"snap_type": SnapShot.TYPE_INTERNAL})
    assert snap.snap_type == "internal"
    restored = SnapShot(snap.to_dict())
    assert restored.snap_type == "internal"


def test_snapshot_mini_carries_snap_type():
    snap = SnapShot({"snap_type": SnapShot.TYPE_INTERNAL})
    snap.lvol = LVol()
    mini = SnapShotMini().from_snapshot(snap)
    assert mini.snap_type == "internal"


def test_lvol_replication_fields_default():
    lvol = LVol()
    assert lvol.replication_mode == "failover"
    assert lvol.replication_interval_min == 0


def test_lvol_replication_model_defaults_and_constants():
    rep = LVolReplication()
    assert rep.mode == "failover"
    assert rep.state == LVolReplication.STATE_REPLICATING
    assert rep.direction == LVolReplication.DIRECTION_TO_TARGET
    assert rep.target_nqn == ""
    assert rep.target_ns_id == 0
    assert LVolReplication.STATE_CUTOVER_PENDING == "cutover_pending"
    assert LVolReplication.STATE_CUTOVER_DONE == "cutover_done"
    assert LVolReplication.STATE_FAILED_OVER == "failed_over"
    assert LVolReplication.DIRECTION_TO_SOURCE == "to_source"


def test_job_schedule_replication_final_constant():
    assert JobSchedule.FN_REPLICATION_FINAL == "replication_final"


def test_transfer_final_step_param_shape():
    captured: dict = {}
    client = _make_client()
    client._request = lambda method, params: captured.update(method=method, params=params)

    client.bdev_lvol_transfer_final_step(
        "lvs/LVOL_1", 42, "lvs/SNAP_1", 2, "hub_bdev", operation="replicate")

    assert captured["method"] == "bdev_lvol_transfer_final_step"
    assert captured["params"] == {
        "lvol_name": "lvs/LVOL_1",
        "lvol_id": 42,
        "snapshot_name": "lvs/SNAP_1",
        "cluster_batch": 2,
        "gateway": "hub_bdev",
        "operation": "replicate",
    }


def test_final_migration_alias_delegates_identically():
    captured: dict = {}
    client = _make_client()
    client._request = lambda method, params: captured.update(method=method, params=params)

    client.bdev_lvol_final_migration("lvs/LVOL_1", 42, "lvs/SNAP_1", 2, "hub_bdev")

    assert captured["method"] == "bdev_lvol_transfer_final_step"
    assert captured["params"]["operation"] == "migrate"
    assert captured["params"]["gateway"] == "hub_bdev"
