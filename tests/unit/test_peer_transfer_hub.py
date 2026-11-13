"""A secondary that leads the target lvstore receives transfers through its own
transfer hub ON THAT lvstore (docs/replication-node-failover.md).

SPDK demuxes hub IO by map id in the hub's own lvstore, so the hub has to be
created in the target lvstore -- not in the lvstore the receiving node is
primary of, where its regular transfer hub lives. Hub blobs are in-memory, so
a non-primary member can create one.
"""
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.services import replication_final_step as rfs
from simplyblock_core.services import snapshot_replication as sr


def _member(node_id, lvstore, sec="", ter="", hub_port=4430):
    rpc = MagicMock()
    rpc.bdev_get.return_value = None
    rpc.bdev_lvol_create_hublvol.return_value = f"uuid-{node_id}"
    rpc.bdev_nvme_attach_controller.return_value = ["ok"]
    node = SimpleNamespace(
        uuid=node_id, cluster_id="CL2", lvstore=lvstore, status=StorageNode.STATUS_ONLINE,
        secondary_node_id=sec, tertiary_node_id=ter, active_rdma=False,
        data_nics=[SimpleNamespace(ip4_address=f"10.0.0.{len(node_id)}", trtype="TCP")],
        hublvol=SimpleNamespace(nvmf_port=hub_port), transfer_hublvol=None,
        expose_bdev=MagicMock())
    node.get_id = lambda: node_id
    node.rpc_client = lambda timeout=None: rpc
    return node


def test_peer_hub_is_named_and_addressed_per_lvstore_and_node():
    tp = _member("TP-11111111", "LVS_T1", sec="TS-22222222")
    ts = _member("TS-22222222", "LVS_T2")
    hub = rfs.receiving_hub(ts, tp)
    assert hub.bdev_name == "LVS_T1/transferhub_TS-22222"
    assert hub.nqn.endswith(":transferhub:LVS_T1:TS-22222222")
    # The target lvstore's hub port -- the one its HA hub already uses there.
    assert hub.nvmf_port == tp.hublvol.nvmf_port
    # Deterministic: the same member and lvstore always name the same hub.
    assert rfs.receiving_hub(ts, tp).nguid == hub.nguid


def test_primary_keeps_its_own_transfer_hub():
    tp = _member("TP-11111111", "LVS_T1")
    tp.transfer_hublvol = SimpleNamespace(bdev_name="LVS_T1/transferhub")
    assert rfs.receiving_hub(tp, tp) is tp.transfer_hublvol
    assert rfs.receiving_hub(tp) is tp.transfer_hublvol


def test_attach_to_a_secondary_creates_the_hub_in_the_target_lvstore():
    tp = _member("TP-11111111", "LVS_T1", sec="TS-22222222")
    ts = _member("TS-22222222", "LVS_T2")
    src_rpc = MagicMock()
    src_rpc.bdev_get.return_value = None
    src_rpc.bdev_nvme_attach_controller.return_value = ["ok"]

    ctrl, remote, err = rfs.ensure_hub_attached(src_rpc, ts, tp)

    assert err is None
    # Created in the TARGET lvstore, never in the secondary's own LVS_T2.
    ts.rpc_client().bdev_lvol_create_hublvol.assert_called_once_with(
        "LVS_T1", name="transferhub_TS-22222")
    expose = ts.expose_bdev.call_args.kwargs
    assert expose["bdev_name"] == "LVS_T1/transferhub_TS-22222"
    assert expose["port"] == 4430
    # The source attaches under the hub's distinct name, so an attachment to
    # the primary's hub is never mistaken for this one.
    assert src_rpc.bdev_nvme_attach_controller.call_args.args[0] == "LVS_T1/transferhub_TS-22222"
    assert (ctrl, remote) == ("LVS_T1/transferhub_TS-22222", "LVS_T1/transferhub_TS-22222n1")


def test_an_existing_peer_hub_is_reused():
    tp = _member("TP-11111111", "LVS_T1", sec="TS-22222222")
    ts = _member("TS-22222222", "LVS_T2")
    ts.rpc_client().bdev_get.side_effect = lambda name: {"uuid": "u1"} if "transferhub" in name else None
    src_rpc = MagicMock()
    src_rpc.bdev_get.return_value = None
    src_rpc.bdev_nvme_attach_controller.return_value = ["ok"]

    _, _, err = rfs.ensure_hub_attached(src_rpc, ts, tp)

    assert err is None
    ts.rpc_client().bdev_lvol_create_hublvol.assert_not_called()
    assert ts.expose_bdev.call_args.kwargs["uuid"] == "u1"


# ---- snapshot replication: who may receive -------------------------------- #

def _task():
    t = SimpleNamespace(uuid="t-1", retry=0, max_retry=8, canceled=False,
                        status=JobSchedule.STATUS_SUSPENDED, function_result="",
                        cluster_id="CL", function_params=dict(
                            snapshot_id="snap-1", source_node_id="P", remote_lvol_id="R",
                            replicate_to_source=False))
    t.write_to_db = lambda *a, **k: None
    t.get_id = lambda: "t-1"
    return t


def _snapshot():
    lvol = SimpleNamespace(node_id="P", nodes=["P"], lvs_name="LVS_1", get_id=lambda: "lvol-1")
    return SimpleNamespace(lvol=lvol, snap_bdev="LVS_1/SNAP_1", get_id=lambda: "snap-1",
                           status="in_replication")


def _start_with_leader(leader_id):
    tp = _member("TP", "LVS_T1", sec="TS", ter="TT")
    nodes = {"TP": tp, "TS": _member("TS", "LVS_T2"), "TT": _member("TT", "LVS_T3")}
    remote = SimpleNamespace(node_id="TP", nodes=["TP", "TS", "TT"], lvs_name="LVS_T1",
                             cluster_id="CL2", top_bdev="LVS_T1/REP_1")
    db = MagicMock()
    db.get_lvol_by_id.return_value = remote
    db.get_storage_node_by_id.side_effect = lambda nid: nodes[nid]
    src = MagicMock()
    task = _task()
    with patch.object(sr, "db", db), \
            patch.object(sr, "_lvs_transfer_hold", return_value=None), \
            patch.object(sr, "_select_source_node", return_value=(src, "primary P")), \
            patch.object(sr, "_unreplicated_local_ancestor", return_value=("ok", None, "")), \
            patch.object(sr, "_stable_target_leader", return_value=(nodes[leader_id], "")), \
            patch("simplyblock_core.services.replication_final_step.ensure_hub_attached",
                  return_value=(None, None, "stop here")) as hub:
        sr.process_snap_replicate_start(task, _snapshot())
    return task, hub, nodes


def test_a_secondary_leader_receives_through_its_hub_on_the_target_lvstore():
    task, hub, nodes = _start_with_leader("TS")
    hub.assert_called_once()
    _, tgt, lvs_primary = hub.call_args.args
    assert tgt is nodes["TS"] and lvs_primary is nodes["TP"]


def test_the_primary_still_receives_through_its_own_hub():
    task, hub, nodes = _start_with_leader("TP")
    _, tgt, lvs_primary = hub.call_args.args
    assert tgt is nodes["TP"] and lvs_primary is nodes["TP"]


def test_a_tertiary_leader_still_waits():
    """SPDK never demuxes hub IO on a tertiary (node_role check in
    vbdev_lvol_submit_request); until the fork allows it, the start waits."""
    task, hub, _ = _start_with_leader("TT")
    hub.assert_not_called()
    assert task.retry == 0
    assert "tertiary" in task.function_result


def test_detach_closes_the_controller_that_was_attached():
    """A secondary's per-lvstore hub has its own name; detaching the node's
    regular transfer hub would leave the real attachment open."""
    task = _task()
    node = SimpleNamespace(transfer_hublvol=SimpleNamespace(bdev_name="LVS_T2/transferhub"))
    task.function_params["hub_ctrl"] = "LVS_T1/transferhub_TS-22222"
    assert sr._attached_hub_ctrl(task, node) == "LVS_T1/transferhub_TS-22222"
    del task.function_params["hub_ctrl"]               # a task from before the record
    assert sr._attached_hub_ctrl(task, node) == "LVS_T2/transferhub"
