"""Campaign c4 fixes of the two-node arbiter (docs/design/two-node-arbitration.md).

F12: a node woken from a freeze can have lost its data devices (alceml's
stuck-IO watchdog shut them down) while every status still says online. It is
never re-admitted by unfence alone; it goes through the restart flow, and a
rebuild stuck on unavailable devices is flagged and recovered the same way.
F17 (campaign c7b, replaces F13): no survivor promotion after the grant; the
survivor's SPDK died in that promotion during a graceful shutdown.

The arbiter is gated per cluster (``two_node_arbitration``); nothing here runs
on any other cluster.
"""

from types import SimpleNamespace

import pytest

from simplyblock_core import constants
from simplyblock_core.arbitration import arbiter as arb
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.nvme_device import NVMeDevice
from tests.unit.test_two_node_arbiter import (  # noqa: F401 - fixture
    CL,
    calls,
    env,
)
from tests.unit.test_two_node_c3_fixes import (
    _b_back_self_fenced_and_readmitted,
    _b_frozen_and_granted,
    tick,
)
from tests.unit.test_two_node_decision import A, B

# ---- mig error bit field ---------------------------------------------------------


@pytest.mark.parametrize("code,text", [
    (8, "DST_NOT_AVAILABLE"),
    (520, "RESHUFFLE|DST_NOT_AVAILABLE"),
    (1536, "MISC|RESHUFFLE"),
    (4, "SRC_NOT_AVAILABLE"),
    (0, "none"),
])
def test_mig_error_is_a_bit_field(code, text):
    assert arb.describe_mig_error(code) == text


@pytest.mark.parametrize("code,stuck", [(8, True), (520, True), (4, True), (1536, False), (0, False)])
def test_unavailable_bits(code, stuck):
    assert bool(code & arb.MIG_UNAVAILABLE_BITS) is stuck


# ---- F12: device check before re-admission ---------------------------------------

def ids(nodes_):
    return [n.get_id() for n in nodes_]


def _wire(a, devices=True):
    a.devices_ok = lambda _db, _node: devices
    a.notify_log = []
    a.notify = lambda cid, node, code, msg: a.notify_log.append((node.get_id(), code))


def test_f12_self_fenced_node_without_devices_goes_through_the_restart_flow(env):  # noqa: F811
    a, db, nodes, clock, readmitted = env
    _wire(a, devices=False)
    _b_frozen_and_granted(a, db, nodes, clock)
    _b_back_self_fenced_and_readmitted(a, db, nodes, clock)
    assert not calls(nodes[B], "jc_unfence")     # never unfenced without its devices
    assert ids(readmitted) == [B]        # restart flow instead
    assert B in db.rec.device_recovery
    assert (B, "TWO_NODE_DEVICE_RECOVERY") in a.notify_log


def test_f12_device_recovery_is_not_repeated_within_the_window(env):  # noqa: F811
    a, db, nodes, clock, readmitted = env
    _wire(a, devices=False)
    _b_frozen_and_granted(a, db, nodes, clock)
    _b_back_self_fenced_and_readmitted(a, db, nodes, clock)
    for _ in range(3):
        clock.t += constants.TWO_NODE_SELF_FENCE_READMIT_MS
        tick(a, nodes)
    assert ids(readmitted) == [B]
    clock.t += constants.TWO_NODE_DEVICE_RECOVERY_RETRY_S * 1000
    tick(a, nodes)
    assert ids(readmitted) == [B, B]


def test_f12_inconclusive_device_check_defers(env):  # noqa: F811
    a, db, nodes, clock, readmitted = env
    _wire(a, devices=None)
    _b_frozen_and_granted(a, db, nodes, clock)
    _b_back_self_fenced_and_readmitted(a, db, nodes, clock)
    assert not calls(nodes[B], "jc_unfence") and readmitted == []
    a.devices_ok = lambda _db, _node: True
    clock.t += constants.TWO_NODE_SELF_FENCE_READMIT_MS
    tick(a, nodes)
    assert calls(nodes[B], "jc_unfence")


def test_f12_devices_present_unfences_as_before(env):  # noqa: F811
    a, db, nodes, clock, readmitted = env
    _wire(a, devices=True)
    _b_frozen_and_granted(a, db, nodes, clock)
    _b_back_self_fenced_and_readmitted(a, db, nodes, clock)
    assert calls(nodes[B], "jc_unfence") and readmitted == []
    assert db.rec.device_recovery == {}


# ---- F12: stuck rebuild ------------------------------------------------------------

def test_f12_rebuild_stuck_on_unavailable_devices_is_flagged_and_recovered(env):  # noqa: F811
    a, db, nodes, clock, readmitted = env
    _wire(a, devices=True)
    a.rebuild_task_errors = lambda _db, _cid: 520
    _b_frozen_and_granted(a, db, nodes, clock)
    _b_back_self_fenced_and_readmitted(a, db, nodes, clock)
    assert db.rec.rebuild[B]["queued"] is True
    db.open_tasks = 6
    clock.t += constants.TWO_NODE_REBUILD_CHECK_S * 1000
    tick(a, nodes)                               # first seen failing
    assert not db.rec.rebuild[B].get("stuck") and B not in db.rec.device_recovery
    clock.t += constants.TWO_NODE_REBUILD_STUCK_S * 1000
    tick(a, nodes)
    mark = db.rec.rebuild[B]
    assert mark["stuck"] is True and mark["stuck_error"] == 520
    assert "stuck" in db.rec.device_recovery[B]["reason"]
    assert B in ids(readmitted)
    assert (B, "TWO_NODE_REBUILD_STUCK") in a.notify_log
    clock.t += constants.TWO_NODE_REBUILD_CHECK_S * 1000
    tick(a, nodes)
    # flagged once, recovered once (the healing's own hand-over is separate)
    assert a.notify_log.count((B, "TWO_NODE_DEVICE_RECOVERY")) == 1


def test_f12_other_migration_errors_are_not_stuck(env):  # noqa: F811
    a, db, nodes, clock, readmitted = env
    _wire(a, devices=True)
    a.rebuild_task_errors = lambda _db, _cid: 1536   # RESHUFFLE|MISC: retried as before
    _b_frozen_and_granted(a, db, nodes, clock)
    _b_back_self_fenced_and_readmitted(a, db, nodes, clock)
    db.open_tasks = 6
    for _ in range(3):
        clock.t += constants.TWO_NODE_REBUILD_STUCK_S * 1000
        tick(a, nodes)
    assert not db.rec.rebuild[B].get("stuck") and B not in db.rec.device_recovery


def test_f12_rebuild_errors_cleared_restart_the_clock(env):  # noqa: F811
    a, db, nodes, clock, readmitted = env
    _wire(a, devices=True)
    errors = {"v": 8}
    a.rebuild_task_errors = lambda _db, _cid: errors["v"]
    _b_frozen_and_granted(a, db, nodes, clock)
    _b_back_self_fenced_and_readmitted(a, db, nodes, clock)
    db.open_tasks = 6
    clock.t += constants.TWO_NODE_REBUILD_CHECK_S * 1000
    tick(a, nodes)
    errors["v"] = 0
    clock.t += constants.TWO_NODE_REBUILD_STUCK_S * 1000
    tick(a, nodes)
    errors["v"] = 8
    clock.t += constants.TWO_NODE_REBUILD_CHECK_S * 1000
    tick(a, nodes)
    assert not db.rec.rebuild[B].get("stuck") and B not in db.rec.device_recovery


# ---- F17: no survivor promotion after the grant ----------------------------------
# Campaign c7b, 04: #1476's promote-at-grant (bdev_lvol_update_lvstore +
# bdev_lvol_set_leader on the survivor right after the grant) ran into the
# graceful-shutdown handoff and the survivor's SPDK died in the update RPC
# (160 client EIO). It was removed: abort-on-wake covers a frozen primary, the
# leadership restoration after an unfence covers partitions, and the shutdown /
# restart flow hands leadership over on a planned loss.

_LEADERSHIP_RPCS = ("bdev_lvol_update_lvstore", "bdev_lvol_set_leader")


def _primary(nodes, nid, lvs, secondary=False):
    nodes[nid].lvstore = lvs
    nodes[nid].is_secondary_node = secondary


def _leadership_rpcs(node):
    return [c for c in node.calls if c[0] in _LEADERSHIP_RPCS]


def test_f17_the_grant_sends_no_leadership_rpc_to_the_survivor(env):  # noqa: F811
    a, db, nodes, clock, _ = env
    _primary(nodes, B, "LVS_B")
    a.run_async = lambda fn: fn()
    _b_frozen_and_granted(a, db, nodes, clock)
    assert calls(nodes[A], "jc_grant_solo")
    for _ in range(3):
        tick(a, nodes)
    assert _leadership_rpcs(nodes[A]) == []


def test_f17_promote_at_grant_is_gone():
    assert not hasattr(arb.Arbiter, "promote_after_grant")
    assert not hasattr(arb, "default_promote_survivor")
    from simplyblock_core.models.arbitration import ClusterArbitration
    assert "promoted" not in ClusterArbitration().get_attrs_map()


# ---- defaults ----------------------------------------------------------------------

class _RPC:
    def __init__(self, present, fail=False):
        self.present, self.fail = present, fail

    def bdev_get(self, name):
        if self.fail:
            raise ConnectionError("timed out")
        return {"name": name} if name in self.present else None


def _dev(name, status=NVMeDevice.STATUS_ONLINE):
    return SimpleNamespace(alceml_bdev=name, alceml_name=name, status=status,
                           get_id=lambda: "dev-" + name)


def _db_with(node):
    return SimpleNamespace(get_storage_node_by_id=lambda _nid: node)


def _node(devs, rpc):
    return SimpleNamespace(nvme_devices=devs, get_id=lambda: "n1", rpc_client=lambda **_: rpc)


def test_default_devices_ok_present():
    n = _node([_dev("alceml_a"), _dev("alceml_b")], _RPC({"alceml_a", "alceml_b"}))
    assert arb.default_devices_ok(_db_with(n), n) is True


def test_default_devices_ok_alceml_gone():
    n = _node([_dev("alceml_a"), _dev("alceml_b")], _RPC({"alceml_a"}))
    assert arb.default_devices_ok(_db_with(n), n) is False


def test_default_devices_ok_device_not_online():
    n = _node([_dev("alceml_a", NVMeDevice.STATUS_UNAVAILABLE)], _RPC({"alceml_a"}))
    assert arb.default_devices_ok(_db_with(n), n) is False


def test_default_devices_ok_skips_removed_and_jm():
    n = _node([_dev("x", NVMeDevice.STATUS_REMOVED), _dev("j", NVMeDevice.STATUS_JM)], _RPC(set()))
    assert arb.default_devices_ok(_db_with(n), n) is True


def test_default_devices_ok_unknown_when_the_node_does_not_answer():
    n = _node([_dev("alceml_a")], _RPC(set(), fail=True))
    assert arb.default_devices_ok(_db_with(n), n) is None


def _task(fn, result, status=JobSchedule.STATUS_SUSPENDED, canceled=False):
    return SimpleNamespace(function_name=fn, function_result=result, status=status,
                           canceled=canceled)


def test_default_rebuild_task_errors_ors_open_migration_errors():
    tasks = [_task(JobSchedule.FN_DEV_MIG, "mig error: 8, retrying"),
             _task(JobSchedule.FN_DEV_MIG, "mig error: 520, retrying"),
             _task(JobSchedule.FN_DEV_MIG, "mig error: 4, retrying", status=JobSchedule.STATUS_DONE),
             _task(JobSchedule.FN_DEV_MIG, "mig error: 2, retrying", canceled=True),
             _task(JobSchedule.FN_NODE_RESTART, "mig error: 1024, retrying"),
             _task(JobSchedule.FN_FAILED_DEV_MIG, "running")]
    db = SimpleNamespace(get_job_tasks=lambda _cid: tasks)
    assert arb.default_rebuild_task_errors(db, CL) == 8 | 520
