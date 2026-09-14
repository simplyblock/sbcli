"""Two control-plane fixes from the 2026-09-05 k8s failover run.

1. The restart flow registered lvol subsystems through a ThreadPoolExecutor
   whose Futures were discarded, so every failure -- raised or returned --
   was thrown away and the flow then declared the node ready. LVS_13's
   tertiary came back with subsystems missing for some of its lvols, through
   a clean phase cycle (pre_block 22:22:02 through cleared 22:22:11), with
   nothing logged.

2. The device-removal path detached the peers' remote controllers
   unconditionally. That cancels SPDK's auto-reconnect poller, and since
   device self-repair now re-attaches after a non-admin removal it is exactly
   the detach-then-attach sequence that produces duplicate IO qpair IDs. It
   also frequently fails to remove anything: the 2026-09-05 detach of
   remote_alceml_794a1db1 left "NVMe path ... still exists after delete" and
   the controller logged "Submitting Keep Alive failed" for 13 minutes.
"""
import inspect
import re

from simplyblock_core import distr_controller, storage_node_ops
from simplyblock_core.controllers import device_controller


class _Lvol:
    def __init__(self, i, ok=True):
        self._id = i
        self.nqn = "nqn.test:lvol:%s" % i
        self.uuid = i
        self._ok = ok

    def get_id(self):
        return self._id


class _Rpc:
    def __init__(self, present=None, raises=False):
        self._present = present if present is not None else {}
        self._raises = raises

    def subsystem_get(self, nqn):
        if self._raises:
            raise RuntimeError("rpc down")
        return [{"nqn": nqn}] if self._present.get(nqn, True) else []


class _Node:
    def __init__(self, rpc=None):
        self._rpc = rpc or _Rpc()

    def get_id(self):
        return "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"

    def rpc_client(self, **kw):
        return self._rpc


def _run(monkeypatch, lvols, results, node=None, label="LVS_13"):
    """Drive _register_lvols_on_node with a stubbed add_lvol_thread.

    `results` maps lvol id -> what add_lvol_thread should do: a (ok, msg)
    tuple, or an Exception instance to raise.
    """
    calls = []

    def _fake(lvol, snode, lvol_ana_state="optimized"):
        calls.append(lvol.get_id())
        r = results.get(lvol.get_id(), (True, None))
        if isinstance(r, Exception):
            raise r
        return r

    monkeypatch.setattr(storage_node_ops, "add_lvol_thread", _fake)
    failed = storage_node_ops._register_lvols_on_node(
        lvols, node or _Node(), "non_optimized", lvs_label=label)
    return failed, calls


class TestFailuresAreNoLongerSwallowed:
    def test_all_good_reports_nothing(self, monkeypatch):
        failed, calls = _run(monkeypatch, [_Lvol("a"), _Lvol("b")], {})
        assert failed == []
        assert sorted(calls) == ["a", "a", "b", "b"][:2] or len(calls) >= 2

    def test_a_returned_false_is_reported(self, monkeypatch):
        failed, _ = _run(monkeypatch, [_Lvol("a"), _Lvol("b")],
                         {"b": (False, "crypto create failed")})
        ids = [i for i, _ in failed]
        assert ids == ["b"]
        assert "crypto create failed" in dict(failed)["b"]

    def test_a_raised_exception_is_reported(self, monkeypatch):
        """The old code captured this in a discarded Future forever."""
        failed, _ = _run(monkeypatch, [_Lvol("a")],
                         {"a": RuntimeError("rpc timeout")})
        assert [i for i, _ in failed] == ["a"]
        assert "rpc timeout" in dict(failed)["a"]

    def test_failures_are_retried_once(self, monkeypatch):
        seen = {"n": 0}

        def _flaky(lvol, snode, lvol_ana_state="optimized"):
            seen["n"] += 1
            return (seen["n"] > 1, "first attempt timed out")

        monkeypatch.setattr(storage_node_ops, "add_lvol_thread", _flaky)
        failed = storage_node_ops._register_lvols_on_node(
            [_Lvol("a")], _Node(), "non_optimized")
        assert failed == [], "a transient failure should be retried and clear"
        assert seen["n"] == 2

    def test_empty_list_is_a_noop(self, monkeypatch):
        failed, calls = _run(monkeypatch, [], {})
        assert failed == [] and calls == []


class TestVerifyRatherThanTrust:
    def test_a_missing_subsystem_is_caught_despite_success(self, monkeypatch):
        """add_lvol_thread said yes; the node says the subsystem is absent."""
        node = _Node(_Rpc(present={"nqn.test:lvol:b": False}))
        failed, _ = _run(monkeypatch, [_Lvol("a"), _Lvol("b")], {}, node=node)
        assert [i for i, _ in failed] == ["b"]
        assert "no subsystem" in dict(failed)["b"]

    def test_verify_uses_the_lvol_nqn(self):
        src = inspect.getsource(storage_node_ops._register_lvols_on_node)
        assert "subsystem_get(lvol.nqn)" in src

    def test_a_broken_probe_does_not_mask_real_failures(self, monkeypatch):
        node = _Node(_Rpc(raises=True))
        failed, _ = _run(monkeypatch, [_Lvol("a")], {"a": (False, "boom")},
                         node=node)
        assert [i for i, _ in failed] == ["a"]


class TestConcurrencyIsBounded:
    def test_worker_cap_is_well_below_the_old_50(self):
        assert 0 < storage_node_ops.LVOL_REGISTER_MAX_WORKERS <= 16

    def test_no_call_site_still_discards_futures(self):
        """Both loops used `executor.submit(add_lvol_thread, ...)` with the
        Future dropped and only `shutdown(wait=True)` to 'check' it.

        Asserted against code with docstrings stripped: the helper's own
        docstring quotes the old pattern verbatim to explain the bug.
        """
        src = re.sub('""".*?"""', "", inspect.getsource(storage_node_ops),
                     flags=re.DOTALL)
        assert "executor.submit(add_lvol_thread" not in src

    def test_both_call_sites_go_through_the_helper(self):
        src = inspect.getsource(storage_node_ops)
        assert src.count("_register_lvols_on_node(") >= 3  # def + 2 sites


class TestSelfHealedPhaseDrainsItsQueue:
    def test_the_self_heal_drains(self):
        """drain_restart_queue is otherwise only called from the two
        transitions in _set_restart_phase, so a phase retired by the
        self-heal stranded everything queued against it."""
        src = inspect.getsource(storage_node_ops.get_restart_phase)
        i = src.index("atomic_update(node, _clear)")
        assert "drain_restart_queue(node_id, lvs_name)" in src[i:]

    def test_the_drain_cannot_break_the_clear(self):
        src = inspect.getsource(storage_node_ops.get_restart_phase)
        i = src.index("drain_restart_queue(node_id, lvs_name)")
        assert "try:" in src[max(0, i - 300):i]


class TestDetachOnlyOnOperatorRemoval:
    def test_disconnect_device_takes_the_flag(self):
        sig = inspect.signature(distr_controller.disconnect_device)
        assert "detach_controllers" in sig.parameters
        assert sig.parameters["detach_controllers"].default is True

    def test_it_returns_before_touching_rpc_when_false(self):
        src = inspect.getsource(distr_controller.disconnect_device)
        i = src.index("if not detach_controllers:")
        j = src.index("db_controller = DBController()")
        assert i < j, "the opt-out must precede any RPC or DB work"
        assert "return" in src[i:j]

    def test_the_remote_devices_list_is_left_intact_when_not_detaching(self):
        """Keeping the record is the point: the peer must still know about
        the device so SPDK can reconnect and self-repair can restore it."""
        src = inspect.getsource(distr_controller.disconnect_device)
        i = src.index("if not detach_controllers:")
        j = src.index("db_controller = DBController()")
        assert "remote_devices" not in src[i:j]

    def test_device_remove_gates_on_admin_cause(self):
        src = inspect.getsource(device_controller.device_remove)
        assert "detach_controllers=(cause == CAUSE_ADMIN_REMOVE)" in src

    def test_graceful_shutdown_keeps_its_own_detach(self):
        """That loop is load-bearing — it stops peers reattaching to a dying
        node — and must not be caught by this change."""
        assert hasattr(storage_node_ops, "_detach_remote_controllers_from_peers")
        src = inspect.getsource(
            storage_node_ops._detach_remote_controllers_from_peers)
        assert "bdev_nvme_detach_controller" in src
