"""Bounded, differential self-repair of an `unavailable` device.

A device is marked `unavailable` by CONSENSUS: more than half the nodes fail to
reach it through the NVMe-oF chain (node -> subsystem -> listener -> namespace
-> alceml bdev -> PCIe device). That verdict is then PERSISTED, so a purely
transient cause -- a brief network problem between nodes -- leaves the device
unavailable indefinitely with nothing attempting to bring it back.

Two things stopped any repair happening today:

  * device_set_unavailable() only sets state, so a consensus-driven unavailable
    device has io_error == False -- and the auto-restart branch required it;
  * that branch sits in an `elif` under `cluster.status == ACTIVE`, so it never
    ran on a healthy cluster at all.

The repair is deliberately NOT restart_device(): that tears down subsystem, PT
and alceml before rebuilding, forcing every consumer to disconnect. Here the
stack is probed first and _def_create_device_stack() -- additive, every layer
guarded by an existence check -- fills in only what is missing.
"""
import inspect

from simplyblock_core import constants
from simplyblock_core.controllers import device_controller
from simplyblock_core.models.nvme_device import NVMeDevice


def _body(fn):
    """Source of `fn` with its docstring stripped.

    The docstrings here deliberately name _def_create_device_stack and
    restart_device, so ordering assertions must look at the code only.
    """
    src = inspect.getsource(fn)
    first = src.find('"""')
    if first == -1:
        return src
    second = src.find('"""', first + 3)
    return src[second + 3:] if second != -1 else src


class TestBackoffSchedule:
    def test_five_attempts(self):
        assert len(constants.DEVICE_REPAIR_BACKOFF_SEC) == 5

    def test_immediate_then_widening(self):
        assert constants.DEVICE_REPAIR_BACKOFF_SEC == [0, 10, 60, 180, 600]

    def test_it_is_monotonic(self):
        sched = constants.DEVICE_REPAIR_BACKOFF_SEC
        assert all(b >= a for a, b in zip(sched, sched[1:]))


class TestDueCalculation:
    def _dev(self, **kw):
        d = NVMeDevice()
        d.status = NVMeDevice.STATUS_UNAVAILABLE
        for k, v in kw.items():
            setattr(d, k, v)
        return d

    def test_first_attempt_is_immediate(self):
        assert device_controller.device_repair_due(
            self._dev(repair_attempts=0, last_repair_tsc=0.0))

    def test_online_device_is_never_due(self):
        d = self._dev(repair_attempts=0)
        d.status = NVMeDevice.STATUS_ONLINE
        assert not device_controller.device_repair_due(d)

    def test_exhausted_device_is_never_due(self):
        assert not device_controller.device_repair_due(
            self._dev(repair_attempts=1, retries_exhausted=True))

    def test_not_due_before_the_backoff_elapses(self):
        import time
        assert not device_controller.device_repair_due(
            self._dev(repair_attempts=1, last_repair_tsc=time.time()))

    def test_due_once_the_backoff_has_elapsed(self):
        import time
        assert device_controller.device_repair_due(
            self._dev(repair_attempts=1, last_repair_tsc=time.time() - 11))

    def test_past_the_last_step_is_never_due(self):
        assert not device_controller.device_repair_due(
            self._dev(repair_attempts=len(constants.DEVICE_REPAIR_BACKOFF_SEC),
                      last_repair_tsc=0.0))


class TestDifferentialRepair:
    """It must know what survived, and not spend attempts it cannot use."""

    def test_it_probes_before_rebuilding(self):
        src = _body(device_controller.device_repair)
        assert src.index("probe_device_stack(") < src.index("_def_create_device_stack(")

    def test_an_intact_stack_costs_no_attempt(self):
        """A device whose local stack is whole is unavailable for a REMOTE
        reason. Rebuilding cannot help, and spending attempts on it would
        strand a healthy device once the budget exhausted."""
        src = inspect.getsource(device_controller.device_repair)
        i = src.index("if not missing:")
        assert "return False" in src[i:i + 500]
        assert "not counting a repair attempt" in src[i:i + 500]

    def test_it_uses_the_additive_builder_not_restart_device(self):
        src = _body(device_controller.device_repair)
        assert "_def_create_device_stack(" in src
        assert "restart_device(" not in src

    def test_it_verifies_before_declaring_success(self):
        src = _body(device_controller.device_repair)
        i = src.index("_def_create_device_stack(")
        assert "missing2" in src[i:i + 900]

    def test_success_reconnects_the_remote_clients(self):
        src = inspect.getsource(device_controller.device_repair)
        assert "connect_peers=True" in src

    def test_probes_are_individually_guarded(self):
        """One wedged layer must not make the repair itself hang or throw."""
        src = inspect.getsource(device_controller.probe_device_stack)
        assert "except Exception" in src

    def test_probe_covers_the_whole_chain(self):
        src = inspect.getsource(device_controller.probe_device_stack)
        for layer in ("nvme_controller", "alceml", "pt", "subsystem",
                      "listener", "namespace"):
            assert layer in src


class TestBudgetReset:
    def test_reaching_online_clears_the_budget(self):
        """Route-blind on purpose: sn restart-device, a node restart (which
        sets its devices online on the way back up), a successful self-repair
        and an operator fixing the network all end the episode."""
        src = inspect.getsource(device_controller.device_set_state)
        assert "device.repair_attempts = 0" in src
        assert "device.last_repair_tsc = 0.0" in src

    def test_the_counters_are_persisted(self):
        src = inspect.getsource(device_controller.device_set_state)
        assert '"repair_attempts": device.repair_attempts' in src
        assert '"last_repair_tsc": device.last_repair_tsc' in src

    def test_exhaustion_is_recorded_on_the_device(self):
        src = inspect.getsource(device_controller.device_repair)
        assert "retries_exhausted = True" in src


class TestTrigger:
    def test_monitor_calls_the_repair(self):
        from simplyblock_core.services import device_monitor
        src = inspect.getsource(device_monitor)
        assert "device_repair_due(" in src
        assert "device_repair(" in src

    def test_trigger_is_not_gated_on_io_error_or_cluster_active(self):
        """Both gates are why a consensus-unavailable device was never
        repaired: device_set_unavailable sets no io_error, and the old branch
        only ran when the cluster was not ACTIVE."""
        from simplyblock_core.services import device_monitor
        src = inspect.getsource(device_monitor)
        i = src.index("if device_controller.device_repair_due(dev):")
        j = src.index("if cluster.status == Cluster.STATUS_ACTIVE:")
        assert i < j, "repair must be evaluated independently of the cluster-status branch"
