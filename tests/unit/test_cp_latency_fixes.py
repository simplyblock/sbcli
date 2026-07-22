"""Unit tests for the 2026-07-21 CP-latency root-cause fixes.

Fix 1 — per-class reflection cache in BaseModel (the measured 6.8 ms/object
        GIL sink behind inflated RPCs and FDB txns).
Fix 2 — hublvol advisory lock pre-acquired OUTSIDE the port-block window
        (acquire txn measured avg 858 ms inside blocked windows).
Fix 3 — global port-block window gate: at most one client port blocked at a
        time, layered on top of the per-LVS recreate locks.
"""
import threading
import time
import types

from simplyblock_core import storage_node_ops
from simplyblock_core.models.base_model import BaseModel
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.utils import hublvol_reconnect


# ---------------------------------------------------------------------------
# Fix 1 — reflection cache
# ---------------------------------------------------------------------------
class TestModelReflectionCache:
    @staticmethod
    def _fat_node_dict():
        base = StorageNode().to_dict()
        dev = NVMeDevice().to_dict()
        base['nvme_devices'] = [dict(dev) for _ in range(4)]
        base['remote_devices'] = [dict(dev) for _ in range(20)]
        return base

    def test_roundtrip_identical(self):
        base = self._fat_node_dict()
        a = StorageNode(base)
        d1 = a.to_dict()
        d2 = StorageNode(d1).to_dict()
        assert d1 == d2

    def test_merge_semantics_on_populated_instance(self):
        # from_dict on a POPULATED instance must keep current values for
        # attrs absent from the incoming dict (defaults come from the
        # instance, not from the class or a cache).
        n = StorageNode(self._fat_node_dict())
        n.mgmt_ip = '10.0.0.9'
        n.from_dict({'hostname': 'merged-host'})
        assert n.mgmt_ip == '10.0.0.9'
        assert n.hostname == 'merged-host'

    def test_no_list_aliasing_between_instances(self):
        base = self._fat_node_dict()
        x = StorageNode(base)
        y = StorageNode(base)
        x.nvme_devices.append('sentinel')
        assert len(y.nvme_devices) == 4

    def test_cache_is_per_class(self):
        a = StorageNode._annotated_attrs()
        b = NVMeDevice._annotated_attrs()
        assert a is not b
        # stable identity on repeat call (i.e., actually cached)
        assert StorageNode._annotated_attrs() is a

    def test_methods_still_filtered(self):
        names = {s for s, _ in BaseModel._annotated_attrs()}
        assert 'get_id' not in names and 'from_dict' not in names
        assert 'uuid' in names and 'status' in names

    def test_construction_is_fast(self):
        base = self._fat_node_dict()
        StorageNode(base)  # warm the cache
        t0 = time.perf_counter()
        for _ in range(20):
            StorageNode(base)
        per_obj_ms = (time.perf_counter() - t0) / 20 * 1000
        # pre-fix: ~6.8 ms for a fatter node; cached must be well under 3 ms
        assert per_obj_ms < 3.0, f'{per_obj_ms:.2f} ms/object — cache not effective'


# ---------------------------------------------------------------------------
# Fix 2 — pre-acquired hublvol advisory lock
# ---------------------------------------------------------------------------
class TestHublvolLockPreAcquire:
    def test_acquire_lock_returns_entered_lock_and_release(self):
        # kv_store=None -> process-local lock path (no FDB in unit tier)
        coord = hublvol_reconnect.HublvolReconnectCoordinator.__new__(
            hublvol_reconnect.HublvolReconnectCoordinator)
        coord._db = types.SimpleNamespace(kv_store=None)
        coord._lock_ttl = 5
        lock = coord.acquire_lock('node-A', 'LVS_T')
        try:
            # second acquire of the same key must block -> verify via a
            # thread that times out
            got = {'ok': False}

            def try_second():
                lk2 = hublvol_reconnect._HublvolLock(None, 'node-A', 'LVS_T',
                                                     acquire_timeout_sec=0.2)
                try:
                    lk2.__enter__()
                    got['ok'] = True
                    lk2.release()
                except hublvol_reconnect.HublvolReconnectError:
                    pass
            t = threading.Thread(target=try_second)
            t.start(); t.join(timeout=5)
            assert got['ok'] is False, 'second acquire should time out while held'
        finally:
            lock.release()
        # after release, the same key is acquirable again
        lk3 = hublvol_reconnect._HublvolLock(None, 'node-A', 'LVS_T',
                                             acquire_timeout_sec=0.5)
        lk3.__enter__()
        lk3.release()

    def test_release_is_idempotent(self):
        coord = hublvol_reconnect.HublvolReconnectCoordinator.__new__(
            hublvol_reconnect.HublvolReconnectCoordinator)
        coord._db = types.SimpleNamespace(kv_store=None)
        coord._lock_ttl = 5
        lock = coord.acquire_lock('node-B', 'LVS_T2')
        lock.release()
        lock.release()  # must not raise

    def test_reconcile_signature_accepts_external_lock(self):
        import inspect
        sig = inspect.signature(
            hublvol_reconnect.HublvolReconnectCoordinator.reconcile)
        assert 'lock' in sig.parameters

    def test_connect_to_hublvol_accepts_coordinator_lock(self):
        import inspect
        sig = inspect.signature(StorageNode.connect_to_hublvol)
        assert 'coordinator_lock' in sig.parameters


# ---------------------------------------------------------------------------
# Fix 3 — global port-block window gate
# ---------------------------------------------------------------------------
class TestPortBlockWindowGate:
    def test_gate_exists_and_is_a_lock(self):
        g = storage_node_ops._port_block_window_gate
        assert g.acquire(blocking=False)
        g.release()

    def test_windows_serialize(self):
        # Two simulated windows using the gate the way both impls do:
        # acquire -> (blocked span) -> release. They must never overlap.
        g = storage_node_ops._port_block_window_gate
        state = {'cur': 0, 'max': 0}
        guard = threading.Lock()

        def window():
            g.acquire()
            try:
                with guard:
                    state['cur'] += 1
                    state['max'] = max(state['max'], state['cur'])
                time.sleep(0.05)
                with guard:
                    state['cur'] -= 1
            finally:
                g.release()

        threads = [threading.Thread(target=window) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)
        assert state['max'] == 1, 'port-block windows overlapped'

    def test_impls_wire_the_gate(self):
        import inspect
        for fn in (storage_node_ops._recreate_lvstore_impl,
                   storage_node_ops._recreate_lvstore_on_non_leader_impl):
            src = inspect.getsource(fn)
            assert '_acquire_block_gate()' in src, fn.__name__
            assert '_release_block_gate()' in src, fn.__name__
            # abort paths must release too
            assert src.count('_release_block_gate()') >= 2, fn.__name__

    def test_gate_release_idempotent_pattern(self):
        # mirror of the _gate_state holder used in the impls
        g = storage_node_ops._port_block_window_gate
        state = {'held': False}

        def acquire():
            g.acquire(); state['held'] = True

        def release():
            if state['held']:
                state['held'] = False
                g.release()
        acquire(); release(); release()  # double release must be harmless
        assert g.acquire(blocking=False)
        g.release()


# ---------------------------------------------------------------------------
# Window-collapse package (2026-07-21 #1-#4): pre-staged hublvol subsystems,
# deferred coordinator stamp, deferred port events, pre-block probes.
# ---------------------------------------------------------------------------
class TestPrestageHublvolSubsystem:
    def _node(self, rpc):
        n = StorageNode()
        n.uuid = "n1"
        n.data_nics = [types.SimpleNamespace(ip4_address="10.0.0.1",
                                             trtype="TCP", if_name="eth0")]
        n.active_rdma = False
        n.rpc_client = lambda *a, **k: rpc
        return n

    def test_creates_subsystem_and_listener_when_missing(self):
        calls = []
        rpc = types.SimpleNamespace(
            subsystem_get=lambda nqn: None,
            subsystem_create=lambda **kw: calls.append(("create", kw)) or True,
            listeners_create=lambda **kw: calls.append(("listener", kw)) or True,
        )
        self._node(rpc).prestage_hublvol_subsystem(
            "nqn.test:hub", "model-1", 4600, ana_state="non_optimized",
            min_cntlid=1000)
        kinds = [c[0] for c in calls]
        assert kinds == ["create", "listener"]
        assert calls[0][1]["min_cntlid"] == 1000
        assert calls[1][1]["ana_state"] == "non_optimized"

    def test_skips_existing_subsystem_and_listener(self):
        calls = []
        rpc = types.SimpleNamespace(
            subsystem_get=lambda nqn: {
                "listen_addresses": [
                    {"trtype": "TCP", "traddr": "10.0.0.1", "trsvcid": "4600"}]},
            subsystem_create=lambda **kw: calls.append("create") or True,
            listeners_create=lambda **kw: calls.append("listener") or True,
        )
        self._node(rpc).prestage_hublvol_subsystem(
            "nqn.test:hub", "model-1", 4600)
        assert calls == []  # fully idempotent: probe only


class TestWindowCollapseWiring:
    def _src(self, fn):
        import inspect
        return inspect.getsource(fn)

    def test_prestage_wired_in_both_impls(self):
        for fn in (storage_node_ops._recreate_lvstore_impl,
                   storage_node_ops._recreate_lvstore_on_non_leader_impl):
            assert "prestage_hublvol_subsystem" in self._src(fn), fn.__name__

    def test_port_events_deferred_in_both_impls(self):
        for fn in (storage_node_ops._recreate_lvstore_impl,
                   storage_node_ops._recreate_lvstore_on_non_leader_impl):
            src = self._src(fn)
            # no direct event emission from the window paths; the flush
            # helper is the only emitter
            assert "_deferred_port_events.append" in src, fn.__name__
            assert "_flush_port_events()" in src, fn.__name__
            emit_lines = [ln for ln in src.splitlines()
                          if ("tcp_ports_events.port_deny(" in ln
                              or "tcp_ports_events.port_allowed(" in ln)
                          and "_flush" not in ln]
            # only the two lines inside _flush_port_events itself remain
            assert len(emit_lines) == 2, (fn.__name__, emit_lines)

    def test_probes_computed_before_block_section(self):
        # anchor on the ### 3 block-section marker (the gate helper's DEF
        # appears earlier in source than its call site)
        for fn, marker in (
                (storage_node_ops._recreate_lvstore_impl,
                 "### 3- block LVS port"),
                (storage_node_ops._recreate_lvstore_on_non_leader_impl,
                 "### 3- block leader port")):
            src = self._src(fn)
            assert src.index("raid_already = _rpc_bdev_exists") \
                < src.index(marker), fn.__name__

    def test_stamp_deferred_on_external_lock(self):
        import inspect
        from simplyblock_core.utils import hublvol_reconnect as hr
        src = inspect.getsource(
            hr.HublvolReconnectCoordinator._reconcile_under_lock)
        assert "pending_stamp" in src and "externally_managed" in src
        # release helpers in ops pay the deferred stamp
        for fn in (storage_node_ops._recreate_lvstore_impl,
                   storage_node_ops._recreate_lvstore_on_non_leader_impl):
            assert "stamp_attach" in self._src(fn), fn.__name__

    def test_acquire_lock_marks_external(self):
        from simplyblock_core.utils import hublvol_reconnect as hr
        coord = hr.HublvolReconnectCoordinator.__new__(
            hr.HublvolReconnectCoordinator)
        coord._db = types.SimpleNamespace(kv_store=None)
        coord._lock_ttl = 5
        lock = coord.acquire_lock("nX", "LVS_ext")
        try:
            assert lock.externally_managed is True
            assert lock.pending_stamp is False
        finally:
            lock.release()


# ---------------------------------------------------------------------------
# #5 — monitor baseline trim: batched blocked-port checks + stretched
# collector cadence (idle baseline was 4,290 RPCs/min cluster-wide).
# ---------------------------------------------------------------------------
class TestBlockedPortsBatching:
    @staticmethod
    def _node(rpc):
        return types.SimpleNamespace(rpc_client=lambda *a, **k: rpc)

    def test_one_fetch_answers_all_ports(self):
        from simplyblock_core.controllers import health_controller
        calls = {"n": 0}

        def get_blocked():
            calls["n"] += 1
            return {"blocked_ports": [{"port": 4500}]}
        rpc = types.SimpleNamespace(nvmf_get_blocked_ports=get_blocked)
        res = health_controller.check_ports_on_node(
            self._node(rpc), [4500, 4502, 4504])
        assert calls["n"] == 1
        assert res == {4500: False, 4502: True, 4504: True}

    def test_empty_ports_no_rpc(self):
        from simplyblock_core.controllers import health_controller
        rpc = types.SimpleNamespace(
            nvmf_get_blocked_ports=lambda: (_ for _ in ()).throw(
                RuntimeError("must not be called")))
        assert health_controller.check_ports_on_node(self._node(rpc), []) == {}

    def test_loops_wired_to_batch(self):
        import inspect
        from simplyblock_core.services import health_check_service
        from simplyblock_core.services import storage_node_monitor
        from simplyblock_core.controllers import health_controller
        for mod in (health_check_service, storage_node_monitor):
            src = inspect.getsource(mod)
            assert "check_ports_on_node" in src, mod.__name__
        assert "check_ports_on_node(snode, _hc_ports)" in inspect.getsource(
            health_controller)

    def test_collector_cadence_stretched(self):
        from simplyblock_core import constants as c
        assert c.DEV_STAT_COLLECTOR_INTERVAL_SEC >= 15
        assert c.PROT_STAT_COLLECTOR_INTERVAL_SEC >= 10
        assert c.DISTR_EVENT_COLLECTOR_INTERVAL_SEC >= 5
        assert c.CACHED_LVOL_STAT_COLLECTOR_INTERVAL_SEC >= 15
        assert c.CAP_MONITOR_INTERVAL_SEC >= 30
        # failure-detection cadence untouched
        assert c.NODE_MONITOR_INTERVAL_SEC == 3
        assert c.DEVICE_MONITOR_INTERVAL_SEC == 5
