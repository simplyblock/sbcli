"""Unit tests for the restart CPU-contention fixes (2026-07-20 FD-0 reboot).

Covers:
  * Fix A — async (queue-based) logging handler: worker threads enqueue,
    a listener thread writes; records are still delivered.
  * Fix B — bounded connect/reconnect worker threads: a global semaphore caps
    how many workers run concurrently across parallel node restarts, without
    changing the spawn/join structure or the target's error handling.
"""
import io
import logging
import logging.handlers
import threading
import time
import types

from simplyblock_core import storage_node_ops, utils
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.models.nvme_device import NVMeDevice


# ---------------------------------------------------------------------------
# Fix B — bounded worker threads
# ---------------------------------------------------------------------------
class TestBoundedThread:
    def test_runs_target_with_args(self):
        got = []
        t = storage_node_ops._bounded_thread(lambda a, b: got.append((a, b)), (1, 2))
        t.start()
        t.join(timeout=5)
        assert got == [(1, 2)]

    def test_caps_concurrency(self, monkeypatch):
        cap = 3
        n = 15
        monkeypatch.setattr(storage_node_ops, "_restart_worker_sem",
                            threading.BoundedSemaphore(cap))
        lock = threading.Lock()
        state = {"cur": 0, "max": 0, "ran": 0}

        def work():
            with lock:
                state["cur"] += 1
                state["max"] = max(state["max"], state["cur"])
                state["ran"] += 1
            time.sleep(0.05)
            with lock:
                state["cur"] -= 1

        threads = [storage_node_ops._bounded_thread(work) for _ in range(n)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        assert state["ran"] == n, "every worker must run"
        assert state["max"] <= cap, f"observed {state['max']} concurrent > cap {cap}"
        assert state["max"] >= 2, "expected real concurrency (not accidentally serial)"

    def test_semaphore_released_on_exception(self, monkeypatch):
        # If a raising target leaked its semaphore slot, later acquires would
        # block and the pool would wedge. Verify all slots are free afterwards.
        cap = 2
        sem = threading.BoundedSemaphore(cap)
        monkeypatch.setattr(storage_node_ops, "_restart_worker_sem", sem)
        monkeypatch.setattr(threading, "excepthook", lambda *a, **k: None)

        def boom():
            raise RuntimeError("intentional")

        threads = [storage_node_ops._bounded_thread(boom) for _ in range(6)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5)

        acquired = [sem.acquire(blocking=False) for _ in range(cap)]
        assert all(acquired), "semaphore slots leaked after target exceptions"


# ---------------------------------------------------------------------------
# Fix A — async logging handler
# ---------------------------------------------------------------------------
class TestAsyncLogging:
    def test_records_delivered_through_queue(self):
        stream = io.StringIO()
        target = logging.StreamHandler(stream=stream)
        target.setFormatter(logging.Formatter("%(message)s"))

        qh = utils.make_async_handler(target)
        try:
            assert isinstance(qh, logging.handlers.QueueHandler)
            lg = logging.getLogger("test_async_logging_isolated")
            lg.propagate = False
            lg.setLevel(logging.DEBUG)
            lg.addHandler(qh)

            for i in range(50):
                lg.info("marker-%d", i)

            # let the listener thread drain the queue
            deadline = time.time() + 5
            while time.time() < deadline and stream.getvalue().count("marker-") < 50:
                time.sleep(0.02)

            out = stream.getvalue()
            assert out.count("marker-") == 50, f"delivered {out.count('marker-')}/50"
        finally:
            qh._listener.stop()

    def test_handler_keeps_listener_ref(self):
        target = logging.StreamHandler(stream=io.StringIO())
        qh = utils.make_async_handler(target)
        try:
            assert getattr(qh, "_listener", None) is not None
        finally:
            qh._listener.stop()


# ---------------------------------------------------------------------------
# Fix C-safe — fast-skip connects to a known-down owning peer (no backoff)
# ---------------------------------------------------------------------------
class TestFastSkipDownPeer:
    @staticmethod
    def _dbc_returning(status):
        node = types.SimpleNamespace(status=status)

        class _DBC:
            def get_storage_node_by_id(self, _id):
                return node
        return _DBC

    def _run(self, monkeypatch, owner_status):
        calls = {"n": 0}

        def fake_connect(name, device, node):
            calls["n"] += 1
            raise RuntimeError("connect failed")

        monkeypatch.setattr(storage_node_ops, "connect_device", fake_connect)
        monkeypatch.setattr(storage_node_ops, "DBController",
                            self._dbc_returning(owner_status))
        monkeypatch.setattr(storage_node_ops.time, "sleep", lambda *_a: None)
        dev = types.SimpleNamespace(node_id="peer-1")
        node = types.SimpleNamespace(get_id=lambda: "local-node")
        storage_node_ops._connect_device_thread("remote_alceml_x", dev, node)
        return calls["n"]

    def test_down_peer_single_attempt(self, monkeypatch):
        # Owning peer rebooting -> exactly one attempt, no 3x backoff retry.
        assert self._run(monkeypatch, StorageNode.STATUS_RESTARTING) == 1

    def test_online_peer_full_retry(self, monkeypatch):
        # Owning peer ONLINE -> full best-effort retry (3 attempts) preserved.
        assert self._run(monkeypatch, StorageNode.STATUS_ONLINE) == 3

    def test_unknown_owner_full_retry(self, monkeypatch):
        # DB lookup failure -> stay on the safe full-retry path.
        calls = {"n": 0}

        def fake_connect(name, device, node):
            calls["n"] += 1
            raise RuntimeError("connect failed")

        class _DBC:
            def get_storage_node_by_id(self, _id):
                raise KeyError("gone")

        monkeypatch.setattr(storage_node_ops, "connect_device", fake_connect)
        monkeypatch.setattr(storage_node_ops, "DBController", _DBC)
        monkeypatch.setattr(storage_node_ops.time, "sleep", lambda *_a: None)
        dev = types.SimpleNamespace(node_id="peer-1")
        node = types.SimpleNamespace(get_id=lambda: "local-node")
        storage_node_ops._connect_device_thread("remote_alceml_x", dev, node)
        assert calls["n"] == 3


# ---------------------------------------------------------------------------
# Fix #1 — per-LVS recreate lock (serialize same LVS, parallelize different)
# ---------------------------------------------------------------------------
class TestPerLvsRecreateLock:
    def test_same_name_returns_same_lock(self):
        a = storage_node_ops._recreate_lvstore_lock("LVS_unit_X")
        b = storage_node_ops._recreate_lvstore_lock("LVS_unit_X")
        assert a is b

    def test_different_name_returns_different_lock(self):
        a = storage_node_ops._recreate_lvstore_lock("LVS_unit_A")
        b = storage_node_ops._recreate_lvstore_lock("LVS_unit_B")
        assert a is not b

    def _concurrency_probe(self):
        lock = threading.Lock()
        state = {"cur": 0, "max": 0, "ran": 0}

        def impl(snode, force=False, lvs_primary=None, activation_mode=False):
            with lock:
                state["cur"] += 1
                state["max"] = max(state["max"], state["cur"])
                state["ran"] += 1
            time.sleep(0.05)
            with lock:
                state["cur"] -= 1
            return True
        return state, impl

    def test_same_lvs_serializes(self, monkeypatch):
        state, impl = self._concurrency_probe()
        monkeypatch.setattr(storage_node_ops, "_recreate_lvstore_impl", impl)
        snode = types.SimpleNamespace(lvstore="LVS_serial")
        threads = [threading.Thread(target=storage_node_ops.recreate_lvstore, args=(snode,))
                   for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)
        assert state["ran"] == 4
        assert state["max"] == 1, "same-LVS recreates must be strictly serialized"

    def test_different_lvs_run_concurrently(self, monkeypatch):
        state, impl = self._concurrency_probe()
        monkeypatch.setattr(storage_node_ops, "_recreate_lvstore_impl", impl)
        nodes = [types.SimpleNamespace(lvstore=f"LVS_par_{i}") for i in range(4)]
        threads = [threading.Thread(target=storage_node_ops.recreate_lvstore, args=(sn,))
                   for sn in nodes]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)
        assert state["ran"] == 4
        assert state["max"] >= 2, "different-LVS recreates must run concurrently"

    def test_activation_mode_bypasses_lock(self, monkeypatch):
        # activation_mode must NOT take the per-LVS lock (globally serialized).
        got = {}

        def impl(snode, force=False, lvs_primary=None, activation_mode=False):
            got["activation_mode"] = activation_mode
            return True
        monkeypatch.setattr(storage_node_ops, "_recreate_lvstore_impl", impl)
        snode = types.SimpleNamespace(lvstore="LVS_act")
        # hold the LVS lock; activation-mode call must still proceed (not block)
        with storage_node_ops._recreate_lvstore_lock("LVS_act"):
            storage_node_ops.recreate_lvstore(snode, activation_mode=True)
        assert got["activation_mode"] is True


# ---------------------------------------------------------------------------
# Fix #2 — degraded recreate: coverage gate excludes down same-FD siblings
#          only under a sanctioned dead-FD recovery (FTT floor)
# ---------------------------------------------------------------------------
class TestDegradedRecreateCoverage:
    @staticmethod
    def _dev(alceml_bdev, status=NVMeDevice.STATUS_ONLINE):
        return types.SimpleNamespace(status=status, alceml_bdev=alceml_bdev)

    @staticmethod
    def _peer(node_id, status, fd, devs):
        return types.SimpleNamespace(
            get_id=lambda _id=node_id: _id, status=status,
            failure_domain=fd, nvme_devices=devs)

    def _run_coverage(self, monkeypatch, fd_recovery, present_bdevs):
        # snode in domain "fd0"; sibling A (same domain, DOWN, device missing);
        # peer B (other domain, ONLINE, device present); peer C (other domain,
        # ONLINE, device silently missing).
        peers = [
            self._peer("A", StorageNode.STATUS_DOWN, "fd0", [self._dev("alc_a1")]),
            self._peer("B", StorageNode.STATUS_ONLINE, "fd1", [self._dev("alc_b1")]),
            self._peer("C", StorageNode.STATUS_ONLINE, "fd1", [self._dev("alc_c1")]),
        ]

        class _FakeDBC:
            def get_storage_nodes_by_cluster_id(self, _cid):
                return peers

        fake_rpc = types.SimpleNamespace(
            get_bdevs=lambda name: name in present_bdevs)
        snode = types.SimpleNamespace(
            get_id=lambda: "snode", cluster_id="c1", failure_domain="fd0",
            rpc_client=lambda timeout, retry: fake_rpc)

        monkeypatch.setattr(storage_node_ops, "DBController", lambda: _FakeDBC())
        monkeypatch.setattr(storage_node_ops, "fd_dead_recovery_allowed",
                            lambda _db, _sn: fd_recovery)
        # repair=False so no connect threads / db writes are exercised here
        return storage_node_ops._verify_online_device_coverage(snode, repair=False)

    def test_excludes_down_sibling_under_fd_recovery(self, monkeypatch):
        # B present, A + C absent. Under sanctioned recovery, the down same-FD
        # sibling A is NOT required; only the genuinely-online-but-missing C is.
        missing = self._run_coverage(
            monkeypatch, fd_recovery=True, present_bdevs={"remote_alc_b1n1"})
        assert missing == ["remote_alc_c1n1"], missing

    def test_requires_down_sibling_without_fd_recovery(self, monkeypatch):
        # Same SPDK state, but recovery NOT sanctioned -> A is still required,
        # so both A and C surface as missing (2026-07-16 protection intact).
        missing = self._run_coverage(
            monkeypatch, fd_recovery=False, present_bdevs={"remote_alc_b1n1"})
        assert missing == ["remote_alc_a1n1", "remote_alc_c1n1"], missing

    def test_full_coverage_when_online_peers_present(self, monkeypatch):
        # Under recovery with A excluded and B+C both present -> no missing.
        missing = self._run_coverage(
            monkeypatch, fd_recovery=True,
            present_bdevs={"remote_alc_b1n1", "remote_alc_c1n1"})
        assert missing == [], missing
