"""Empirical benchmark of the FDB *client-side usage pattern* bottlenecks
discussed for the lvol-create latency investigation.

Spins up a single-node FoundationDB via testcontainers (same mechanism as
``tests/integration/conftest.py``) and measures, against a real cluster:

  A. Chatty per-call auto-transactions (the actual pattern used by
     ``BaseModel.read_from_db``/``write_to_db`` when called with the raw
     ``Database`` handle — every call is its own transaction) vs. the same
     operations batched into ONE ``fdb.transactional`` function.
  B. Full-table-scan cost vs. a small "mini projection" table, at
     increasing row counts — reproducing the "full SnapShot-table scan on
     every create" incident (commit f15aaa0ae).
  C. GIL contention from per-object Python reflection (the cost the
     ``_annotated_attrs`` cache in ``base_model.py`` was added to fix,
     commit 8e8d61adc) under concurrent THREADS in one process vs.
     concurrent PROCESSES — reproducing why replicating the whole OS
     process (not just adding worker threads) alleviated load.
  D. Whether a worker's wall-clock time is spent actually running on a
     CPU, blocked on FDB network I/O, or blocked waiting for the GIL —
     using ``time.thread_time()`` (excludes all off-CPU time, for
     whatever reason) against ``time.perf_counter()`` (wall time), plus
     explicit timing around the FDB calls in isolation, at low (K=1) vs.
     high (K=8) concurrency.

Not a pytest test (deliberately excluded from discovery, like the rest of
``tests/perf/``) — a standalone diagnostic script.

Usage:
    python tests/perf/bench_fdb_client_bottlenecks.py [--skip-a] [--skip-b] [--skip-c]

Requires Docker (or a rootless podman socket via $DOCKER_HOST) and
libfdb_c on the host, exactly like ``tox run -e integration``.
"""
import argparse
import json
import logging
import multiprocessing
import os
import statistics
import sys
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

logging.disable(logging.CRITICAL)  # isolate DB/CPU cost from logging I/O


# --------------------------------------------------------------------------
# FDB container bootstrap (reuses tests/integration/conftest.py's helpers)
# --------------------------------------------------------------------------

def _bootstrap_fdb():
    sys.path.insert(0, str(REPO_ROOT / "tests" / "integration"))
    import conftest as it_conftest  # type: ignore

    container = it_conftest._start_fdb_container()
    cluster_file = Path("/tmp") / "sbcli-bench-fdb.cluster"
    cluster_file.write_text(it_conftest.FDB_CLUSTER_CONTENTS)

    os.environ["FDB_CLUSTER_FILE"] = str(cluster_file)
    from simplyblock_core import constants
    constants.KVD_DB_FILE_PATH = str(cluster_file)

    return container, str(cluster_file)


def _stats(samples):
    xs = sorted(samples)
    n = len(xs)
    p95 = xs[int(0.95 * (n - 1))]
    return {
        "n": n,
        "mean_ms": statistics.mean(xs) * 1000,
        "median_ms": statistics.median(xs) * 1000,
        "p95_ms": p95 * 1000,
        "min_ms": xs[0] * 1000,
        "max_ms": xs[-1] * 1000,
    }


def _fmt(label, s):
    return (f"{label:<28} n={s['n']:<4} mean={s['mean_ms']:8.2f}ms  "
            f"median={s['median_ms']:8.2f}ms  p95={s['p95_ms']:8.2f}ms  "
            f"max={s['max_ms']:8.2f}ms")


# --------------------------------------------------------------------------
# Scenario A: chatty per-call auto-transactions vs. one batched transaction
# --------------------------------------------------------------------------

def scenario_a(kv_store, n_ops=25, repeats=20):
    import fdb

    print("\n" + "=" * 78)
    print(f"SCENARIO A — {n_ops} sequential ops/iteration, {repeats} iterations")
    print("Reproduces: add_lvol_ha's ~20-40 separate db_controller round trips")
    print("=" * 78)

    keys = [f"bench/a/{i}".encode() for i in range(n_ops)]
    for k in keys:
        kv_store[k] = json.dumps({"v": "x" * 200}).encode()

    # --- reads ---
    def chatty_reads():
        t0 = time.perf_counter()
        for k in keys:
            kv_store.get(k)
        return time.perf_counter() - t0

    def _batched_reads_tx(tr, ks):
        return [tr.get(k).wait() for k in ks]
    batched_reads = fdb.transactional(_batched_reads_tx)

    def batched_reads_once():
        t0 = time.perf_counter()
        batched_reads(kv_store, keys)
        return time.perf_counter() - t0

    chatty_read_samples = [chatty_reads() for _ in range(repeats)]
    batched_read_samples = [batched_reads_once() for _ in range(repeats)]

    # --- writes ---
    def chatty_writes(i):
        t0 = time.perf_counter()
        for j, k in enumerate(keys):
            kv_store.set(k, json.dumps({"v": "y" * 200, "i": i, "j": j}).encode())
        return time.perf_counter() - t0

    def _batched_writes_tx(tr, ks, i):
        for j, k in enumerate(ks):
            tr[k] = json.dumps({"v": "z" * 200, "i": i, "j": j}).encode()
    batched_writes = fdb.transactional(_batched_writes_tx)

    def batched_writes_once(i):
        t0 = time.perf_counter()
        batched_writes(kv_store, keys, i)
        return time.perf_counter() - t0

    chatty_write_samples = [chatty_writes(i) for i in range(repeats)]
    batched_write_samples = [batched_writes_once(i) for i in range(repeats)]

    r1, r2 = _stats(chatty_read_samples), _stats(batched_read_samples)
    w1, w2 = _stats(chatty_write_samples), _stats(batched_write_samples)
    print(_fmt("chatty reads (N txns)", r1))
    print(_fmt("batched reads (1 txn)", r2))
    print(f"  -> {r1['mean_ms'] / r2['mean_ms']:.1f}x faster batched (reads, mean)")
    print(_fmt("chatty writes (N txns)", w1))
    print(_fmt("batched writes (1 txn)", w2))
    print(f"  -> {w1['mean_ms'] / w2['mean_ms']:.1f}x faster batched (writes, mean)")

    return {"chatty_reads": r1, "batched_reads": r2,
            "chatty_writes": w1, "batched_writes": w2}


# --------------------------------------------------------------------------
# Scenario B: full-table scan vs. mini-projection scan, at growing sizes
# --------------------------------------------------------------------------

def scenario_b(kv_store, sizes=(200, 2000, 8000)):
    from simplyblock_core.models.base_model import BaseModel

    print("\n" + "=" * 78)
    print("SCENARIO B — full-record scan vs. mini-projection scan")
    print("Reproduces: full SnapShot-table scan (embeds full LVol dict) on")
    print("every create, vs. reading the small Mini table (commit f15aaa0ae)")
    print("=" * 78)

    class BenchFull(BaseModel):
        uuid: str = ""
        node_id: str = ""
        status: str = ""
        # ~2KB blob to stand in for a full record embedding a nested
        # 70+-field object, as SnapShot embeds the complete LVol dict.
        blob: str = ""

    class BenchMini(BaseModel):
        uuid: str = ""
        node_id: str = ""
        status: str = ""

    results = {}
    for m in sizes:
        kv_store.clear_range(b"bench/b/", b"bench/b0")
        t_setup = time.perf_counter()
        for i in range(m):
            full = BenchFull()
            full.uuid = str(i)
            full.node_id = f"node-{i % 16}"
            full.status = "online"
            full.blob = "x" * 2000
            full.write_to_db(kv_store)

            mini = BenchMini()
            mini.uuid = str(i)
            mini.node_id = f"node-{i % 16}"
            mini.status = "online"
            mini.write_to_db(kv_store)
        setup_s = time.perf_counter() - t_setup

        t0 = time.perf_counter()
        full_rows = BenchFull().read_from_db(kv_store)
        full_scan_s = time.perf_counter() - t0

        t0 = time.perf_counter()
        mini_rows = BenchMini().read_from_db(kv_store)
        mini_scan_s = time.perf_counter() - t0

        assert len(full_rows) == m and len(mini_rows) == m
        ratio = full_scan_s / mini_scan_s if mini_scan_s > 0 else float("inf")
        print(f"  M={m:<6} setup={setup_s:6.2f}s   "
              f"full_scan={full_scan_s * 1000:8.2f}ms   "
              f"mini_scan={mini_scan_s * 1000:8.2f}ms   "
              f"ratio={ratio:5.1f}x")
        results[m] = {"full_scan_ms": full_scan_s * 1000,
                       "mini_scan_ms": mini_scan_s * 1000, "ratio": ratio}

    kv_store.clear_range(b"bench/b/", b"bench/b0")
    return results


# --------------------------------------------------------------------------
# Scenario C: GIL contention (threads vs. processes) — cached vs. uncached
# reflection, reproducing commit 8e8d61adc's "GIL convoy" finding.
# --------------------------------------------------------------------------

def _old_get_attrs_map(self):
    """Pre-8e8d61adc BaseModel.get_attrs_map: re-derives the annotation walk
    on every call instead of using the per-class ``_annotated_attrs`` cache."""
    from inspect import ismethod
    attrs = {}
    for s, t in self.all_annotations().items():
        if not s.startswith("_") and not ismethod(getattr(self, s)):
            attrs[s] = {"type": t, "default": getattr(self, s)}
    return attrs


@contextmanager
def reflection_mode(cached: bool):
    from simplyblock_core.models.base_model import BaseModel
    if cached:
        yield
        return
    original = BaseModel.get_attrs_map
    BaseModel.get_attrs_map = _old_get_attrs_map
    try:
        yield
    finally:
        BaseModel.get_attrs_map = original


def _build_storage_node_dict(i: int, n_devices: int = 8) -> dict:
    devices = [
        {
            "uuid": f"dev-{i}-{j}",
            "device_name": f"nvme{j}n1",
            "size": 1_000_000_000_000,
            "status": "online",
            "serial_number": f"SN{i:04d}{j:02d}",
            "pcie_address": f"0000:{i:02x}:{j:02x}.0",
            "model_id": "SBLK-NVME",
            "cluster_device_order": j,
        }
        for j in range(n_devices)
    ]
    return {
        "uuid": f"node-{i}",
        "hostname": f"storage-node-{i}",
        "status": "online",
        "cluster_id": "bench-cluster",
        "mgmt_ip": f"10.0.0.{i % 250}",
        "cpu": 32,
        "cpu_hz": 2400000000,
        "nvme_devices": devices,
    }


def _worker_unit(kv_store, n_objects, cached, prefix):
    """One unit of work: construct n_objects StorageNode-sized objects from
    dict (CPU-bound reflection happens here), write each, then read the
    whole batch back. Returns wall-clock seconds."""
    from simplyblock_core.models.storage_node import StorageNode

    t0 = time.perf_counter()
    with reflection_mode(cached=cached):
        for i in range(n_objects):
            node = StorageNode(_build_storage_node_dict(i))
            node.uuid = f"{prefix}-{i}"
            node.write_to_db(kv_store)
        StorageNode().read_from_db(kv_store, id=prefix)
    return time.perf_counter() - t0


def _thread_worker(kv_store, n_objects, cached, worker_id):
    return _worker_unit(kv_store, n_objects, cached, f"bench/c/t{worker_id}")


def _process_worker(cluster_file, n_objects, cached, worker_id):
    """Runs in its own OS process: opens its own FDB connection, exactly
    like one of the 15 replicated API processes would."""
    import sys as _sys
    _sys.path.insert(0, str(REPO_ROOT))
    logging.disable(logging.CRITICAL)
    os.environ["FDB_CLUSTER_FILE"] = cluster_file
    from simplyblock_core import constants
    constants.KVD_DB_FILE_PATH = cluster_file
    from simplyblock_core.db_controller import DBController
    kv_store = DBController().kv_store
    return _worker_unit(kv_store, n_objects, cached, f"bench/c/p{worker_id}")


def scenario_c(kv_store, cluster_file, total_objects=48, worker_counts=(1, 4, 8)):
    print("\n" + "=" * 78)
    print("SCENARIO C — GIL contention: threads vs. processes,")
    print("cached (current) vs. uncached (pre-8e8d61adc) reflection")
    print("=" * 78)

    results = {}
    for cached in (True, False):
        label = "cached (current)" if cached else "uncached (pre-fix)"
        print(f"\n-- reflection: {label} --")
        for k in worker_counts:
            n_per_worker = max(1, total_objects // k)

            kv_store.clear_range(b"bench/c/", b"bench/c0")
            t0 = time.perf_counter()
            with ThreadPoolExecutor(max_workers=k) as ex:
                futs = [ex.submit(_thread_worker, kv_store, n_per_worker, cached, w)
                        for w in range(k)]
                for f in futs:
                    f.result()
            thread_wall_s = time.perf_counter() - t0

            kv_store.clear_range(b"bench/c/", b"bench/c0")
            t0 = time.perf_counter()
            # spawn, not the Linux default fork: the parent process already
            # has an open FDB Database (its background network thread is
            # running). fork() only duplicates the calling thread, so a
            # forked child inherits a Database handle wired to a network
            # thread that doesn't exist in the child -> every FDB call in
            # the child hangs forever. This is itself a real client-side
            # finding: the fdb C client is not fork-safe, and the real app
            # never forks (each replica is a freshly-exec'd process), which
            # is exactly the discipline `spawn` reproduces here.
            ctx = multiprocessing.get_context("spawn")
            with ProcessPoolExecutor(max_workers=k, mp_context=ctx) as ex:
                futs = [ex.submit(_process_worker, cluster_file, n_per_worker, cached, w)
                        for w in range(k)]
                for f in futs:
                    f.result()
            process_wall_s = time.perf_counter() - t0

            total_done = n_per_worker * k
            thread_throughput = total_done / thread_wall_s
            process_throughput = total_done / process_wall_s
            speedup = thread_wall_s / process_wall_s if process_wall_s > 0 else float("inf")
            print(f"  K={k:<2} objs={total_done:<4} "
                  f"threads={thread_wall_s * 1000:8.1f}ms ({thread_throughput:7.1f} obj/s)   "
                  f"processes={process_wall_s * 1000:8.1f}ms ({process_throughput:7.1f} obj/s)   "
                  f"process/thread speedup={speedup:4.2f}x")
            results[(cached, k)] = {
                "thread_ms": thread_wall_s * 1000, "process_ms": process_wall_s * 1000,
                "thread_obj_s": thread_throughput, "process_obj_s": process_throughput,
                "speedup": speedup,
            }

    kv_store.clear_range(b"bench/c/", b"bench/c0")
    return results


def _process_warmup(cluster_file):
    """Pays the one-time cost (interpreter import + fdb.open) for a
    persistent worker process, run BEFORE the timed section below — this is
    what makes scenario_c_warm represent an already-running replica instead
    of a process spawned fresh per unit of work."""
    import sys as _sys
    _sys.path.insert(0, str(REPO_ROOT))
    logging.disable(logging.CRITICAL)
    os.environ["FDB_CLUSTER_FILE"] = cluster_file
    from simplyblock_core import constants
    constants.KVD_DB_FILE_PATH = cluster_file
    from simplyblock_core.db_controller import DBController
    DBController()
    return os.getpid()


def _process_worker_batch(cluster_file, n_objects, cached, batch_id):
    """Same DBController() call as _process_warmup, but by the time this
    runs the singleton is already cached in this worker process (set up by
    _process_warmup above) — so unlike _process_worker in scenario_c, this
    pays no fdb.open() cost. Mirrors a warm replica handling request N."""
    import sys as _sys
    _sys.path.insert(0, str(REPO_ROOT))
    logging.disable(logging.CRITICAL)
    os.environ["FDB_CLUSTER_FILE"] = cluster_file
    from simplyblock_core import constants
    constants.KVD_DB_FILE_PATH = cluster_file
    from simplyblock_core.db_controller import DBController
    kv_store = DBController().kv_store
    return _worker_unit(kv_store, n_objects, cached, f"bench/cw/p{batch_id}")


def _thread_worker_batch(kv_store, n_objects, cached, batch_id):
    return _worker_unit(kv_store, n_objects, cached, f"bench/cw/t{batch_id}")


def scenario_c_warm(kv_store, cluster_file, total_batches=80, n_objects_per_batch=3,
                     worker_counts=(1, 4, 8)):
    """Steady-state throughput of ALREADY-RUNNING workers: startup cost is
    paid once outside the timer, then many small batches (like many
    concurrent requests hitting a live replica) stream through the warm
    pool. This is the topology that actually matches production — 15
    long-lived replicas, not 15 processes spawned per request — unlike
    scenario_c, which (by creating a fresh pool per measurement) mostly
    measured spawn/import/fdb.open cold-start cost."""
    print("\n" + "=" * 78)
    print("SCENARIO C-WARM — same as C, but workers are pre-warmed and reused")
    print("across many batches (startup cost excluded from the timed run)")
    print("=" * 78)

    results = {}
    for cached in (True, False):
        label = "cached (current)" if cached else "uncached (pre-fix)"
        print(f"\n-- reflection: {label} --")
        for k in worker_counts:
            kv_store.clear_range(b"bench/cw/", b"bench/cw0")
            with ThreadPoolExecutor(max_workers=k) as ex:
                t0 = time.perf_counter()
                futs = [ex.submit(_thread_worker_batch, kv_store, n_objects_per_batch, cached, b)
                        for b in range(total_batches)]
                for f in futs:
                    f.result()
                thread_elapsed = time.perf_counter() - t0

            kv_store.clear_range(b"bench/cw/", b"bench/cw0")
            ctx = multiprocessing.get_context("spawn")
            with ProcessPoolExecutor(max_workers=k, mp_context=ctx) as ex:
                warm_futs = [ex.submit(_process_warmup, cluster_file) for _ in range(k)]
                pids = {f.result() for f in warm_futs}  # blocks until all k workers are warm
                t0 = time.perf_counter()
                futs = [ex.submit(_process_worker_batch, cluster_file, n_objects_per_batch, cached, b)
                        for b in range(total_batches)]
                for f in futs:
                    f.result()
                process_elapsed = time.perf_counter() - t0

            total_done = total_batches * n_objects_per_batch
            thread_tp = total_done / thread_elapsed
            process_tp = total_done / process_elapsed
            speedup = thread_elapsed / process_elapsed if process_elapsed > 0 else float("inf")
            print(f"  K={k:<2} warm_procs={len(pids):<2} objs={total_done:<5} "
                  f"threads={thread_elapsed * 1000:8.1f}ms ({thread_tp:7.1f} obj/s)   "
                  f"processes={process_elapsed * 1000:8.1f}ms ({process_tp:7.1f} obj/s)   "
                  f"process/thread speedup={speedup:4.2f}x")
            results[(cached, k)] = {
                "thread_ms": thread_elapsed * 1000, "process_ms": process_elapsed * 1000,
                "thread_obj_s": thread_tp, "process_obj_s": process_tp, "speedup": speedup,
            }

    kv_store.clear_range(b"bench/cw/", b"bench/cw0")
    return results


# --------------------------------------------------------------------------
# Scenario D: is off-CPU time genuine I/O wait, or GIL contention?
#
# time.thread_time() only counts CPU actually consumed by the calling
# thread/process — it excludes ALL off-CPU time, whether the thread is
# blocked in a socket syscall (I/O wait) or blocked trying to reacquire the
# GIL (scheduling wait). By also timing the FDB calls explicitly (wall
# clock, in isolation) we get a second, independent estimate of I/O wait.
# At K=1 there's no GIL contention (only one runnable thread), so
# wall - cpu_thread there IS the I/O-wait baseline. If off-CPU time at K=8
# grows well beyond that baseline for threads, but not for processes (which
# have no shared GIL), the growth is attributable to the GIL, not to FDB
# getting slower under load.
# --------------------------------------------------------------------------

def _profiled_unit(kv_store, n_objects, cached, prefix):
    from simplyblock_core.models.storage_node import StorageNode

    wall_t0 = time.perf_counter()
    cpu_t0 = time.thread_time()
    cpu_construct = 0.0
    io_s = 0.0
    with reflection_mode(cached=cached):
        for i in range(n_objects):
            c0 = time.perf_counter()
            node = StorageNode(_build_storage_node_dict(i))
            node.uuid = f"{prefix}-{i}"
            cpu_construct += time.perf_counter() - c0

            w0 = time.perf_counter()
            node.write_to_db(kv_store)
            io_s += time.perf_counter() - w0

        r0 = time.perf_counter()
        StorageNode().read_from_db(kv_store, id=prefix)
        io_s += time.perf_counter() - r0

    wall_s = time.perf_counter() - wall_t0
    cpu_thread_s = time.thread_time() - cpu_t0
    return {"wall_s": wall_s, "cpu_thread_s": cpu_thread_s,
            "cpu_construct_s": cpu_construct, "io_s": io_s}


def _thread_profiled(kv_store, n_objects, cached, wid):
    return _profiled_unit(kv_store, n_objects, cached, f"bench/d/t{wid}")


def _process_profiled(cluster_file, n_objects, cached, wid):
    import sys as _sys
    _sys.path.insert(0, str(REPO_ROOT))
    logging.disable(logging.CRITICAL)
    os.environ["FDB_CLUSTER_FILE"] = cluster_file
    from simplyblock_core import constants
    constants.KVD_DB_FILE_PATH = cluster_file
    from simplyblock_core.db_controller import DBController
    kv_store = DBController().kv_store
    return _profiled_unit(kv_store, n_objects, cached, f"bench/d/p{wid}")


def _avg(dicts, key):
    return statistics.mean(d[key] for d in dicts)


def scenario_d(kv_store, cluster_file, n_objects=15, worker_counts=(1, 8)):
    print("\n" + "=" * 78)
    print("SCENARIO D — off-CPU time: genuine I/O wait, or GIL contention?")
    print("=" * 78)

    for cached in (True, False):
        label = "cached (current)" if cached else "uncached (pre-fix)"
        print(f"\n-- reflection: {label} --")
        io_baseline_ms = None
        for k in worker_counts:
            kv_store.clear_range(b"bench/d/", b"bench/d0")
            with ThreadPoolExecutor(max_workers=k) as ex:
                futs = [ex.submit(_thread_profiled, kv_store, n_objects, cached, w)
                        for w in range(k)]
                thread_results = [f.result() for f in futs]

            kv_store.clear_range(b"bench/d/", b"bench/d0")
            ctx = multiprocessing.get_context("spawn")
            with ProcessPoolExecutor(max_workers=k, mp_context=ctx) as ex:
                warm = [ex.submit(_process_warmup, cluster_file) for _ in range(k)]
                for f in warm:
                    f.result()
                futs = [ex.submit(_process_profiled, cluster_file, n_objects, cached, w)
                        for w in range(k)]
                process_results = [f.result() for f in futs]

            for kind, results in (("threads", thread_results), ("processes", process_results)):
                wall = _avg(results, "wall_s") * 1000
                cpu = _avg(results, "cpu_thread_s") * 1000
                construct = _avg(results, "cpu_construct_s") * 1000
                io = _avg(results, "io_s") * 1000
                off_cpu = wall - cpu
                busy_pct = 100 * cpu / wall if wall else 0
                print(f"  K={k:<2} {kind:<10} wall={wall:7.1f}ms  "
                      f"cpu(thread_time)={cpu:7.1f}ms ({busy_pct:5.1f}% busy)  "
                      f"construct={construct:6.1f}ms  measured_io={io:6.1f}ms  "
                      f"off_cpu={off_cpu:7.1f}ms")
                if k == worker_counts[0] and kind == "threads":
                    io_baseline_ms = off_cpu  # K=1: no GIL contention possible
            if io_baseline_ms is not None and k != worker_counts[0]:
                extra_thread_off_cpu = _avg(thread_results, "wall_s") * 1000 - _avg(thread_results, "cpu_thread_s") * 1000
                print(f"    -> threads' off-CPU time at K={k} is {extra_thread_off_cpu - io_baseline_ms:+.1f}ms "
                      f"vs. the K=1 I/O-wait baseline ({io_baseline_ms:.1f}ms) "
                      f"-- {'GIL contention' if extra_thread_off_cpu > io_baseline_ms * 1.3 else 'no clear GIL signal'}")

    kv_store.clear_range(b"bench/d/", b"bench/d0")


# --------------------------------------------------------------------------
# Scenario E: memory footprint of process duplication vs. shared-memory
# threads. Orthogonal to the GIL — this cost applies whether or not the GIL
# is disabled, since it comes from N full interpreter + import + FDB
# connection copies vs. one copy shared by N threads.
# --------------------------------------------------------------------------

def scenario_e(cluster_file, worker_counts=(1, 4, 8)):
    import psutil

    print("\n" + "=" * 78)
    print("SCENARIO E — memory footprint: N warm processes vs. one process")
    print("with N threads (RSS, MiB). Independent of the GIL.")
    print("=" * 78)

    this_proc = psutil.Process()
    base_rss = this_proc.memory_info().rss
    print(f"  baseline (this interpreter, before pool): {base_rss / 1e6:7.1f} MiB")

    for k in worker_counts:
        ctx = multiprocessing.get_context("spawn")
        with ProcessPoolExecutor(max_workers=k, mp_context=ctx) as ex:
            futs = [ex.submit(_process_warmup, cluster_file) for _ in range(k)]
            pids = [f.result() for f in futs]
            time.sleep(0.2)  # let RSS accounting settle
            total_rss = 0
            seen = set()
            for pid in pids:
                if pid in seen:
                    continue
                seen.add(pid)
                try:
                    total_rss += psutil.Process(pid).memory_info().rss
                except psutil.NoSuchProcess:
                    pass
        print(f"  K={k:<2} {len(seen)} distinct warm processes: "
              f"total RSS={total_rss / 1e6:8.1f} MiB "
              f"({total_rss / len(seen) / 1e6:6.1f} MiB/process, "
              f"{(total_rss - base_rss) / 1e6:8.1f} MiB above this process's own baseline)")

    # threads: all share this process's single address space + single FDB
    # connection, so RSS growth from K=1 -> K=8 is thread-stack-sized, not
    # interpreter-sized.
    from simplyblock_core.db_controller import DBController
    DBController()  # ensure this process itself has FDB open, like the pool workers did
    rss_with_own_fdb = this_proc.memory_info().rss
    for k in worker_counts:
        with ThreadPoolExecutor(max_workers=k) as ex:
            futs = [ex.submit(lambda: this_proc.memory_info().rss) for _ in range(k)]
            for f in futs:
                f.result()
        rss_after = this_proc.memory_info().rss
        print(f"  K={k:<2} {k} threads in 1 process: "
              f"RSS={rss_after / 1e6:8.1f} MiB "
              f"({(rss_after - rss_with_own_fdb) / 1e6:+6.1f} MiB vs. this process's own 1-connection baseline)")


# --------------------------------------------------------------------------
# Scenario F: is the fdb C client itself thread-safe?
#
# Every earlier scenario had each thread operate on its own disjoint keys —
# that stresses throughput, not the client's internal thread safety, since
# nothing forces two threads to touch the same in-flight state at once.
# This scenario deliberately maximizes contention: K threads share ONE
# Database handle and race to increment the SAME key via a non-atomic
# read-modify-write wrapped in fdb.transactional. FDB's serializable
# isolation + automatic conflict-retry means the final count is only
# EXACTLY right if every retry, every Future callback, and every
# transaction object the ctypes binding juggles under real concurrent
# (GIL-free) access stays correctly isolated internally. A wrong final
# count, a crash, or a hang all indicate a real thread-safety bug in the
# client (as opposed to FDB's server-side transaction protocol, which is
# separately and extensively verified upstream) — not just a performance
# characteristic.
# --------------------------------------------------------------------------

def _increment_tx(tr, key):
    raw = tr.get(key).wait()
    val = int(bytes(raw)) if raw.present() else 0
    tr.set(key, str(val + 1).encode())


def _race_increment_worker(kv_store, key, iterations):
    import fdb
    increment = fdb.transactional(_increment_tx)
    for _ in range(iterations):
        increment(kv_store, key)
    return True


def _race_process_worker(cluster_file, key, iterations):
    import sys as _sys
    _sys.path.insert(0, str(REPO_ROOT))
    logging.disable(logging.CRITICAL)
    os.environ["FDB_CLUSTER_FILE"] = cluster_file
    from simplyblock_core import constants
    constants.KVD_DB_FILE_PATH = cluster_file
    from simplyblock_core.db_controller import DBController
    kv_store = DBController().kv_store
    return _race_increment_worker(kv_store, key, iterations)


def scenario_f(kv_store, cluster_file, n_threads=24, iterations_per_thread=25):
    print("\n" + "=" * 78)
    print("SCENARIO F — fdb client thread-safety stress test: many threads,")
    print("ONE shared Database handle, all racing to increment the SAME key.")
    print("Correct final count is the only acceptable outcome.")
    print("=" * 78)

    key = b"bench/f/counter"
    expected = n_threads * iterations_per_thread

    kv_store.set(key, b"0")
    t0 = time.perf_counter()
    crashed = None
    try:
        with ThreadPoolExecutor(max_workers=n_threads) as ex:
            futs = [ex.submit(_race_increment_worker, kv_store, key, iterations_per_thread)
                    for _ in range(n_threads)]
            for f in futs:
                f.result(timeout=60)
    except Exception as e:  # noqa: BLE001 - we want to report, not raise, a client crash
        crashed = repr(e)
    elapsed = time.perf_counter() - t0

    actual = int(bytes(kv_store.get(key))) if crashed is None else None
    status = "CORRECT" if actual == expected else ("CRASHED/HUNG" if crashed else "*** WRONG COUNT ***")
    print(f"  threads (shared connection): expected={expected}  actual={actual}  "
          f"elapsed={elapsed * 1000:.1f}ms  -> {status}")
    if crashed:
        print(f"    exception: {crashed}")

    # Control: same total work, but via separate processes (separate
    # connections) instead of shared-connection threads. Should also be
    # exactly correct (it's a different code path entirely — no shared
    # client state at all — so this isn't testing the same thing, it's a
    # sanity check that the counter protocol itself is sound).
    kv_store.set(key, b"0")
    ctx = multiprocessing.get_context("spawn")
    t0 = time.perf_counter()
    with ProcessPoolExecutor(max_workers=n_threads, mp_context=ctx) as ex:
        futs = [ex.submit(_race_process_worker, cluster_file, key, iterations_per_thread)
                for _ in range(n_threads)]
        for f in futs:
            f.result(timeout=60)
    elapsed_p = time.perf_counter() - t0
    actual_p = int(bytes(kv_store.get(key)))
    status_p = "CORRECT" if actual_p == expected else "*** WRONG COUNT ***"
    print(f"  processes (separate connections): expected={expected}  actual={actual_p}  "
          f"elapsed={elapsed_p * 1000:.1f}ms  -> {status_p}")

    kv_store.clear_range(b"bench/f/", b"bench/f0")
    return {"threads_correct": actual == expected, "threads_crashed": crashed is not None,
            "processes_correct": actual_p == expected}


# --------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip-a", action="store_true")
    parser.add_argument("--skip-b", action="store_true")
    parser.add_argument("--skip-c", action="store_true", help="skip the cold-start process variant")
    parser.add_argument("--skip-c-warm", action="store_true")
    parser.add_argument("--skip-d", action="store_true")
    parser.add_argument("--skip-e", action="store_true")
    parser.add_argument("--skip-f", action="store_true")
    args = parser.parse_args()

    print("Starting containerized FoundationDB (testcontainers)...")
    container, cluster_file = _bootstrap_fdb()
    try:
        from simplyblock_core.db_controller import DBController
        db = DBController()
        assert db.kv_store is not None, "FDB did not open"
        kv_store = db.kv_store
        print(f"FDB ready, cluster file: {cluster_file}")

        if not args.skip_a:
            scenario_a(kv_store)
        if not args.skip_b:
            scenario_b(kv_store)
        if not args.skip_c:
            scenario_c(kv_store, cluster_file)
        if not args.skip_c_warm:
            scenario_c_warm(kv_store, cluster_file)
        if not args.skip_d:
            scenario_d(kv_store, cluster_file)
        if not args.skip_e:
            scenario_e(cluster_file)
        if not args.skip_f:
            scenario_f(kv_store, cluster_file)

    finally:
        print("\nStopping FDB container...")
        container.stop()


if __name__ == "__main__":
    main()
