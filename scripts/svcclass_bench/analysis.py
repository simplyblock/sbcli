"""Metrics of the replication service-class benchmark, computed from raw records.

Pure functions only, so they can be tested without a cluster:

* fio per-second IOPS/bandwidth logs -> client IOPS and throughput per window;
* the snapshot records the management-node agent collected -> replication
  iterations, time to steady state, and per-iteration statistics.

A replication *iteration* is one internal snapshot of one volume: it starts when
the snapshot is taken (``created_at``) and ends when its copy exists on the
target (the copy's ``created_at``, written when the transfer was finished and
converted). The cadence never queues work: the next snapshot of a volume is only
taken once the previous one replicated, so a transfer that overruns the interval
stretches the period rather than building a backlog. Steady state is therefore a
property of the durations, not of whether they fit the interval.
"""
from __future__ import annotations

import glob
import itertools
import os
import re
import statistics
from collections import defaultdict
from dataclasses import dataclass

# --------------------------------------------------------------------------- #
# fio
# --------------------------------------------------------------------------- #

_LOG_NAME = re.compile(r"_(iops|bw)\.\d+\.log$")


def load_fio_series(log_dir: str, start_epoch: float) -> dict[str, dict[int, float]]:
    """Per-second totals over every fio job log in *log_dir*.

    fio writes one ``<prefix>_iops.<n>.log`` and ``<prefix>_bw.<n>.log`` per job
    (``log_avg_msec=1000``): lines of ``msec, value, ddir, bs, offset``, msec
    relative to the job's start and ddir 0 for reads, 1 for writes. Bandwidth is
    in KiB/s. *start_epoch* is when the jobs started; every second is keyed by
    its absolute epoch so windows can be cut by wall-clock time.

    Returns ``{"read_iops", "write_iops", "read_kibps", "write_kibps"}`` each as
    ``{epoch_second: summed value}``.
    """
    series: dict[str, dict[int, float]] = {
        k: defaultdict(float) for k in ("read_iops", "write_iops", "read_kibps", "write_kibps")}
    for path in glob.glob(os.path.join(log_dir, "**", "*.log"), recursive=True):
        m = _LOG_NAME.search(os.path.basename(path))
        if not m:
            continue
        kind = "iops" if m.group(1) == "iops" else "kibps"
        with open(path, encoding="utf-8", errors="replace") as f:
            for line in f:
                parts = [p.strip() for p in line.split(",")]
                if len(parts) < 3:
                    continue
                try:
                    msec, value, ddir = int(parts[0]), float(parts[1]), int(parts[2])
                except ValueError:
                    continue
                if ddir not in (0, 1):
                    continue
                key = ("read_" if ddir == 0 else "write_") + kind
                series[key][int(start_epoch + msec / 1000.0)] += value
    return {k: dict(v) for k, v in series.items()}


def client_window(series: dict[str, dict[int, float]], t0: float, t1: float) -> dict:
    """Average client IOPS and MB/s over [t0, t1), reads, writes, and total."""
    seconds = range(int(t0), int(t1))
    n = max(1, len(seconds))

    def avg(key):
        s = series.get(key, {})
        return sum(s.get(t, 0.0) for t in seconds) / n

    r_iops, w_iops = avg("read_iops"), avg("write_iops")
    r_mbs, w_mbs = avg("read_kibps") / 1024.0, avg("write_kibps") / 1024.0
    return {
        "seconds": len(seconds),
        "read_iops": r_iops, "write_iops": w_iops, "total_iops": r_iops + w_iops,
        "read_mbs": r_mbs, "write_mbs": w_mbs, "total_mbs": r_mbs + w_mbs,
    }


# --------------------------------------------------------------------------- #
# replication iterations
# --------------------------------------------------------------------------- #

@dataclass
class Iteration:
    volume: str
    snapshot: str
    started: float      # snapshot taken
    finished: float     # copy exists on the target

    @property
    def duration(self) -> float:
        return self.finished - self.started


def iterations_from_records(records: list[dict], t0: float) -> dict[str, list[Iteration]]:
    """Completed iterations per volume, oldest first, from the agent's records.

    A record is one internal snapshot as last seen: ``volume``, ``snapshot``,
    ``created_at`` and, once replicated, ``replicated_at``. The latest record of
    a snapshot wins; snapshots taken before *t0* (an earlier run) are ignored.
    """
    latest: dict[str, dict] = {}
    for r in records:
        merged = {**latest.get(r["snapshot"], {}), **{k: v for k, v in r.items() if v}}
        # -1 is "replicated, but the target copy is gone": retention prunes it
        # later, and that must not erase the completion time recorded first
        if r.get("replicated_at", 0) < 0 and latest.get(r["snapshot"], {}).get("replicated_at", 0) > 0:
            merged["replicated_at"] = latest[r["snapshot"]]["replicated_at"]
        latest[r["snapshot"]] = merged
    out: dict[str, list[Iteration]] = defaultdict(list)
    for r in latest.values():
        if not r.get("replicated_at") or r.get("created_at", 0) < t0:
            continue
        out[r["volume"]].append(Iteration(r["volume"], r["snapshot"],
                                          float(r["created_at"]), float(r["replicated_at"])))
    for v in out.values():
        v.sort(key=lambda i: i.started)
    return dict(out)


def steady_start(iters: list[Iteration], window: int = 5, rel_tol: float = 0.25,
                 abs_tol: float = 2.0) -> int | None:
    """Index of the first iteration of the first steady window, or None.

    The first iteration copies the whole volume and is never part of it. A
    window is *window* consecutive iterations whose durations all lie within
    max(rel_tol * median, abs_tol) seconds of the window's median; the absolute
    floor keeps second-granular timestamps from making short transfers look
    unstable.
    """
    for i in range(1, len(iters) - window + 1):
        durs = [it.duration for it in iters[i:i + window]]
        med = statistics.median(durs)
        tol = max(rel_tol * med, abs_tol)
        if all(abs(d - med) <= tol for d in durs):
            return i
    return None


def _stats(values: list[float]) -> dict:
    if not values:
        return {"n": 0}
    s = sorted(values)
    p95 = s[min(len(s) - 1, round(0.95 * (len(s) - 1)))]
    return {"n": len(s), "mean": statistics.fmean(s), "p50": statistics.median(s),
            "p95": p95, "max": s[-1]}


def replication_summary(by_volume: dict[str, list[Iteration]], t0: float, **steady_kw) -> dict:
    """Time to steady state and per-iteration statistics, cluster-wide.

    The cluster reaches steady state when its slowest volume does. Before that
    is the ramp (the initial full copies and whatever settling follows); from it
    on is steady state. Periods are the gaps between consecutive snapshots of a
    volume, i.e. the effective replication interval (RPO) achieved.
    """
    per_volume = {}
    ramp_durs, steady_durs, steady_periods = [], [], []
    steady_at = []
    for vol, iters in by_volume.items():
        idx = steady_start(iters, **steady_kw)
        ramp = iters if idx is None else iters[:idx]
        steady = [] if idx is None else iters[idx:]
        ramp_durs += [i.duration for i in ramp]
        steady_durs += [i.duration for i in steady]
        steady_periods += [b.started - a.started for a, b in itertools.pairwise(steady)]
        when = None if idx is None else iters[idx].started
        if when is not None:
            steady_at.append(when)
        per_volume[vol] = {
            "iterations": len(iters),
            "initial_full_copy_s": iters[0].duration if iters else None,
            "steady_from_s": None if when is None else when - t0,
        }
    reached = bool(by_volume) and len(steady_at) == len(by_volume)
    return {
        "steady_state_reached": reached,
        "time_to_steady_s": (max(steady_at) - t0) if reached else None,
        "ramp_iteration_s": _stats(ramp_durs),
        "steady_iteration_s": _stats(steady_durs),
        "steady_period_s": _stats(steady_periods),
        "volumes": per_volume,
        "steady_epoch": max(steady_at) if reached else None,
    }
