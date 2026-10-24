#!/usr/bin/env python3
"""Before, during and after replication, for one scenario (b) run with --after.

    python phase_report.py results/<run-b>

Prints, from fio's per-second logs and the collector's records:

* client IOPS and MB/s per phase (before replication, the ramp to steady
  state, steady state, after replication stopped), and the drop against
  before;
* convergence (initial full copies, time to steady state) against the
  sustained lag in steady state (the control plane's lag_seconds);
* replication speed per cycle (bytes / duration) and the bytes replicated
  per second of each phase;
* a per-minute table of client IOPS and MB/s, lag and outstanding bytes.
"""
import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path

import analysis

MB = 1024 * 1024


def phases(meta, warm_skip=60):
    """(name, start, end) of every phase the run recorded, in order."""
    t0, end = meta["t0"], meta["t_end"]
    stop = meta.get("t_repl_stop") or end
    steady = meta.get("steady_epoch")
    out = [("before", meta["fio_start"] + warm_skip, t0)]
    if steady:
        out += [("ramp", t0, steady), ("steady", steady, stop)]
    else:
        out.append(("replicating", t0, stop))
    if meta.get("t_repl_stop"):
        out.append(("after", stop, end))
    return out


def cycles(records):
    """Every completed replication cycle once: (volume, finished, seconds, bytes),
    from the control plane's newest-cycle figures in the repl_info records."""
    seen = {}
    for r in records:
        if r.get("kind") != "repl_info" or not r.get("last_replicated_at"):
            continue
        key = (r["volume"], r["last_replicated_at"])
        if key not in seen and r.get("last_cycle_seconds"):
            seen[key] = (r["volume"], r["t"], float(r["last_cycle_seconds"]),
                         float(r.get("last_cycle_bytes") or 0))
    return sorted(seen.values(), key=lambda c: c[1])


def lag_samples(records, a, b):
    """lag_seconds samples of every volume within [a, b)."""
    return [float(r["lag_seconds"]) for r in records
            if r.get("kind") == "repl_info" and a <= r["t"] < b and r.get("lag_seconds") is not None]


def _pct(values, q):
    s = sorted(values)
    return s[min(len(s) - 1, round(q * (len(s) - 1)))] if s else None


def _f(v, nd=0):
    return "-" if v is None else f"{v:,.{nd}f}"


def main(run_dir):
    run = Path(run_dir)
    meta = json.loads((run / "meta.json").read_text())
    series = analysis.load_fio_series(str(run / "fio"), meta["fio_start"])
    recs = [json.loads(line) for line in (run / "snapshots.jsonl").read_text().splitlines()
            if line.strip()]
    t0 = meta["t0"]
    cyc = cycles(recs)

    print(f"run {meta['run_id']}  share {meta['share']}%  fio {meta.get('fio')}\n")
    print("| phase | from (s) | length (s) | read IOPS | write IOPS | MB/s | IOPS vs before |"
          " MB/s vs before | replicated MB/s | lag p50 / p95 / max (s) |")
    print("|---|---|---|---|---|---|---|---|---|---|")
    base = None
    for name, a, b in phases(meta):
        w = analysis.client_window(series, a, b)
        base = base or w
        repl = sum(c[3] for c in cyc if a <= c[1] < b) / max(1.0, b - a) / MB
        lags = lag_samples(recs, a, b)
        print(f"| {name} | {a - t0:+.0f} | {b - a:.0f} | {_f(w['read_iops'])} | {_f(w['write_iops'])} |"
              f" {_f(w['total_mbs'])} | {w['total_iops'] / base['total_iops'] - 1:+.0%} |"
              f" {w['total_mbs'] / base['total_mbs'] - 1:+.0%} |"
              f" {_f(repl, 1) if name != 'before' else '-'} |"
              f" {_f(_pct(lags, .5))} / {_f(_pct(lags, .95))} / {_f(max(lags) if lags else None)} |")

    snaps = [r for r in recs if r.get("kind") == "snapshot"]
    rep = analysis.replication_summary(analysis.iterations_from_records(snaps, t0), t0)
    print("\nconvergence:")
    for vol, v in rep["volumes"].items():
        print(f"  {meta['volumes'].get(vol, {}).get('name', vol)}: initial full copy "
              f"{_f(v['initial_full_copy_s'])}s, steady from t+{_f(v['steady_from_s'])}s, "
              f"{v['iterations']} iterations")
    print(f"  cluster steady after {_f(rep['time_to_steady_s'])}s; steady iteration "
          f"mean {_f(rep['steady_iteration_s'].get('mean'), 1)}s "
          f"p95 {_f(rep['steady_iteration_s'].get('p95'), 1)}s; period p50 "
          f"{_f(rep['steady_period_s'].get('p50'), 1)}s")

    print("\nreplication speed per cycle (bytes / duration):")
    by_phase = defaultdict(list)
    for vol, t, secs, nbytes in cyc:
        ph = next((n for n, a, b in phases(meta) if a <= t < b), "?")
        by_phase[ph].append((secs, nbytes))
    for ph, cs in by_phase.items():
        speeds = [n / s / MB for s, n in cs if s > 0]
        print(f"  {ph}: {len(cs)} cycles, {_f(sum(n for _, n in cs) / MB / 1024, 1)} GiB, "
              f"duration p50 {_f(statistics.median(s for s, _ in cs), 1)}s, "
              f"speed p50 {_f(statistics.median(speeds), 1) if speeds else '-'} MB/s "
              f"max {_f(max(speeds), 1) if speeds else '-'} MB/s")

    print("\nminute  phase        read_kIOPS  write_kIOPS   MB/s   lag_max(s)  outstanding(GiB)")
    bounds = phases(meta)
    start, end = bounds[0][1], bounds[-1][2]
    for m in range(int((end - start) // 60)):
        a = start + m * 60
        w = analysis.client_window(series, a, a + 60)
        ph = next((n for n, x, y in bounds if x <= a < y), "?")
        lags = lag_samples(recs, a, a + 60)
        polls = defaultdict(float)              # all volumes of one poll share its t
        for r in recs:
            if r.get("kind") == "repl_info" and a <= r["t"] < a + 60:
                polls[r["t"]] += float(r["outstanding_bytes"] or 0)
        out = list(polls.values())
        print(f"{(a - t0) / 60:6.0f}  {ph:<11}  {w['read_iops'] / 1000:10.1f}  "
              f"{w['write_iops'] / 1000:11.1f}  {w['total_mbs']:6.0f}  "
              f"{_f(max(lags) if lags else None):>10}  "
              f"{_f(max(out) / MB / 1024 if out else None, 1):>16}")


if __name__ == "__main__":
    main(sys.argv[1])
