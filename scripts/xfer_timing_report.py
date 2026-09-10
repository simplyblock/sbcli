#!/usr/bin/env python3
"""Turn XFER-TIMING lines into the breakdown the soak could not produce.

The question this exists to answer: of the time a convergence round takes, how
much is DATA TRANSFER and how much is orchestration? Round duration was
previously the only number available, and it spans the landing-volume create,
the hub attach, the transfer, the detach, add_clone and convert on two nodes,
DB writes, and up to TASK_EXEC_INTERVAL_SEC of task-runner latency per state
change. Every hardware-level theory we tested against that number came out an
order of magnitude off, because it is not a throughput.

Usage:
    python scripts/xfer_timing_report.py <collected.log> [--csv out.csv]

Input is anything containing XFER-TIMING lines (a `docker service logs` dump is
fine). Lines look like:

    XFER-TIMING t=1787868791.244 phase=transfer_complete lvol=1c8874f3 \
                snap=a0f48bf5 round=2 ms=1843.2 bytes=33554432 mbps=18.2 ok=1
"""
import argparse
import re
import sys
from collections import defaultdict

LINE = re.compile(r"XFER-TIMING\s+(.*)$")
KV = re.compile(r"(\w+)=(\S+)")

# pipeline order for display; anything unlisted is appended
ROUND_PHASES = [
    "take_shrink_snapshot", "landing_volume_create", "hub_attach",
    "transfer_submit", "transfer_complete", "hub_detach",
    "chain_add_clone", "chain_convert", "round_total",
    "round_gap_to_next_snapshot",
]
FREEZE_PHASES = [
    "final_hub_attach", "fence_source", "final_step_transfer",
    "final_peer_add_clone", "enable_target_paths", "freeze_total",
]
# what counts as moving data, as opposed to arranging for data to move
TRANSFER_PHASES = {"transfer_complete", "final_step_transfer"}


def parse(path):
    events = []
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        for raw in fh:
            m = LINE.search(raw)
            if not m:
                continue
            rec = {}
            for k, v in KV.findall(m.group(1)):
                if k in ("t", "ms", "mbps"):
                    try:
                        rec[k] = float(v)
                    except ValueError:
                        rec[k] = None
                elif k in ("round", "bytes", "offset"):
                    try:
                        rec[k] = int(v)
                    except ValueError:
                        rec[k] = None
                else:
                    rec[k] = v
            if "phase" in rec:
                events.append(rec)
    events.sort(key=lambda r: r.get("t") or 0)
    return events


def transfer_rate_from_progress(events):
    """MB/s per (lvol, snap) inferred from transfer_running offsets.

    This is the one direct read on throughput: `offset` is bytes moved, so the
    slope between the first and last sample is the real rate.
    """
    runs = defaultdict(list)
    for e in events:
        if e["phase"] == "transfer_running" and e.get("offset") is not None:
            runs[(e.get("lvol"), e.get("snap"))].append((e["t"], e["offset"]))
    out = {}
    for key, samples in runs.items():
        if len(samples) < 2:
            continue
        samples.sort()
        (t0, o0), (t1, o1) = samples[0], samples[-1]
        dt, do = t1 - t0, o1 - o0
        if dt > 0 and do > 0:
            out[key] = (do / 1e6 / dt, do, dt, len(samples))
    return out


def report(events, csv_path=None):
    if not events:
        print("no XFER-TIMING lines found -- was the instrumented build deployed?")
        return 1

    span = events[-1]["t"] - events[0]["t"]
    print("%d timing events over %.1fs\n" % (len(events), span))

    # ---- per-phase totals -------------------------------------------------
    agg = defaultdict(lambda: [0, 0.0, 0.0])   # count, total_ms, max_ms
    for e in events:
        if e.get("ms") is None:
            continue
        a = agg[e["phase"]]
        a[0] += 1
        a[1] += e["ms"]
        a[2] = max(a[2], e["ms"])

    ordered = [p for p in ROUND_PHASES + FREEZE_PHASES if p in agg]
    ordered += [p for p in sorted(agg) if p not in ordered]

    print("%-28s %6s %12s %10s %10s" % ("phase", "n", "total_s", "mean_ms", "max_ms"))
    print("-" * 70)
    for p in ordered:
        n, tot, mx = agg[p]
        print("%-28s %6d %12.1f %10.1f %10.1f"
              % (p, n, tot / 1000.0, tot / n, mx))

    # ---- the split that matters -------------------------------------------
    # round_total/freeze_total are envelopes; don't double-count them.
    envelopes = {"round_total", "freeze_total"}
    moved = sum(agg[p][1] for p in agg if p in TRANSFER_PHASES)
    arranged = sum(agg[p][1] for p in agg
                   if p not in TRANSFER_PHASES and p not in envelopes)
    if moved + arranged > 0:
        pct = 100.0 * moved / (moved + arranged)
        print("\nDATA MOVEMENT   %8.1fs  (%.1f%%)" % (moved / 1000.0, pct))
        print("ORCHESTRATION   %8.1fs  (%.1f%%)" % (arranged / 1000.0, 100 - pct))
        if pct < 25:
            print("  -> the pipeline is dominated by orchestration, not throughput;")
            print("     tuning the transfer path cannot fix this.")

    # ---- what the instrumentation cannot explain -------------------------
    # If round_total dwarfs the sum of its measured parts, the missing time is
    # somewhere we are not looking -- and that gap is the finding, not a
    # rounding error.
    inner = [p for p in ROUND_PHASES
             if p not in ("round_total", "round_gap_to_next_snapshot")]
    measured = sum(agg[p][1] for p in inner if p in agg)
    envelope = agg.get("round_total", [0, 0.0, 0.0])[1]
    if envelope > 0:
        unaccounted = envelope - measured
        pct = 100.0 * unaccounted / envelope
        print()
        print("round envelopes    %8.1fs" % (envelope / 1000.0))
        print("measured phases    %8.1fs" % (measured / 1000.0))
        print("UNACCOUNTED        %8.1fs  (%.1f%% of the envelope)"
              % (unaccounted / 1000.0, pct))
        if pct > 30:
            print("  -> most of a round is NOT in any instrumented phase.")
            print("     Look at task_pass spacing first (scheduler latency),")
            print("     then add phases where the gap actually is.")

    # ---- measured transfer throughput ------------------------------------
    rates = transfer_rate_from_progress(events)
    if rates:
        print("\nmeasured transfer throughput (from transfer_running offsets):")
        print("%-12s %-10s %10s %12s %8s" % ("lvol", "snap", "MB/s", "bytes", "samples"))
        for (lvol, snap), (mbps, nbytes, dt, n) in sorted(
                rates.items(), key=lambda kv: -kv[1][0]):
            print("%-12s %-10s %10.1f %12d %8d" % (lvol, snap, mbps, nbytes, n))
    else:
        print("\nno transfer_running samples with offsets -- cannot measure")
        print("throughput directly; only envelope durations are available.")

    # ---- the freeze, per volume ------------------------------------------
    freezes = [e for e in events if e["phase"] == "freeze_total"]
    if freezes:
        print("\nclient-visible freeze windows (fence -> paths live):")
        for e in sorted(freezes, key=lambda r: -(r.get("ms") or 0)):
            flag = "  <-- OVER the 8s fast_io_fail_tmo" if (e.get("ms") or 0) > 8000 else ""
            print("   lvol=%-10s %8.2fs%s" % (e.get("lvol"), (e["ms"] or 0) / 1000.0, flag))

    # ---- task-runner latency --------------------------------------------
    passes = defaultdict(list)
    for e in events:
        if e["phase"] == "task_pass":
            passes[e.get("lvol")].append(e["t"])
    if passes:
        gaps = []
        for _lvol, times in passes.items():
            times.sort()
            gaps += [b - a for a, b in zip(times, times[1:])]
        if gaps:
            gaps.sort()
            print("\ntask-runner pass spacing: n=%d  median=%.1fs  max=%.1fs"
                  % (len(gaps), gaps[len(gaps) // 2], gaps[-1]))
            print("  (each state change of a cutover costs about one of these)")

    if csv_path:
        import csv
        cols = ["t", "phase", "lvol", "snap", "round", "ms", "bytes", "mbps",
                "offset", "state", "node", "ok"]
        with open(csv_path, "w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=cols, extrasaction="ignore")
            w.writeheader()
            for e in events:
                w.writerow(e)
        print("\nwrote %s" % csv_path)
    return 0


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("logfile")
    ap.add_argument("--csv", default=None, help="also write the raw events as CSV")
    args = ap.parse_args()
    return report(parse(args.logfile), args.csv)


if __name__ == "__main__":
    sys.exit(main())
