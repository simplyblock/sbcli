#!/usr/bin/env python3
"""Per-minute view of a scenario (c) run: what snapshots alone cost client IO.

    python cadence_report.py results/<run-c>

Prints the read and write IOPS of every minute with the snapshots taken (S)
and deleted (D) in it, the warm-up baseline, and for each round of snapshots
how long writes took to climb back to RECOVERED of the baseline, or that they
did not before the next round.
"""
import json
import sys
from pathlib import Path

import analysis

RECOVERED = 0.8


def main(run_dir):
    run = Path(run_dir)
    meta = json.loads((run / "meta.json").read_text())
    series = analysis.load_fio_series(str(run / "fio"), meta["fio_start"])
    t0, t_end = meta["t0"], meta["t_end"]
    base = analysis.client_window(series, meta["fio_start"] + 60, t0 - 5)
    snaps = meta.get("snapshots", [])

    print(f"warm-up baseline: reads {base['read_iops']:,.0f} IOPS {base['read_mbs']:,.0f} MB/s, "
          f"writes {base['write_iops']:,.0f} IOPS {base['write_mbs']:,.0f} MB/s")
    whole = analysis.client_window(series, t0, t_end)
    print(f"with snapshots:   reads {whole['read_iops']:,.0f} IOPS {whole['read_mbs']:,.0f} MB/s "
          f"({whole['read_iops'] / base['read_iops'] - 1:+.0%}), "
          f"writes {whole['write_iops']:,.0f} IOPS {whole['write_mbs']:,.0f} MB/s "
          f"({whole['write_iops'] / base['write_iops'] - 1:+.0%})\n")

    print("minute  write_kIOPS  read_kIOPS  events")
    for minute in range(int((t_end - t0) // 60)):
        a = t0 + minute * 60
        w = analysis.client_window(series, a, a + 60)
        taken = sum(1 for s in snaps if a <= s["t"] < a + 60)
        deleted = sum(1 for s in snaps if s.get("deleted") and a <= s["deleted"] < a + 60)
        print(f"{minute:6d}  {w['write_iops'] / 1000:11.1f}  {w['read_iops'] / 1000:10.1f}  "
              f"{'S' * taken}{'D' * deleted}")

    # one round = the snapshots of all volumes taken together
    rounds = []
    for t in sorted(s["t"] for s in snaps):
        if not rounds or t - rounds[-1] > 60:
            rounds.append(t)
    target = RECOVERED * base["write_iops"]
    print(f"\nrecovery of writes to {RECOVERED:.0%} of baseline ({target:,.0f} IOPS), per round:")
    for i, start in enumerate(rounds):
        end = rounds[i + 1] if i + 1 < len(rounds) else t_end
        recovered = None
        for sec in range(int(start), int(end) - 30, 10):
            if analysis.client_window(series, sec, sec + 30)["write_iops"] >= target:
                recovered = int(sec - start)
                break
        low = min(analysis.client_window(series, s, s + 30)["write_iops"]
                  for s in range(int(start), max(int(start) + 1, int(end) - 30), 10))
        print(f"  round {i + 1:2d} at t+{start - t0:5.0f}s: lowest {low:9,.0f} write IOPS, "
              + (f"recovered after {recovered:4d}s" if recovered is not None
                 else f"not recovered within {end - start:.0f}s"))


if __name__ == "__main__":
    main(sys.argv[1])
