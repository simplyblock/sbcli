#!/usr/bin/env python3
"""Extract critical information from SPDK storage-node logs for failure analysis.

Categories extracted (one output file per category, per input log):
  01_cnt            - Distrib CNT counter lines (per-second, 30 counters)
  02_lvs            - LVS layer records: LSTAT, IO redirect CNT, IO hublvol CNT
  03_nvmf_delay     - nvmf_tcp_dump_delay_req_status slow/stuck IO prints
  04_jc_errors      - JC / journal client errors (JCERR, helper_*, ctx_per_jm retry/loss)
  05_distrib_errors - DISTRIBD warnings/errors (distr error, failed ...)
  06_lvs_errors     - LVS / blobstore / lvol errors (failed IO on upper layer)
  07_writer_conflict- Writer conflict & conflict/unfreeze signal records
  08_hublvol        - Hublvol create/open/close/redirect events
  09_other_errors   - Any remaining *ERROR* / *WARNING* / abort not captured above
  00_summary.txt    - Per-category line counts and time range

Usage:
  python extract_spdk_critical.py <input>               # file or directory of spdk*.log
  python extract_spdk_critical.py <input> -o <outdir>
  python extract_spdk_critical.py <input> --from "2026-04-19 17:31:00" --to "2026-04-19 17:35:00"

Timestamps are parsed from the leading "[YYYY-MM-DD HH:MM:SS.ffffff]" bracket when present.
Lines without a timestamp (e.g. raw CNT printf) are kept and attributed to the most recent
timestamp seen above them.
"""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Iterable

TS_RE = re.compile(r"\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+)\]")
TS_FMT = "%Y-%m-%d %H:%M:%S.%f"

# Category detectors. Order matters: first match wins, so that a DISTRIBD error
# with the word "failed" is not double-counted under 06_lvs_errors or 09_other.
CNT_RE = re.compile(r"(^|\s)CNT\s+(\d+\s+){29}\d+\s*$")
LVS_RE = re.compile(r"(spdk_bs_monitoring_poller.*LSTAT|spdk_lvs_IO_redirect|spdk_lvs_IO_hublvol)")
NVMF_DELAY_RE = re.compile(r"nvmf_tcp_dump_delay_req_status")
JC_RE = re.compile(
    r"(JCERR|helper_service_history_append|helper_reader_jumping|"
    r"ctx_per_jm_RetryConnect|t_ctx_per_jm|alg_journal\.cpp.*\*(ERROR|WARNING)\*)"
)
DISTRIB_ERR_RE = re.compile(
    r"(DISTRIBD.*(\*ERROR\*|\*WARNING\*|failed|error|distr\s*error)|"
    r"bdev_distrib.*\*(ERROR|WARNING)\*)"
)
LVS_ERR_RE = re.compile(
    r"((blobstore|lvol|vbdev_lvol)\.c.*(\*ERROR\*|\*WARNING\*|abort|failed|io\s*error))",
    re.IGNORECASE,
)
WRITER_CONFLICT_RE = re.compile(
    r"(writer_conflict|spdk_lvs_conflict_signal|spdk_lvs_unfreeze_on_conflict|"
    r"io\s+conflict|conflict\s+detected|signal.*conflict)"
)
HUBLVOL_RE = re.compile(r"hublvol|Hub\s*lvol|hub_lvol", re.IGNORECASE)
OTHER_ERR_RE = re.compile(r"\*ERROR\*|\*WARNING\*|\babort(ed|ing)?\b", re.IGNORECASE)

CATEGORIES: list[tuple[str, re.Pattern]] = [
    ("01_cnt",             CNT_RE),
    ("02_lvs",             LVS_RE),
    ("03_nvmf_delay",      NVMF_DELAY_RE),
    ("04_jc_errors",       JC_RE),
    ("05_distrib_errors",  DISTRIB_ERR_RE),
    ("06_lvs_errors",      LVS_ERR_RE),
    ("07_writer_conflict", WRITER_CONFLICT_RE),
    ("08_hublvol",         HUBLVOL_RE),
    ("09_other_errors",    OTHER_ERR_RE),
]


@dataclass
class Counters:
    counts: dict[str, int] = field(default_factory=lambda: {c: 0 for c, _ in CATEGORIES})
    first_ts: str | None = None
    last_ts:  str | None = None
    total_lines: int = 0


def parse_ts(line: str) -> datetime | None:
    m = TS_RE.search(line)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), TS_FMT)
    except ValueError:
        return None


def classify(line: str) -> str | None:
    for name, rx in CATEGORIES:
        if rx.search(line):
            return name
    return None


def process_file(
    path: Path,
    outdir: Path,
    t_from: datetime | None,
    t_to:   datetime | None,
) -> Counters:
    stem = path.stem
    target = outdir / stem
    target.mkdir(parents=True, exist_ok=True)

    writers = {name: open(target / f"{name}.log", "w", encoding="utf-8", errors="replace")
               for name, _ in CATEGORIES}
    cnt = Counters()
    last_ts_str: str | None = None
    last_ts_dt:  datetime | None = None

    try:
        with open(path, "r", encoding="utf-8", errors="replace") as fh:
            for raw in fh:
                cnt.total_lines += 1
                line = raw.rstrip("\n")

                ts_dt = parse_ts(line)
                if ts_dt is not None:
                    last_ts_dt = ts_dt
                    last_ts_str = TS_RE.search(line).group(1)  # type: ignore[union-attr]
                    if cnt.first_ts is None:
                        cnt.first_ts = last_ts_str
                    cnt.last_ts = last_ts_str

                effective_dt = ts_dt or last_ts_dt
                if t_from and effective_dt and effective_dt < t_from:
                    continue
                if t_to and effective_dt and effective_dt > t_to:
                    continue

                category = classify(line)
                if category is None:
                    continue

                # Prefix lines that lack their own timestamp with the nearest preceding one
                # so that CNT records and similar printf output remain time-correlated.
                out_line = line if TS_RE.search(line) else f"[{last_ts_str or '????'}] {line.lstrip()}"
                writers[category].write(out_line + "\n")
                cnt.counts[category] += 1
    finally:
        for w in writers.values():
            w.close()

    # Drop empty category files to keep the output directory tidy.
    for name, _ in CATEGORIES:
        fp = target / f"{name}.log"
        if fp.exists() and fp.stat().st_size == 0:
            fp.unlink()

    summary = target / "00_summary.txt"
    with open(summary, "w", encoding="utf-8") as fh:
        fh.write(f"source:      {path}\n")
        fh.write(f"total_lines: {cnt.total_lines}\n")
        fh.write(f"first_ts:    {cnt.first_ts}\n")
        fh.write(f"last_ts:     {cnt.last_ts}\n")
        if t_from or t_to:
            fh.write(f"filter_from: {t_from}\n")
            fh.write(f"filter_to:   {t_to}\n")
        fh.write("\ncategory                 matched_lines\n")
        fh.write("-" * 40 + "\n")
        for name, _ in CATEGORIES:
            fh.write(f"{name:<24} {cnt.counts[name]:>10}\n")
    return cnt


def iter_inputs(input_path: Path) -> Iterable[Path]:
    if input_path.is_file():
        yield input_path
        return
    if not input_path.is_dir():
        raise FileNotFoundError(input_path)
    # Match spdk_xxxx.log anywhere under the tree (skip spdk_proxy_xxxx.log,
    # which is python proxy output, not SPDK target logs).
    seen: set[Path] = set()
    for p in sorted(input_path.rglob("spdk_*.log")):
        if not p.is_file():
            continue
        if p.name.startswith("spdk_proxy_"):
            continue
        rp = p.resolve()
        if rp in seen:
            continue
        seen.add(rp)
        yield p


def parse_time(s: str | None) -> datetime | None:
    if not s:
        return None
    for fmt in (TS_FMT, "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    raise ValueError(f"Cannot parse time: {s!r}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("input", type=Path, help="spdk log file or directory containing spdk_*.log files")
    ap.add_argument("-o", "--outdir", type=Path, default=Path("spdk_critical"),
                    help="output directory (default: ./spdk_critical)")
    ap.add_argument("--from", dest="t_from", default=None,
                    help='start of window, e.g. "2026-04-19 17:31:00"')
    ap.add_argument("--to", dest="t_to", default=None,
                    help='end of window, e.g. "2026-04-19 17:35:00"')
    args = ap.parse_args()

    t_from = parse_time(args.t_from)
    t_to   = parse_time(args.t_to)

    inputs = list(iter_inputs(args.input))
    if not inputs:
        print(f"No spdk_*.log files found under {args.input}", file=sys.stderr)
        return 2

    args.outdir.mkdir(parents=True, exist_ok=True)

    totals: dict[str, int] = {c: 0 for c, _ in CATEGORIES}
    for path in inputs:
        print(f"processing {path} ...", flush=True)
        c = process_file(path, args.outdir, t_from, t_to)
        for k, v in c.counts.items():
            totals[k] += v
        print(f"  lines={c.total_lines}  span={c.first_ts} .. {c.last_ts}")
        for name, _ in CATEGORIES:
            if c.counts[name]:
                print(f"    {name:<22} {c.counts[name]:>8}")

    print("\n== totals ==")
    for name, _ in CATEGORIES:
        print(f"  {name:<22} {totals[name]:>8}")
    print(f"\noutput: {args.outdir.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
