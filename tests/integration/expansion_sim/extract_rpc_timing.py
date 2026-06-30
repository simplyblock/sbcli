# coding=utf-8
"""Extract per-RPC timing from a sb_logs tarball's ``spdk_proxy_*.log``
files.

Each spdk_proxy log line follows one of two shapes::

    YYYY-MM-DD HH:MM:SS.mmm  ...  INFO: Request:<id> function: <name>, params: ...
    YYYY-MM-DD HH:MM:SS.mmm  ...  <ip> - - [...] "POST / HTTP/1.1" 200 -

Each "INFO: Request: function: X" is followed by exactly one
"POST / HTTP/1.1 200" reply line on the same proxy connection. Pairing
them in file order gives an end-to-end duration for each RPC. The proxy
does not interleave concurrent requests within a single log file (each
proxy serves one SPDK process), so the next-POST-line heuristic is
reliable.

Usage
-----

    python3 -m tests.expansion_sim.extract_rpc_timing \
        --logs-dir /tmp/sb_logs_for_timing/sb_logs_20260423_202000_15m \
        --out tests/expansion_sim/rpc_timing.json

Aggregates median/p95/count per RPC name and writes JSON. The timed
simulator (``RpcServerSim``) loads this table to delay responses by a
realistic amount.
"""

from __future__ import annotations

import argparse
import json
import re
from datetime import datetime
from pathlib import Path
from statistics import median


_TS_RE = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}\.\d{3})"
)
_REQ_RE = re.compile(
    r"INFO:\s*Request:(?P<id>\d+)\s+function:\s*(?P<fn>[A-Za-z0-9_]+)"
)
_POST_RE = re.compile(r'"POST\s/\sHTTP/1\.1"\s(?P<code>\d{3})')


def _parse_ts(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d %H:%M:%S.%f")


def _walk_proxy_log(path: Path):
    """Yield (rpc_name, duration_ms) tuples for one spdk_proxy log."""
    pending_fn: str | None = None
    pending_t: datetime | None = None
    with path.open(errors="replace") as fh:
        for line in fh:
            ts_m = _TS_RE.match(line)
            if ts_m is None:
                continue
            t = _parse_ts(ts_m.group("ts"))
            req = _REQ_RE.search(line)
            if req is not None:
                pending_fn = req.group("fn")
                pending_t = t
                continue
            post = _POST_RE.search(line)
            if post is not None and pending_fn is not None:
                if post.group("code") == "200":
                    dt = (t - pending_t).total_seconds() * 1000.0
                    if 0 <= dt <= 30_000:  # filter obvious clock-skew outliers
                        yield (pending_fn, dt)
                pending_fn = None
                pending_t = None


def collect(logs_dir: Path) -> dict:
    """Walk every spdk_proxy log under ``logs_dir`` and aggregate timing."""
    samples: dict[str, list[float]] = {}
    for proxy_log in logs_dir.rglob("spdk_proxy_*.log"):
        for fn, dt in _walk_proxy_log(proxy_log):
            samples.setdefault(fn, []).append(dt)

    summary = {}
    for fn, vals in samples.items():
        vals_sorted = sorted(vals)
        n = len(vals_sorted)
        p50 = median(vals_sorted)
        p95 = vals_sorted[max(0, int(n * 0.95) - 1)] if n else 0.0
        summary[fn] = {
            "count": n,
            "p50_ms": round(p50, 2),
            "p95_ms": round(p95, 2),
            "min_ms": round(vals_sorted[0], 2) if n else 0.0,
            "max_ms": round(vals_sorted[-1], 2) if n else 0.0,
        }
    return {
        "source": str(logs_dir),
        "rpcs": dict(sorted(summary.items())),
    }


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--logs-dir", required=True, type=Path)
    p.add_argument("--out", required=True, type=Path)
    args = p.parse_args(argv)

    if not args.logs_dir.is_dir():
        raise SystemExit(f"logs_dir does not exist: {args.logs_dir}")

    summary = collect(args.logs_dir)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("w") as fh:
        json.dump(summary, fh, indent=2)

    print(f"wrote {args.out} ({len(summary['rpcs'])} unique RPCs, "
          f"{sum(v['count'] for v in summary['rpcs'].values())} samples)")


if __name__ == "__main__":
    main()
