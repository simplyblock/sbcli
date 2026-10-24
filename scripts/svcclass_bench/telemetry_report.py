#!/usr/bin/env python3
"""Where replication's time and the network go, for one scenario (b) run.

    python telemetry_report.py results/<run-b>

Reads the collector's records (snapshots.jsonl: kinds qpairs, xfer_stat) and
the TCP samples of every host (net.jsonl) and prints:

* queue pairs per traffic class -- client IO, replication, cluster-internal
  (devices, hublvol redirects, journals) -- per sample, and their ratio;
* TCP connections and bandwidth per class (byte counter deltas between
  samples), and their ratio;
* per replication transfer: requests, bytes, rate, the requests in flight
  (Little's law), and the time of a request split into source read
  (replication client side), network and target write (replication server
  side).

Classes: a connection is classified by the endpoint that listens, i.e. by the
subsystem port of the node it reaches; hublvol traffic is replication when its
peer is a node of the other cluster, an internal redirect otherwise. Only the
listening side of a connection is counted, so no byte is counted twice.
"""
import json
import statistics
import sys
from collections import Counter, defaultdict
from pathlib import Path

MB = 1024 * 1024
CLASSES = ("client", "replication", "internal")


def _load(path):
    if not path.exists():
        return []
    out = []
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            out.append(json.loads(line))
        except ValueError:
            continue
    return out


def _class(kind, local_cluster, peer_cluster):
    if kind == "client":
        return "client"
    if kind == "hublvol" and peer_cluster is not None and peer_cluster != local_cluster:
        return "replication"
    if kind == "other":
        return None
    return "internal"


def endpoints(qpair_recs):
    """{ip: cluster} of the storage nodes, and {(ip, port): subsystem kind} of
    every listener (the kind most of a port's subsystems have)."""
    ip_cluster = {}
    port_kinds = defaultdict(Counter)
    for r in qpair_recs:
        ip_cluster[r["node_ip"]] = r.get("cluster")
        for port in r.get("listen") or []:
            port_kinds[(r["node_ip"], str(port))][r["type"]] += 1
    return ip_cluster, {k: c.most_common(1)[0][0] for k, c in port_kinds.items()}


def qpair_table(qpair_recs, ip_cluster):
    """{sample time: Counter(class -> qpairs)}; hublvol qpairs split by peer."""
    out = defaultdict(Counter)
    for r in qpair_recs:
        for peer, n in (r.get("peers") or {}).items():
            peer_ip = peer.rsplit(":", 1)[0]
            cls = _class(r["type"], r.get("cluster"), ip_cluster.get(peer_ip))
            if cls:
                out[round(r["t"])][cls] += n
    return out


def _split(addr):
    ip, _, port = addr.rpartition(":")
    return ip.strip("[]").removeprefix("::ffff:"), port


def net_table(net_recs, ip_cluster, listeners):
    """Per sample interval: connections and bytes per class.

    Returns [(t, Counter(class -> connections), Counter(class -> bytes))],
    the bytes being the counter deltas since the previous sample of the
    same connection (the first sample of a connection counts nothing)."""
    prev = {}
    rows = defaultdict(lambda: (Counter(), Counter()))
    for r in sorted((r for r in net_recs if "conns" in r), key=lambda r: r["t"]):
        t = round(r["t"] / 60) * 60
        for c in r["conns"]:
            lip, lport = _split(c["l"])
            pip, _ = _split(c["p"])
            kind = listeners.get((lip, lport))
            if kind is None:
                continue            # the connecting side: counted where it lands
            cls = _class(kind, ip_cluster.get(lip), ip_cluster.get(pip))
            if cls is None:
                continue
            conns, nbytes = rows[t]
            conns[cls] += 1
            key = (r["host"], c["l"], c["p"])
            total = c["sent"] + c["recv"]
            if key in prev and total >= prev[key]:
                nbytes[cls] += total - prev[key]
            prev[key] = total
    return [(t, *rows[t]) for t in sorted(rows)]


def transfer_table(xfer_recs):
    """Per transfer (snapshot): the last statistics and the time split.

    stat_read_us_avg is the source read (replication client side);
    stat_write_us_avg is the write to the target (network + target); the
    target's own share is the landing bdev's write latency there, so
    network = write - target. Approximate: a 64 KiB request is written as up
    to two writes, timed together on the source and singly on the target."""
    last = {}
    for r in xfer_recs:
        if r.get("stat_reqs") is not None:
            last[r["snapshot"]] = r
    out = []
    for snap, r in sorted(last.items(), key=lambda kv: kv[1]["t"]):
        n, elapsed_s = r["stat_reqs"], (r.get("stat_elapsed_ms") or 0) / 1000
        read_us, write_us = r.get("stat_read_us_avg") or 0, r.get("stat_write_us_avg") or 0
        land = r.get("landing") or {}
        target_us = None
        if land.get("num_write_ops") and land.get("tick_rate"):
            target_us = land["write_latency_ticks"] / land["num_write_ops"] * 1e6 / land["tick_rate"]
        out.append({
            "snapshot": snap, "volume": r.get("volume"), "partial": r.get("allow_partial"),
            "reqs": n, "bytes": r.get("stat_bytes") or 0, "elapsed_s": elapsed_s,
            "mb_s": (r.get("stat_bytes") or 0) / MB / elapsed_s if elapsed_s else 0,
            "in_flight": n * (read_us + write_us) / 1e6 / elapsed_s if elapsed_s else 0,
            "read_us": read_us, "write_us": write_us, "target_us": target_us,
            "network_us": max(write_us - target_us, 0) if target_us is not None else None,
        })
    return out


def _ratio(c):
    total = sum(c.values()) or 1
    return " / ".join(f"{100 * c[k] / total:.0f}%" for k in CLASSES)


def main(run_dir):
    run_dir = Path(run_dir)
    recs = _load(run_dir / "snapshots.jsonl")
    qp = [r for r in recs if r.get("kind") == "qpairs"]
    ip_cluster, listeners = endpoints(qp)

    print("== Queue pairs per class (client / replication / internal) ==")
    qt = qpair_table(qp, ip_cluster)
    for t in sorted(qt):
        c = qt[t]
        print(f"  {t}: " + "  ".join(f"{k} {c[k]}" for k in CLASSES) + f"   ({_ratio(c)})")
    if not qt:
        print("  no qpairs records")

    print("\n== TCP connections and bandwidth per class (listening side, per minute) ==")
    rows = net_table(_load(run_dir / "net.jsonl"), ip_cluster, listeners)
    prev_t = None
    totals = Counter()
    for t, conns, nbytes in rows:
        dt = t - prev_t if prev_t else None
        prev_t = t
        bw = "  ".join(f"{k} {nbytes[k] / MB / dt:7.1f} MB/s" for k in CLASSES) if dt else "(first sample)"
        totals.update(nbytes)
        print(f"  {t}: conns " + " ".join(f"{k}={conns[k]}" for k in CLASSES) + f"   {bw}")
    if rows:
        span = rows[-1][0] - rows[0][0]
        if span:
            print("  mean:  " + "  ".join(f"{k} {totals[k] / MB / span:.1f} MB/s" for k in CLASSES)
                  + f"   ({_ratio(totals)})")
    else:
        print("  no net.jsonl samples")

    print("\n== Replication transfers: time per request ==")
    print(f"  {'snapshot':36} {'kind':5} {'reqs':>9} {'GB':>7} {'s':>6} {'MB/s':>7} {'inflt':>6}"
          f" {'read us':>8} {'net us':>8} {'tgt us':>8}")
    xt = transfer_table([r for r in recs if r.get("kind") == "xfer_stat"])
    for x in xt:
        net = f"{x['network_us']:.0f}" if x["network_us"] is not None else "-"
        tgt = f"{x['target_us']:.0f}" if x["target_us"] is not None else "-"
        print(f"  {x['snapshot']:36} {'delta' if x['partial'] else 'full':5} {x['reqs']:9} "
              f"{x['bytes'] / 1e9:7.1f} {x['elapsed_s']:6.0f} {x['mb_s']:7.1f} {x['in_flight']:6.1f}"
              f" {x['read_us']:8.0f} {net:>8} {tgt:>8}")
    if xt:
        for part in ("read_us", "network_us", "target_us"):
            vals = [x[part] for x in xt if x[part] is not None]
            if vals:
                print(f"  median {part}: {statistics.median(vals):.0f}")
    else:
        print("  no xfer_stat records")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else ".")
