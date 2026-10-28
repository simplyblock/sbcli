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


def _kind(r):
    """Subsystem kind of a qpairs record (older collectors filed the
    transferhub under "other")."""
    if ":transferhub:" in r.get("nqn", ""):
        return "transferhub"
    return r["type"]


def _class(kind, local_cluster, peer_cluster):
    if kind == "client":
        return "client"
    if kind in ("hublvol", "transferhub") and peer_cluster is not None and peer_cluster != local_cluster:
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
            port_kinds[(r["node_ip"], str(port))][_kind(r)] += 1
    return ip_cluster, {k: c.most_common(1)[0][0] for k, c in port_kinds.items()}


def qpair_table(qpair_recs, ip_cluster):
    """{sample time: Counter(class -> qpairs)}; hublvol qpairs split by peer."""
    out = defaultdict(Counter)
    for r in qpair_recs:
        for peer, n in (r.get("peers") or {}).items():
            peer_ip = peer.rsplit(":", 1)[0]
            cls = _class(_kind(r), r.get("cluster"), ip_cluster.get(peer_ip))
            if cls:
                out[round(r["t"])][cls] += n
    return out


def _split(addr):
    ip, _, port = addr.rpartition(":")
    # ss prints a local address bound to a device as ip%dev
    return ip.strip("[]").removeprefix("::ffff:").split("%")[0], port


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


def _target_write_us(iostat_recs, bdev, t1, t2):
    """Mean write latency of *bdev* over [t1, t2] (widened to the enclosing
    samples) on the node where it took the most writes, or None."""
    best = None
    by_node = defaultdict(list)
    for r in iostat_recs:
        by_node[r["node"]].append(r)
    for recs in by_node.values():
        recs.sort(key=lambda r: r["t"])
        before = [r for r in recs if r["t"] <= t1] or recs[:1]
        after = [r for r in recs if r["t"] >= t2] or recs[-1:]
        a, b = before[-1], after[0]
        if a is b:
            continue
        x = next((d for d in a["bdevs"] if d["name"] == bdev), None)
        y = next((d for d in b["bdevs"] if d["name"] == bdev), None)
        if not x or not y or not b.get("tick_rate"):
            continue
        ops = (y["num_write_ops"] or 0) - (x["num_write_ops"] or 0)
        if ops > 0 and (best is None or ops > best[0]):
            ticks = (y["write_latency_ticks"] or 0) - (x["write_latency_ticks"] or 0)
            best = (ops, ticks / ops * 1e6 / b["tick_rate"])
    return best[1] if best else None


def transfer_table(xfer_recs, iostat_recs=()):
    """Per transfer (snapshot): the last statistics and the time split.

    stat_read_us_avg is the source read (replication client side);
    stat_write_us_avg is the write to the target (network + target). The
    transfer writes into the target's lvstore directly, not through the
    landing bdev, so the target's share is the write latency of the
    lvstore's base bdev (raid0_<n> of the landing's LVS_<n>) on the target
    over the transfer; network = write - target. Approximate: that bdev also
    serves the merges, and a 64 KiB request is written as up to two writes,
    timed together on the source and singly on the target."""
    first, last = {}, {}
    sampled = defaultdict(list)         # samples taken while the transfer ran
    for r in xfer_recs:
        if r.get("stat_reqs") is not None:
            first.setdefault(r["snapshot"], r)
            last[r["snapshot"]] = r
            if r.get("transfer_state") == "In progress":
                sampled[r["snapshot"]].append(r)
    out = []
    for snap, r in sorted(last.items(), key=lambda kv: kv[1]["t"]):
        n, elapsed_s = r["stat_reqs"], (r.get("stat_elapsed_ms") or 0) / 1000
        read_us, write_us = r.get("stat_read_us_avg") or 0, r.get("stat_write_us_avg") or 0
        land = r.get("landing") or {}
        target_us = None
        lvs = (land.get("bdev") or "").split("/")[0]
        if lvs.startswith("LVS_"):
            target_us = _target_write_us(iostat_recs, "raid0_" + lvs[4:],
                                         r["t"] - elapsed_s, r["t"])
        runs = sampled.get(snap) or []
        out.append({
            "snapshot": snap, "volume": r.get("volume"), "partial": r.get("allow_partial"),
            "reqs": n, "bytes": r.get("stat_bytes") or 0, "elapsed_s": elapsed_s,
            "mb_s": (r.get("stat_bytes") or 0) / MB / elapsed_s if elapsed_s else 0,
            "in_flight": n * (read_us + write_us) / 1e6 / elapsed_s if elapsed_s else 0,
            "read_us": read_us, "write_us": write_us, "target_us": target_us,
            "network_us": max(write_us - target_us, 0) if target_us is not None else None,
            # where a request waits outside its IO (SPDK with in-place refill)
            "inline_pct": (100 * r["stat_inline_reqs"] / n
                           if n and r.get("stat_inline_reqs") is not None else None),
            "ready_wait_us": r.get("stat_ready_wait_us_avg"),
            "free_wait_us": r.get("stat_free_wait_us_avg"),
            "inflight_sampled": (statistics.mean(x.get("stat_inflight") or 0 for x in runs)
                                 if runs and "stat_inflight" in runs[0] else None),
            "inflight_max": r.get("stat_inflight_max"),
            "md_gap_ms": (max((x.get("md_tick_gap_max_us") or 0) for x in runs) / 1000
                          if runs and "md_tick_gap_max_us" in runs[0] else None),
            "deferred": bool(r.get("deferred_to")),
        })
    return out


def merge_table(xfer_recs):
    """Per landing volume: the merge job's last state and duration, and
    whether a transfer of the same volume ran while it merged (overlap)."""
    last, merging = {}, defaultdict(list)
    for r in xfer_recs:
        m = r.get("merge")
        if isinstance(m, dict) and m:
            last[r["snapshot"]] = (r, m)
            if m.get("state") == "running":
                merging[r["volume"]].append(r["t"])
    overlap = defaultdict(int)
    for r in xfer_recs:
        if r.get("transfer_state") == "In progress" and r["t"] in set(merging.get(r.get("volume"), ())):
            overlap[r["volume"]] += 1
    return [{"snapshot": s, "volume": r.get("volume"), "state": m.get("state"),
             "ms": m.get("ms"), "bytes": m.get("bytes"), "clusters": m.get("clusters"),
             "overlap_samples": overlap.get(r.get("volume"), 0)}
            for s, (r, m) in sorted(last.items(), key=lambda kv: kv[1][0]["t"])]


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
          f" {'read us':>8} {'write us':>8} {'net us':>8} {'tgt us':>8}")
    xt = transfer_table([r for r in recs if r.get("kind") == "xfer_stat"],
                        [r for r in recs if r.get("kind") == "iostat"])
    for x in xt:
        net = f"{x['network_us']:.0f}" if x["network_us"] is not None else "-"
        tgt = f"{x['target_us']:.0f}" if x["target_us"] is not None else "-"
        print(f"  {x['snapshot']:36} {'delta' if x['partial'] else 'full':5} {x['reqs']:9} "
              f"{x['bytes'] / 1e9:7.1f} {x['elapsed_s']:6.0f} {x['mb_s']:7.1f} {x['in_flight']:6.1f}"
              f" {x['read_us']:8.0f} {x['write_us']:8.0f} {net:>8} {tgt:>8}")
    if xt:
        for part in ("read_us", "network_us", "target_us"):
            vals = [x[part] for x in xt if x[part] is not None]
            if vals:
                print(f"  median {part}: {statistics.median(vals):.0f}")
    else:
        print("  no xfer_stat records")

    print("\n== Replication transfers: waits outside the IO ==")
    print(f"  {'snapshot':36} {'kind':5} {'defer':5} {'inline%':>7} {'ready us':>8} {'free us':>8}"
          f" {'inflt(L)':>8} {'inflt(s)':>8} {'max':>5} {'md gap ms':>9}")

    def _f(v, fmt):
        width = fmt.split(".")[0].rstrip("df")
        return format(v, fmt) if v is not None else format("-", ">" + width)
    for x in xt:
        print(f"  {x['snapshot']:36} {'delta' if x['partial'] else 'full':5} "
              f"{'yes' if x['deferred'] else 'no':5} {_f(x['inline_pct'], '7.0f')} "
              f"{_f(x['ready_wait_us'], '8.0f')} {_f(x['free_wait_us'], '8.0f')} "
              f"{x['in_flight']:8.1f} {_f(x['inflight_sampled'], '8.1f')} "
              f"{_f(x['inflight_max'], '5d')} {_f(x['md_gap_ms'], '9.1f')}")

    print("\n== Landing merges (and transfers of the same volume running meanwhile) ==")
    mt = merge_table([r for r in recs if r.get("kind") == "xfer_stat"])
    for m in mt:
        print(f"  {m['snapshot']:36} {m['state'] or '-':8} {(m['ms'] or 0) / 1000:7.0f} s "
              f"{(m['bytes'] or 0) / 1e9:7.1f} GB {m['clusters'] or 0:8} clusters  "
              f"overlap samples {m['overlap_samples']}")
    if not mt:
        print("  no merge records")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else ".")
