"""Per-layer bdev latency histogram probe for hunting tail-latency spikes.

Enables SPDK bdev histograms on the given bdevs of one storage node (e.g.
the raid1 bdev and its two base nvme bdevs), then polls them on an interval,
diffing consecutive snapshots so each report line covers only the I/Os of
that interval. Prints a timestamped line whenever an interval contains I/Os
above the latency threshold — correlate those timestamps across layers:

* spike in a base nvme bdev too   -> device / path below; raid1 inherits it
* spike only in the raid1 bdev    -> added by the raid layer (rebuild-window
                                     quiesce, ENOMEM requeue) or a reactor
                                     stall delaying completion fan-in
* spike in ALL bdevs at once      -> reactor stall fingerprint

Usage:
    python scripts/latency_histogram_probe.py <node-uuid> <bdev> [<bdev> ...]
        [--interval 5] [--threshold-us 10000] [--duration 600]
        [--opc read|write] [--keep]

The histogram is cumulative from enable; the script resets it by toggling
disable->enable on start and disables again on exit (unless --keep).
"""

import argparse
import base64
import struct
import time
from datetime import datetime

from simplyblock_core.db_controller import DBController


def decode_buckets(result):
    raw = base64.b64decode(result["histogram"])
    counts = struct.unpack(f"<{len(raw) // 8}Q", raw)
    return counts, result["bucket_shift"], result["tsc_rate"]


def bucket_bounds_us(index, bucket_shift, tsc_rate):
    """Upper bound of bucket `index` in microseconds (SPDK log2/linear layout)."""
    i, j = index >> bucket_shift, index & ((1 << bucket_shift) - 1)
    if i == 0:
        ticks = j + 1
    else:
        ticks = (1 << (i + bucket_shift - 1)) + ((j + 1) << (i - 1))
    return ticks * 1_000_000.0 / tsc_rate


def summarize_delta(prev, cur, bucket_shift, tsc_rate, threshold_us):
    total = slow = 0
    max_us = 0.0
    for idx, (a, b) in enumerate(zip(prev, cur)):
        d = b - a
        if d <= 0:
            continue
        upper = bucket_bounds_us(idx, bucket_shift, tsc_rate)
        total += d
        max_us = upper
        if upper > threshold_us:
            slow += d
    return total, slow, max_us


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("node_id")
    p.add_argument("bdevs", nargs="+")
    p.add_argument("--interval", type=float, default=5.0)
    p.add_argument("--threshold-us", type=float, default=10000.0)
    p.add_argument("--duration", type=float, default=600.0)
    p.add_argument("--opc", default=None, help="single I/O type, e.g. write (SPDK >= 24.01)")
    p.add_argument("--keep", action="store_true", help="leave histograms enabled on exit")
    args = p.parse_args()

    db = DBController()
    node = db.get_storage_node_by_id(args.node_id)
    rpc = node.rpc_client()
    print(f"node={node.get_id()} {node.mgmt_ip}:{node.rpc_port}")

    for bdev in args.bdevs:
        rpc.bdev_enable_histogram(bdev, enable=False)
        ret = rpc.bdev_enable_histogram(bdev, enable=True, opc=args.opc)
        print(f"histogram enabled on {bdev}: {ret}")

    prev = {}
    deadline = time.time() + args.duration
    try:
        while time.time() < deadline:
            time.sleep(args.interval)
            now = datetime.now().strftime("%H:%M:%S")
            for bdev in args.bdevs:
                result = rpc.bdev_get_histogram(bdev)
                if not result:
                    print(f"{now} {bdev}: bdev_get_histogram failed")
                    continue
                counts, shift, tsc = decode_buckets(result)
                if bdev in prev:
                    total, slow, max_us = summarize_delta(
                        prev[bdev], counts, shift, tsc, args.threshold_us)
                    if slow:
                        print(f"{now} {bdev}: {slow}/{total} IOs > "
                              f"{args.threshold_us:.0f}us, max ~{max_us / 1000.0:.1f}ms")
                prev[bdev] = counts
    finally:
        if not args.keep:
            for bdev in args.bdevs:
                rpc.bdev_enable_histogram(bdev, enable=False)
            print("histograms disabled")


if __name__ == "__main__":
    main()
