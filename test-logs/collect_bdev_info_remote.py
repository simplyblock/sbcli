#!/usr/bin/env python3
"""
collect_bdev_info_remote.py

Connects to specific storage nodes (via the bridge_utils SSH tunnel, same as
the other test-logs scripts) and runs `bdev_get_bdevs` directly against each
one's SPDK RPC socket, saving the result to its own file -- e.g. to compare
a bdev's state (clone/snapshot relationship, size, blobid, ...) on the
source vs. the target node of a migration.

Runs the same command by hand you'd otherwise type per node:
  docker exec -u root spdk_<port> python3 /root/spdk/scripts/rpc.py \\
      -s /mnt/ramdisk/spdk_<port>/spdk.sock bdev_get_bdevs

where <port> is that node's SPDK RPC port (the number in its hostname, e.g.
vm07_4420 -> port 4420 -- see `sbctl sn list`).

This does not set up the cluster -- point --cluster at one that's already
running (see bridge_utils.CLUSTERS).

Usage (copy a private key into this folder named "simplyblock" first, or
pass --key to point at a different one):
  python3 collect_bdev_info_remote.py --nodes src=192.168.10.147:4420,tgt=192.168.10.148:4422
  python3 collect_bdev_info_remote.py --nodes src=192.168.10.147:4420,tgt=192.168.10.148:4422 --bdev LVS_1/CLN_39m
  python3 collect_bdev_info_remote.py --nodes 192.168.10.147:4420 --output-dir bdev_logs
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import bridge_utils as bu

DEFAULT_KEY_PATH = "./simplyblock"


def parse_args():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--key", default=DEFAULT_KEY_PATH,
                   help=f"Path to the SSH private key for the bridge host "
                        f"(default: {DEFAULT_KEY_PATH})")
    p.add_argument("--cluster", default="default",
                   help=f"Cluster profile from bridge_utils.CLUSTERS. Choices: {list(bu.CLUSTERS)}")
    p.add_argument("--nodes", required=True, metavar="[LABEL=]IP:PORT,...",
                   help="Comma-separated nodes to query, each as ip:port (SPDK "
                        "RPC port, i.e. the number in that node's hostname -- "
                        "see `sbctl sn list`). Optionally prefix each with a "
                        "label, e.g. src=192.168.10.147:4420,tgt=192.168.10.148:4422 "
                        "-- labels are just used for the output filenames; "
                        "without one, the ip is used.")
    p.add_argument("--bdev", default=None, metavar="NAME",
                   help="Restrict to a single bdev (e.g. LVS_1/CLN_39m) instead "
                        "of the full bdev_get_bdevs listing")
    p.add_argument("--output-dir", default=None, metavar="DIR",
                   help="Directory to write bdev_get_bdevs_<label>.json files "
                        "into (default: logs/bdev_info_<timestamp>/)")
    return p.parse_args()


def parse_nodes(spec):
    """'[label=]ip:port,...' -> [(label, ip, port), ...]."""
    nodes = []
    for entry in spec.split(","):
        entry = entry.strip()
        if not entry:
            continue
        label = None
        if "=" in entry:
            label, entry = entry.split("=", 1)
        if ":" not in entry:
            raise SystemExit(f"--nodes entry {entry!r} must be ip:port")
        ip, port = entry.rsplit(":", 1)
        if not port.isdigit():
            raise SystemExit(f"--nodes entry {entry!r}: port must be numeric")
        nodes.append((label or ip, ip, port))
    return nodes


def main():
    args = parse_args()
    nodes = parse_nodes(args.nodes)

    out_dir = Path(args.output_dir) if args.output_dir else (
        Path(__file__).resolve().parent / "logs" /
        f"bdev_info_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    )
    out_dir.mkdir(parents=True, exist_ok=True)
    print(f"Output directory: {out_dir}")

    bridge = bu.connect_bridge(args.key)
    try:
        for label, ip, port in nodes:
            cmd = (f"docker exec -u root spdk_{port} python3 "
                  f"/root/spdk/scripts/rpc.py -s /mnt/ramdisk/spdk_{port}/spdk.sock "
                  f"bdev_get_bdevs")
            if args.bdev:
                cmd += f" -b {args.bdev}"
            try:
                node = bu.connect_node(bridge, ip)
            except Exception as e:  # noqa: BLE001 -- one unreachable node
                # must not stop the rest from being collected.
                bu.log(label, f"FAILED to connect to {ip}: {e}")
                continue
            try:
                _, stdout, stderr = node.exec_command(cmd, timeout=60)
                out = stdout.read().decode("utf-8", errors="replace")
                err = stderr.read().decode("utf-8", errors="replace")
                rc = stdout.channel.recv_exit_status()
                if rc != 0:
                    bu.log(label, f"bdev_get_bdevs failed (rc={rc}) on {ip}:{port}: "
                                 f"{err.strip()[:300]}")
                    continue
                out_file = out_dir / f"bdev_get_bdevs_{label}.json"
                out_file.write_text(out, encoding="utf-8")
                bu.log(label, f"{ip}:{port} -> saved {len(out)} bytes -> {out_file}")
            finally:
                node.close()
    finally:
        bridge.close()

    print(f"\nDone. bdev_get_bdevs output in {out_dir}")


if __name__ == "__main__":
    main()
