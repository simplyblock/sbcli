#!/usr/bin/env python3
"""
collect_dmesg_remote.py

Connects to every client node (via the bridge_utils SSH tunnel, same as the
other test-logs scripts) and saves each one's `dmesg` output to its own file
locally -- one file per client, so a kernel-level issue (NVMe-oF disconnects,
OOM kills, I/O errors, etc.) can be attributed to the specific client it
happened on.

This does not set up the cluster -- point --cluster at one that's already
running (see bridge_utils.CLUSTERS).

Usage (copy a private key into this folder named "simplyblock" first, or
pass --key to point at a different one):
  python3 collect_dmesg_remote.py
  python3 collect_dmesg_remote.py --clients 192.168.10.147,192.168.10.148
  python3 collect_dmesg_remote.py --output-dir dmesg_logs --clear
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
    p.add_argument("--clients", default=None, metavar="IP,IP,...",
                   help="Comma-separated client nodes to collect from "
                        "(default: the cluster profile's sn_ips)")
    p.add_argument("--output-dir", default=None, metavar="DIR",
                   help="Directory to write dmesg_<ip>.log files into "
                        "(default: logs/dmesg_<timestamp>/)")
    p.add_argument("--no-timestamps", action="store_true",
                   help="Use plain `dmesg` instead of `dmesg -T` (skip "
                        "human-readable timestamp conversion)")
    p.add_argument("--clear", action="store_true",
                   help="Also clear each node's kernel ring buffer after "
                        "reading it (dmesg -C), so the next collection only "
                        "shows what happened since this run")
    return p.parse_args()


def main():
    args = parse_args()
    mgmt_ip, sn_ips = bu.get_cluster(args.cluster)
    clients = args.clients.split(",") if args.clients else sn_ips

    out_dir = Path(args.output_dir) if args.output_dir else (
        Path(__file__).resolve().parent / "logs" /
        f"dmesg_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    )
    out_dir.mkdir(parents=True, exist_ok=True)
    print(f"Output directory: {out_dir}")

    dmesg_cmd = "dmesg" if args.no_timestamps else "dmesg -T"

    bridge = bu.connect_bridge(args.key)
    try:
        for ip in clients:
            try:
                node = bu.connect_node(bridge, ip)
            except Exception as e:  # noqa: BLE001 -- one unreachable client
                # must not stop the rest from being collected.
                bu.log(ip, f"FAILED to connect: {e}")
                continue
            try:
                _, stdout, stderr = node.exec_command(dmesg_cmd, timeout=60)
                out = stdout.read().decode("utf-8", errors="replace")
                err = stderr.read().decode("utf-8", errors="replace")
                rc = stdout.channel.recv_exit_status()
                if rc != 0:
                    bu.log(ip, f"dmesg failed (rc={rc}): {err.strip()[:300]}")
                    continue
                out_file = out_dir / f"dmesg_{ip}.log"
                out_file.write_text(out, encoding="utf-8")
                bu.log(ip, f"saved {len(out.splitlines())} lines -> {out_file}")
                if args.clear:
                    node.exec_command("dmesg -C", timeout=30)
                    bu.log(ip, "cleared kernel ring buffer")
            finally:
                node.close()
    finally:
        bridge.close()

    print(f"\nDone. dmesg logs in {out_dir}")


if __name__ == "__main__":
    main()
