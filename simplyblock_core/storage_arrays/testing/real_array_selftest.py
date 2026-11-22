"""Short self-test of an array plug-in against a REAL array: provisioning,
resize and removal, with the data path checked on this host.

Run as root on a Linux host that reaches the array's management endpoint and
its data ports, with open-iscsi (iSCSI) or nvme-cli (NVMe/TCP) installed. The
array plug-in of the multi-attach mode (``simplyblock_core.array.san``) drives
the array exactly as the control plane does, and the node agent's own
functions (``scan_for_new_luns``, ``find_lun_devices``,
``rescan_block_device``) find the LUN on the host.

Credentials come from the environment, one variable per credential field:
``SB_ARRAY_USERNAME``, ``SB_ARRAY_PASSWORD``, ``SB_ARRAY_API_TOKEN``, ...

    SB_ARRAY_USERNAME=fsxadmin SB_ARRAY_PASSWORD=... \\
      python -m simplyblock_core.storage_arrays.testing.real_array_selftest \\
        --vendor netapp-ontap --endpoint https://198.19.255.10 --no-verify-tls \\
        --option svm=sbsvm --portal 10.0.1.20:3260 --portal 10.0.2.20:3260

Everything the test creates on the array carries a cluster id of its own
(``rt<random>``) and is removed at the end, also after a failure.
"""
from __future__ import annotations

import argparse
import os
import secrets
import socket
import subprocess
import sys
import time

GIB = 1 << 30


def sh(*cmd: str, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(list(cmd), check=check, capture_output=True, text=True)


def step(text: str) -> None:
    print(f"--- {text}", flush=True)


class Initiator:
    """This host's initiator, logged in to the array's data ports."""

    def __init__(self, protocol: str, portals: list[str], nqn_subsys: str | None):
        self.protocol = protocol
        self.portals = portals
        self.nqn_subsys = nqn_subsys
        if protocol == "iscsi":
            iqn = open("/etc/iscsi/initiatorname.iscsi").read().split("=", 1)[1].strip()
            self.initiators = {"iscsi_iqn": iqn}
        elif protocol == "nvme-tcp":
            nqn = open("/etc/nvme/hostnqn").read().strip()
            self.initiators = {"nvme_hostnqn": nqn}
        else:
            raise SystemExit(f"protocol {protocol} is not handled by this test")

    def login(self) -> None:
        if self.protocol == "iscsi":
            sh("systemctl", "start", "iscsid", check=False)
            for p in self.portals:
                sh("iscsiadm", "-m", "discovery", "-t", "st", "-p", p)
            sh("iscsiadm", "-m", "node", "--login", check=False)
        else:
            sh("modprobe", "nvme-tcp")
            for p in self.portals:
                ip, _, port = p.rpartition(":")
                sh("nvme", "connect-all", "-t", "tcp", "-a", ip, "-s", port or "4420", check=False)

    def rescan(self) -> None:
        if self.protocol == "iscsi":
            sh("iscsiadm", "-m", "session", "--rescan", check=False)
        else:
            for ctrl in os.listdir("/sys/class/nvme"):
                sh("nvme", "ns-rescan", f"/dev/{ctrl}", check=False)
        sh("udevadm", "settle", check=False)

    def logout(self) -> None:
        if self.protocol == "iscsi":
            for p in self.portals:
                sh("iscsiadm", "-m", "node", "-p", p, "--logout", check=False)
                sh("iscsiadm", "-m", "node", "-p", p, "-o", "delete", check=False)
        else:
            sh("nvme", "disconnect-all", check=False)


def devices_of(node_utils, ini: Initiator, wwn: str, timeout: float) -> list[dict]:
    deadline = time.monotonic() + timeout
    while True:
        node_utils.scan_for_new_luns()
        ini.rescan()
        found = node_utils.find_lun_devices(wwn)
        if found or time.monotonic() >= deadline:
            return found
        time.sleep(2)


def size_of(dev: str) -> int:
    return int(sh("blockdev", "--getsize64", dev).stdout.strip())


def write_pattern(dev: str, nbytes: int = 4 << 20) -> bytes:
    import mmap
    data = os.urandom(nbytes)
    buf = mmap.mmap(-1, nbytes)
    buf.write(data)
    fd = os.open(dev, os.O_WRONLY | os.O_DIRECT | os.O_SYNC)
    try:
        os.write(fd, buf)
    finally:
        os.close(fd)
    return data


def read_pattern(dev: str, nbytes: int = 4 << 20) -> bytes:
    import mmap
    buf = mmap.mmap(-1, nbytes)
    with os.fdopen(os.open(dev, os.O_RDONLY | os.O_DIRECT), "rb", buffering=0) as f:
        n = f.readinto(buf)
    if n != nbytes:
        raise OSError(f"short read of {dev}: {n} bytes")
    return buf[:]


def drop_stale(devs: list[dict]) -> None:
    """Remove the host's SCSI devices of a LUN the array no longer presents."""
    for d in devs:
        path = f"/sys/block/{d['name']}/device/delete"
        if os.path.exists(path):
            with open(path, "w") as f:
                f.write("1")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--vendor", required=True)
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--protocol", default="iscsi")
    parser.add_argument("--portal", action="append", default=[], help="data port IP:PORT (repeatable)")
    parser.add_argument("--option", action="append", default=[], metavar="KEY=VALUE")
    parser.add_argument("--ca-bundle")
    parser.add_argument("--no-verify-tls", action="store_true")
    parser.add_argument("--size-gib", type=int, default=4)
    parser.add_argument("--grow-gib", type=int, default=6)
    parser.add_argument("--timeout", type=float, default=60.0, help="seconds to wait for the host to see a change")
    args = parser.parse_args()

    if sys.platform != "linux" or os.geteuid() != 0:
        print("run as root on Linux", file=sys.stderr)
        return 2
    from simplyblock_core import array
    from simplyblock_web import node_utils

    creds = {k[len("SB_ARRAY_"):].lower(): v for k, v in os.environ.items() if k.startswith("SB_ARRAY_")}
    options: dict = dict(o.split("=", 1) for o in args.option)
    options["protocol"] = args.protocol
    if args.ca_bundle:
        options["ca_bundle"] = open(args.ca_bundle).read()
    if args.no_verify_tls:
        options["verify_tls"] = False
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    cluster_id = "rt" + secrets.token_hex(15)       # 32 chars, like a cluster UUID without dashes

    failures: list[str] = []

    def check(ok: bool, what: str) -> bool:
        print(("PASS " if ok else "FAIL ") + what, flush=True)
        if not ok:
            failures.append(what)
        return ok

    def raises(fn) -> Exception | None:
        try:
            fn()
        except Exception as e:  # noqa: BLE001
            return e
        return None

    ini = Initiator(args.protocol, args.portal, None)
    plugin = array.plugin_class(args.vendor)(endpoint=args.endpoint, options=options, cluster_id=cluster_id)
    lun = None
    host_id = None
    devs: list[dict] = []
    t0 = time.monotonic()
    try:
        step("connect")
        plugin.connect(args.endpoint, creds, options)
        info = plugin._run(lambda c: c.get_info())
        print(f"    array: {info}", flush=True)

        step("P1 host object")
        host_id = plugin.ensure_host(socket.gethostname(), ini.initiators)
        check(bool(host_id), f"host object {host_id} for {ini.initiators}")
        check(plugin.ensure_host(socket.gethostname(), ini.initiators) == host_id,
              "ensure_host is idempotent (same id on a repeat)")

        step(f"P2 create a {args.size_gib} GiB LUN")
        t = time.monotonic()
        lun = plugin.create_lun("selftest", args.size_gib * GIB, {})
        print(f"    {lun} in {time.monotonic() - t:.1f} s", flush=True)
        view = plugin.get(lun["id"])
        check(view["size"] >= args.size_gib * GIB, f"the array reports {view['size']} bytes")
        check(view["hosts"] == [], "a new LUN is presented to no host")
        again = plugin.create_lun("selftest", args.size_gib * GIB, {})
        check(again["id"] == lun["id"], "create_lun of the same name returns the same LUN (retry-safe)")

        step("P3 attach and find it on this host")
        ini.login()
        t = time.monotonic()
        plugin.attach(lun["id"], host_id)
        plugin.attach(lun["id"], host_id)    # a repeat must not fail
        check(plugin.get(lun["id"])["hosts"] == [host_id], "the array reports this host, once")
        devs = devices_of(node_utils, ini, lun["wwn"], args.timeout)
        if not check(bool(devs), f"LUN {lun['wwn']} appears on the host ({[d['name'] for d in devs]}) "
                                 f"in {time.monotonic() - t:.1f} s"):
            return 1
        dev = devs[0]["device_path"]
        check(size_of(dev) == view["size"], f"the host sees {size_of(dev)} bytes")
        data = write_pattern(dev)
        check(read_pattern(dev) == data, "data written reads back")

        step(f"R1 grow to {args.grow_gib} GiB")
        t = time.monotonic()
        plugin.resize(lun["id"], args.grow_gib * GIB)
        check(plugin.get(lun["id"])["size"] >= args.grow_gib * GIB,
              f"the array reports {plugin.get(lun['id'])['size']} bytes in {time.monotonic() - t:.1f} s")
        deadline = time.monotonic() + args.timeout
        seen = 0
        while time.monotonic() < deadline:
            ini.rescan()
            seen = node_utils.rescan_block_device(dev)["size"]
            if seen >= args.grow_gib * GIB:
                break
            time.sleep(2)
        check(seen >= args.grow_gib * GIB, f"the host sees {seen} bytes after a rescan")
        check(read_pattern(dev) == data, "the data survives the growth")

        step("R2 resize to the size it has")
        e = raises(lambda: plugin.resize(lun["id"], args.grow_gib * GIB))
        check(e is None, f"a resize to the current size is accepted ({e})")

        step("R3 shrink")
        before = plugin.get(lun["id"])["size"]
        e = raises(lambda: plugin.resize(lun["id"], args.size_gib * GIB))
        after = plugin.get(lun["id"])["size"]
        check(e is not None and after == before,
              f"a shrink is refused and the size stays {after} ({type(e).__name__}: {str(e)[:120]})")

        step("D1 detach")
        t = time.monotonic()
        plugin.detach(lun["id"], host_id)
        check(plugin.get(lun["id"])["hosts"] == [], f"the array reports no host ({time.monotonic() - t:.1f} s)")
        e = raises(lambda: read_pattern(dev))
        if e is None:
            ini.rescan()
            e = raises(lambda: read_pattern(dev))
        check(e is not None, f"the host can no longer read the LUN ({str(e)[:80]})")
        check(raises(lambda: plugin.detach(lun["id"], host_id)) is None, "a repeated detach is accepted")
        drop_stale(devs)

        step("D2 delete")
        t = time.monotonic()
        plugin.delete_lun(lun["id"])
        gone = raises(lambda: plugin.get(lun["id"]))
        check(gone is not None and "not found" in str(gone),
              f"the LUN is gone from the array ({time.monotonic() - t:.1f} s)")
        check(raises(lambda: plugin.delete_lun(lun["id"])) is None, "a repeated delete is accepted")
        lun = None

        step("D3 host object")
        e = raises(lambda: plugin._run(lambda c: c.delete_host(host_id)))
        check(e is None, f"the host object is removed ({e})")
        host_id = None
    finally:
        if lun is not None:
            for fn in (lambda: plugin.detach(lun["id"], host_id), lambda: plugin.delete_lun(lun["id"])):
                e = raises(fn)
                if e:
                    print(f"cleanup: {e}", flush=True)
            drop_stale(devs)
        if host_id is not None:
            e = raises(lambda: plugin._run(lambda c: c.delete_host(host_id)))
            if e:
                print(f"cleanup: {e}", flush=True)
        ini.logout()
    print(f"{'FAILED' if failures else 'PASSED'}: {len(failures)} failure(s) in {time.monotonic() - t0:.0f} s",
          flush=True)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
