"""End-to-end self-test of an array plug-in against a mock array with a real
data path, on one Linux host.

The mock array (a vendor fake with its LIO mirror, ``testing.lio``) is served
over HTTPS, and two iSCSI interfaces of this host, with two initiator names,
play two storage nodes. The array plug-in of the multi-attach mode
(``simplyblock_core.array.san``) drives the mock exactly as the control plane
does, and the node agent's own functions (``scan_for_new_luns``,
``find_lun_devices``, ``rescan_block_device``) find the LUNs on the host. The
checks are about the data path, on real block devices:

1. a LUN created and attached to host A appears on A after a scan;
2. attached to host B as well, B reads the data A wrote (multi-attach);
3. fenced from A, A's I/O fails while B's goes on;
4. grown, the new size is seen by B after a rescan;
5. unfenced, A sees the LUN again;
6. deleted, it is gone from the target.

Run as root on a host with LIO (``target_core_iblock``, ``iscsi_target_mod``),
open-iscsi, and python3-rtslib-fb::

    python -m simplyblock_core.storage_arrays.testing.lio_selftest --vendor pure-flasharray

On WSL, the portal goes on a dummy interface, because WSL's loopback refuses
the kernel target's listener::

    ... lio_selftest --vendor pure-flasharray --portal 192.168.250.1:3260 --dummy-portal
"""
from __future__ import annotations

import argparse
import hashlib
import importlib
import os
import subprocess
import sys
import threading
import time

from ..registry import ADAPTERS, canonical_vendor
from . import FakeTransport, serve
from .lio import ConfigfsLio, LioMirror

GIB = 1 << 30
TARGET = "iqn.2026-11.io.simplyblock:selftest"
IQN_A = "iqn.2026-11.io.simplyblock:selftest-host-a"
IQN_B = "iqn.2026-11.io.simplyblock:selftest-host-b"
PATTERN_BYTES = 4 << 20


def sh(*cmd: str, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(list(cmd), check=check, capture_output=True, text=True)


def step(text: str) -> None:
    print(f"--- {text}", flush=True)


class Hosts:
    """Two iSCSI interfaces of this host, logged in to the mock's target."""

    def __init__(self, portal: str):
        self.portal = portal
        if sh("pgrep", "-x", "iscsid", check=False).returncode != 0:
            # Socket-activated under systemd; started by hand elsewhere.
            if sh("systemctl", "start", "iscsid", check=False).returncode != 0:
                subprocess.run(["iscsid"], check=True)
            time.sleep(1)
        for iface, iqn in (("sbtest-a", IQN_A), ("sbtest-b", IQN_B)):
            sh("iscsiadm", "-m", "iface", "-I", iface, "-o", "new", check=False)
            sh("iscsiadm", "-m", "iface", "-I", iface, "-o", "update", "-n", "iface.initiatorname", "-v", iqn)
        sh("iscsiadm", "-m", "discovery", "-t", "st", "-p", portal, "-I", "sbtest-a", "-I", "sbtest-b")
        sh("iscsiadm", "-m", "node", "-T", TARGET, "-p", portal, "--login")

    def sid_of(self, iqn: str) -> str:
        """The SCSI host (``hostN``) of the session with initiator ``iqn``."""
        for sess in os.listdir("/sys/class/iscsi_session"):
            base = f"/sys/class/iscsi_session/{sess}"
            if open(f"{base}/initiatorname").read().strip() == iqn:
                # .../hostN/sessionM: the session's SCSI host is in its path.
                path = os.path.realpath(f"{base}/device")
                return next(p for p in path.split("/") if p.startswith("host"))
        raise RuntimeError(f"no iSCSI session of {iqn}")

    def close(self) -> None:
        sh("iscsiadm", "-m", "node", "-T", TARGET, "-p", self.portal, "--logout", check=False)
        sh("iscsiadm", "-m", "node", "-T", TARGET, "-p", self.portal, "-o", "delete", check=False)
        for iface in ("sbtest-a", "sbtest-b"):
            sh("iscsiadm", "-m", "iface", "-I", iface, "-o", "delete", check=False)


def device_of(node_utils, wwn: str, scsi_host: str, timeout: float = 20.0) -> str | None:
    """The block device of LUN ``wwn`` behind ``scsi_host``, after scans."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        node_utils.scan_for_new_luns()
        subprocess.run(["udevadm", "settle"], check=False)
        for d in node_utils.find_lun_devices(wwn):
            link = os.path.realpath(f"/sys/block/{d['name']}/device")
            if f"/{scsi_host}/" in link:
                return d["device_path"]
        time.sleep(1)
    return None


def write_pattern(dev: str) -> str:
    flags = os.O_WRONLY
    if sys.platform == "linux":
        flags |= os.O_DIRECT | os.O_SYNC
    data = os.urandom(PATTERN_BYTES)
    fd = os.open(dev, flags)
    try:
        import mmap
        buf = mmap.mmap(-1, PATTERN_BYTES)
        buf.write(data)
        os.write(fd, buf)
    finally:
        os.close(fd)
    return hashlib.sha256(data).hexdigest()


def read_pattern(dev: str) -> str:
    r = subprocess.run(["dd", f"if={dev}", "bs=1M", f"count={PATTERN_BYTES >> 20}", "iflag=direct",
                        "status=none"], capture_output=True)
    if r.returncode != 0 or len(r.stdout) != PATTERN_BYTES:
        raise OSError(r.stderr.decode(errors="replace").strip() or f"short read of {dev}")
    return hashlib.sha256(r.stdout).hexdigest()


MODULES = ("configfs", "target_core_mod", "iscsi_target_mod", "target_core_iblock", "iscsi_tcp", "loop")


def prepare_host(portal_ip: str, dummy_portal: bool) -> None:
    """Load the target and initiator modules and mount configfs (all
    idempotent). With ``dummy_portal`` the portal address is put on a dummy
    interface: hosts whose loopback refuses kernel listeners (WSL's mirrored
    networking) need it."""
    for module in MODULES:
        sh("modprobe", module)
    if sh("mountpoint", "-q", "/sys/kernel/config", check=False).returncode != 0:
        sh("mount", "-t", "configfs", "none", "/sys/kernel/config")
    if dummy_portal and sh("ip", "link", "show", "sbdummy", check=False).returncode != 0:
        sh("ip", "link", "add", "sbdummy", "type", "dummy")
        sh("ip", "addr", "add", f"{portal_ip}/32", "dev", "sbdummy")
        sh("ip", "link", "set", "sbdummy", "up")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--vendor", default="pure-flasharray", help=", ".join(sorted(ADAPTERS)))
    parser.add_argument("--port", type=int, default=8443)
    parser.add_argument("--portal", default="127.0.0.1:3260")
    parser.add_argument("--lio-dir", default="/var/tmp/sb-lio-selftest")
    parser.add_argument("--dummy-portal", action="store_true",
                        help="put the portal address on a dummy interface (WSL)")
    args = parser.parse_args()

    if sys.platform != "linux":
        print("runs on Linux only", file=sys.stderr)
        return 2
    if os.geteuid() != 0:
        print("run as root", file=sys.stderr)
        return 2
    from simplyblock_core import array
    from simplyblock_web import node_utils

    vendor = canonical_vendor(args.vendor)
    module = importlib.import_module(f"{__package__}.{ADAPTERS[vendor].split(':')[0].split('.')[-1]}")
    setup = module.contract_setup()
    ip, _, port = args.portal.rpartition(":")
    prepare_host(ip, args.dummy_portal)
    backend = ConfigfsLio(TARGET, args.lio_dir, (ip, int(port)))
    mirror = LioMirror(setup.fake, setup.config, setup.credential, backend,
                       transport=FakeTransport(setup.fake), allow=[IQN_A, IQN_B])
    mirror.reconcile()
    server = serve(setup.fake, "127.0.0.1", args.port, after_request=mirror.reconcile)
    threading.Thread(target=server.serve_forever, daemon=True).start()

    hosts = Hosts(args.portal)
    failures: list[str] = []

    def check(ok: bool, what: str) -> None:
        print(("PASS " if ok else "FAIL ") + what, flush=True)
        if not ok:
            failures.append(what)

    plugin = None
    lun = None
    try:
        endpoint = f"https://127.0.0.1:{args.port}"
        options = dict(setup.config.options, protocol="iscsi", ca_bundle=open(server.certfile).read())
        plugin = array.plugin_class(vendor)(endpoint=endpoint, options=options, cluster_id="selftest")
        plugin.connect(endpoint, {n: setup.credential.get(n) for n in setup.credential.field_names}, options)
        host_a = plugin.ensure_host("selftest-a", {"iscsi_iqn": IQN_A})
        host_b = plugin.ensure_host("selftest-b", {"iscsi_iqn": IQN_B})
        scsi_a, scsi_b = hosts.sid_of(IQN_A), hosts.sid_of(IQN_B)

        step("1. create and attach to host A")
        lun = plugin.create_lun("selftest-lun", GIB, {})
        plugin.attach(lun["id"], host_a)
        dev_a = device_of(node_utils, lun["wwn"], scsi_a)
        check(dev_a is not None, f"LUN {lun['wwn']} appears on host A ({dev_a})")
        if dev_a is None:
            return 1
        digest = write_pattern(dev_a)

        step("2. attach to host B as well (multi-attach)")
        plugin.attach(lun["id"], host_b)
        dev_b = device_of(node_utils, lun["wwn"], scsi_b)
        check(dev_b is not None, f"LUN appears on host B ({dev_b})")
        if dev_b is None:
            return 1
        check(read_pattern(dev_b) == digest, "host B reads what host A wrote")
        check(sorted(plugin.get(lun["id"])["hosts"]) == sorted([host_a, host_b]),
              "the array reports both hosts")

        step("3. fence host A")
        plugin.fence(lun["id"], host_a)
        try:
            read_pattern(dev_a)
            check(False, "host A's I/O fails after the fence")
        except OSError as e:
            check(True, f"host A's I/O fails after the fence ({str(e)[:60]})")
        check(read_pattern(dev_b) == digest, "host B still reads the data")

        step("4. grow")
        plugin.resize(lun["id"], 2 * GIB)
        grown = node_utils.rescan_block_device(dev_b)
        check(grown["size"] >= 2 * GIB, f"host B sees {grown['size']} bytes after a rescan")

        step("5. unfence host A")
        plugin.unfence(lun["id"], host_a)
        sh("iscsiadm", "-m", "session", "--rescan", check=False)
        dev_a2 = device_of(node_utils, lun["wwn"], scsi_a)
        check(dev_a2 is not None and read_pattern(dev_a2) == digest, "host A reads the data again")

        step("6. delete")
        plugin.delete_lun(lun["id"])
        check(mirror.key(lun["id"]) not in backend.list_volumes(), "the LUN is gone from the target")
        lun = None
    finally:
        if plugin is not None and lun is not None:
            try:
                plugin.delete_lun(lun["id"])
            except Exception as e:  # noqa: BLE001
                print(f"cleanup: {e}")
        hosts.close()
        server.shutdown()
        backend_cleanup(backend)
    print(f"{'FAILED' if failures else 'PASSED'}: {len(failures)} failure(s)", flush=True)
    return 1 if failures else 0


def backend_cleanup(backend: ConfigfsLio) -> None:
    for key in list(backend.list_volumes()):
        for iqn, luns in backend.list_acls().items():
            for n, k in luns.items():
                if k == key:
                    backend.unmap_lun(iqn, n)
        backend.delete_volume(key)
    backend.target.delete()


if __name__ == "__main__":
    sys.exit(main())
