"""End-to-end self-test of an array plug-in against a mock array with a real
data path, on one Linux host.

The mock array (a vendor fake with its LIO mirror, ``fakes.lio``) is served
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

    python -m tests.unit.storage_arrays.fakes.lio_selftest --vendor pure-flasharray

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

from simplyblock_core.storage_arrays.registry import ADAPTERS, canonical_vendor

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
    """O_DIRECT read into a page-aligned buffer, as SPDK reads the device."""
    import mmap
    flags = os.O_RDONLY
    if sys.platform == "linux":
        flags |= os.O_DIRECT
    buf = mmap.mmap(-1, PATTERN_BYTES)
    with os.fdopen(os.open(dev, flags), "rb", buffering=0) as f:
        n = f.readinto(buf)
    if n != PATTERN_BYTES:
        raise OSError(f"short read of {dev}: {n} bytes")
    return hashlib.sha256(buf[:]).hexdigest()


MODULES = ("configfs", "target_core_mod", "iscsi_target_mod", "target_core_iblock", "iscsi_tcp", "loop",
           "dm_multipath")


MULTIPATH_CONF = """# written by the simplyblock LIO self-test; restored afterwards
defaults {
    user_friendly_names yes
    find_multipaths yes
    polling_interval 1
}
# LIO's built-in device entry queues for 12 checks; a fenced map fails at once.
overrides {
    no_path_retry fail
}
blacklist {
    device {
        vendor ".*"
        product ".*"
    }
}
blacklist_exceptions {
    device {
        vendor "LIO-ORG"
        product ".*"
    }
}
"""


class Multipathd:
    """multipathd with a configuration that claims only the mock's LUNs, for
    the duration of the multipath scenario."""

    CONF = "/etc/multipath.conf"
    BACKUP = "/etc/multipath.conf.sb-selftest"

    def __init__(self) -> None:
        if os.path.exists(self.CONF):
            os.replace(self.CONF, self.BACKUP)
        with open(self.CONF, "w") as f:
            f.write(MULTIPATH_CONF)
        sh("systemctl", "start", "multipathd", check=False)
        self.direct = sh("systemctl", "is-active", "multipathd", check=False).stdout.strip() != "active"
        if self.direct:
            # systemd skips the unit where it detects a container (WSL).
            sh("multipathd")
            for _ in range(20):
                if sh("multipathd", "show", "daemon", check=False).returncode == 0:
                    break
                time.sleep(0.5)
        sh("multipathd", "reconfigure", check=False)

    def close(self) -> None:
        sh("multipath", "-F", check=False)
        if self.direct:
            sh("multipathd", "shutdown", check=False)
        else:
            sh("systemctl", "stop", "multipathd", "multipathd.socket", check=False)
        os.unlink(self.CONF)
        if os.path.exists(self.BACKUP):
            os.replace(self.BACKUP, self.CONF)


def map_of(node_utils, wwn: str, paths: int, timeout: float = 30.0) -> dict | None:
    """The multipath map of LUN ``wwn`` once it has ``paths`` paths."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        node_utils.scan_for_new_luns()
        subprocess.run(["udevadm", "settle"], check=False)
        for d in node_utils.find_lun_devices(wwn):
            if d.get("type") == "mpath":
                live: dict = next((x for x in node_utils.get_block_devices_info() if x["name"] == d["name"]), {})
                if len(live.get("paths", [])) >= paths:
                    return d
        time.sleep(1)
    return None


def run_multipath(args, setup, vendor: str, portals: list[str], plugin_cls, node_utils, mirror, backend,
                  server, check) -> None:
    """One host reaching every LUN over two portals, under dm-multipath."""
    step("multipath: host A logs in over two portals")
    sh("iscsiadm", "-m", "iface", "-I", "sbtest-a", "-o", "new", check=False)
    sh("iscsiadm", "-m", "iface", "-I", "sbtest-a", "-o", "update", "-n", "iface.initiatorname", "-v", IQN_A)
    for portal in portals:
        sh("iscsiadm", "-m", "discovery", "-t", "st", "-p", portal, "-I", "sbtest-a")
    sh("iscsiadm", "-m", "node", "-T", TARGET, "--login")
    mpd = Multipathd()
    lun = None
    plugin = None
    try:
        endpoint = f"https://127.0.0.1:{args.port}"
        options = dict(setup.config.options, protocol="iscsi", ca_bundle=open(server.certfile).read())
        plugin = plugin_cls(endpoint=endpoint, options=options, cluster_id="selftest")
        plugin.connect(endpoint, {n: setup.credential.get(n) for n in setup.credential.field_names}, options)
        host_a = plugin.ensure_host("selftest-a", {"iscsi_iqn": IQN_A})

        step("M1. create and attach: the LUN is a multipath map with two paths")
        lun = plugin.create_lun("selftest-mpath", GIB, {})
        plugin.attach(lun["id"], host_a)
        m = map_of(node_utils, lun["wwn"], paths=2)
        check(m is not None, f"LUN {lun['wwn']} found as a multipath map ({m and m['device_path']})")
        if m is None:
            print(sh("systemctl", "is-active", "multipathd", check=False).stdout)
            r = sh("multipath", "-v3", check=False)
            print("\n".join((r.stdout + r.stderr).splitlines()[-40:]))
            print(sh("multipath", "-ll", check=False).stdout)
            print(sh("lsblk", "-o", "NAME,KNAME,TYPE,SIZE,SERIAL,WWN,VENDOR", check=False).stdout)
            return
        dev = m["device_path"]
        digest = write_pattern(dev)
        try:
            check(read_pattern(dev) == digest, "the map reads back what was written over both paths")
        except OSError as e:
            check(False, f"the map reads back what was written over both paths ({e})")

        step("M2. losing a path does not stop the I/O")
        sh("iscsiadm", "-m", "node", "-T", TARGET, "-p", portals[1], "-I", "sbtest-a", "--logout")
        errors: list[str] = []
        got = ""
        for _ in range(10):
            try:
                got = read_pattern(dev)
                break
            except OSError as e:
                errors.append(str(e))
                small = subprocess.run(["dd", f"if={dev}", "bs=4k", "count=1", "iflag=direct",
                                        "status=none"], capture_output=True)
                errors.append(f"4k read rc={small.returncode} {small.stderr.decode(errors='replace').strip()}")
                time.sleep(1)
        print(f"    reads before success: {len(errors) // 2}, errors: {errors[:4]}")
        check(got == digest, "the map reads the data over the remaining path")
        if got != digest:
            print(sh("multipath", "-ll", check=False).stdout)
            print(sh("cat", f"/sys/block/{m['kname']}/queue/max_sectors_kb",
                     f"/sys/block/{m['kname']}/queue/logical_block_size", check=False).stdout)
            for what, cmd in (("path direct", ["dd", "if=/dev/sde", "bs=4k", "count=1", "iflag=direct",
                                               "of=/dev/null"]),
                              ("map buffered", ["dd", f"if={dev}", "bs=4k", "count=1", "of=/dev/null"]),
                              ("dm table", ["dmsetup", "table", m["name"]]),
                              ("dm status", ["dmsetup", "status", m["name"]])):
                r = sh(*cmd, check=False)
                print(f"    {what}: rc={r.returncode} {(r.stdout + r.stderr).strip()[:300]}")
            print("\n".join(sh("dmesg", check=False).stdout.splitlines()[-8:]))
            return
        sh("iscsiadm", "-m", "node", "-T", TARGET, "-p", portals[1], "-I", "sbtest-a", "--login")
        check(map_of(node_utils, lun["wwn"], paths=2) is not None, "the path is back in the map")

        step("M3. grow")
        plugin.resize(lun["id"], 2 * GIB)
        grown = node_utils.rescan_block_device(dev)
        check(grown["size"] >= 2 * GIB and any(r.startswith("multipathd:") for r in grown["rescanned"]),
              f"the map is {grown['size']} bytes after rescanning its paths and resizing it")

        step("M4. fence: the map's I/O fails")
        plugin.fence(lun["id"], host_a)
        failed = ""
        for _ in range(15):
            try:
                read_pattern(dev)
            except OSError as e:
                failed = str(e)
                break
            time.sleep(1)
        check(bool(failed), f"the map's I/O fails after the fence ({failed[:60]})")

        step("M5. unfence: the map serves again")
        plugin.unfence(lun["id"], host_a)
        sh("iscsiadm", "-m", "session", "--rescan", check=False)
        back = ""
        for _ in range(30):
            sh("multipathd", "reconfigure", check=False)
            try:
                back = read_pattern(dev)
                break
            except OSError:
                time.sleep(1)
        check(back == digest, "the map reads the data again")

        step("M6. delete")
        sh("multipath", "-f", m["name"], check=False)
        plugin.delete_lun(lun["id"])
        check(mirror.key(lun["id"]) not in backend.list_volumes(), "the LUN is gone from the target")
        lun = None
    finally:
        if plugin is not None and lun is not None:
            try:
                plugin.delete_lun(lun["id"])
            except Exception as e:  # noqa: BLE001
                print(f"cleanup: {e}")
        mpd.close()
        sh("iscsiadm", "-m", "node", "-T", TARGET, "--logout", check=False)
        sh("iscsiadm", "-m", "node", "-T", TARGET, "-o", "delete", check=False)
        sh("iscsiadm", "-m", "iface", "-I", "sbtest-a", "-o", "delete", check=False)


def prepare_host(portal_ip: str, dummy_portal: bool) -> None:
    """Load the target and initiator modules and mount configfs (all
    idempotent). With ``dummy_portal`` the portal address is put on a dummy
    interface: hosts whose loopback refuses kernel listeners (WSL's mirrored
    networking) need it."""
    for module in MODULES:
        sh("modprobe", module)
    if sh("mountpoint", "-q", "/sys/kernel/config", check=False).returncode != 0:
        sh("mount", "-t", "configfs", "none", "/sys/kernel/config")
    if dummy_portal:
        if sh("ip", "link", "show", "sbdummy", check=False).returncode != 0:
            sh("ip", "link", "add", "sbdummy", "type", "dummy")
            sh("ip", "link", "set", "sbdummy", "up")
        if f" {portal_ip}/" not in sh("ip", "-o", "addr", "show", "dev", "sbdummy").stdout:
            sh("ip", "addr", "add", f"{portal_ip}/32", "dev", "sbdummy")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--vendor", default="pure-flasharray", help=", ".join(sorted(ADAPTERS)))
    parser.add_argument("--port", type=int, default=8443)
    parser.add_argument("--portal", default="127.0.0.1:3260")
    parser.add_argument("--lio-dir", default="/var/tmp/sb-lio-selftest")
    parser.add_argument("--dummy-portal", action="store_true",
                        help="put the portal address on a dummy interface (WSL)")
    parser.add_argument("--multipath", metavar="IP:PORT",
                        help="run the multipath scenario: a second portal, and one host under dm-multipath "
                             "(instead of the two-host scenario)")
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
    portals = [(ip, int(port))]
    if args.multipath:
        ip2, _, port2 = args.multipath.rpartition(":")
        prepare_host(ip2, args.dummy_portal)
        portals.append((ip2, int(port2)))
    backend = ConfigfsLio(TARGET, args.lio_dir, portals)
    mirror = LioMirror(setup.fake, setup.config, setup.credential, backend,
                       transport=FakeTransport(setup.fake), allow=[IQN_A, IQN_B])
    mirror.reconcile()
    server = serve(setup.fake, "127.0.0.1", args.port, after_request=mirror.reconcile)
    threading.Thread(target=server.serve_forever, daemon=True).start()

    failures: list[str] = []

    def check(ok: bool, what: str) -> None:
        print(("PASS " if ok else "FAIL ") + what, flush=True)
        if not ok:
            failures.append(what)

    if args.multipath:
        try:
            run_multipath(args, setup, vendor, [args.portal, args.multipath], array.plugin_class(vendor),
                          node_utils, mirror, backend, server, check)
        finally:
            server.shutdown()
            backend_cleanup(backend)
        print(f"{'FAILED' if failures else 'PASSED'}: {len(failures)} failure(s)", flush=True)
        return 1 if failures else 0

    hosts = Hosts(args.portal)

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
