"""The checks of the array pre-check, in the order they run.

Read-only checks (always): the management endpoint, the login, what the
array reports about itself, and the host's SAN setup. Nothing is changed on
the array or the host.

Write checks (``--write``): one small LUN goes through what simplyblock does
with LUNs (host object, create, attach, find on the host, grow, fence and
unfence, detach, delete; a second one in the journal's pool when journal
options are given). Everything is named after the run id and removed at the
end, also when a check fails; ``cleanup --run-id`` removes what a killed run
left behind.
"""
from __future__ import annotations

import socket
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from typing import Any
from urllib.parse import urlsplit

from . import host as H

GIB = 1 << 30

#: Names of the LUNs a run creates (the cluster id is the run id).
DATA_LUN = "check-data"
JOURNAL_LUN = "check-journal"

#: Longest a host may take to fail I/O to a LUN it lost (a storage node gives
#: up on a LUN I/O after 60 seconds; see the deployment preparation docs).
IO_DEADLINE_S = 75.0


class Outcome(Exception):
    status = "FAIL"

    def __init__(self, detail: str, hint: str = ""):
        super().__init__(detail)
        self.detail = detail
        self.hint = hint


class Fail(Outcome):
    status = "FAIL"


class Warn(Outcome):
    status = "WARN"


class Skip(Outcome):
    status = "SKIP"


@dataclass
class Result:
    id: str
    title: str
    status: str
    seconds: float
    detail: str = ""
    hint: str = ""


@dataclass
class Config:
    vendor: str
    endpoint: str
    protocol: str
    options: dict[str, Any]
    credentials: dict[str, str]
    run_id: str
    host_name: str
    journal_options: dict[str, Any] = field(default_factory=dict)
    portals: list[str] = field(default_factory=list)
    write: bool = False
    size_gib: int = 1
    grow_gib: int = 2
    timeout_s: float = 90.0
    initiators: dict[str, Any] = field(default_factory=dict)
    ca_file: str | None = None
    verify_tls: bool = True


class Runner:
    def __init__(self, echo: Callable[[str], None] = print):
        self.results: list[Result] = []
        self.status: dict[str, str] = {}
        self.echo = echo

    def run(self, cid: str, title: str, fn: Callable[[], Any], needs: tuple[str, ...] = ()) -> Any:
        missing = [n for n in needs if self.status.get(n) not in ("PASS", "WARN")]
        start = time.monotonic()
        value = None
        if missing:
            res = Result(cid, title, "SKIP", 0.0, f"needs {', '.join(missing)}")
        else:
            try:
                value = fn()
                detail = value if isinstance(value, str) else ""
                res = Result(cid, title, "PASS", 0.0, detail)
            except Outcome as o:
                res = Result(cid, title, o.status, 0.0, o.detail, o.hint)
            except Exception as e:  # noqa: BLE001
                res = Result(cid, title, "FAIL", 0.0, f"{type(e).__name__}: {e}")
        res.seconds = round(time.monotonic() - start, 2)
        self.results.append(res)
        self.status[cid] = res.status
        line = f"{res.status:<4} {cid:<22} {title}"
        if res.detail:
            line += f"\n       {res.detail}"
        if res.hint:
            line += f"\n       hint: {res.hint}"
        self.echo(line)
        return value

    def summary(self) -> dict[str, int]:
        out: dict[str, int] = {}
        for r in self.results:
            out[r.status] = out.get(r.status, 0) + 1
        return out

    def as_dicts(self) -> list[dict[str, Any]]:
        return [asdict(r) for r in self.results]


class State:
    """What the checks share: the plug-in, the created objects, the host's
    view of the LUN."""

    def __init__(self) -> None:
        self.plugin: Any = None
        self.info: Any = None
        self.caps: Any = None
        self.host_id: str | None = None
        self.host_created = False
        self.luns: dict[str, dict] = {}
        self.devs: list[dict] = []
        self.dev: str | None = None
        self.data: bytes | None = None
        #: The LUN's size as created, and the size the growth asks for (one
        #: size step of the array, at least 1 GiB, above it).
        self.size = 0
        self.grow_to = 0


def _proto(cfg: Config):
    from .. import base as sa
    return sa.Protocol(cfg.protocol)


def run_checks(cfg: Config, make_plugin: Callable[[dict], Any], node_utils: Any, runner: Runner,
               live: bool = True) -> State:
    """All checks. ``make_plugin(options)`` returns an unconnected plug-in;
    ``node_utils`` is None where the host is not checked (replay)."""
    from ...array.san import _INITIATOR_KEYS
    st = State()
    proto = _proto(cfg)
    scsi = proto.is_scsi
    url = urlsplit(cfg.endpoint)
    mgmt_host, mgmt_port = url.hostname or "", url.port or (443 if url.scheme == "https" else 80)

    # ------------------------------------------------------------ read-only

    def endpoint() -> str:
        why = H.tcp_reachable(mgmt_host, mgmt_port)
        if why:
            raise Fail(f"{mgmt_host}:{mgmt_port} does not answer: {why}",
                       "a firewall between this host and the array's management address?")
        if url.scheme != "https":
            raise Warn("the endpoint is plain HTTP; credentials go over the network unencrypted")
        tls = H.tls_probe(mgmt_host, mgmt_port, cfg.ca_file, cfg.verify_tls)
        if "error" in tls:
            raise Fail(tls["error"], "give the array's CA certificate (--ca-bundle), or --no-verify-tls "
                                     "to check everything else")
        detail = f"{tls.get('tls_version')}, certificate {tls.get('subject') or '(not verified)'}"
        if not cfg.verify_tls:
            raise Warn(detail + "; the certificate was NOT verified (--no-verify-tls)")
        return detail

    if live:
        runner.run("api.endpoint", "management endpoint reachable over TLS", endpoint)

    def login() -> str:
        st.plugin = make_plugin(cfg.options)
        st.plugin.connect(cfg.endpoint, cfg.credentials, cfg.options)
        st.info = st.plugin._run(lambda c: c.get_info())
        return (f"{st.info.vendor} model={st.info.model or '-'} version={st.info.version or '-'} "
                f"serial={st.info.serial or '-'}")

    runner.run("api.login", "log in; options and protocol accepted", login)

    def capabilities() -> str:
        caps = st.plugin._client().capabilities
        st.caps = caps
        protos = ", ".join(sorted(p.value for p in caps.protocols))
        return (f"protocols {protos}; size granularity {caps.size_granularity_bytes} B; "
                f"names <= {caps.max_name_len}; grow only {caps.grow_only}")

    runner.run("api.capabilities", "plug-in capabilities for this array", capabilities, needs=("api.login",))

    if node_utils is not None:
        def initiator() -> str:
            got = node_utils.get_san_initiators()
            key = _INITIATOR_KEYS[proto]
            if not got.get(key):
                raise Fail(f"this host has no {key} (found: {', '.join(got) or 'none'})",
                           "see the host preparation table of the deployment preparation docs")
            cfg.initiators = {key: got[key]}
            return f"{key} = {got[key]}"

        runner.run("host.initiator", f"host has a {proto.value} initiator identity", initiator)

        if cfg.portals:
            def portals() -> str:
                bad = []
                for p in cfg.portals:
                    ip, _, port = p.rpartition(":")
                    why = H.tcp_reachable(ip, int(port))
                    if why:
                        bad.append(f"{p}: {why}")
                if bad:
                    raise Fail("; ".join(bad), "the data network does not reach these array ports")
                return f"{len(cfg.portals)} data port(s) answer"

            runner.run("host.data_ports", "array data ports reachable", portals)

        if proto.value == "iscsi":
            def sessions() -> str:
                have = H.iscsi_sessions()
                if not have:
                    raise Fail("no iSCSI session is logged in",
                               "log in to the array's portals (one session per path), node.startup automatic")
                missing = [p for p in cfg.portals if p not in have]
                if missing:
                    raise Warn(f"sessions to {', '.join(have)}; none to {', '.join(missing)}")
                if len(have) < 2:
                    raise Warn(f"one session only ({have[0]}): a single path to every LUN")
                return f"sessions to {', '.join(have)}"

            runner.run("host.sessions", "iSCSI sessions logged in", sessions)
        elif proto.is_nvme:
            def discovery() -> str:
                conf = H.host_file("/etc/nvme/discovery.conf")
                entries = [ln.split("#", 1)[0].strip() for ln in conf.splitlines()]
                entries = [e for e in entries if e]
                if not entries:
                    raise Fail("/etc/nvme/discovery.conf has no entries",
                               "one line per discovery address of the array, e.g. -t tcp -a 10.0.0.5 -s 8009")
                bad = []
                for e in entries:
                    rc, out = H.sh("nvme", "discover", *e.split())
                    if rc != 0:
                        bad.append(f"{e}: {out.splitlines()[-1] if out else rc}")
                if bad:
                    raise Fail("; ".join(bad))
                return f"{len(entries)} discovery entr{'y' if len(entries) == 1 else 'ies'} answer"

            runner.run("host.nvme_discovery", "NVMe discovery entries answer", discovery)

        if scsi:
            def multipath() -> str:
                s = H.multipath_settings()
                if s is None:
                    raise Fail("multipathd is not running", "SCSI LUNs are used through dm-multipath")
                retry = s.get("overrides.no_path_retry") or s.get("defaults.no_path_retry")
                poll = s.get("defaults.polling_interval") or s.get("overrides.polling_interval") or "5"
                if retry is None:
                    raise Warn("no_path_retry is not set in defaults or overrides: the array's built-in device "
                               "entry decides, and some queue I/O without limit (NetApp: queue_if_no_path)",
                               "set overrides { no_path_retry 6 } (see the deployment preparation docs)")
                if retry == "queue":
                    raise Fail("no_path_retry queue: a fenced LUN's I/O is held for good",
                               "set overrides { no_path_retry 6 }")
                if retry.isdigit() and poll.isdigit() and int(retry) * int(poll) >= 60:
                    raise Warn(f"no_path_retry {retry} x polling_interval {poll} >= 60 s")
                return f"no_path_retry {retry}, polling_interval {poll}"

            runner.run("host.multipath", "multipath fails I/O within 60 s", multipath)

    if not cfg.write:
        return st

    # --------------------------------------------------------------- writes

    plugin = st.plugin
    initiators = cfg.initiators
    w = ("api.login",) + (("host.initiator",) if node_utils is not None else ())

    def host_object() -> str:
        try:
            st.host_id = plugin.ensure_host(cfg.host_name, initiators)
        except Exception as e:
            if "already" in str(e).lower() or "conflict" in type(e).__name__.lower():
                raise Fail(str(e), "this host's initiator belongs to another host object on the array (of a "
                                   "running simplyblock cluster?): the write checks need it free") from e
            raise
        st.host_created = True
        again = plugin.ensure_host(cfg.host_name, initiators)
        if again != st.host_id:
            raise Fail(f"a repeat returned {again}, not {st.host_id}")
        return f"host object {st.host_id}"

    runner.run("lun.host_object", "create the host object (repeat-safe)", host_object, needs=w)

    def create() -> str:
        t = time.monotonic()
        lun = plugin.create_lun(DATA_LUN, cfg.size_gib * GIB, {})
        st.luns[DATA_LUN] = lun
        took = time.monotonic() - t
        again = plugin.create_lun(DATA_LUN, cfg.size_gib * GIB, {})
        if again["id"] != lun["id"]:
            raise Fail(f"a repeated create made a second LUN ({again['id']}): retries would leak LUNs")
        v = plugin.get(lun["id"])
        if v["size"] < cfg.size_gib * GIB:
            raise Fail(f"the array reports {v['size']} bytes, asked {cfg.size_gib * GIB}")
        if v["hosts"]:
            raise Fail(f"a new LUN is presented to {v['hosts']}")
        st.size = v["size"]
        step = max(GIB, st.caps.size_granularity_bytes if st.caps else 0)
        st.grow_to = -(-(st.size + step) // step) * step
        return f"{v['name']} id={lun['id']} wwn={lun['wwn']} {v['size']} B in {took:.1f} s"

    runner.run("lun.create", "create a LUN (repeat-safe)", create, needs=w)

    def attach() -> str:
        lun = st.luns[DATA_LUN]
        plugin.attach(lun["id"], st.host_id)
        plugin.attach(lun["id"], st.host_id)
        hosts = plugin.get(lun["id"])["hosts"]
        if hosts != [st.host_id]:
            raise Fail(f"the array reports hosts {hosts}, expected [{st.host_id}]")
        return "presented to this host (a repeated attach is accepted)"

    runner.run("lun.attach", "present the LUN to this host", attach, needs=("lun.host_object", "lun.create"))

    def find_devices(wwn: str) -> list[dict]:
        deadline = time.monotonic() + cfg.timeout_s
        while True:
            node_utils.scan_for_new_luns()
            if proto.value == "iscsi":
                H.sh("iscsiadm", "-m", "session", "--rescan")
            H.sh("udevadm", "settle")
            found = node_utils.find_lun_devices(wwn)
            if found or time.monotonic() >= deadline:
                return found
            time.sleep(2)

    if node_utils is not None:
        def host_sees() -> str:
            lun = st.luns[DATA_LUN]
            t = time.monotonic()
            st.devs = find_devices(lun["wwn"])
            if not st.devs:
                seen = [f"{d.get('name')}={d.get('wwn') or d.get('serial')}"
                        for d in node_utils.get_block_devices_info() if d.get("type") in ("disk", "mpath")]
                raise Fail(f"no device with identity {lun['wwn']} after {cfg.timeout_s:.0f} s; the host has "
                           f"{', '.join(seen) or 'no disks'}",
                           "an identity the plug-in does not expect means a plug-in fix: send the bundle")
            st.dev = st.devs[0]["device_path"]
            size = H.size_of(st.dev)
            if size != plugin.get(lun["id"])["size"]:
                raise Fail(f"the host sees {size} B, the array reports {plugin.get(lun['id'])['size']} B")
            st.data = H.write_pattern(st.dev)
            if H.read_pattern(st.dev) != st.data:
                raise Fail("data written does not read back")
            names = ", ".join(d["name"] for d in st.devs)
            return f"{names} in {time.monotonic() - t:.1f} s; 4 MiB written and read back"

        runner.run("lun.host_sees", "the host finds the LUN and does I/O", host_sees, needs=("lun.attach",))

    def grow() -> str:
        lun = st.luns[DATA_LUN]
        t = time.monotonic()
        plugin.resize(lun["id"], st.grow_to)
        size = plugin.get(lun["id"])["size"]
        if size < st.grow_to:
            raise Fail(f"the array reports {size} B after growing to {st.grow_to}")
        detail = f"array {size} B in {time.monotonic() - t:.1f} s"
        if node_utils is not None and st.dev:
            deadline = time.monotonic() + cfg.timeout_s
            seen = 0
            while time.monotonic() < deadline:
                if proto.value == "iscsi":
                    H.sh("iscsiadm", "-m", "session", "--rescan")
                seen = node_utils.rescan_block_device(st.dev)["size"]
                if seen >= st.grow_to:
                    break
                time.sleep(2)
            if seen < st.grow_to:
                raise Fail(f"the host still sees {seen} B after {cfg.timeout_s:.0f} s")
            if H.read_pattern(st.dev) != st.data:
                raise Fail("the data changed with the growth")
            detail += f"; host {seen} B, data intact"
        return detail

    runner.run("lun.grow", "grow the LUN online", grow, needs=("lun.create",))

    def same_and_shrink() -> str:
        lun = st.luns[DATA_LUN]
        before = plugin.get(lun["id"])["size"]
        plugin.resize(lun["id"], before)
        try:
            plugin.resize(lun["id"], st.size)
        except Exception as e:  # noqa: BLE001
            refused = str(e)
        else:
            refused = ""
        after = plugin.get(lun["id"])["size"]
        if not refused or after != before:
            raise Fail(f"a shrink was not refused (size {before} -> {after})")
        return "same size accepted; shrink refused"

    runner.run("lun.resize_edges", "resize to the same size; shrink refused", same_and_shrink,
               needs=("lun.grow",))

    def lost_io(what: str) -> str:
        """The host's I/O to the LUN fails within the deadline."""
        assert st.dev is not None
        how, val = H.read_within(st.dev, IO_DEADLINE_S)
        if how == "error":
            return f"host I/O fails ({str(val)[:60]})"
        if how == "hung":
            mpath = next((d["name"] for d in st.devs if d.get("type") == "mpath"), None)
            if mpath:
                H.release_queued_io(mpath)
            raise Fail(f"host I/O still queued {IO_DEADLINE_S:.0f} s after the {what}",
                       "multipath queues I/O with no path left: set overrides { no_path_retry 6 }")
        raise Fail(f"the host still reads the LUN after the {what}")

    def fence() -> str:
        lun = st.luns[DATA_LUN]
        t = time.monotonic()
        plugin.fence(lun["id"], st.host_id)
        took = time.monotonic() - t
        if st.host_id in plugin.get(lun["id"])["hosts"]:
            raise Fail("the array still presents the LUN to the fenced host")
        detail = f"applied in {took:.1f} s"
        if node_utils is not None and st.dev:
            detail += "; " + lost_io("fence")
        return detail

    runner.run("lun.fence", "fence this host from the LUN", fence, needs=("lun.attach",))

    def unfence() -> str:
        lun = st.luns[DATA_LUN]
        plugin.unfence(lun["id"], st.host_id)
        if st.host_id not in plugin.get(lun["id"])["hosts"]:
            raise Fail("the array does not present the LUN again")
        if node_utils is not None and st.dev:
            H.drop_devices(st.devs)
            st.devs = find_devices(lun["wwn"])
            if not st.devs:
                raise Fail("the host does not find the LUN again")
            st.dev = st.devs[0]["device_path"]
            if H.read_pattern(st.dev) != st.data:
                raise Fail("the data read after the unfence differs")
            return f"host reads the data again ({st.devs[0]['name']})"
        return "presented again"

    runner.run("lun.unfence", "unfence: access and data are back", unfence, needs=("lun.fence",))

    def detach() -> str:
        lun = st.luns[DATA_LUN]
        plugin.detach(lun["id"], st.host_id)
        if plugin.get(lun["id"])["hosts"]:
            raise Fail("the array still presents the LUN")
        detail = "no host"
        if node_utils is not None and st.dev:
            detail += "; " + lost_io("detach")
            H.drop_devices(st.devs)
        plugin.detach(lun["id"], st.host_id)
        return detail + "; a repeated detach is accepted"

    runner.run("lun.detach", "detach the LUN", detach, needs=("lun.attach",))

    def delete() -> str:
        lun = st.luns[DATA_LUN]
        t = time.monotonic()
        plugin.delete_lun(lun["id"])
        try:
            plugin.get(lun["id"])
        except Exception as e:  # noqa: BLE001
            if "not found" not in str(e):
                raise Fail(f"after the delete, get failed with {e}") from e
        else:
            raise Fail("the LUN is still there after the delete")
        took = time.monotonic() - t
        plugin.delete_lun(lun["id"])
        st.luns.pop(DATA_LUN)
        return f"gone in {took:.1f} s; a repeated delete is accepted"

    runner.run("lun.delete", "delete the LUN", delete, needs=("lun.create",))

    if cfg.journal_options:
        def journal() -> str:
            opts = {**cfg.options, **cfg.journal_options}
            jp = make_plugin(opts)
            jp.connect(cfg.endpoint, cfg.credentials, opts)
            lun = jp.create_lun(JOURNAL_LUN, cfg.size_gib * GIB, {})
            st.luns[JOURNAL_LUN] = lun
            v = jp.get(lun["id"])
            jp.delete_lun(lun["id"])
            st.luns.pop(JOURNAL_LUN)
            return f"created and deleted with {cfg.journal_options}: {v['name']} {v['size']} B"

        runner.run("lun.journal_pool", "a LUN with the journal options", journal, needs=w)

    def host_removed() -> str:
        plugin._run(lambda c: c.delete_host(st.host_id))
        st.host_created = False
        return f"host object {st.host_id} removed"

    runner.run("lun.host_removed", "remove the host object", host_removed, needs=("lun.host_object",))
    return st


def cleanup(st: State, cfg: Config, make_plugin: Callable[[dict], Any], echo: Callable[[str], None] = print) -> None:
    """Remove what the run created and still exists (after a failed check)."""
    plugin = st.plugin
    if plugin is None:
        return
    for name, lun in list(st.luns.items()):
        try:
            p = plugin
            if name == JOURNAL_LUN:
                opts = {**cfg.options, **cfg.journal_options}
                p = make_plugin(opts)
                p.connect(cfg.endpoint, cfg.credentials, opts)
            p.delete_lun(lun["id"])
            echo(f"cleanup: deleted LUN {lun['id']}")
        except Exception as e:  # noqa: BLE001
            echo(f"cleanup: LUN {lun['id']}: {e}")
    if st.devs:
        H.drop_devices(st.devs)
    if st.host_created and st.host_id:
        try:
            plugin._run(lambda c: c.delete_host(st.host_id))
            echo(f"cleanup: removed host object {st.host_id}")
        except Exception as e:  # noqa: BLE001
            echo(f"cleanup: host object {st.host_id}: {e}")


def cleanup_run(cfg: Config, make_plugin: Callable[[dict], Any], echo: Callable[[str], None] = print) -> int:
    """Remove the LUNs and host object of an earlier run (by its run id)."""
    from ...array.san import lun_uuid
    plugin = make_plugin(cfg.options)
    plugin.connect(cfg.endpoint, cfg.credentials, cfg.options)
    found = 0
    for name in (DATA_LUN, JOURNAL_LUN):
        vol = plugin._run(lambda c, n=name: c.find_volume(lun_uuid(cfg.run_id, n)))
        if vol is not None:
            plugin.delete_lun(vol.id)
            found += 1
            echo(f"deleted {name}: {vol.name} ({vol.id})")
    if cfg.initiators:
        try:
            hid = plugin.ensure_host(cfg.host_name, cfg.initiators)
            plugin._run(lambda c: c.delete_host(hid))
            echo(f"removed host object {hid}")
        except Exception as e:  # noqa: BLE001
            echo(f"host object: {e}")
    echo(f"run {cfg.run_id}: {found} LUN(s) removed")
    return 0


def default_host_name() -> str:
    return f"check-{socket.gethostname()}"
