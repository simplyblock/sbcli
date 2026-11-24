"""Command line of the array pre-check; see the package docstring."""
from __future__ import annotations

import argparse
import datetime
import getpass
import json
import os
import platform
import secrets as _secrets
import shutil
import sys
import tarfile
import tempfile
from typing import Any

from . import checks as C
from .trace import RecordingAdapter, Redactor, ReplayAdapter

TOOL = "simplyblock array pre-check"
REPORT_VERSION = 1


def _version() -> str:
    try:
        from simplyblock_core._version import __dist_version__
        return str(__dist_version__)
    except Exception:  # noqa: BLE001
        return "unknown"


def _kv(items: list[str]) -> dict[str, str]:
    out = {}
    for item in items:
        if "=" not in item:
            raise SystemExit(f"expected KEY=VALUE, got {item!r}")
        k, v = item.split("=", 1)
        out[k.strip()] = v
    return out


def _credentials(vendor: str, args: argparse.Namespace) -> dict[str, str]:
    """SB_ARRAY_<FIELD> variables, a KEY=VALUE file, or a prompt."""
    from ..registry import adapter_class
    shapes = adapter_class(vendor).credential_fields
    creds = {k[len("SB_ARRAY_"):].lower(): v for k, v in os.environ.items() if k.startswith("SB_ARRAY_")}
    if args.credentials_file:
        with open(args.credentials_file) as f:
            creds.update(_kv([ln.strip() for ln in f if ln.strip() and not ln.lstrip().startswith("#")]))
    if any(shape <= set(creds) for shape in shapes):
        return creds
    if not sys.stdin.isatty():
        wanted = " or ".join("+".join(sorted(s)) for s in shapes)
        raise SystemExit(f"credentials needed: {wanted} (SB_ARRAY_<FIELD> variables or --credentials-file)")
    shape = min(shapes, key=len)
    for name in sorted(shape):
        if name not in creds:
            prompt = f"{vendor} {name}: "
            creds[name] = getpass.getpass(prompt) if ("pass" in name or "token" in name or "secret" in name) \
                else input(prompt)
    return creds


def _add_connection_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--vendor", required=True, help="array plug-in, e.g. netapp-ontap, pure-flasharray")
    p.add_argument("--endpoint", required=True, help="management URL, e.g. https://10.0.0.10")
    p.add_argument("--protocol", required=True, help="iscsi, fc, nvme-tcp, nvme-rdma, nvme-fc or sdc")
    p.add_argument("--option", action="append", default=[], metavar="KEY=VALUE",
                   help="plug-in option, as in the StorageCluster's multiAttach.array.options (repeatable)")
    p.add_argument("--ca-bundle", metavar="FILE", help="CA certificate(s) of the array's management endpoint")
    p.add_argument("--no-verify-tls", action="store_true", help="do not verify the endpoint's certificate")
    p.add_argument("--credentials-file", metavar="FILE",
                   help="KEY=VALUE lines (else SB_ARRAY_<FIELD> variables, else a prompt)")
    p.add_argument("--host-name", default=C.default_host_name(), help="name of the host object the run creates")


def _config(args: argparse.Namespace, run_id: str) -> C.Config:
    from ..registry import canonical_vendor
    vendor = canonical_vendor(args.vendor)
    options: dict[str, Any] = _kv(args.option)
    options["protocol"] = args.protocol
    if args.ca_bundle:
        with open(args.ca_bundle) as f:
            options["ca_bundle"] = f.read()
    if args.no_verify_tls:
        options["verify_tls"] = False
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    return C.Config(
        vendor=vendor, endpoint=args.endpoint, protocol=args.protocol, options=options,
        credentials=_credentials(vendor, args), run_id=run_id, host_name=args.host_name,
        journal_options=_kv(getattr(args, "journal_option", []) or []),
        portals=getattr(args, "portal", []) or [], write=getattr(args, "write", False),
        size_gib=getattr(args, "size_gib", 1), grow_gib=getattr(args, "size_gib", 1) + 1,
        timeout_s=getattr(args, "timeout", 90.0), ca_file=args.ca_bundle, verify_tls=not args.no_verify_tls)


def _plugin_factory(cfg: C.Config, transport: Any, session_kwargs: dict | None = None):
    from ...array import plugin_class

    def make(options: dict) -> Any:
        p: Any = plugin_class(cfg.vendor)(endpoint=cfg.endpoint, options=options, cluster_id=cfg.run_id)
        p.transport = transport
        if session_kwargs:
            p.session_kwargs = session_kwargs
        return p
    return make


def _public_options(options: dict[str, Any], redactor: Redactor) -> dict[str, Any]:
    out = dict(redactor.body(options))
    if "ca_bundle" in out:
        out["ca_bundle"] = "(given)"
    return out


def _write_report_text(path: str, meta: dict, results: list[dict], summary: dict) -> None:
    with open(path, "w") as f:
        f.write(f"{TOOL} {meta['tool_version']}  run {meta['run_id']}  {meta['started']}\n")
        f.write(f"array {meta['vendor']} {meta['endpoint']} ({meta['protocol']}): {meta.get('array') or '-'}\n")
        f.write(f"host {meta['host']}  write checks: {'yes' if meta['write'] else 'no'}\n\n")
        for r in results:
            f.write(f"{r['status']:<4} {r['id']:<22} {r['title']}  ({r['seconds']} s)\n")
            if r["detail"]:
                f.write(f"       {r['detail']}\n")
            if r["hint"]:
                f.write(f"       hint: {r['hint']}\n")
        f.write("\n" + ", ".join(f"{v} {k}" for k, v in sorted(summary.items())) + "\n")


def cmd_check(args: argparse.Namespace) -> int:
    if args.write and (sys.platform != "linux" or os.geteuid() != 0):
        print("--write runs as root on the Linux host that is checked", file=sys.stderr)
        return 2
    started = datetime.datetime.now(datetime.UTC)
    run_id = f"check-{started:%Y%m%d%H%M%S}-{_secrets.token_hex(2)}"
    cfg = _config(args, run_id)
    redactor = Redactor(list(cfg.credentials.values()))
    recorder = RecordingAdapter(redactor)
    make = _plugin_factory(cfg, recorder)
    node_utils = None
    if sys.platform == "linux" and not args.no_host:
        from simplyblock_web import node_utils as _nu
        node_utils = _nu

    print(f"{TOOL} {_version()}: run {run_id}"
          + ("" if args.write else " (read-only; --write also checks LUN create/attach/grow/fence/delete)"))
    runner = C.Runner()
    st = C.State()
    try:
        st = C.run_checks(cfg, make, node_utils, runner)
    except KeyboardInterrupt:
        print("interrupted")
    finally:
        if args.write:
            C.cleanup(st, cfg, make)

    summary = runner.summary()
    meta = {
        "report_version": REPORT_VERSION, "tool": TOOL, "tool_version": _version(),
        "run_id": run_id, "started": started.isoformat(timespec="seconds"),
        "vendor": cfg.vendor, "endpoint": cfg.endpoint, "protocol": cfg.protocol,
        "options": _public_options(cfg.options, redactor),
        "journal_options": _public_options(cfg.journal_options, redactor),
        "credential_fields": sorted(cfg.credentials), "portals": cfg.portals,
        "host": platform.node(), "host_name": cfg.host_name, "initiators": cfg.initiators,
        "write": cfg.write, "size_gib": cfg.size_gib, "python": platform.python_version(),
        "array": (f"{st.info.vendor} {st.info.model} {st.info.version} {st.info.serial}" if st.info else None),
    }
    results = runner.as_dicts()

    name = f"array-check-{cfg.vendor}-{run_id}"
    work = tempfile.mkdtemp(prefix="sb-array-check-")
    root = os.path.join(work, name)
    os.makedirs(root)
    with open(os.path.join(root, "report.json"), "w") as f:
        json.dump(redactor.scrub({"meta": meta, "summary": summary, "results": results}), f, indent=2)
    _write_report_text(os.path.join(root, "report.txt"), redactor.scrub(meta), redactor.scrub(results), summary)
    with open(os.path.join(root, "trace.jsonl"), "w") as f:
        f.writelines(json.dumps(e) + "\n" for e in recorder.export())
    if node_utils is not None:
        H_dir = os.path.join(root, "host")
        from .host import collect_facts
        collect_facts(H_dir)
    os.makedirs(args.out, exist_ok=True)
    bundle = os.path.join(args.out, name + ".tar.gz")
    with tarfile.open(bundle, "w:gz") as tar:
        tar.add(root, arcname=name)
    shutil.rmtree(work, ignore_errors=True)

    print("\n" + ", ".join(f"{v} {k}" for k, v in sorted(summary.items())))
    print(f"bundle: {bundle}")
    print("It holds the report, the array's REST requests and responses (credentials, tokens and cookies "
          "removed) and the host's SAN setup; send it to simplyblock support.")
    return 1 if summary.get("FAIL") else 0


def cmd_cleanup(args: argparse.Namespace) -> int:
    cfg = _config(args, args.run_id)
    if sys.platform == "linux":
        from simplyblock_web import node_utils

        from ...array.san import _INITIATOR_KEYS
        from .. import base as sa
        key = _INITIATOR_KEYS[sa.Protocol(cfg.protocol)]
        got = node_utils.get_san_initiators()
        if got.get(key):
            cfg.initiators = {key: got[key]}
    return C.cleanup_run(cfg, _plugin_factory(cfg, None))


def load_bundle(path: str) -> tuple[dict, list[dict]]:
    with tarfile.open(path) as tar:
        names = tar.getnames()
        report_name = next(n for n in names if n.endswith("/report.json"))
        trace_name = next(n for n in names if n.endswith("/trace.jsonl"))
        report = json.load(tar.extractfile(report_name))  # type: ignore[arg-type]
        trace_file = tar.extractfile(trace_name)
        entries = [json.loads(ln) for ln in trace_file.read().decode().splitlines() if ln.strip()]  # type: ignore[union-attr]
    return report, entries


def replay(path: str, echo=print) -> tuple[C.Runner, ReplayAdapter, dict]:
    """Re-run a bundle's array checks against its recorded responses (no
    array, no host checks)."""
    from ..registry import adapter_class
    report, entries = load_bundle(path)
    meta = report["meta"]
    shape = min(adapter_class(meta["vendor"]).credential_fields, key=len)
    options = {k: v for k, v in meta["options"].items() if k != "ca_bundle"}
    cfg = C.Config(
        vendor=meta["vendor"], endpoint=meta["endpoint"], protocol=meta["protocol"], options=options,
        credentials={k: "replay" for k in (meta.get("credential_fields") or shape)}, run_id=meta["run_id"],
        host_name=meta["host_name"], journal_options=meta.get("journal_options") or {},
        write=meta["write"], size_gib=meta["size_gib"], grow_gib=meta["size_gib"] + 1,
        initiators=meta.get("initiators") or {}, verify_tls=False)
    cfg.options["verify_tls"] = False
    adapter = ReplayAdapter(entries)
    runner = C.Runner(echo=echo)
    make = _plugin_factory(cfg, adapter, {"sleep": lambda s: None})
    C.run_checks(cfg, make, None, runner, live=False)
    return runner, adapter, report


def cmd_replay(args: argparse.Namespace) -> int:
    runner, adapter, report = replay(args.bundle)
    recorded = {r["id"]: r["status"] for r in report["results"]}
    changed = [(r.id, recorded.get(r.id), r.status) for r in runner.results
               if r.id in recorded and recorded[r.id] != r.status]
    print()
    for cid, was, now in changed:
        print(f"changed: {cid} {was} -> {now}")
    if adapter.unmatched:
        print(f"{len(adapter.unmatched)} request(s) not in the trace (the plug-in now asks differently):")
        for u in adapter.unmatched[:20]:
            print(f"  {u}")
    return 1 if runner.summary().get("FAIL") else 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m simplyblock_core.storage_arrays.preflight",
                                     description="Pre-check a storage array for a simplyblock Unified DataStore.")
    sub = parser.add_subparsers(dest="cmd", required=True)

    c = sub.add_parser("check", help="run the checks and write a bundle")
    _add_connection_args(c)
    c.add_argument("--portal", action="append", default=[], metavar="IP:PORT",
                   help="array data port to test (iSCSI portal, NVMe discovery address; repeatable)")
    c.add_argument("--write", action="store_true",
                   help="also create, attach, grow, fence and delete one small LUN (needs root on this host)")
    c.add_argument("--journal-option", action="append", default=[], metavar="KEY=VALUE",
                   help="with --write: also create a LUN with these journal options (multiAttach.array.journal)")
    c.add_argument("--size-gib", type=int, default=1, help="size of the test LUN (grown by 1 GiB)")
    c.add_argument("--timeout", type=float, default=90.0, help="seconds the host may take to see a change")
    c.add_argument("--no-host", action="store_true", help="array checks only, no host checks")
    c.add_argument("--out", default=".", help="directory for the bundle")
    c.set_defaults(fn=cmd_check)

    k = sub.add_parser("cleanup", help="remove the LUNs and host object an interrupted run left")
    _add_connection_args(k)
    k.add_argument("--run-id", required=True)
    k.set_defaults(fn=cmd_cleanup)

    r = sub.add_parser("replay", help="re-run a bundle's array checks against its recorded responses")
    r.add_argument("bundle")
    r.set_defaults(fn=cmd_replay)

    args = parser.parse_args(argv)
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())
