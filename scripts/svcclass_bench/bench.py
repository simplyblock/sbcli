#!/usr/bin/env python3
"""Replication service-class benchmark: client IO with and without replication.

Two scenarios, each run at several replication shares (the percentage of IO
scheduling the replication service class gets):

  a) client IO only: the mixed fio load (mixed.fio) on one volume per source
     storage node, 6 jobs at iodepth 16 per volume;
  b) the same load, plus replication of every volume into the target cluster at
     a 1-minute cadence, until replication reaches steady state and then for a
     measured window in it.

Measured: client IOPS and MB/s (fio per-second logs, summed over all jobs), and
for (b) the time to steady state and the replication time per iteration before
and in it (analysis.py).

    python bench.py prepare                       # volumes, connect, precondition
    python bench.py run --scenario b --share 15   # one run
    python bench.py matrix --shares 15,30,60      # a and b for every share
    python bench.py report                        # tables over every run

Reads cluster_metadata_repl.json (written by deploy.py) from the scripts
directory; writes everything under results/ next to this file.
"""
import argparse
import json
import shlex
import sys
import threading
import time
from pathlib import Path

import paramiko

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import analysis  # noqa: E402

META = HERE.parent / "cluster_metadata_repl.json"
RESULTS = HERE / "results"
STATE = RESULTS / "state.json"
REMOTE = "/tmp/svcclass_bench"
SBCTL = "sudo /usr/local/bin/sbctl"


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


# --------------------------------------------------------------------------- #
# hosts
# --------------------------------------------------------------------------- #

class Host:
    """One SSH session to a host, re-established when the network drops it.

    Runs last hours, and a session reset (WinError 10054) is routine on this
    path. A command whose channel could not be opened never ran, so it is
    always retried on a fresh connection; one that was reset while its output
    was being read may have run, so it is retried only when the caller says
    it is safe to repeat (idempotent=True: reads, polls).
    """

    def __init__(self, ip, user, key):
        self.ip, self.user, self.key = ip, user, key
        self.c = None
        self._connect()

    def _connect(self):
        if self.c is not None:
            self.c.close()
        self.c = paramiko.SSHClient()
        self.c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        for attempt in range(10):
            try:
                self.c.connect(self.ip, username=self.user, key_filename=self.key, timeout=30)
                self.c.get_transport().set_keepalive(30)
                return
            except Exception:                              # noqa: BLE001
                if attempt == 9:
                    raise
                time.sleep(10)

    def run(self, cmd, check=True, timeout=3600, idempotent=False):
        for attempt in range(5):
            try:
                transport = self.c.get_transport() if self.c else None
                if transport is None or not transport.is_active():
                    # a reconnect that failed leaves a client without a
                    # transport, which exec_command reports as AttributeError
                    raise paramiko.SSHException("no active session")
                _, out, err = self.c.exec_command(cmd, timeout=timeout)
            except (paramiko.SSHException, OSError, EOFError) as e:
                log(f"{self.ip}: reconnecting ({e})")
                try:
                    self._connect()
                except Exception as ce:                    # noqa: BLE001
                    log(f"{self.ip}: reconnect failed ({ce}); retrying")
                    time.sleep(30)
                continue
            try:
                # read before waiting for the exit: an output larger than the
                # channel window otherwise blocks the remote command forever
                o, e = out.read().decode(errors="replace"), err.read().decode(errors="replace")
                rc = out.channel.recv_exit_status()
            except (paramiko.SSHException, OSError, EOFError) as exc:
                if not idempotent or attempt == 4:
                    raise
                log(f"{self.ip}: reconnecting after a reset mid-command ({exc})")
                self._connect()
                continue
            if check and rc != 0:
                raise RuntimeError(f"{self.ip}: `{cmd}` rc={rc}\n{o}\n{e}")
            return o
        raise RuntimeError(f"{self.ip}: could not run `{cmd}` after 5 connections")

    def put(self, local, remote):
        self.run(f"mkdir -p {shlex.quote(str(Path(remote).parent.as_posix()))}")
        with self.c.open_sftp() as s:
            s.put(str(local), remote)

    def get(self, remote, local):
        Path(local).parent.mkdir(parents=True, exist_ok=True)
        with self.c.open_sftp() as s:
            s.get(remote, str(local))


class Bed:
    def __init__(self):
        self.meta = json.loads(META.read_text())
        user, key = self.meta["user"], self.meta["key_path"]
        self.mgmt = Host(self.meta["mgmt"]["public_ip"], user, key)
        self.clients = [Host(c["public_ip"], user, key) for c in self.meta["clients"]]
        self.src = self.meta["clusters"]["src"]
        self.tgt = self.meta["clusters"]["tgt"]
        self.mgmt.put(HERE / "agent.py", f"{REMOTE}/agent.py")

    def agent(self, args, check=True):
        # the interpreter sbctl runs under is the one that has sbcli; every
        # agent command but qos-set only reads, and qos-set is declarative
        out = self.mgmt.run(f"sudo $(head -1 /usr/local/bin/sbctl | sed 's/^#!//') "
                            f"{REMOTE}/agent.py {args}", check=check, idempotent=True)
        return json.loads(out.strip().splitlines()[-1]) if out.strip() else None

    def sbctl(self, args):
        return self.mgmt.run(f"{SBCTL} {args}")


def load_state():
    return json.loads(STATE.read_text())


# --------------------------------------------------------------------------- #
# prepare
# --------------------------------------------------------------------------- #

_DEVICE_BY_NQN = r"""
for s in /sys/class/nvme-subsystem/nvme-subsys*; do
  [ "$(cat $s/subsysnqn 2>/dev/null)" = "%s" ] && ls $s | grep -E '^nvme[0-9]+n[0-9]+$' | head -1
done
"""


def _busy(client):
    return client.run("pgrep -x fio >/dev/null && echo busy || echo done",
                      idempotent=True).strip() == "busy"


def _wait_idle(bed, vols):
    """Until no fio runs on any client: a precondition still writing would be
    measured as client load."""
    for v in vols.values():
        c = bed.clients[v["client"]]
        while _busy(c):
            time.sleep(30)


def prepare(args):
    """One volume per source storage node, each connected on its own client and
    filled once, so every run starts from the same written state.

    Resumable: with a state file present, the volumes exist and are being or
    have been filled, so it only waits for the fill to end."""
    bed = Bed()
    if STATE.exists() and load_state().get("cluster") != bed.src["cluster_uuid"]:
        # the volumes of an earlier bed: this one has none of them
        stale = STATE.with_name(f"state-{int(time.time())}.json")
        STATE.rename(stale)
        log(f"state file belongs to another cluster; moved to {stale.name}")
    if STATE.exists():
        log("volumes already prepared; waiting for the preconditioning to finish")
        _wait_idle(bed, load_state()["volumes"])
        log("prepared")
        return
    out = bed.sbctl(f"sn list --cluster-id {bed.src['cluster_uuid']} --json")
    nodes = [n["UUID"] for n in json.loads(out) if n.get("Status") == "online"]
    if len(bed.clients) < len(nodes):
        sys.exit(f"{len(nodes)} source nodes but only {len(bed.clients)} clients")
    vols = {}
    for i, node in enumerate(nodes):
        name = f"bench{i}"
        out = bed.sbctl(f"volume add {name} {args.size} {bed.src['pool']} --host-id {node}")
        vid = out.strip().splitlines()[-1].strip()
        log(f"volume {name} = {vid} on node {node}")
        vols[vid] = {"name": name, "node": node, "client": i}
    info = bed.agent(f"volume-info --volumes {','.join(vols)}")
    for vid, v in vols.items():
        client = bed.clients[v["client"]]
        for line in bed.sbctl(f"volume connect {vid}").splitlines():
            if line.strip().startswith(("sudo nvme connect", "nvme connect")):
                client.run(line if line.startswith("sudo") else f"sudo {line}")
        nqn = info[vid]["nqn"]
        dev = ""
        for _ in range(30):
            dev = client.run("bash -c " + shlex.quote(_DEVICE_BY_NQN % nqn), check=False).strip()
            if dev:
                break
            time.sleep(2)
        if not dev:
            sys.exit(f"volume {vid}: no block device for {nqn} on {client.ip}")
        v.update(device=f"/dev/{dev}", nqn=nqn, size=info[vid]["size"])
        log(f"volume {vid}: {client.ip} {v['device']}")
    RESULTS.mkdir(exist_ok=True)
    STATE.write_text(json.dumps({"cluster": bed.src["cluster_uuid"], "volumes": vols}, indent=2))
    log("preconditioning: one sequential pass over every volume")
    for vid, v in vols.items():
        c = bed.clients[v["client"]]
        _guard(c, v)
        c.run(f"sudo setsid nohup fio --name=precondition --filename={v['device']} --rw=write "
              f"--bs=1m --iodepth=32 --ioengine=libaio --direct=1 --output=/dev/null "
              f"> /dev/null 2>&1 < /dev/null &")
    _wait_idle(bed, vols)
    log("prepared")


def _guard(client, vol):
    """Refuse to write a device whose subsystem is not this volume's: a device
    picked by anything weaker than its NQN can be the client's own disk."""
    dev = Path(vol["device"]).name
    out = client.run("bash -c " + shlex.quote(_DEVICE_BY_NQN % vol["nqn"]), check=False).strip()
    if out != dev:
        sys.exit(f"{client.ip}: {vol['device']} is not volume {vol['nqn']} (found {out!r}); not writing")


# --------------------------------------------------------------------------- #
# run
# --------------------------------------------------------------------------- #

def _start_fio(bed, vols, run_dir_remote, runtime, profile="mixed.fio"):
    starts = {}
    for vid, v in vols.items():
        c = bed.clients[v["client"]]
        _guard(c, v)
        c.put(HERE / profile, f"{REMOTE}/{profile}")
        d = f"{run_dir_remote}/{v['name']}"
        c.run(f"sudo rm -rf {d} && sudo mkdir -p {d}")
        # The redirection into the run directory is root's (the directory is
        # root's, and a redirection the user's shell cannot open fails the
        # backgrounded command without a word: a whole run measured no IO).
        # The backgrounding is the user's: a job backgrounded INSIDE `sudo
        # bash -c` runs, but in a session no later command can see, so
        # nothing can tell whether the load is alive.
        inner = (f"cd {d} && DEV={v['device']} RUNTIME={runtime} LOGDIR={d} "
                 f"exec fio {REMOTE}/{profile} --output-format=json --output={d}/fio.json "
                 f"> {d}/fio.out 2>&1")
        starts[vid] = float(c.run(f"sudo setsid nohup sh -c {shlex.quote(inner)} > /dev/null 2>&1 "
                                  f"< /dev/null & date +%s.%N").strip().splitlines()[-1])
    # Prove the load is running before anything is measured against it.
    time.sleep(10)
    for vid, v in vols.items():
        c = bed.clients[v["client"]]
        if not _busy(c):
            out = c.run(f"sudo cat {run_dir_remote}/{v['name']}/fio.out", check=False)
            raise RuntimeError(f"fio is not running on {c.ip} for {v['name']}:\n{out}")
    return starts


def _stop_fio(bed, vols):
    for v in vols.values():
        bed.clients[v["client"]].run("sudo pkill -INT -x fio", check=False)
    for v in vols.values():
        c = bed.clients[v["client"]]
        for _ in range(60):
            if not _busy(c):
                break
            time.sleep(2)


def _fetch_fio(bed, vols, run_dir_remote, run_dir):
    for v in vols.values():
        c = bed.clients[v["client"]]
        tgz = f"{run_dir_remote}-{v['name']}.tgz"
        c.run(f"sudo tar czf {tgz} -C {run_dir_remote} {v['name']} && sudo chmod a+r {tgz}")
        local = run_dir / "fio" / f"{v['name']}.tgz"
        c.get(tgz, local)
        import tarfile
        with tarfile.open(local) as t:
            t.extractall(run_dir / "fio", filter="data")


_MGMT_LOG_SERVICES = ("SnapshotReplication", "SnapshotMonitor", "TasksRunnerReplicationFinal",
                      "TasksRunnerLVolSyncDelete", "LVolMonitor", "StorageNodeMonitor",
                      "HealthCheck", "TasksRunnerRestart", "WebAppAPI")


def _fetch_logs(bed, run_id, run_dir, since):
    """The SPDK and proxy logs of every storage node of both clusters, and the
    control-plane services that replicate, since the client load started.

    bench.py records the phase boundaries (fio_start, t0, steady_epoch,
    t_repl_stop, t_end) in meta.json, so the logs can be cut by phase."""
    iso = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(since))
    user, key = bed.meta["user"], bed.meta["key_path"]
    for side in ("src", "tgt"):
        for ip in bed.meta["clusters"][side]["storage_public_ips"]:
            try:
                h = Host(ip, user, key)
                names = [n for n in h.run("sudo docker ps --format '{{.Names}}'").split()
                         if n.startswith("spdk")]
                for n in names:
                    f = f"{REMOTE}/{run_id}-{n}.log.gz"
                    h.run(f"sudo mkdir -p {REMOTE} && sudo sh -c 'docker logs --timestamps --since {iso} {n} "
                          f"2>&1 | gzip > {f}' && sudo chmod a+r {f}")
                    h.get(f, run_dir / "logs" / side / ip / f"{n}.log.gz")
            except Exception as e:                         # noqa: BLE001
                log(f"WARNING: logs of {side} node {ip}: {e}")
    for svc in _MGMT_LOG_SERVICES:
        f = f"{REMOTE}/{run_id}-{svc}.log.gz"
        try:
            bed.mgmt.run(f"sudo sh -c 'docker service logs --timestamps --since {iso} app_{svc} "
                         f"2>&1 | gzip > {f}' && sudo chmod a+r {f}")
            bed.mgmt.get(f, run_dir / "logs" / "mgmt" / f"{svc}.log.gz")
        except Exception as e:                             # noqa: BLE001
            log(f"WARNING: logs of {svc}: {e}")


def _replicate(bed, vols, policy):
    tgt_name = f"tgt_{bed.tgt['cluster_uuid'][:8]}"
    bed.sbctl(f"cluster replication-policy-add {bed.src['cluster_uuid']} {policy} "
              f"--target {tgt_name} --interval-min 1")
    t0 = time.time()
    for vid in vols:
        bed.sbctl(f"volume replication-policy-set {vid} {policy}")
    return t0


def _unreplicate(bed, vols, policy):
    """Back to the state every run starts from: no replication, no internal
    snapshots on either side.

    Clearing replication deletes the internal snapshots, but not one whose
    transfer is still winding down when the purge runs: a run that ends during
    a full initial copy leaves it behind, and the next run would chain onto
    it. Those are deleted once their tasks have finished.
    """
    ids = ",".join(vols)
    for vid in vols:
        bed.sbctl(f"volume replication-policy-clear {vid}")
    for attempt in range(120):
        recs = bed.agent(f"snapshots --volumes {ids}")
        if not recs:
            break
        if attempt and attempt % 6 == 0:            # once a minute
            # the purge keeps each volume's newest replicated pair as its last
            # recovery point; the benchmark wants none, target copy first
            for r in recs:
                for sid in (r.get("target"), r["snapshot"]):
                    if sid:
                        bed.mgmt.run(f"{SBCTL} snapshot delete {sid}", check=False)
        time.sleep(10)
    else:
        log(f"WARNING: internal snapshots left after clearing replication: {len(recs)}")
    out = bed.mgmt.run(f"{SBCTL} cluster replication-policy-list "
                       f"--cluster-id {bed.src['cluster_uuid']} --json", check=False, idempotent=True)
    for p in json.loads(out[out.find("["):] if "[" in out else "[]"):
        if p.get("Name") == policy:
            bed.sbctl(f"cluster replication-policy-remove {p['ID']}")


def _snapshot_cadence(bed, vols, meta, t0, args):
    """Scenario (c): the snapshots of (b) without the replication.

    Every volume is snapshotted every --snap-interval seconds and only the
    newest --keep are kept, as replication's retention does. What client IO
    loses here is the cost of the snapshots themselves (copy-on-write after
    each one); what (b) loses beyond it is the cost of the transfers.
    """
    meta["snapshots"] = []
    kept = {vid: [] for vid in vols}
    next_at = t0
    while time.time() - t0 < args.measure:
        if time.time() >= next_at:
            for vid, v in vols.items():
                out = bed.sbctl(f"snapshot add {vid} bench-c-{v['name']}-{int(time.time())}")
                rec = {"volume": vid, "snapshot": out.strip().splitlines()[-1].strip(),
                       "t": time.time()}
                meta["snapshots"].append(rec)
                kept[vid].append(rec)
                while len(kept[vid]) > args.keep:
                    old = kept[vid].pop(0)
                    bed.mgmt.run(f"{SBCTL} snapshot delete {old['snapshot']}", check=False)
                    old["deleted"] = time.time()
            log(f"t+{time.time() - t0:.0f}s snapshots taken: {len(meta['snapshots'])}")
            next_at += args.snap_interval
        time.sleep(5)


def _delete_snapshots(bed, ids):
    for sid in ids:
        bed.mgmt.run(f"{SBCTL} snapshot delete {sid}", check=False)


def _parse_ss(out):
    """`ss -tinH state established`: one line per connection (Recv-Q Send-Q
    Local Peer), followed by an indented line of TCP info with the byte
    counters. Returns [{"l": local, "p": peer, "sent": bytes, "recv": bytes}]."""
    conns, cur = [], None
    for line in out.splitlines():
        if not line.strip():
            continue
        if not line[0].isspace():
            parts = line.split()
            if len(parts) >= 4:
                cur = {"l": parts[2], "p": parts[3], "sent": 0, "recv": 0}
                conns.append(cur)
            continue
        if cur is None:
            continue
        for tok in line.split():
            if tok.startswith("bytes_sent:"):
                cur["sent"] = int(tok.split(":", 1)[1])
            elif tok.startswith("bytes_received:"):
                cur["recv"] = int(tok.split(":", 1)[1])
    return conns


def _net_sampler(bed, path, stop, every=60):
    """Every `every` seconds, the TCP connections of every storage node and
    client with their byte counters (telemetry_report.py classifies them by
    port into client, replication and cluster-internal traffic)."""
    user, key = bed.meta["user"], bed.meta["key_path"]
    hosts = {}
    for side in ("src", "tgt"):
        for pub, priv in zip(bed.meta["clusters"][side]["storage_public_ips"],
                             bed.meta["clusters"][side]["storage_private_ips"]):
            hosts[pub] = (side, priv)
    for c in bed.meta["clients"]:
        hosts[c["public_ip"]] = ("client", c["private_ip"])
    conns = {}
    with open(path, "a", encoding="utf-8") as f:
        while not stop.is_set():
            for pub, (role, priv) in hosts.items():
                try:
                    if pub not in conns:
                        conns[pub] = Host(pub, user, key)
                    out = conns[pub].run("ss -tinH state established", check=False, idempotent=True)
                    f.write(json.dumps({"t": time.time(), "host": pub, "ip": priv, "role": role,
                                        "conns": _parse_ss(out)}) + "\n")
                except Exception as e:                     # noqa: BLE001
                    f.write(json.dumps({"t": time.time(), "host": pub, "error": str(e)}) + "\n")
            f.flush()
            stop.wait(every)


def run_one(bed, scenario, share, args):
    state = load_state()
    vols = state["volumes"]
    run_id = f"{time.strftime('%Y%m%d-%H%M%S')}-{scenario}-{share}"
    run_dir = RESULTS / run_id
    run_dir.mkdir(parents=True)
    remote = f"{REMOTE}/runs/{run_id}"
    log(f"=== {run_id}")

    qos = {}
    for name in ("src", "tgt"):
        res = bed.agent(f"qos-set --cluster {bed.meta['clusters'][name]['cluster_uuid']} --share {share}",
                        check=False)
        qos[name] = res
        if not res or res.get("push_failed"):
            sys.exit(f"QoS share {share} did not reach every {name} node: {res}")
    log(f"weights src {qos['src']['config']['weights']}")

    _wait_idle(bed, vols)
    cap = args.warmup + (args.measure if scenario == "a" else args.cap + args.after)
    starts = _start_fio(bed, vols, remote, cap + 120, args.fio)
    net_stop = threading.Event()
    net_thread = threading.Thread(target=_net_sampler, daemon=True,
                                  args=(bed, run_dir / "net.jsonl", net_stop))
    net_thread.start()
    fio_start = min(starts.values())
    meta = {"run_id": run_id, "scenario": scenario, "share": share, "qos": qos,
            "fio": args.fio, "fio_start": fio_start, "fio_starts": starts,
            "warmup": args.warmup, "volumes": vols}
    log(f"warm-up {args.warmup}s")
    time.sleep(args.warmup)
    t0 = time.time()
    meta["t0"] = t0
    summary = None
    try:
        if scenario == "a":
            time.sleep(args.measure)
        elif scenario == "c":
            _snapshot_cadence(bed, vols, meta, t0, args)
        else:
            policy = f"bench-{run_id}"
            meta["policy"] = policy
            stop = f"{REMOTE}/{run_id}.stop"
            out = f"{REMOTE}/{run_id}.jsonl"
            t0 = meta["t0"] = _replicate(bed, vols, policy)
            bed.mgmt.run(f"sudo setsid nohup $(head -1 /usr/local/bin/sbctl | sed 's/^#!//') "
                         f"{REMOTE}/agent.py collect "
                         f"--volumes {','.join(vols)} --out {out} --stop-file {stop} "
                         f"> {REMOTE}/{run_id}.collect.log 2>&1 < /dev/null &")
            steady_epoch = None
            while time.time() - t0 < args.cap:
                time.sleep(60)
                # the collector's history, not the snapshots that exist now:
                # retention keeps only the newest replicated pair per volume.
                # A failed poll only delays the steady-state check; the
                # history keeps being written on the mgmt node regardless.
                try:
                    recs = [r for r in (json.loads(line) for line in bed.mgmt.run(
                                f"sudo grep -F '\"kind\": \"snapshot\"' {out}", check=False,
                                idempotent=True).splitlines()
                                if line.strip()) if r.get("kind") == "snapshot"]
                except Exception as e:                     # noqa: BLE001
                    log(f"t+{time.time() - t0:.0f}s poll failed: {e}")
                    continue
                summary = analysis.replication_summary(
                    analysis.iterations_from_records(recs, t0), t0)
                if steady_epoch is None and summary["steady_state_reached"]:
                    steady_epoch = meta["steady_epoch"] = summary["steady_epoch"]
                    log(f"steady state after {summary['time_to_steady_s']:.0f}s; "
                        f"measuring {args.measure}s more")
                if steady_epoch and time.time() - steady_epoch >= args.measure:
                    break
                log(f"t+{time.time() - t0:.0f}s iterations "
                    f"{sum(v['iterations'] for v in summary['volumes'].values())}")
            if args.after:
                # replication stops, the client load goes on: the recovery
                # after replication, with the collector still recording
                meta["t_repl_stop"] = time.time()
                log(f"stopping replication: {bed.agent('repl-stop --volumes ' + ','.join(vols))}; "
                    f"client load for {args.after}s more")
                time.sleep(args.after)
            bed.mgmt.run(f"sudo touch {stop}")
            time.sleep(15)
            bed.mgmt.get(out, run_dir / "snapshots.jsonl")
            svc = bed.mgmt.run("sudo docker service ls --format '{{.Name}}' | grep SnapshotReplication",
                               check=False).strip().splitlines()
            if svc:
                since = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(t0 - 120))
                bed.mgmt.run(f"sudo docker service logs --since {since} {svc[0]} "
                             f"> {REMOTE}/{run_id}.xfer.log 2>&1", check=False)
                bed.mgmt.get(f"{REMOTE}/{run_id}.xfer.log", run_dir / "replication-service.log")
    finally:
        # Every step on its own: one failing must not cost the others, and
        # meta.json goes first, since without it nothing of the run can be
        # analysed.
        meta["t_end"] = time.time()
        net_stop.set()
        (run_dir / "meta.json").write_text(json.dumps(meta, indent=2, default=str))
        steps = [("stop fio", lambda: _stop_fio(bed, vols)),
                 ("fetch fio", lambda: _fetch_fio(bed, vols, remote, run_dir))]
        if args.logs:
            steps.append(("fetch logs", lambda: _fetch_logs(bed, run_id, run_dir, fio_start - 60)))
        if scenario == "c":
            steps.append(("delete snapshots", lambda: _delete_snapshots(
                bed, [s["snapshot"] for s in meta.get("snapshots", []) if not s.get("deleted")])))
        if scenario == "b" and meta.get("policy"):
            steps += [("stop collector", lambda: bed.mgmt.run(f"sudo touch {REMOTE}/{run_id}.stop",
                                                               check=False)),
                      ("fetch history", lambda: bed.mgmt.get(f"{REMOTE}/{run_id}.jsonl",
                                                             run_dir / "snapshots.jsonl")),
                      ("clear replication", lambda: _unreplicate(bed, vols, meta["policy"]))]
        for name, step in steps:
            try:
                step()
            except Exception as e:                         # noqa: BLE001
                log(f"WARNING: {name} failed: {e}")
    log(f"=== {run_id} done")
    return run_dir


def run(args):
    run_one(Bed(), args.scenario, args.share, args)


def matrix(args):
    bed = Bed()
    shares = [int(s) for s in args.shares.split(",")]
    for r in range(args.repeat):
        for share in shares:
            # alternate the order between repeats so drift over the day does
            # not always favour the same scenario
            for scenario in (("a", "b") if r % 2 == 0 else ("b", "a")):
                run_one(bed, scenario, share, args)
                time.sleep(args.pause)


# --------------------------------------------------------------------------- #
# report
# --------------------------------------------------------------------------- #

def summarize(run_dir):
    meta = json.loads((run_dir / "meta.json").read_text())
    series = analysis.load_fio_series(str(run_dir / "fio"), meta["fio_start"])
    t0, t_end = meta["t0"], meta["t_end"]
    out = {"run": meta["run_id"], "scenario": meta["scenario"], "share": meta["share"],
           "client_total": analysis.client_window(series, t0, t_end)}
    if meta["scenario"] == "b" and (run_dir / "snapshots.jsonl").exists():
        recs = [json.loads(line) for line in (run_dir / "snapshots.jsonl").read_text().splitlines()
                if line.strip()]
        snaps = [r for r in recs if r.get("kind") == "snapshot"]
        rep = analysis.replication_summary(analysis.iterations_from_records(snaps, t0), t0)
        out["replication"] = rep
        out["landing_classes"] = [r for r in recs if r.get("kind") == "landing_class"]
        if rep["steady_epoch"]:
            out["client_ramp"] = analysis.client_window(series, t0, rep["steady_epoch"])
            out["client_steady"] = analysis.client_window(
                series, rep["steady_epoch"], meta.get("t_repl_stop") or t_end)
    return out


def _fmt(v, nd=0):
    return "-" if v is None else f"{v:,.{nd}f}"


def report(args):
    runs = sorted(p for p in RESULTS.iterdir() if (p / "meta.json").exists())
    rows = [summarize(p) for p in runs]
    (RESULTS / "summary.json").write_text(json.dumps(rows, indent=2, default=str))
    lines = [
        "| run | share | client IOPS | client MB/s | to steady (s) | ramp iter mean (s) |"
        " steady iter mean / p95 (s) | steady period p50 (s) | IOPS in steady | MB/s in steady |"
        " landing class ok |",
        "|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for r in rows:
        rep = r.get("replication", {})
        st = rep.get("steady_iteration_s", {})
        cs = r.get("client_steady", {})
        lc = r.get("landing_classes", [])
        lines.append(
            f"| {r['run']} | {r['share']}% | {_fmt(r['client_total']['total_iops'])} |"
            f" {_fmt(r['client_total']['total_mbs'], 1)} |"
            f" {_fmt(rep.get('time_to_steady_s')) if r['scenario'] == 'b' else 'n/a'} |"
            f" {_fmt(rep.get('ramp_iteration_s', {}).get('mean'), 1)} |"
            f" {_fmt(st.get('mean'), 1)} / {_fmt(st.get('p95'), 1)} |"
            f" {_fmt(rep.get('steady_period_s', {}).get('p50'), 1)} |"
            f" {_fmt(cs.get('total_iops'))} | {_fmt(cs.get('total_mbs'), 1)} |"
            f" {('yes' if lc and all(x['priority_class'] == x['expected'] for x in lc) else ('no' if lc else '-'))} |")
    text = "\n".join(lines)
    (RESULTS / "summary.md").write_text(text + "\n")
    print(text)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("prepare")
    s.add_argument("--size", default="500G")
    for name in ("run", "matrix"):
        s = sub.add_parser(name)
        if name == "run":
            s.add_argument("--scenario", choices=("a", "b", "c"), required=True,
                           help="a: client IO; b: + replication; c: + snapshots only, at a fixed cadence")
            s.add_argument("--snap-interval", type=int, default=300,
                           help="(c) seconds between snapshots of each volume")
            s.add_argument("--keep", type=int, default=2,
                           help="(c) snapshots kept per volume, as replication's retention keeps")
            s.add_argument("--share", type=int, required=True)
        else:
            s.add_argument("--shares", default="15,30,60")
            s.add_argument("--repeat", type=int, default=1)
            s.add_argument("--pause", type=int, default=120, help="seconds between runs")
        s.add_argument("--warmup", type=int, default=300)
        s.add_argument("--measure", type=int, default=1800,
                       help="(a) measured seconds; (b) seconds measured in steady state")
        s.add_argument("--cap", type=int, default=7200, help="(b) longest replication phase")
        s.add_argument("--after", type=int, default=0,
                       help="(b) seconds of client load after replication is stopped")
        s.add_argument("--fio", default="mixed.fio", help="fio job file next to bench.py")
        s.add_argument("--logs", action="store_true",
                       help="fetch SPDK and control-plane logs of the run")
    sub.add_parser("report")
    args = p.parse_args()
    {"prepare": prepare, "run": run, "matrix": matrix, "report": report}[args.cmd](args)


if __name__ == "__main__":
    main()
