"""
remote_bridge_lib.py — bridge-tunnelled execution backend for migration_test_lib.

migration_test_lib.py's scripts all assume they run on a host that already has
direct access to the cluster: `sbctl` on PATH, and the storage node IPs
reachable for nvme-connect/mkfs/fio. That's true when the script runs on the
mgmt node itself. It's not true when you're running it from your own machine
and only bridge_utils gives you a path in.

This module closes that gap by monkeypatching migration_test_lib's execution
primitives to route through a bridge_utils SSH tunnel instead of local
subprocess / a direct LAN connection:

  sbctl(), node_ssh()                       -> always patched
  local_run()                                -> patched, targets the
                                                 *current* client (see
                                                 as_client() below)
  fio_prefill(), start_fio_bg(), stop_fio(),
  check_fio_output()                         -> also patched. These four
                                                 bypass local_run() in
                                                 migration_test_lib (raw
                                                 subprocess.Popen /
                                                 Path.read_text), so without
                                                 this they would silently run
                                                 against your own machine
                                                 instead of the client node.

Every other function in migration_test_lib (parsing, polling, all the
orchestration: create_lvol, start_batch_migration, wait_for_migration, ...)
is untouched and keeps working exactly as written.

Multi-client support: migration_test_lib was written assuming a single,
fixed client host for the whole script. as_client(ip) lets a script move
"the current client" for local_run()/fio calls to a different cluster node
for a scoped block, so different members of the same run — or different
batch groups — can be connected/mounted/fio'd from different real nodes:

    with bridge.as_client("192.168.10.148"):
        device = lib.connect_lvol(lvol_id)
        lib.format_and_mount(device, mount_point)
        handle = lib.start_fio_bg(f"{mount_point}/data.bin", fio_log)
    ...
    with bridge.as_client("192.168.10.148"):   # must match the client used above
        lib.stop_fio(handle, post_wait=0)
        lib.check_fio_output(fio_log)

Scripts that never call enable() are completely unaffected — importing this
module has no side effects.

One known gap: migration_test_lib.discover_cluster_id() calls subprocess
directly instead of going through sbctl(), so it is NOT patched by enable().
Use remote_bridge_lib.discover_cluster_id() instead (same behavior, routed
through the bridge).

Usage:
    import migration_test_lib as lib
    import remote_bridge_lib as bridge

    bridge.enable(key_path="~/.ssh/id_rsa", cluster="default",
                 client_ip="192.168.10.147")
    cluster_id = bridge.discover_cluster_id()
    pool_id = lib.ensure_pool("mypool", cluster_id)
    ...
    bridge.disable()   # optional; restores lib.* and closes SSH sessions
"""

import json
import re
import sys
import time
from contextlib import contextmanager
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import bridge_utils as bu
import migration_test_lib as lib

# Default key path: copy the private key into the same folder as the
# scripts and name it "simplyblock" — no --key flag needed at that point.
DEFAULT_KEY_PATH = "./simplyblock"


_state = {
    "bridge": None,
    "mgmt_ssh": None,
    "node_sessions": {},   # ip -> paramiko SSHClient (bridge-tunnelled)
    "client_ip": None,     # the "current" client for local_run()/fio calls
    "orig": {},            # saved lib.* originals, restored by disable()
    "verbose": False,      # print every remote command + its output live
}


def _raw_exec(ssh, cmd, timeout, label=None):
    """Run cmd on an already-open SSHClient, return (stdout, stderr, rc)
    without raising — the same non-raising contract
    migration_test_lib.node_ssh() itself uses, whether the connection is
    direct or bridge-tunnelled.

    Prints the command and its output live (bridge_utils.run()'s own
    style) when enable(verbose=True) is set — every command that goes
    through here otherwise runs completely silently, which makes a
    failure like "nvme connect didn't actually connect" impossible to
    diagnose after the fact, since migration_test_lib's callers routinely
    swallow the result with `|| true` and never log it themselves. Off by
    default now that the connect/fio/migrate path is known-working.
    """
    prefix = f"[{label}] " if label else "  "
    if _state["verbose"]:
        print(f"{prefix}$ {cmd}", flush=True)
    # No get_pty: a PTY session sends SIGHUP to its foreground process
    # group when the channel closes, which kills anything backgrounded
    # with `cmd &` almost immediately after start_fio_bg() launches it --
    # nohup alone doesn't reliably survive that. The earlier nvme-connect
    # failure turned out to be --nr-io-queues, not the missing PTY, so
    # there's no upside to allocating one here.
    _, stdout, stderr = ssh.exec_command(cmd, timeout=timeout)
    out = stdout.read().decode("utf-8", errors="replace")
    err = stderr.read().decode("utf-8", errors="replace")
    rc = stdout.channel.recv_exit_status()
    if not _state["verbose"]:
        return out.strip(), err.strip(), rc
    for line in out.splitlines():
        if line.strip():
            print(f"{prefix}  {line}", flush=True)
    for line in err.splitlines():
        if line.strip():
            print(f"{prefix}  [stderr] {line}", flush=True)
    if rc != 0:
        print(f"{prefix}  (rc={rc})", flush=True)
    return out.strip(), err.strip(), rc


def _get_node_session(ip):
    """Cached bridge-tunnelled SSHClient to `ip`, opened on first use."""
    sessions = _state["node_sessions"]
    if ip not in sessions:
        sessions[ip] = bu.connect_node(_state["bridge"], ip)
    return sessions[ip]


def _current_client_ssh():
    ip = _state["client_ip"]
    if not ip:
        raise RuntimeError("remote_bridge_lib: no current client — call enable() first")
    return _get_node_session(ip)


@contextmanager
def as_client(ip):
    """Temporarily make `ip` the client that local_run()/fio_*() target.

    Opens (or reuses a cached) bridge-tunnelled session to `ip` for the
    duration of the block, then restores whichever client was current
    before. Nest freely; not thread-safe (module-level state), so don't
    share one enable()'d session across concurrent threads each trying to
    be a different "current" client.
    """
    _get_node_session(ip)  # ensure it exists before switching
    previous = _state["client_ip"]
    _state["client_ip"] = ip
    try:
        yield
    finally:
        _state["client_ip"] = previous


def enable(key_path=None, cluster="default", client_ip=None, mgmt_timeout=300, verbose=False):
    """Monkeypatch migration_test_lib's execution primitives to route
    through a bridge_utils SSH tunnel. See module docstring for exactly
    what gets patched and the as_client() multi-client mechanism.

    key_path: SSH private key for the bridge host. Defaults to
    DEFAULT_KEY_PATH ("./simplyblock") — copy the key into the same folder
    as the scripts under that name and no --key flag is needed.

    verbose: print every remote command and its output live as it runs
    (bridge_utils.run()'s own style). Off by default; turn on when
    diagnosing something like a connect failure that migration_test_lib
    would otherwise swallow silently.
    """
    key_path = key_path or DEFAULT_KEY_PATH
    if _state["bridge"] is not None:
        raise RuntimeError("remote_bridge_lib.enable() already called; call disable() first")
    _state["verbose"] = verbose

    mgmt_ip, sn_ips = bu.get_cluster(cluster)
    client_ip = client_ip or sn_ips[0]

    bridge = bu.connect_bridge(key_path)
    mgmt_ssh = bu.connect_node(bridge, mgmt_ip)
    client_ssh = bu.connect_node(bridge, client_ip)

    _state["bridge"] = bridge
    _state["mgmt_ssh"] = mgmt_ssh
    _state["client_ip"] = client_ip
    _state["node_sessions"][client_ip] = client_ssh
    _state["orig"] = {
        "sbctl": lib.sbctl,
        "local_run": lib.local_run,
        "node_ssh": lib.node_ssh,
        "fio_prefill": lib.fio_prefill,
        "start_fio_bg": lib.start_fio_bg,
        "stop_fio": lib.stop_fio,
        "check_fio_output": lib.check_fio_output,
    }

    def _sbctl(*args, parse_json=False):
        cmd = "sbctl " + " ".join(str(a) for a in args)
        out, err, rc = _raw_exec(mgmt_ssh, cmd, mgmt_timeout, label=f"mgmt:{mgmt_ip}")
        if rc != 0:
            lib.log.warning(f"sbctl {' '.join(str(a) for a in args[:4])} rc={rc}: {err[:200]}")
        if not parse_json:
            return out
        for i, ch in enumerate(out):
            if ch in ("{", "["):
                try:
                    return json.loads(out[i:])
                except json.JSONDecodeError:
                    pass
        return None

    def _local_run(cmd, check_rc=False, timeout=300):
        ip = _state["client_ip"]
        if "nvme connect" in cmd:
            # The connect string comes from the cluster's client_qpair_count
            # setting server-side (--nr-io-queues=3 here) -- these client
            # VMs can't actually satisfy that many I/O queues, and the
            # fabrics connect fails outright ("could not add new
            # controller: failed to write to nvme-fabrics device") rather
            # than degrading gracefully. Force 1 queue for every connect
            # this script issues.
            cmd = re.sub(r'--nr-io-queues=\d+', '--nr-io-queues=1', cmd)
        out, err, rc = _raw_exec(_current_client_ssh(), cmd, timeout, label=f"client:{ip}")
        if check_rc and rc != 0:
            raise RuntimeError(f"Command failed (rc={rc}): {cmd}\nstderr: {err}")
        return out, err, rc

    def _node_ssh(node_ip, cmd, timeout=60):
        return _raw_exec(_get_node_session(node_ip), cmd, timeout, label=f"node:{node_ip}")

    def _fio_prefill(fio_file, fio_log, size="2G", offset=None):
        ssh = _current_client_ssh()
        ip = _state["client_ip"]
        offset_flag = f"--offset={offset} " if offset else ""
        cmd = (f"sudo fio --name=job1 --filename={fio_file} --size={size} {offset_flag}"
              f"--numjobs=1 --direct=1 --ioengine=libaio --iodepth=8 --rw=write "
              f"--bs=128k --verify=md5 --do_verify=0 --end_fsync=1 --output={fio_log}")
        out, err, code = _raw_exec(ssh, cmd, 3600, label=f"client:{ip}")
        if code != 0:
            lib.log.warning(f"  fio prefill rc={code}: {err[:300]}")
        return code == 0

    def _start_fio_bg(fio_file, fio_log, size="2G", runtime=7200, offset=None,
                      iodepth=1, numjobs=1, bs=None):
        ssh = _current_client_ssh()
        client_ip = _state["client_ip"]
        label = f"client:{client_ip}"
        pid_file = fio_log + ".pid"
        raw_log = fio_log + ".raw"
        ts_log = fio_log + ".ts"
        ts_pid_file = ts_log + ".pid"
        # --output/--output-format=json writes the *structured* end-of-run report
        # (one job list, each with a proper "error" errno field) to fio_log --
        # same as fio_migration_test.py's /logs/result.json. --status-interval
        # makes fio append a fresh full report every interval instead of just
        # once, so _check_fio_output() parses the file as a stream of
        # concatenated JSON documents and keeps only the last (true
        # end-of-run) one. --verify_dump/--verify_backlog* match
        # fio_migration_test.py's own invocation: they make a verify mismatch
        # print the full "wanted <hash>, got <hash>" detail to the console
        # (not just "a mismatch happened"), and keep re-checking recently
        # written blocks throughout the run instead of only at job end.
        offset_flag = f"--offset={offset} " if offset else ""
        bs_flag = f"--bs={bs}" if bs else "--bsrange=4k:128k"
        fio_cmd = (f"sudo fio --name=job1 --filename={fio_file} --size={size} {offset_flag}"
                  f"--numjobs={numjobs} --direct=1 --ioengine=libaio --iodepth={iodepth} --verify=md5 "
                  f"--verify_backlog=4096 --verify_backlog_batch=4096 --verify_dump=1 "
                  f"--verify_fatal=0 "
                  f"--readwrite=randrw {bs_flag} --time_based "
                  f"--runtime={runtime} --status-interval=10 "
                  f"--output-format=json --output={fio_log}")
        # setsid + nohup + a pidfile is how you background a job over
        # exec_command: there's no local Popen handle for a remote
        # process, so lifecycle (is it running? stop it?) has to be
        # tracked through files on the client instead. setsid fully
        # detaches into a new session so the process survives this
        # exec_command's channel closing (nohup alone ignores SIGHUP, but
        # doesn't guarantee it stays out of the closing session's process
        # group). fio's own stdout/stderr (verify-failure diagnostics
        # included) still goes to raw_log, untouched -- fio's PID (pid_file)
        # must stay fio's actual PID so _stop_fio's kill -INT reaches it.
        _raw_exec(ssh, f"setsid nohup {fio_cmd} </dev/null >{raw_log} 2>&1 & echo $! > {pid_file}",
                 30, label=label)
        time.sleep(2)
        pid_out, _, _ = _raw_exec(ssh, f"cat {pid_file} 2>/dev/null", 10, label=label)
        pid = pid_out.strip()
        alive_out, _, _ = (_raw_exec(ssh, f"kill -0 {pid} 2>/dev/null && echo alive", 10, label=label)
                           if pid else ("", "", 1))
        if not pid or "alive" not in alive_out:
            lib.log.warning(f"  fio exited immediately or failed to start on "
                            f"{client_ip} (log={fio_log})")
            return None
        # Separate, best-effort tailer: annotates every line raw_log ever
        # gets with the wall-clock time it was actually written, into its own
        # file (ts_log). Runs as an independent process (its own PID, tracked
        # separately) so it can never interfere with fio's own PID/lifecycle
        # -- if this fails to start, _check_fio_output() just falls back to
        # the unstamped raw_log.
        tailer_cmd = (
            f"setsid nohup bash -c "
            f"'tail -n +1 -F {raw_log} | while IFS= read -r line; do "
            f"printf \"%s %s\\n\" \"$(date -u +%Y-%m-%dT%H:%M:%S.%3N)\" \"$line\"; "
            f"done' </dev/null >{ts_log} 2>&1 & echo $! > {ts_pid_file}"
        )
        _raw_exec(ssh, tailer_cmd, 15, label=label)
        lib.log.info(f"  fio bg started on {client_ip} (pid={pid})  log: {fio_log}")
        return {"client_ip": client_ip, "pid": pid, "pid_file": pid_file,
                "ts_pid_file": ts_pid_file}

    def _stop_fio(proc, post_wait=30):
        if not proc:
            return
        lib.log.info(f"  Waiting {post_wait}s before stopping fio...")
        time.sleep(post_wait)
        ssh = _get_node_session(proc["client_ip"])
        label = f"client:{proc['client_ip']}"
        pid = proc["pid"]
        _raw_exec(ssh, f"sudo kill -INT {pid} 2>/dev/null || true", 10, label=label)
        time.sleep(3)
        _raw_exec(ssh, f"sudo kill -9 {pid} 2>/dev/null || true", 10, label=label)
        # Best-effort: stop the timestamping tailer too (harmless if this
        # fails or the pidfile is missing -- it's diagnostic-only).
        ts_pid_file = proc.get("ts_pid_file")
        if ts_pid_file:
            ts_pid_out, _, _ = _raw_exec(ssh, f"cat {ts_pid_file} 2>/dev/null", 10, label=label)
            ts_pid = ts_pid_out.strip()
            if ts_pid:
                # setsid made it its own process group leader, so the tailer's
                # PID == its PGID -- kill the whole group (negative PID) to
                # reach the `tail | while read` pipeline's children too, not
                # just the outer bash -c wrapper.
                _raw_exec(ssh, f"sudo kill -- -{ts_pid} 2>/dev/null || sudo kill {ts_pid} 2>/dev/null || true",
                         10, label=label)

    # fio sets this specific errno (EILSEQ) on a job, and only on a job, whose
    # --verify=md5 caught a checksum mismatch on read-back -- i.e. real data
    # corruption. Same convention as fio_migration_test.py's
    # FIO_ERRNO_DATA_INTEGRITY. Any other errno (EIO, ETIMEDOUT, ...) is a
    # transient transport error, not corruption.
    _FIO_ERRNO_DATA_INTEGRITY = 84
    _VERIFY_TEXT_RE = re.compile(
        r'bad magic header|verify type mismatch|verify failed|checksum mismatch|'
        r'bad checksum|bad header|verify.*wanted', re.I)
    # ts_log lines are "<UTC timestamp> <original fio line>", written by the
    # tailer process _start_fio_bg() launches alongside fio.
    _TS_LINE_RE = re.compile(r'^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}) (.*)$')

    def _find_verify_hits(text):
        """[(timestamp_or_None, line), ...] for every line matching the
        verify-failure text patterns, in file order (oldest first). The
        fio-message line itself already contains the "wanted <hash>, got
        <hash>" detail (from --verify_dump/--verify_backlog), so no further
        parsing of the header value is needed here -- just surface the line."""
        hits = []
        for line in text.splitlines():
            m = _TS_LINE_RE.match(line)
            ts, body = (m.group(1), m.group(2)) if m else (None, line)
            if _VERIFY_TEXT_RE.search(body):
                hits.append((ts, body))
        return hits

    def _last_json_object(content):
        """--status-interval makes fio dump a full JSON status report into
        --output every interval, not just once at the end -- so the file holds
        several back-to-back JSON documents, not one. json.loads() only reads
        the first and errors on "Extra data" for the rest. Walk the stream with
        a decoder and keep the LAST successfully parsed object (the true
        end-of-run report); earlier ones are just mid-run snapshots."""
        decoder = json.JSONDecoder()
        n = len(content)
        idx, last = 0, None
        while idx < n:
            if content[idx] != "{":
                # Not the start of a report object (whitespace, a stray
                # scalar, a truncated/garbled chunk) -- skip to the next
                # '{' rather than aborting the whole scan.
                nxt = content.find("{", idx)
                if nxt == -1:
                    break
                idx = nxt
                continue
            try:
                obj, end = decoder.raw_decode(content, idx)
            except json.JSONDecodeError:
                nxt = content.find("{", idx + 1)
                if nxt == -1:
                    break
                idx = nxt
                continue
            if isinstance(obj, dict):
                last = obj
            idx = end
        return last

    def _check_fio_output(fio_log, fault_injected=False):
        ssh = _current_client_ssh()
        ip = _state["client_ip"]
        label = f"client:{ip}"
        # --status-interval keeps appending fresh full JSON reports for as long
        # as fio runs, so the file grows unboundedly with run length -- cat'ing
        # the whole thing over a tunneled SSH channel is what timed out here.
        # Only the LAST report is ever used (see _last_json_object), so pull a
        # bounded tail instead of the whole file: comfortably larger than any
        # single fio JSON report, but O(1) regardless of how long fio ran.
        content, _, code = _raw_exec(ssh, f"tail -c 1000000 {fio_log} 2>/dev/null",
                                     90, label=label)
        if code != 0 or not content.strip():
            lib.log.warning(f"  fio log not found: {fio_log}")
            return True, 0
        result = _last_json_object(content)
        if result is None:
            lib.log.warning(f"  fio JSON report unparsable; last 500 chars: {content[-500:]!r}")
            result = {}
        # Same signal as fio_migration_test.py: each job's "error" field is the
        # errno of its LAST error (not a count), so this is a set, not a sum.
        errnos = {job.get("error", 0) for job in result.get("jobs", []) if job.get("error")}
        integrity_errno = _FIO_ERRNO_DATA_INTEGRITY in errnos
        io_errnos = sorted(errnos - {_FIO_ERRNO_DATA_INTEGRITY})

        # Same reasoning for the raw stdout/stderr capture (it also grows with
        # runtime, from the periodic eta-newline progress lines): bound it to a
        # generous tail rather than the whole file. A genuine, ongoing
        # corruption issue keeps re-triggering verify_backlog mismatches
        # throughout the run, so it will still show up near the end; a single
        # one-off very early in an unusually long run is the only case this
        # could miss.
        # Prefer the timestamped copy (.ts, written by the tailer process
        # started alongside fio) so each hit carries a real wall-clock time;
        # fall back to the unstamped .raw if the tailer never started (older
        # in-flight fio process, or it failed to launch) -- still correct,
        # just without exact timing.
        raw_content, _, raw_code = _raw_exec(
            ssh, f"tail -c 2000000 {fio_log}.ts 2>/dev/null", 90, label=label)
        if raw_code != 0 or not raw_content.strip():
            raw_content, _, raw_code = _raw_exec(
                ssh, f"tail -c 2000000 {fio_log}.raw 2>/dev/null", 90, label=label)
        verify_hits = _find_verify_hits(raw_content) if raw_code == 0 else []

        corruption = integrity_errno or bool(verify_hits)
        if corruption:
            first_ts, first_line = verify_hits[0] if verify_hits else (None, "")
            when = f"first seen at {first_ts} UTC" if first_ts else "timestamp unavailable"
            lib.log.error(f"  fio: DATA INTEGRITY FAILURE — checksum mismatch on "
                          f"read-back (errno_84={integrity_errno}, verify_log_hits={len(verify_hits)}, {when})")
            if first_line:
                lib.log.error(f"    {first_line}")
        elif io_errnos:
            lib.log.info(f"  fio: job reported errno(s) {io_errnos} "
                        f"(transient IO error, not data corruption -- not a failure)")
        else:
            lib.log.info("  fio: OK (no errors)")
        return not corruption, (1 if corruption else 0)

    lib.sbctl = _sbctl
    lib.local_run = _local_run
    lib.node_ssh = _node_ssh
    lib.fio_prefill = _fio_prefill
    lib.start_fio_bg = _start_fio_bg
    lib.stop_fio = _stop_fio
    lib.check_fio_output = _check_fio_output

    lib.log.info(f"remote_bridge_lib: enabled — mgmt={mgmt_ip} client={client_ip} "
                f"(cluster={cluster!r})")


def disable():
    """Restore migration_test_lib's original local-execution primitives and
    close every SSH session opened by enable()."""
    if _state["bridge"] is None:
        return
    for name, fn in _state["orig"].items():
        setattr(lib, name, fn)
    for ssh in _state["node_sessions"].values():
        try:
            ssh.close()
        except Exception:
            pass
    try:
        _state["bridge"].close()
    except Exception:
        pass
    _state.update(bridge=None, mgmt_ssh=None, node_sessions={}, client_ip=None, orig={}, verbose=False)
    lib.log.info("remote_bridge_lib: disabled")


def discover_cluster_id():
    """Bridge-routed equivalent of migration_test_lib.discover_cluster_id(),
    which bypasses sbctl() (calls subprocess directly) and so is not fixed
    by enable()'s monkeypatching.
    """
    data = lib.sbctl("cluster", "list", "--json", parse_json=True)
    if isinstance(data, list):
        data = data[0] if data else {}
    elif isinstance(data, dict):
        data = (data.get("results") or [{}])[0]
    return lib.get(data, "id") or ""


def mgmt_run(cmd, timeout=300):
    """Run an arbitrary shell command on the mgmt node (outside of sbctl),
    for anything a script needs beyond what migration_test_lib exposes."""
    return _raw_exec(_state["mgmt_ssh"], cmd, timeout, label="mgmt")


def node_run(ip, cmd, timeout=60):
    """Run an arbitrary shell command on any cluster node by IP (opens/reuses
    a cached bridge-tunnelled session)."""
    return _raw_exec(_get_node_session(ip), cmd, timeout, label=f"node:{ip}")


# ---------------------------------------------------------------------------
# Artifact collection — dmesg, sb_logs, and downloading remote files (fio
# logs, tarballs, ...) back to this machine, all reusing the SAME
# already-open bridge connection enable() set up (no extra bridge hops).
# ---------------------------------------------------------------------------

COLLECT_LOGS_PY = "/usr/local/lib/python3.9/site-packages/simplyblock_core/scripts/collect_logs.py"


def collect_dmesg(ip, human_readable=True):
    """dmesg -T output from a single node, via the shared bridge tunnel."""
    cmd = "dmesg -T" if human_readable else "dmesg"
    out, err, rc = _raw_exec(_get_node_session(ip), cmd, 60, label=f"node:{ip}")
    if rc != 0:
        lib.log.warning(f"  dmesg failed on {ip} (rc={rc}): {err[:200]}")
        return None
    return out


def download_remote_file(remote_path, local_path, client_ip=None):
    """SFTP a file from a cluster node (or mgmt, if client_ip is None) to
    local_path, reusing the already-open bridge-tunnelled session -- no new
    bridge connection needed. Returns True on success."""
    ssh = _state["mgmt_ssh"] if client_ip is None else _get_node_session(client_ip)
    try:
        sftp = ssh.open_sftp()
        try:
            sftp.get(remote_path, str(local_path))
        finally:
            sftp.close()
        return True
    except Exception as e:  # noqa: BLE001 -- a missing/unreadable remote file
        # must not abort a bulk collection loop over many files.
        lib.log.warning(f"  download {remote_path} -> {local_path} failed: {e}")
        return False


def collect_sb_logs(start_dt, duration_minutes, local_dir):
    """Run collect_logs.py on mgmt for the given UTC window (start_dt: 'YYYY-MM-DD HH:MM:SS'),
    then download every sb_logs_*.tar.gz it produced in /root into local_dir.
    Same mechanism as download_sb_logs.py, reusing this session's mgmt_ssh
    instead of opening a fresh bridge connection. Returns the list of local
    paths downloaded.
    """
    mgmt_ssh = _state["mgmt_ssh"]
    cmd = f'python3 {COLLECT_LOGS_PY} "{start_dt}" {duration_minutes} --namespace ""'
    out, err, rc = _raw_exec(mgmt_ssh, cmd, 300, label="mgmt")
    if rc != 0:
        lib.log.warning(f"  collect_logs.py exited rc={rc} (tarball may still exist): {err[-400:]}")
    local_dir = Path(local_dir)
    local_dir.mkdir(parents=True, exist_ok=True)
    downloaded = []
    try:
        sftp = mgmt_ssh.open_sftp()
        try:
            remote_files = [f for f in sftp.listdir("/root")
                           if f.startswith("sb_logs_") and f.endswith(".tar.gz")]
            for fname in remote_files:
                local_path = local_dir / fname
                sftp.get(f"/root/{fname}", str(local_path))
                downloaded.append(local_path)
                lib.log.info(f"  sb_logs downloaded -> {local_path}")
        finally:
            sftp.close()
    except Exception as e:  # noqa: BLE001
        lib.log.warning(f"  sb_logs tarball listing/download failed: {e}")
    if not downloaded:
        lib.log.warning("  No sb_logs_*.tar.gz found in /root on mgmt")
    return downloaded


# ---------------------------------------------------------------------------
# vm-number placement helpers — node UUIDs are re-generated on every
# redeploy, but hostnames ("vm07_4420") and their sn_ips position are fixed.
# These resolve a stable vm label to whatever the *current* deploy's uuid is,
# and classify a (source, target) pair's real HA relationship straight from
# the live `sn list` instead of hardcoding it — the ring shape (whose
# secondary/tertiary is whose) is a deploy-time fact, not something to guess.
# ---------------------------------------------------------------------------
def resolve_by_hostname(nodes, vm_label):
    """Find the online node whose hostname starts with vm_label (e.g.
    'vm07' matches 'vm07_4420'), case-insensitive. Returns its current id.
    """
    label = vm_label.strip().lower()
    for n in nodes:
        hostname = (lib.get(n, "hostname") or "").lower()
        if hostname.startswith(label):
            return lib.get(n, "id")
    raise RuntimeError(f"No online node found with hostname starting with {vm_label!r} "
                       f"(available: {[lib.get(n, 'hostname') for n in nodes]})")


def classify_pair(nodes, src_id, tgt_id):
    """Describe src/tgt's real HA relationship: 'no_overlap',
    'target_is_source_secondary', 'target_is_source_tertiary',
    'source_is_target_secondary', or 'source_is_target_tertiary'.
    """
    if tgt_id == lib.get_node_secondary_id(src_id, nodes):
        return "target_is_source_secondary"
    if tgt_id == lib.get_node_tertiary_id(src_id, nodes):
        return "target_is_source_tertiary"
    if src_id == lib.get_node_secondary_id(tgt_id, nodes):
        return "source_is_target_secondary"
    if src_id == lib.get_node_tertiary_id(tgt_id, nodes):
        return "source_is_target_tertiary"
    return "no_overlap"
