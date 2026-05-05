"""Lab variant of setup_perf_test1.py.

Runs FROM the jump host. Targets 5 pre-existing nodes on the lab subnet:
  192.168.10.210         -> management node (also acts as the perf client)
  192.168.10.201..204    -> storage nodes

All nodes share the same root password, which must be supplied at startup
(via --password, the SBCLI_ROOT_PASSWORD env var, or an interactive prompt).

Upload to the jump host and run it there, e.g.:
  scp -i ~/simplyblock -P 13987 scripts/setup_lab_perf_test1.py \
      simplyblock@95.216.93.11:~/
  ssh -i ~/simplyblock -p 13987 simplyblock@95.216.93.11
  python3 ~/setup_lab_perf_test1.py            # will prompt for the password

Requirements on the jump host: sshpass, python3 (no paramiko / boto3 needed).
"""

import argparse
import getpass
import json
import os
import re
import select
import shlex
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor

# --- Lab topology (fixed) ---
MGMT_IP = "192.168.10.210"
SN_IPS = [
    "192.168.10.201",
    "192.168.10.202",
    "192.168.10.203",
    "192.168.10.204",
]
SN_COUNT = len(SN_IPS)

USER = "root"
IFACE = "eth0"
DATA_IFACE = "eth1"
BRANCH = "inline-checksum-validation"
MAX_LVOL = "25"

# Same volume plan layout as the AWS variant; consumed by downstream perf tooling.
VOLUME_PLAN = [
    {"idx": 0, "node_idx": 0, "qty": 5, "size": "100G", "client": "client1", "io_queues": 12},
    {"idx": 1, "node_idx": 1, "qty": 5, "size": "100G", "client": "client2", "io_queues": 12},
]

ROOT_PASSWORD = ""  # populated from CLI / env / prompt in main()

SSH_OPTS = [
    "-o", "StrictHostKeyChecking=no",
    "-o", "UserKnownHostsFile=/dev/null",
    "-o", "LogLevel=ERROR",
    "-o", "ConnectTimeout=10",
    "-o", "ServerAliveInterval=30",
]


def _ssh_argv(ip, cmd):
    return [
        "sshpass", "-e",
        "ssh", *SSH_OPTS,
        f"{USER}@{ip}",
        cmd,
    ]


def _ssh_env():
    env = os.environ.copy()
    env["SSHPASS"] = ROOT_PASSWORD
    return env


def wait_for_ssh(ip, timeout=300):
    print(f"--> Attempting SSH handshake on {ip}...")
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            proc = subprocess.run(
                _ssh_argv(ip, "true"),
                env=_ssh_env(),
                capture_output=True,
                timeout=15,
            )
            if proc.returncode == 0:
                print(f"SUCCESS: {ip} is ready.")
                return True
        except subprocess.TimeoutExpired:
            pass
        except Exception:
            pass
        time.sleep(2)
    print(f"FAILURE: Timed out on {ip}")
    return False


def ssh_exec(ip, cmds, get_output=False, check=False):
    results = []
    for cmd in cmds:
        print(f"  [{ip}] $ {cmd}")
        proc = subprocess.run(
            _ssh_argv(ip, cmd),
            env=_ssh_env(),
            capture_output=True,
            text=True,
            timeout=600,
        )
        out = proc.stdout
        err = proc.stderr
        rc = proc.returncode
        if get_output:
            results.append(out)
        if rc != 0:
            print(f"  [{ip}] FAILED (rc={rc}): {cmd}")
            if out.strip():
                print(f"    --- stdout ({len(out.splitlines())} lines) ---")
                for line in out.rstrip().split("\n"):
                    print(f"    stdout: {line}")
            if err.strip():
                print(f"    --- stderr ({len(err.splitlines())} lines) ---")
                for line in err.rstrip().split("\n"):
                    print(f"    stderr: {line}")
            if check:
                raise RuntimeError(f"Command failed on {ip} (rc={rc}): {cmd}")
        else:
            lines = out.strip().split("\n")
            for line in lines[-2:]:
                if line.strip():
                    print(f"    {line}")
    return results


def ssh_exec_stream(ip, cmd, check=False):
    print(f"  [{ip}] $ {cmd}")
    proc = subprocess.Popen(
        _ssh_argv(ip, cmd),
        env=_ssh_env(),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )
    out_chunks = []
    err_chunks = []
    streams = {proc.stdout: out_chunks, proc.stderr: err_chunks}
    while streams:
        readable, _, _ = select.select(list(streams.keys()), [], [], 0.5)
        for stream in readable:
            line = stream.readline()
            if not line:
                streams.pop(stream, None)
                continue
            streams[stream].append(line)
            sys.stdout.write(line)
            sys.stdout.flush()
        if proc.poll() is not None:
            for stream in list(streams.keys()):
                rest = stream.read()
                if rest:
                    streams[stream].append(rest)
                    sys.stdout.write(rest)
                    sys.stdout.flush()
            streams.clear()
    rc = proc.wait()
    out = "".join(out_chunks)
    err = "".join(err_chunks)
    if rc != 0 and check:
        raise RuntimeError(f"Command failed on {ip} (rc={rc}): {cmd}")
    return out, err


def get_sn_uuids(mgmt_ip):
    print("Fetching Storage Node UUIDs...")
    node_list_raw = ssh_exec(mgmt_ip, ["/usr/local/bin/sbctl -d sn list"], get_output=True)[0]

    uuids = []
    for line in node_list_raw.splitlines():
        parts = [p.strip() for p in line.split("|")]
        if len(parts) > 1:
            potential_uuid = parts[1]
            if re.match(r"[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}", potential_uuid):
                uuids.append(potential_uuid)

    if not uuids:
        print("DEBUG: Raw table received:\n", node_list_raw)
        raise Exception("Failed to parse Node UUIDs from table.")

    return uuids


def fetch_cluster_topology(mgmt_ip, cluster_uuid):
    script = f"""python3 - <<'PY'
import json
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.storage_node import StorageNode


def normalize_ref(value):
    if isinstance(value, str):
        return value
    if isinstance(value, list) and value:
        first = value[0]
        if isinstance(first, str):
            return first
        if isinstance(first, dict):
            for key in ("node_id", "uuid", "id"):
                if first.get(key):
                    return first[key]
    if isinstance(value, dict):
        for key in ("node_id", "uuid", "id"):
            if value.get(key):
                return value[key]
    return ""


db = DBController()
cluster = db.get_cluster_by_id({cluster_uuid!r})
nodes = db.get_storage_nodes_by_cluster_id({cluster_uuid!r}) or []
by_id = {{node.get_id(): node for node in nodes}}

node_items = []
lvstores = {{}}

for node in nodes:
    sec_ref = normalize_ref(
        getattr(node, "lvstore_stack_secondary", "")
        or getattr(node, "lvstore_stack_secondary_1", "")
    )
    tert_ref = normalize_ref(
        getattr(node, "lvstore_stack_tertiary", "")
        or getattr(node, "lvstore_stack_secondary_2", "")
    )

    node_lvs = []
    if getattr(node, "lvstore", ""):
        node_lvs.append({{"name": node.lvstore, "role": "primary"}})
    if sec_ref and sec_ref in by_id and getattr(by_id[sec_ref], "lvstore", ""):
        node_lvs.append({{"name": by_id[sec_ref].lvstore, "role": "secondary"}})
    if tert_ref and tert_ref in by_id and getattr(by_id[tert_ref], "lvstore", ""):
        node_lvs.append({{"name": by_id[tert_ref].lvstore, "role": "tertiary"}})

    node_items.append(
        {{
            "uuid": node.get_id(),
            "hostname": getattr(node, "hostname", ""),
            "management_ip": getattr(node, "mgmt_ip", ""),
            "lvs": node_lvs,
            "lvs_display": [f"{{item['name']}} ({{item['role']}})" for item in node_lvs],
        }}
    )

    lvs_name = getattr(node, "lvstore", "")
    if not lvs_name:
        continue

    hublvol = getattr(node, "hublvol", None)
    hublvol_nqn = getattr(hublvol, "nqn", "") or StorageNode.hublvol_nqn_for_lvstore(
        cluster.nqn, lvs_name
    )
    lvstores[lvs_name] = {{
        "hublvol_nqn": hublvol_nqn,
        "client_port": node.get_lvol_subsys_port(lvs_name),
        "hublvol_port": node.get_hublvol_port(lvs_name),
    }}

result = {{
    "cluster_uuid": cluster.uuid,
    "cluster_nqn": cluster.nqn,
    "nodes": node_items,
    "lvstores": dict(sorted(lvstores.items())),
}}
print(json.dumps(result, indent=2))
PY"""
    output = ssh_exec(mgmt_ip, [script], get_output=True, check=True)[0]
    return json.loads(output)


def fetch_alceml_modes(mgmt_ip, cluster_uuid):
    """Return per-alceml mode info for every storage device in the cluster.

    Mirrors simplyblock_core.utils.alceml_checksum_params:
      0 = off                   (cluster.inline_checksum False)
      1 = md-on-device          (cluster ON, device md_supported)
      2 = fallback / emulation  (cluster ON, device has no md-capable LBAF)
    """
    script = f"""python3 - <<'PY'
import json
from simplyblock_core.db_controller import DBController

db = DBController()
cluster = db.get_cluster_by_id({cluster_uuid!r})
nodes = db.get_storage_nodes_by_cluster_id({cluster_uuid!r}) or []
inline = bool(getattr(cluster, "inline_checksum", False))

rows = []
for node in nodes:
    label = getattr(node, "hostname", "") or node.get_id()
    for dev in (getattr(node, "nvme_devices", None) or []):
        md_supported = bool(getattr(dev, "md_supported", False))
        md_size = int(getattr(dev, "md_size", 0) or 0)
        if not inline:
            method, mode_label = 0, "off"
        elif md_supported:
            method, mode_label = 1, "md-on-device"
        else:
            method, mode_label = 2, "fallback (emulation)"
        rows.append({{
            "node": label,
            "alceml": getattr(dev, "alceml_name", "") or getattr(dev, "uuid", ""),
            "method": method,
            "mode": mode_label,
            "md_supported": md_supported,
            "md_size": md_size,
        }})

print(json.dumps({{"inline_checksum": inline, "devices": rows}}, indent=2))
PY"""
    output = ssh_exec(mgmt_ip, [script], get_output=True, check=True)[0]
    return json.loads(output)


def print_alceml_summary(summary):
    inline = summary.get("inline_checksum", False)
    devices = summary.get("devices", [])
    print("\n--- ALCEML inline-checksum modes ---")
    print(f"Cluster inline_checksum: {'ENABLED' if inline else 'disabled'}")
    if not devices:
        print("  (no devices reported)")
        return
    by_node = {}
    for row in devices:
        by_node.setdefault(row["node"], []).append(row)
    for node, rows in sorted(by_node.items()):
        print(f"  {node}:")
        for row in rows:
            print(
                f"    - {row['alceml'] or '(unnamed)':<40} "
                f"method={row['method']} {row['mode']:<22} "
                f"md_size={row['md_size']} md_supported={row['md_supported']}"
            )
    md_count = sum(1 for r in devices if r["method"] == 1)
    fb_count = sum(1 for r in devices if r["method"] == 2)
    off_count = sum(1 for r in devices if r["method"] == 0)
    print(
        f"Totals: md-on-device={md_count}  fallback={fb_count}  off={off_count}  "
        f"(of {len(devices)} devices)"
    )


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--password",
        help="Root password shared by all lab nodes. If omitted, falls back to the "
             "SBCLI_ROOT_PASSWORD env var, then to an interactive prompt.",
    )
    parser.add_argument("--branch", default=BRANCH, help=f"sbcli branch to install (default: {BRANCH}).")
    parser.add_argument("--iface", default=IFACE, help=f"Management/control NIC on storage nodes (default: {IFACE}).")
    parser.add_argument("--data-iface", default=DATA_IFACE, help=f"Data-plane NIC for --data-nics (default: {DATA_IFACE}).")
    parser.add_argument("--max-lvol", default=MAX_LVOL, help=f"--max-lvol passed to sn configure (default: {MAX_LVOL}).")
    parser.add_argument(
        "--metadata-out",
        default="cluster_metadata_base.json",
        help="Where to write the cluster metadata JSON (default: ./cluster_metadata_base.json).",
    )
    parser.add_argument(
        "--no-inline-checksum",
        action="store_true",
        help=(
            "Disable inline CRC checksum validation. By default the cluster is "
            "created with --enable-inline-checksum (matching the inline-checksum-"
            "validation branch + ultra:checksum-validation-latest image). The "
            "flag is frozen at create time and cannot be changed later."
        ),
    )
    return parser.parse_args()


def resolve_password(cli_value):
    if cli_value:
        return cli_value
    env_value = os.environ.get("SBCLI_ROOT_PASSWORD")
    if env_value:
        return env_value
    return getpass.getpass("Root password for lab nodes (.210, .201-.204): ")


def main():
    global ROOT_PASSWORD
    args = parse_args()
    ROOT_PASSWORD = resolve_password(args.password)
    if not ROOT_PASSWORD:
        print("ERROR: empty root password.", file=sys.stderr)
        sys.exit(2)

    if subprocess.run(["which", "sshpass"], capture_output=True).returncode != 0:
        print("ERROR: sshpass not found in PATH. Install it on the jump host first.", file=sys.stderr)
        sys.exit(2)

    mgmt_ip = MGMT_IP
    sn_ips = list(SN_IPS)
    all_setup_ips = [mgmt_ip] + sn_ips

    print(f"Targeting mgmt={mgmt_ip}  storage={sn_ips}")
    print(f"Waiting for SSH readiness on {len(all_setup_ips)} nodes...")
    for ip in all_setup_ips:
        if not wait_for_ssh(ip):
            raise RuntimeError(f"Could not SSH to {ip} as root. Check the password and network.")

    # --- Phase 1: install sbcli on every node ---
    install_cmds = [
        "dnf install git python3-pip nvme-cli -y",
        "/usr/bin/python3 -m pip install --upgrade pip setuptools wheel",
        "/usr/bin/python3 -m pip install ruamel.yaml",
        f"pip install git+https://github.com/simplyblock-io/sbcli@{shlex.quote(args.branch)} "
        "--upgrade --force --ignore-installed requests",
        "grep -q 'export PATH=/usr/local/bin:\\$PATH' ~/.bashrc || "
        "echo 'export PATH=/usr/local/bin:$PATH' >> ~/.bashrc",
    ]

    print("Phase 1: Starting Universal Parallel Setup...")
    with ThreadPoolExecutor(max_workers=len(all_setup_ips)) as executor:
        setup_tasks = [executor.submit(ssh_exec, ip, install_cmds, check=True) for ip in all_setup_ips]
        for t in setup_tasks:
            t.result()
    print("Phase 1: DONE - all nodes have sbcli installed.")

    # --- Phase 1.5: cleanup leftover state from any prior deploy ---
    # Order matters:
    #   1. sn deploy-cleaner first (tears down SPDK containers + NVMe state).
    #   2. docker rm -f any stragglers, then `docker system prune -af --volumes`.
    #      Per the deployment notes: SAFE before cluster create (no active FDB
    #      volumes yet); NEVER run after activate (it would wipe FDB).
    #   3. Fresh `docker pull` of the simplyblock + ultra images named in the
    #      installed env_var, so we don't reuse a stale cached layer.
    print("Phase 1.5a: Running sbctl sn deploy-cleaner on every node...")
    deploy_cleaner_cmds = ["/usr/local/bin/sbctl -d sn deploy-cleaner"]
    with ThreadPoolExecutor(max_workers=len(all_setup_ips)) as executor:
        tasks = [executor.submit(ssh_exec, ip, deploy_cleaner_cmds, check=False)
                 for ip in all_setup_ips]
        for t in tasks:
            t.result()
    print("Phase 1.5a: DONE.")

    print("Phase 1.5b: Removing any straggler containers and pruning Docker...")
    docker_cleanup_cmds = [
        "containers=$(docker ps -aq); "
        "if [ -n \"$containers\" ]; then docker rm -f $containers; fi",
        "docker system prune -af --volumes",
    ]
    with ThreadPoolExecutor(max_workers=len(all_setup_ips)) as executor:
        tasks = [executor.submit(ssh_exec, ip, docker_cleanup_cmds, check=False)
                 for ip in all_setup_ips]
        for t in tasks:
            t.result()
    print("Phase 1.5b: DONE.")

    # NVMe partition cleanup. deploy-cleaner already pulls SPDK off the
    # drives, but a prior deploy may have left GPT tables / filesystem
    # signatures / leftover namespace state behind. Wipe signatures, then
    # nvme-format every non-root NVMe so the data plane sees a clean slate.
    # sn configure --enable-inline-checksum --force will reformat to a
    # metadata-capable LBAF on top of this. Storage nodes only -- the mgmt
    # node is never used for SPDK data devices.
    print("Phase 1.5d: Wiping partitions and formatting NVMes on storage nodes...")
    nvme_cleanup_script = r"""set -u
root_src=$(findmnt -no SOURCE / 2>/dev/null || true)
root_dev=$(echo "$root_src" | sed -E 's|p?[0-9]+$||')
echo "Root NVMe (will be skipped): $root_dev"
for d in $(lsblk -dno NAME,TYPE | awk '$2=="disk" && $1 ~ /^nvme/ {print "/dev/"$1}'); do
    [ -b "$d" ] || continue
    if [ "$d" = "$root_dev" ]; then
        echo "Skip $d (root)"
        continue
    fi
    for p in ${d}p*; do
        [ -b "$p" ] || continue
        umount -f "$p" 2>/dev/null || true
    done
    echo "Wiping $d (wipefs)"
    wipefs -af "$d" 2>/dev/null || true
    echo "Formatting $d (nvme format -s 0)"
    nvme format "$d" -f -s 0 2>/dev/null || \
        echo "  WARN: nvme format failed on $d (continuing; sn configure will retry)"
done
"""
    with ThreadPoolExecutor(max_workers=len(sn_ips)) as executor:
        tasks = [executor.submit(ssh_exec, ip, [nvme_cleanup_script], check=False)
                 for ip in sn_ips]
        for t in tasks:
            t.result()
    print("Phase 1.5d: DONE.")

    print("Phase 1.5c: Fresh-pulling simplyblock + ultra images on every node...")
    # Pull with retry: public.ecr.aws occasionally returns transient errors
    # (IPv6 source-address races, S3 signed-URL hiccups, etc.). Retry up to
    # 6 times with 15s backoff so one node's blip doesn't abort the deploy.
    pull_script = """python3 - <<'PY'
import os, subprocess, sys, time
import simplyblock_core
envf = os.path.join(os.path.dirname(simplyblock_core.__file__), 'env_var')
images = []
with open(envf) as f:
    for line in f:
        if '=' not in line:
            continue
        k, v = line.strip().split('=', 1)
        if k in ('SIMPLY_BLOCK_DOCKER_IMAGE', 'SIMPLY_BLOCK_SPDK_ULTRA_IMAGE') and v:
            images.append(v)
if not images:
    print('no images found in env_var', file=sys.stderr)
    sys.exit(1)
for img in images:
    print(f'Pulling {img}', flush=True)
    last_rc = 1
    for attempt in range(1, 7):
        last_rc = subprocess.call(['docker', 'pull', img])
        if last_rc == 0:
            break
        print(f'  pull failed (rc={last_rc}), attempt {attempt}/6 - retry in 15s', flush=True)
        time.sleep(15)
    if last_rc != 0:
        print(f'  giving up on {img} after 6 attempts', file=sys.stderr)
        sys.exit(last_rc)
PY"""
    with ThreadPoolExecutor(max_workers=len(all_setup_ips)) as executor:
        tasks = [executor.submit(ssh_exec, ip, [pull_script], check=True)
                 for ip in all_setup_ips]
        for t in tasks:
            t.result()
    print("Phase 1.5c: DONE - all nodes have fresh images.")

    inline_checksum = not args.no_inline_checksum
    checksum_flag = " --enable-inline-checksum" if inline_checksum else ""
    print(f"Inline checksum validation: {'ENABLED' if inline_checksum else 'disabled'}")

    # --- Phase 2: cluster create + sn configure/deploy ---
    print("Phase 2a: Creating cluster on management node...")
    ssh_exec(mgmt_ip, [
        "/usr/local/bin/sbctl -d cluster create --enable-node-affinity"
        " --data-chunks-per-stripe 2 --parity-chunks-per-stripe 2"
        + checksum_flag
    ], check=True)
    print("Phase 2a: DONE - cluster created.")

    print("Phase 2b: Configuring storage nodes...")
    with ThreadPoolExecutor(max_workers=len(sn_ips)) as executor:
        tasks = [executor.submit(ssh_exec, ip, [
            f"/usr/local/bin/sbctl -d sn configure --max-lvol {shlex.quote(args.max_lvol)}"
            + checksum_flag + (" --force" if inline_checksum else "")
        ], check=True) for ip in sn_ips]
        for t in tasks:
            t.result()
    print("Phase 2b: DONE - all SNs configured.")

    print("Phase 2c: Deploying storage nodes...")
    with ThreadPoolExecutor(max_workers=len(sn_ips)) as executor:
        tasks = [executor.submit(ssh_exec, ip, [
            f"/usr/local/bin/sbctl -d sn deploy --isolate-cores --ifname {shlex.quote(args.iface)}"
        ], check=True) for ip in sn_ips]
        for t in tasks:
            t.result()
    print("Phase 2c: DONE - all SNs deployed. Rebooting...")

    with ThreadPoolExecutor(max_workers=len(sn_ips)) as executor:
        [executor.submit(ssh_exec, ip, ["reboot"]) for ip in sn_ips]

    print("Waiting for SN reboot recovery...")
    time.sleep(30)
    for ip in sn_ips:
        wait_for_ssh(ip)
    print("All storage nodes back online after reboot.")

    print("Waiting 60s for SPDK containers to start...")
    time.sleep(60)

    # --- Phase 3: add nodes ---
    cluster_list = ssh_exec(mgmt_ip, ["/usr/local/bin/sbctl -d cluster list"], get_output=True)[0]
    cluster_match = re.search(r"([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})", cluster_list)
    if not cluster_match:
        raise Exception("Could not find Cluster UUID")
    cluster_uuid = cluster_match.group(1)
    print(f"Cluster UUID: {cluster_uuid}")

    print("Phase 3: Adding storage nodes to cluster...")
    for sn_ip in sn_ips:
        for attempt in range(5):
            try:
                ssh_exec(mgmt_ip, [
                    f"/usr/local/bin/sbctl -d sn add-node {cluster_uuid} {sn_ip}:5000 "
                    f"{shlex.quote(args.iface)} --data-nics {shlex.quote(args.data_iface)} "
                    f"--ha-jm-count 4"
                ], check=True)
                break
            except RuntimeError:
                if attempt < 4:
                    print(f"  Retrying add-node for {sn_ip} in 30s (attempt {attempt+2}/5)...")
                    time.sleep(30)
                else:
                    raise
    print("Phase 3: DONE - all nodes added.")

    print("Verifying node status...")
    sn_list = ssh_exec(mgmt_ip, ["/usr/local/bin/sbctl -d sn list"], get_output=True)[0]
    print(sn_list)
    online_count = sn_list.count("online")
    if online_count < SN_COUNT:
        raise Exception(f"Only {online_count} nodes online, expected {SN_COUNT}")
    print(f"Verified: {online_count} nodes online.")

    print("Phase 4: Activating cluster...")
    time.sleep(10)
    ssh_exec_stream(
        mgmt_ip,
        f"/usr/local/bin/sbctl -d cluster activate {cluster_uuid}",
        check=True,
    )
    print("Phase 4: DONE - cluster activated.")

    print("Creating pool...")
    ssh_exec(mgmt_ip, [
        f"/usr/local/bin/sbctl -d pool add pool01 {cluster_uuid}"
    ], check=True)
    print("Pool created.")

    # --- Phase 5: client prep on the mgmt node (it is the perf client in the lab) ---
    client_prep_cmds = [
        "dnf install nvme-cli fio -y",
        "modprobe nvme-tcp",
        "echo 'nvme-tcp' > /etc/modules-load.d/nvme-tcp.conf",
    ]
    print("Prepping client on mgmt node...")
    ssh_exec(mgmt_ip, client_prep_cmds, check=True)

    # --- Phase 6: metadata ---
    storage_metadata = [{"private_ip": ip, "public_ip": ip} for ip in sn_ips]
    client_metadata = [{"private_ip": mgmt_ip, "public_ip": mgmt_ip, "role": "mgmt+client"}]

    topology = fetch_cluster_topology(mgmt_ip, cluster_uuid)

    final_metadata = {
        "environment": "lab",
        "mgmt": {
            "public_ip": mgmt_ip,
            "private_ip": mgmt_ip,
        },
        "storage_nodes": storage_metadata,
        "clients": client_metadata,
        "cluster_uuid": cluster_uuid,
        "topology": topology,
        "user": USER,
        "iface": args.iface,
        "data_iface": args.data_iface,
        "branch": args.branch,
    }

    with open(args.metadata_out, "w") as f:
        json.dump(final_metadata, f, indent=4)

    try:
        alceml_summary = fetch_alceml_modes(mgmt_ip, cluster_uuid)
        print_alceml_summary(alceml_summary)
    except Exception as exc:
        print(f"WARNING: failed to fetch ALCEML mode summary: {exc}")

    print("\n--- Setup Complete ---")
    print(f"Cluster {cluster_uuid} is active. Metadata saved to {args.metadata_out}.")


if __name__ == "__main__":
    main()
