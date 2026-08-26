"""Deploy ONE control plane managing TWO clusters for async-replication testing.

Topology (one management node / control plane):
    * Cluster "src"  — 2 storage nodes (1+1 HA pair)   -> bootstraps the CP
    * Cluster "tgt"  — 3 storage nodes                  -> added to the same CP
    * Replication configured src -> tgt (snapshot replication target).

This is the two-cluster analogue of setup_perf_test1.py. The control plane is
created once with `cluster create` (which also brings up FoundationDB and the
CP services); the second cluster is attached with `cluster add`.

IMPORTANT (deploying the async-replication code under test):
    The CP services and SPDK image come from the installed sbcli package and the
    Docker image, NOT just from this script. To exercise the new replication
    code (snapshot_replication retention, tasks_runner_replication_final, the
    new CLI/API), set BRANCH to the pushed branch AND make sure the swarm image
    ($SIMPLYBLOCK_DOCKER_IMAGE) was built from that branch — otherwise the new
    TasksRunnerReplicationFinal service / final-step RPC will be missing.
"""
import os
import json
import sys
import re
import time
from concurrent.futures import ThreadPoolExecutor

import boto3
import paramiko

# --- INPUT PARAMETERS ---
AMI_ID = "ami-0dfc569a8686b9320"           # Rocky 9 us-east-1
KEY_NAME = "mtes01"
KEY_PATH = os.path.expanduser("~/.ssh/mtes01.pem")
AZ = "us-east-1a"
SUBNET_ID = "subnet-0593459d6b931ee4c"
STORAGE_SG_ID = "sg-02e89a1372e9f39e9"

# Branch whose code (and matching Docker image) should be deployed. The async
# replication work has since merged to main (snapshot_replication,
# tasks_runner_replication_final, replication_final_step, cluster
# add-replication), so main is what we test; the default
# SIMPLY_BLOCK_DOCKER_IMAGE (simplyblock/simplyblock:main) matches it.
# Overridable for the repl_soak.py one-liner: SBCLI_BRANCH selects the sbcli
# checkout installed on every node (and the hotfix source), SPDK_IMAGE the
# pinned ultra image, SIMPLYBLOCK_DOCKER_IMAGE the control-plane image.
BRANCH = os.environ.get("SBCLI_BRANCH", "replication-features")

SN_TYPE = "i3en.2xlarge"
MGMT_TYPE = "m6i.2xlarge"
CLIENT_TYPE = "m6in.8xlarge"
CLIENT_COUNT = 2                            # client(s) used by the test process
#: 2, not 1: case 7 spreads 20 namespaced volumes across at least two
#: clients, and growing an existing lab with `add_client` after the fact
#: is an extra manual step before every namespaced run.

USER = "ec2-user"
IFACE = "eth0"
# Hard-capped by constants.MAX_SUBSYSTEMS_PER_NODE (75); `sn configure` rejects
# anything above it at ingress, so do not raise this without raising that.
MAX_LVOL = "75"

# --- Two-cluster topology on a single control plane ---
# The FIRST cluster (bootstrap=True) is created with `cluster create`; every
# other cluster is attached to the same CP with `cluster add`.
CLUSTERS = [
    # THREE nodes per cluster, everywhere. Two-node clusters are not a
    # supported configuration (product minimum is 3), and the 2026-08-24/25
    # campaign showed exactly why: with one node down the survivor holds 1 of
    # 2 journal members and the JC aborts it, and the restart rebalance has no
    # third failure domain to place into, so its device_migration loops on
    # "no allowed placement" forever and pins the cluster in REBALANCING.
    {
        "name": "src",
        "nodes": 3,
        "ndcs": 1,                # data-chunks-per-stripe
        "npcs": 1,                # parity-chunks-per-stripe (FT=1)
        "ha_jm_count": 3,
        "bootstrap": True,        # `cluster create`
        "pool": "pool_src",
    },
    {
        "name": "tgt",            # 3 nodes
        "nodes": 3,
        "ndcs": 1,
        "npcs": 1,
        "ha_jm_count": 3,
        "bootstrap": False,       # `cluster add`
        "pool": "pool_tgt",
    },
    # Pristine, never-written cluster used only by the fail-back-to-a-fresh-site
    # case (test_async_replication case4): failing back there must replicate the
    # FULL dataset, not a delta. Drop this entry (and save 2 instances) if you
    # only run cases 1-3/5/6.
    {
        "name": "fresh",
        "nodes": 3,
        "ndcs": 1,
        "npcs": 1,
        "ha_jm_count": 3,
        "bootstrap": False,
        "pool": "pool_fresh",
    },
]

# Snapshot replication direction (source cluster -> target cluster).
REPLICATION = {"source": "src", "target": "tgt", "timeout": 3600}

# SPDK image, pinned BY DIGEST. Two reasons this is a digest and not a tag:
# the replication-transfer pipeline fixes (dispatch window fill + fragmented
# parallel reads, spdk R26.3 bdd97c1d8/ce876a169) exist only from the
# 2026-08-22 build onward, and ultra:main-latest's manifest list has a live
# race that leaves its amd64 entry pointing at the PREVIOUS build (observed
# 2026-08-17, -21 and -22). This digest = main-d91ff03a-amd64, 2026-08-25:
# the first ultra build FROM spdk-core:master-latest (spdk master = R26.3
# merged + the ANA-transition change reverted). NOTE the digest printed in
# the CI push log is docker.io's; ECR's differs -- resolve it against
# public.ecr.aws (docker manifest inspect -v <tag>). Previous pin --
# the first build carrying the promotion-window ANA-transition fix
# (spdk R26.3 554c80f11), verified built FROM spdk-core:R26.3-latest
# whose manifest was created 18:41:57, before this ultra build started.
SPDK_IMAGE = os.environ.get(
    "SPDK_IMAGE",
    "public.ecr.aws/simply-block/ultra@sha256:d929b4d7ececee0fa0e1ad5973f87fa4e4cf79f7079cdf454bfbb27e2df51cb6")

SN_COUNT = sum(c["nodes"] for c in CLUSTERS)
SBCTL = "sudo /usr/local/bin/sbctl"

ec2 = boto3.resource("ec2", region_name="us-east-1")


# --------------------------------------------------------------------------- #
# SSH / AWS helpers (same patterns as setup_perf_test1.py)
# --------------------------------------------------------------------------- #
def wait_for_ssh(ip, timeout=900):
    # 300s was not enough headroom: a freshly launched instance can take
    # longer than that to finish cloud-init, and the deploy then aborts
    # with every instance healthy moments later (deploy 17).
    print(f"--> SSH handshake on {ip} ...")
    start = time.time()
    while time.time() - start < timeout:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            ssh.connect(ip, username=USER, key_filename=KEY_PATH, timeout=5,
                        banner_timeout=10, allow_agent=False, look_for_keys=False)
            ssh.close()
            print(f"SUCCESS: {ip} ready.")
            return True
        except Exception:
            pass
        time.sleep(2)
    raise RuntimeError(f"Timed out waiting for SSH on {ip}")


#: Overall budget for one remote command. `cluster create` brings up
#: FoundationDB and the whole CP service stack and prints NOTHING while it does
#: so; the old exec_command(timeout=600) was a per-read socket timeout, so ten
#: silent minutes killed the client while the command was still succeeding
#: (deploy 2026-08-20 17:06: the cluster was created, the deployer aborted with
#: a paramiko socket.timeout and left the lab 20% built). Silence is not
#: failure, so progress is judged by the channel's exit status, not by output.
LONG_CMD_TIMEOUT = 5400


def ssh_exec(ip, cmds, get_output=False, check=False, timeout=LONG_CMD_TIMEOUT):
    ssh = None
    results = []
    for cmd in cmds:
        print(f"  [{ip}] $ {cmd}")
        # Reconnect-retry on the transport phase only: a 10054 reset mid-run has
        # now killed two deployments at the finish line (same class as the
        # perf-deploy fix dea07344). A reset before exec_command returns means
        # the command never ran, so replaying is safe.
        for attempt in range(4):
            try:
                if ssh is None:
                    ssh = paramiko.SSHClient()
                    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                    ssh.connect(ip, username=USER, key_filename=KEY_PATH,
                                allow_agent=False, look_for_keys=False)
                stdin, stdout, stderr = ssh.exec_command(cmd)
                break
            except (paramiko.SSHException, OSError, EOFError) as exc:
                try:
                    ssh.close()
                except Exception:
                    pass
                ssh = None
                if attempt == 3:
                    raise
                print(f"  [{ip}] transport failure ({exc}); reconnecting in {10*(attempt+1)}s")
                time.sleep(10 * (attempt + 1))
        # Drain as output arrives and wait on the EXIT STATUS, not on the next
        # byte: a long silent command must not look like a dead one. Nothing
        # here blocks longer than `poll`, so the deadline is honoured even when
        # the command never writes a thing.
        chan = stdout.channel
        chan.settimeout(5)
        out_parts, err_parts = [], []
        deadline = time.time() + timeout

        # Stream as it arrives. Every command already runs with `sbctl -d`, but
        # buffering the output until the command returned meant a hang showed
        # NOTHING -- the 2026-08-20 `cluster create` stall was a blank screen
        # for ten minutes with the debug log sitting unread in the channel.
        # Printing each line as it lands is what makes a hang locatable.
        pending = {"out": "", "err": ""}

        def _emit(stream, text):
            pending[stream] += text
            while "\n" in pending[stream]:
                line, pending[stream] = pending[stream].split("\n", 1)
                if line.strip():
                    # Remote output is arbitrary UTF-8; a Windows console is often
                    # cp1252, and ONE unencodable character (a unicode arrow in an
                    # sbctl log line) killed a whole deployment (run 20260821_1932).
                    # Streaming must never be the thing that fails the run.
                    out = f"    [{ip}] {line.rstrip()}"
                    try:
                        print(out, flush=True)
                    except UnicodeEncodeError:
                        print(out.encode("ascii", "replace").decode("ascii"), flush=True)

        def _drain():
            while chan.recv_ready():
                chunk = chan.recv(65536).decode("utf-8", "replace")
                out_parts.append(chunk)
                _emit("out", chunk)
            while chan.recv_stderr_ready():
                chunk = chan.recv_stderr(65536).decode("utf-8", "replace")
                err_parts.append(chunk)
                _emit("err", chunk)

        started = time.time()
        last_beat = started
        while True:
            _drain()
            if chan.exit_status_ready():
                break
            if time.time() > deadline:
                ssh.close()
                raise RuntimeError(
                    f"Command exceeded {timeout}s on {ip}: {cmd}\n"
                    f"It may still be running remotely — verify the outcome "
                    f"before retrying, do not assume it failed.")
            # Say how long it has been quiet. A command that prints nothing is
            # indistinguishable from a dead one otherwise, which is how a slow
            # `cluster create` read as a hang.
            if time.time() - last_beat >= 60:
                last_beat = time.time()
                print(f"  [{ip}] still running after "
                      f"{int(time.time() - started)}s: {cmd.split()[-4:]}",
                      flush=True)
            time.sleep(2)
        _drain()
        out = "".join(out_parts)
        err = "".join(err_parts)
        rc = chan.recv_exit_status()
        if get_output:
            results.append(out)
        if rc == -1:
            # No exit status: the channel closed under us. `sn deploy
            # --isolate-cores` reconfigures the host and drops the session
            # while completing normally -- deploy 2026-08-20 18:07 aborted on
            # exactly this for two nodes whose SNodeAPI was up and healthy 26
            # minutes later. A lost channel says nothing about the outcome, so
            # do not call it a failure; the next phase (which needs SNodeAPI)
            # is the real verification and fails loudly if it truly did not run.
            print(f"  [{ip}] channel closed with no exit status: {cmd}")
            print(f"  [{ip}] the command may have completed; continuing, the "
                  f"next step verifies it")
            try:
                ssh.close()
            except Exception:
                pass
            ssh = None
            wait_for_ssh(ip, timeout=300)
            continue
        if rc != 0:
            print(f"  [{ip}] FAILED (rc={rc}): {cmd}")
            for line in (out + err).rstrip().split("\n")[-10:]:
                if line.strip():
                    print(f"    {line}")
            if check:
                ssh.close()
                raise RuntimeError(f"Command failed on {ip} (rc={rc}): {cmd}")
        else:
            for line in out.strip().split("\n")[-2:]:
                if line.strip():
                    print(f"    {line}")
    if ssh is not None:
        ssh.close()
    return results


def launch_instances(name, instance_type, count, root_gb=30, with_net=True):
    kwargs = dict(
        ImageId=AMI_ID, InstanceType=instance_type, MinCount=count, MaxCount=count,
        KeyName=KEY_NAME,
        BlockDeviceMappings=[{
            "DeviceName": "/dev/sda1",
            "Ebs": {"VolumeSize": root_gb, "DeleteOnTermination": True, "VolumeType": "gp3"},
        }],
        TagSpecifications=[{"ResourceType": "instance", "Tags": [{"Key": "Name", "Value": name}]}],
    )
    if with_net:
        kwargs["NetworkInterfaces"] = [{
            "DeviceIndex": 0, "SubnetId": SUBNET_ID,
            "Groups": [STORAGE_SG_ID], "AssociatePublicIpAddress": True,
        }]
    else:
        kwargs["Placement"] = {"AvailabilityZone": AZ}
    return ec2.create_instances(**kwargs)


def list_cluster_uuids(mgmt_ip):
    raw = ssh_exec(mgmt_ip, [f"{SBCTL} cluster list"], get_output=True)[0]
    return set(re.findall(r"[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}", raw))


def get_pool_uuid(mgmt_ip, pool_name):
    """Resolve a pool name to its UUID.

    `cluster add-replication --target-pool` documents "ID or name" but
    cluster_ops.add_replication only does get_pool_by_id(), so a name fails with
    a raw KeyError. Always hand it the UUID.
    """
    raw = ssh_exec(mgmt_ip, [f"{SBCTL} pool list"], get_output=True)[0]
    for line in raw.splitlines():
        cols = [c.strip() for c in line.split("|")]
        if len(cols) > 2 and cols[2] == pool_name:
            return cols[1]
    raise RuntimeError(f"Could not resolve UUID for pool {pool_name!r}")


def fetch_cluster_topology(mgmt_ip, cluster_uuid):
    """Reuse the topology dumper from setup_perf_test1 (kept identical)."""
    from setup_perf_test1 import fetch_cluster_topology as _f
    return _f(mgmt_ip, cluster_uuid)


# --------------------------------------------------------------------------- #
# Cluster bring-up
# --------------------------------------------------------------------------- #
def create_or_add_cluster(mgmt_ip, cfg):
    """Create (bootstrap) or add a cluster on the control plane; return its UUID."""
    before = set() if cfg["bootstrap"] else list_cluster_uuids(mgmt_ip)
    verb = "create" if cfg["bootstrap"] else "add"
    ssh_exec(mgmt_ip, [
        f"{SBCTL} -d cluster {verb} --enable-node-affinity --max-subsys {MAX_LVOL}"
        f" --data-chunks-per-stripe {cfg['ndcs']} --parity-chunks-per-stripe {cfg['npcs']}"
    ], check=True)
    after = list_cluster_uuids(mgmt_ip)
    new = after - before
    if len(new) != 1:
        raise RuntimeError(f"Expected exactly one new cluster for '{cfg['name']}', got {new}")
    uuid = new.pop()
    print(f"Cluster '{cfg['name']}' = {uuid}")
    return uuid


def add_nodes_to_cluster(mgmt_ip, cluster_uuid, priv_ips, ha_jm_count):
    jm_flag = f" --ha-jm-count {ha_jm_count}" if ha_jm_count else ""

    def add_one(priv_ip):
        for attempt in range(5):
            try:
                ssh_exec(mgmt_ip, [
                    # --dev unlocks --spdk-image (developer_mode, cli.py); the
                    # plain -d is only debug logging and does NOT.
                    f"{SBCTL} --dev -d sn add-node {cluster_uuid} {priv_ip}:5000 {IFACE}"
                    f"{jm_flag} --spdk-image {SPDK_IMAGE}"
                ], check=True)
                return
            except RuntimeError:
                if attempt < 4:
                    print(f"  retry add-node {priv_ip} in 30s ({attempt + 2}/5)...")
                    time.sleep(30)
                else:
                    raise

    with ThreadPoolExecutor(max_workers=max(1, len(priv_ips))) as ex:
        futures = {ex.submit(add_one, ip): ip for ip in priv_ips}
        for f in futures:
            f.result()


# --------------------------------------------------------------------------- #
CLIENT_PREP_CMDS = [
    "sudo dnf install nvme-cli fio -y",
    "sudo modprobe nvme-tcp",
    "echo 'nvme-tcp' | sudo tee /etc/modules-load.d/nvme-tcp.conf",
]


def add_client():
    """Add one client instance to an EXISTING deployment (case 7 needs >= 2
    clients; redeploying a healthy two-cluster lab for that is wasteful)."""
    with open("cluster_metadata_repl.json") as f:
        metadata = json.load(f)
    print("Launching 1 additional client...")
    clients = launch_instances("SB-Repl-Client", CLIENT_TYPE, 1)
    for inst in clients:
        inst.wait_until_running()
        inst.reload()
    ip = clients[0].public_ip_address
    wait_for_ssh(ip)
    print(f"Prepping client {ip}...")
    ssh_exec(ip, CLIENT_PREP_CMDS, check=True)
    metadata.setdefault("clients", []).append(
        {"public_ip": ip, "private_ip": clients[0].private_ip_address})
    with open("cluster_metadata_repl.json", "w") as f:
        json.dump(metadata, f, indent=4)
    print(f"Client added: {ip} ({len(metadata['clients'])} clients in metadata).")


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "add_client":
        add_client()
        return
    print(f"Launching control plane + {SN_COUNT} storage nodes + {CLIENT_COUNT} client(s)...")
    mgmt = launch_instances("SB-Repl-Mgmt", MGMT_TYPE, 1, with_net=False)
    sns = launch_instances("SB-Repl-Storage", SN_TYPE, SN_COUNT)
    clients = launch_instances("SB-Repl-Client", CLIENT_TYPE, CLIENT_COUNT) if CLIENT_COUNT else []

    all_instances = list(mgmt) + list(sns) + list(clients)
    for inst in all_instances:
        inst.wait_until_running()
        inst.reload()

    mgmt_ip = mgmt[0].public_ip_address
    sn_pub_ips = [i.public_ip_address for i in sns]
    sn_priv_ips = [i.private_ip_address for i in sns]
    client_pub_ips = [c.public_ip_address for c in clients]

    setup_ips = [mgmt_ip] + sn_pub_ips
    for ip in setup_ips:
        wait_for_ssh(ip)

    # --- Phase 1: install sbcli everywhere ---
    install_cmds = [
        "sudo dnf install git python3-pip nvme-cli -y",
        "sudo /usr/bin/python3 -m pip install --upgrade pip setuptools wheel",
        "sudo /usr/bin/python3 -m pip install ruamel.yaml",
        f"sudo pip install git+https://github.com/simplyblock-io/sbcli@{BRANCH}"
        " --upgrade --force --ignore-installed requests",
        "echo 'export PATH=/usr/local/bin:$PATH' >> ~/.bashrc",
    ]
    print(f"Phase 1: installing sbcli@{BRANCH} on {len(setup_ips)} nodes...")
    with ThreadPoolExecutor(max_workers=len(setup_ips)) as ex:
        for t in [ex.submit(ssh_exec, ip, install_cmds, check=True) for ip in setup_ips]:
            t.result()

    # --- Phase 2: control plane + two clusters ---
    print("Phase 2: creating control plane + clusters...")
    cluster_uuids = {}
    for cfg in CLUSTERS:                       # bootstrap cluster MUST be first
        cluster_uuids[cfg["name"]] = create_or_add_cluster(mgmt_ip, cfg)

    # Map storage instances to clusters in declaration order.
    sn_by_cluster = {}
    cursor = 0
    for cfg in CLUSTERS:
        n = cfg["nodes"]
        sn_by_cluster[cfg["name"]] = {
            "pub": sn_pub_ips[cursor:cursor + n],
            "priv": sn_priv_ips[cursor:cursor + n],
        }
        cursor += n

    # --- Phase 3: configure + deploy ALL storage nodes ---
    print("Phase 3a: configuring storage nodes...")
    with ThreadPoolExecutor(max_workers=len(sn_pub_ips)) as ex:
        for t in [ex.submit(ssh_exec, ip, [f"{SBCTL} -d sn configure"], check=True)
                  for ip in sn_pub_ips]:
            t.result()

    print("Phase 3b: deploying storage nodes...")
    with ThreadPoolExecutor(max_workers=len(sn_pub_ips)) as ex:
        for t in [ex.submit(ssh_exec, ip, [f"{SBCTL} -d sn deploy --isolate-cores --ifname {IFACE}"], check=True)
                  for ip in sn_pub_ips]:
            t.result()

    print("Phase 3c: rebooting storage nodes...")
    with ThreadPoolExecutor(max_workers=len(sn_pub_ips)) as ex:
        [ex.submit(ssh_exec, ip, ["sudo reboot"]) for ip in sn_pub_ips]
    time.sleep(30)
    for ip in sn_pub_ips:
        wait_for_ssh(ip)
    print("Waiting 60s for SPDK containers...")
    time.sleep(60)

    # --- Phase 4: add nodes to their clusters, then activate ---
    for cfg in CLUSTERS:
        name = cfg["name"]
        uuid = cluster_uuids[name]
        priv = sn_by_cluster[name]["priv"]
        print(f"Phase 4: adding {len(priv)} nodes to cluster '{name}' ({uuid})...")
        add_nodes_to_cluster(mgmt_ip, uuid, priv, cfg["ha_jm_count"])

    sn_list = ssh_exec(mgmt_ip, [f"{SBCTL} -d sn list"], get_output=True)[0]
    online = sn_list.count("online")
    if online < SN_COUNT:
        raise RuntimeError(f"Only {online}/{SN_COUNT} nodes online")
    print(f"Verified: {online} nodes online.")

    for cfg in CLUSTERS:
        uuid = cluster_uuids[cfg["name"]]
        print(f"Activating cluster '{cfg['name']}' ({uuid})...")
        time.sleep(10)
        ssh_exec(mgmt_ip, [f"{SBCTL} -d cluster activate {uuid}"], check=True)

    # --- Phase 5: pools + replication ---
    for cfg in CLUSTERS:
        uuid = cluster_uuids[cfg["name"]]
        print(f"Creating pool '{cfg['pool']}' in cluster '{cfg['name']}'...")
        ssh_exec(mgmt_ip, [f"{SBCTL} -d pool add {cfg['pool']} {uuid}"], check=True)

    src_uuid = cluster_uuids[REPLICATION["source"]]
    tgt_uuid = cluster_uuids[REPLICATION["target"]]
    tgt_pool = next(c["pool"] for c in CLUSTERS if c["name"] == REPLICATION["target"])
    tgt_pool_uuid = get_pool_uuid(mgmt_ip, tgt_pool)
    # Register the destination and a cadence policy. NOT `cluster
    # add-replication`: that deprecated verb writes
    # cluster.snapshot_replication_target_pool, and the replication service
    # reads that field off the DESTINATION cluster -- so a cluster configured
    # as a source hands out its target's pool when something later replicates
    # INTO it (fail-back). Targets and policies keep the pool where it belongs.
    print(f"Configuring replication {REPLICATION['source']} -> {REPLICATION['target']}"
          f" (target pool {tgt_pool} = {tgt_pool_uuid})...")
    ssh_exec(mgmt_ip, [
        f"{SBCTL} -d cluster replication-target-add {src_uuid} tgt_{tgt_uuid[:8]}"
        f" {tgt_uuid} --target-pool {tgt_pool_uuid} --timeout {REPLICATION['timeout']}"
    ], check=True)

    # --- Phase 6: prep clients ---
    if client_pub_ips:
        print("Prepping clients...")
        client_cmds = CLIENT_PREP_CMDS
        for ip in client_pub_ips:
            wait_for_ssh(ip)
        with ThreadPoolExecutor(max_workers=max(1, len(client_pub_ips))) as ex:
            for t in [ex.submit(ssh_exec, ip, client_cmds, check=True) for ip in client_pub_ips]:
                t.result()

    # --- Phase 7: metadata ---
    clusters_meta = {}
    for cfg in CLUSTERS:
        name = cfg["name"]
        uuid = cluster_uuids[name]
        clusters_meta[name] = {
            "cluster_uuid": uuid,
            "pool": cfg["pool"],
            "nodes": cfg["nodes"],
            "storage_public_ips": sn_by_cluster[name]["pub"],
            "storage_private_ips": sn_by_cluster[name]["priv"],
            "topology": fetch_cluster_topology(mgmt_ip, uuid),
        }

    metadata = {
        "mgmt": {"public_ip": mgmt_ip, "private_ip": mgmt[0].private_ip_address},
        "clusters": clusters_meta,
        "replication": {
            "source_cluster": src_uuid,
            "target_cluster": tgt_uuid,
            "target_pool": tgt_pool,
            "timeout": REPLICATION["timeout"],
        },
        "clients": [{"public_ip": c.public_ip_address, "private_ip": c.private_ip_address}
                    for c in clients],
        "user": USER,
        "key_path": KEY_PATH,
        "branch": BRANCH,
    }
    with open("cluster_metadata_repl.json", "w") as f:
        json.dump(metadata, f, indent=4)

    print("\n--- Two-cluster control plane ready ---")
    print(f"  mgmt:   {mgmt_ip}")
    for name, m in clusters_meta.items():
        print(f"  cluster {name}: {m['cluster_uuid']} ({m['nodes']} nodes, pool {m['pool']})")
    print(f"  replication: {src_uuid} -> {tgt_uuid} (target pool {tgt_pool})")
    print("  metadata: cluster_metadata_repl.json")


if __name__ == "__main__":
    main()
