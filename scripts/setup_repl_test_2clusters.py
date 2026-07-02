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
# replication work lives on new-failure-domain.
BRANCH = "new-failure-domain"

SN_TYPE = "i3en.2xlarge"
MGMT_TYPE = "m6i.2xlarge"
CLIENT_TYPE = "m6in.8xlarge"
CLIENT_COUNT = 1                            # client(s) used by the test process

USER = "ec2-user"
IFACE = "eth0"
MAX_LVOL = "100"

# --- Two-cluster topology on a single control plane ---
# The FIRST cluster (bootstrap=True) is created with `cluster create`; every
# other cluster is attached to the same CP with `cluster add`.
CLUSTERS = [
    {
        "name": "src",            # 1+1 HA pair
        "nodes": 2,
        "ndcs": 1,                # data-chunks-per-stripe
        "npcs": 1,                # parity-chunks-per-stripe (FT=1)
        "ha_jm_count": 2,         # <= node count
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
]

# Snapshot replication direction (source cluster -> target cluster).
REPLICATION = {"source": "src", "target": "tgt", "timeout": 3600}

SN_COUNT = sum(c["nodes"] for c in CLUSTERS)
SBCTL = "sudo /usr/local/bin/sbctl"

ec2 = boto3.resource("ec2", region_name="us-east-1")


# --------------------------------------------------------------------------- #
# SSH / AWS helpers (same patterns as setup_perf_test1.py)
# --------------------------------------------------------------------------- #
def wait_for_ssh(ip, timeout=300):
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


def ssh_exec(ip, cmds, get_output=False, check=False):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(ip, username=USER, key_filename=KEY_PATH,
                allow_agent=False, look_for_keys=False)
    results = []
    for cmd in cmds:
        print(f"  [{ip}] $ {cmd}")
        stdin, stdout, stderr = ssh.exec_command(cmd, timeout=600)
        out = stdout.read().decode("utf-8")
        err = stderr.read().decode("utf-8")
        rc = stdout.channel.recv_exit_status()
        if get_output:
            results.append(out)
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
        f"{SBCTL} -d cluster {verb} --enable-node-affinity"
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
    def add_one(priv_ip):
        for attempt in range(5):
            try:
                ssh_exec(mgmt_ip, [
                    f"{SBCTL} -d sn add-node {cluster_uuid} {priv_ip}:5000 {IFACE}"
                    f" --ha-jm-count {ha_jm_count}"
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
def main():
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
        for t in [ex.submit(ssh_exec, ip, [f"{SBCTL} -d sn configure --max-subsys {MAX_LVOL}"], check=True)
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
    print(f"Configuring replication {REPLICATION['source']} -> {REPLICATION['target']}...")
    ssh_exec(mgmt_ip, [
        f"{SBCTL} -d cluster add-replication {src_uuid} {tgt_uuid}"
        f" --target-pool {tgt_pool} --timeout {REPLICATION['timeout']}"
    ], check=True)

    # --- Phase 6: prep clients ---
    if client_pub_ips:
        print("Prepping clients...")
        client_cmds = [
            "sudo dnf install nvme-cli fio -y",
            "sudo modprobe nvme-tcp",
            "echo 'nvme-tcp' | sudo tee /etc/modules-load.d/nvme-tcp.conf",
        ]
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
