# coding=utf-8
"""Deploy simplyblock onto the provisioned e2e environment (= test 1).

Steps:
1. Wait for every k3s cluster to be Ready (cloud-init installed them).
2. Bootstrap the central control plane + the 3-node hyperscale storage
   cluster on the central workers. The CP bootstrap itself comes from the
   simplyblock-deploy repo (docs/k8s_mgmt.md); override the exact command
   with EDGE_E2E_BOOTSTRAP_CMD if your flow differs. After this step the
   state file must contain central.api_url / central.cluster_id /
   central.cluster_secret — set them manually if you bootstrap by hand.
3. For every edge cluster:
   - split the raw volume with sgdisk on the *-2p variants,
   - mint a ServiceAccount token + CA on the edge cluster for the CP,
   - create the edge cluster via POST /api/v2/clusters/edge,
   - add each node (device paths from the topology matrix) and wait ONLINE,
   - create the standard test volume.

Run:  python edge_e2e/deploy.py [--skip-central]
"""
import argparse
import base64
import json
import os
import pathlib
import subprocess
import sys

# Allow running as a script (`python edge_e2e/x.py`) as well as `-m`:
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from edge_e2e import helpers
from edge_e2e.topology import CENTRAL, EDGE_CLUSTERS

VOLUME_NAME = "edge-e2e-vol"
VOLUME_SIZE = 30 * 1024 ** 3
CENTRAL_POOL = "edge-e2e-pool"
CENTRAL_VOLUME = "edge-e2e-central-vol"

# The bootstrap scripts live in the PUBLIC simplyBlockDeploy repo, under
# bare-metal/ — same source the k8s e2e workflows use
# (.github/workflows/e2e-bootstrap-k8s.yml clones it and runs
# bare-metal/bootstrap-k3s.sh; k8s-e2e.yaml runs bootstrap-cluster.sh with
# the cluster geometry flags). k3s itself is already installed here by
# provision.py's cloud-init, so only the cluster bootstrap runs.
DEPLOY_REPO = "https://github.com/simplyblock-io/simplyBlockDeploy.git"

# bootstrap-cluster.sh takes its topology from ENVIRONMENT VARIABLES (MNODES,
# STORAGE_PRIVATE_IPS, KEY, ...) — the CLI flags only carry cluster geometry,
# and the flag names have moved on from the ones in the older k8s-e2e
# workflow (--max-lvol -> --max-subsys, --distr-ndcs ->
# --data-chunks-per-stripe). Verified against the script's own --help on a
# live node, 2026-08-10.
BOOTSTRAP_FLAGS = ("--sbcli-cmd sbctl --k8s-snode --ha-type ha "
                   "--max-subsys 10 --max-snap 10 --number-of-devices 1")


# bootstrap-cluster.sh targets simplyBlockDeploy's terraform topology: it
# SSHes to storage nodes as ROOT, through a BASTION (ProxyCommand), using a
# key path HARDCODED on line 4 (`KEY="$HOME/.ssh/simplyblock-us-east-2.pem"`
# — an assignment, not `${KEY:-...}`, so the env var is ignored). A flat
# public-subnet fleet has to be adapted to those three assumptions.
BOOTSTRAP_KEY_PATH = "~/.ssh/simplyblock-us-east-2.pem"

ENABLE_ROOT_SSH = (
    "sudo mkdir -p /root/.ssh && "
    "sudo cp /home/ubuntu/.ssh/authorized_keys /root/.ssh/authorized_keys && "
    "sudo chmod 600 /root/.ssh/authorized_keys && "
    "sudo sed -i 's/^#\\?PermitRootLogin.*/PermitRootLogin prohibit-password/' "
    "/etc/ssh/sshd_config && sudo systemctl reload ssh")


def prepare_bootstrap_ssh(state, key_path):
    """Give the bootstrap script the SSH shape it expects: root login on every
    central node, and the private key at its hardcoded filename on the mgmt
    node (which doubles as its own bastion)."""
    server = f"{CENTRAL.name}-mgmt"
    for node in [server] + list(state["central"]["workers"]):
        helpers.ssh(state, node, ENABLE_ROOT_SSH, timeout=300)
    subprocess.run(
        ["scp", "-i", key_path, *helpers.SSH_OPTS, key_path,
         f"{helpers.SSH_USER}@{helpers.instance(state, server)['public_ip']}:"
         f"{BOOTSTRAP_KEY_PATH.replace('~', '/home/ubuntu')}"],
        check=True, capture_output=True, timeout=300)
    helpers.ssh(state, server, f"chmod 600 {BOOTSTRAP_KEY_PATH}", timeout=120)


def default_bootstrap_cmd(state) -> str:
    """Clone the deploy repo and run the cluster bootstrap with this fleet's
    mgmt/storage private IPs and SSH key."""
    mgmt_ip = helpers.instance(state, f"{CENTRAL.name}-mgmt")["private_ip"]
    storage_ips = " ".join(
        helpers.instance(state, worker)["private_ip"]
        for worker in state["central"]["workers"])
    return (
        f"rm -rf simplyBlockDeploy && git clone -q {DEPLOY_REPO} simplyBlockDeploy && "
        "cd simplyBlockDeploy/bare-metal && chmod +x ./bootstrap-cluster.sh && "
        f"MNODES='{mgmt_ip}' STORAGE_PRIVATE_IPS='{storage_ips}' "
        f"BASTION_IP='{mgmt_ip}' "
        f"./bootstrap-cluster.sh {BOOTSTRAP_FLAGS}")


# sbctl is not on the image; the admin host needs it plus the FDB client
# (docs/k8s_mgmt.md step 2).
INSTALL_SBCTL = (
    "which sbctl >/dev/null 2>&1 || { "
    "sudo apt-get update -y && sudo apt-get install -y python3-pip && "
    "sudo pip3 install -q sbctl; }")


def wait_k3s_ready(state, server_name, expected_nodes):
    helpers.wait_for(
        f"k3s on {server_name}: {expected_nodes} Ready nodes",
        lambda: helpers.kubectl(
            state, server_name, "get nodes --no-headers", check=False
        ).count(" Ready") >= expected_nodes,
        timeout=900, interval=15)


def bootstrap_central(state):
    """Install the CP + hyperscale storage cluster on the central cluster."""
    server = f"{CENTRAL.name}-mgmt"
    wait_k3s_ready(state, server, expected_nodes=1 + CENTRAL.workers)

    print(f"Installing sbctl on {server}...")
    helpers.ssh(state, server, INSTALL_SBCTL, timeout=1800)

    key_path = state.get("key_path") or f"~/.ssh/{state['key_name']}.pem"
    print("Preparing bootstrap SSH (root login + hardcoded key path)...")
    prepare_bootstrap_ssh(state, pathlib.Path(key_path).expanduser().as_posix())

    command = os.getenv("EDGE_E2E_BOOTSTRAP_CMD") or default_bootstrap_cmd(state)
    print(f"Bootstrapping central CP on {server}...")
    print(helpers.ssh(state, server, command, timeout=3600))

    # The bootstrap prints/stores cluster id + secret; pick them up via sbctl.
    cluster_id = helpers.ssh(
        state, server, "sbctl cluster list --json | jq -r '.[0].uuid'").strip()
    secret = helpers.ssh(
        state, server, f"sbctl cluster get-secret {cluster_id}").strip()
    state["central"].update({
        "api_url": f"http://{helpers.instance(state, server)['public_ip']}",
        "cluster_id": cluster_id,
        "cluster_secret": secret,
    })
    helpers.save_state(state)


def prepare_central_workload(state):
    """Create the pool + lvol the central (hyperscale) cluster's fio pod runs
    against, and stash its connect info in the state file. Without this,
    test 2's central leg silently skips."""
    server = f"{CENTRAL.name}-mgmt"
    cluster_id = state["central"]["cluster_id"]

    pools = helpers.ssh(state, server, "sbctl storage-pool list --json", check=False)
    if CENTRAL_POOL not in pools:
        helpers.ssh(state, server,
                    f"sbctl storage-pool add {CENTRAL_POOL} {cluster_id}")

    volumes = helpers.ssh(state, server, "sbctl volume list --json", check=False)
    if CENTRAL_VOLUME not in volumes:
        helpers.ssh(state, server,
                    f"sbctl volume add {CENTRAL_VOLUME} {VOLUME_SIZE // 1024 ** 3}G "
                    f"{CENTRAL_POOL}")

    raw = helpers.ssh(state, server,
                      f"sbctl volume connect {CENTRAL_VOLUME} --json", check=False)
    entries = _parse_connect(raw)
    if not entries:
        raise RuntimeError(f"could not parse central connect info from: {raw[:400]}")
    state["central"]["fio_connect"] = entries
    helpers.save_state(state)
    print(f"central: workload volume {CENTRAL_VOLUME} ready ({len(entries)} path(s))")


def _parse_connect(raw) -> list:
    """Normalize `sbctl volume connect --json` output into the entry shape the
    fio pod builder consumes (ip/port/nqn), tolerating both the hyphenated
    v1 keys and the underscored variants."""
    try:
        payload = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return []
    if isinstance(payload, dict):
        payload = payload.get("results") or payload.get("data") or [payload]
    entries = []
    for item in payload if isinstance(payload, list) else []:
        if not isinstance(item, dict):
            continue
        ip = item.get("ip") or item.get("traddr")
        port = item.get("port") or item.get("trsvcid")
        nqn = item.get("nqn") or item.get("subnqn")
        if ip and port and nqn:
            entries.append({"ip": ip, "port": port, "nqn": nqn})
    return entries


def prepare_partitions(state, spec):
    """Split the raw data volume into N partitions on the *-2p variants."""
    for node_name in state["edge"][spec.name]["nodes"]:
        for index, drive in enumerate(spec.drives, start=1):
            if drive.partitions <= 1:
                continue
            device = f"/dev/nvme{index}n1"
            parts = " ".join(
                f"-n {p}:0:{'+{}G'.format(drive.size_gb // drive.partitions) if p < drive.partitions else '0'}"
                for p in range(1, drive.partitions + 1))
            helpers.ssh(state, node_name,
                        f"sudo sgdisk --zap-all {device} && sudo sgdisk {parts} {device} "
                        f"&& sudo partprobe {device}")


def mint_edge_credentials(state, spec) -> dict:
    """ServiceAccount token + CA the central CP uses against this edge k8s."""
    server = state["edge"][spec.name]["nodes"][0]
    helpers.kubectl(state, server, "create namespace simplyblock", check=False)
    helpers.kubectl(state, server,
                    "-n simplyblock create serviceaccount simplyblock-cp", check=False)
    helpers.kubectl(state, server,
                    "create clusterrolebinding simplyblock-cp "
                    "--clusterrole=cluster-admin "
                    "--serviceaccount=simplyblock:simplyblock-cp", check=False)
    token = helpers.kubectl(
        state, server,
        "-n simplyblock create token simplyblock-cp --duration=8760h").strip()
    ca_b64 = helpers.kubectl(
        state, server,
        "config view --raw -o jsonpath='{.clusters[0].cluster.certificate-authority-data}'"
    ).strip()
    api_url = f"https://{helpers.instance(state, server)['private_ip']}:6443"
    return {"api_url": api_url, "token": token,
            "ca_cert": base64.b64decode(ca_b64).decode()}


def deploy_edge_cluster(state, spec, admin_session):
    entry = state["edge"][spec.name]
    wait_k3s_ready(state, entry["nodes"][0], expected_nodes=spec.nodes)
    prepare_partitions(state, spec)
    credentials = mint_edge_credentials(state, spec)

    base = state["central"]["api_url"]
    response = admin_session.post(f"{base}/api/v2/clusters/edge", json={
        "name": spec.name,
        "k8s_api_url": credentials["api_url"],
        "k8s_token": credentials["token"],
        "k8s_ca_cert": credentials["ca_cert"],
    }, timeout=60)
    response.raise_for_status()
    created = response.json()
    entry.update({"cluster_id": created["uuid"], "secret": created["secret"]})
    helpers.save_state(state)

    api = helpers.EdgeApi(base, created["uuid"], created["secret"])
    for node_name in entry["nodes"]:
        node_info = helpers.instance(state, node_name)
        api.add_node(hostname=node_name, mgmt_ip=node_info["private_ip"],
                     partitions=entry["device_paths"],
                     spdk_cpus=int(os.getenv("EDGE_E2E_SPDK_CPUS", "1")))
        helpers.wait_node_status(api, node_name, "online", timeout=900)
    helpers.wait_cluster_status(api, "active", timeout=300)

    volume = api.create_volume(VOLUME_NAME, VOLUME_SIZE)
    entry["volume_id"] = volume["uuid"]
    helpers.save_state(state)
    print(f"{spec.name}: deployed, ACTIVE, volume {volume['uuid']}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--skip-central", action="store_true",
                        help="central already bootstrapped (state has api_url/secret)")
    args = parser.parse_args()

    state = helpers.load_state()
    if not args.skip_central:
        bootstrap_central(state)
    if not state["central"].get("api_url"):
        sys.exit("state.central.api_url missing — bootstrap central first")
    prepare_central_workload(state)

    import requests
    admin_session = requests.Session()
    admin_session.headers["Authorization"] = \
        f"Bearer {state['central']['cluster_secret']}"
    admin_session.verify = False

    failures = []
    for spec in EDGE_CLUSTERS:
        try:
            deploy_edge_cluster(state, spec, admin_session)
        except Exception as e:
            failures.append((spec.name, str(e)))
            print(f"FAILED {spec.name}: {e}")
    if failures:
        sys.exit(f"Deploy failed for: {failures}")
    print("All clusters deployed — test 1 passed.")


if __name__ == "__main__":
    main()
