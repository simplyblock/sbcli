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

Run:  python e2e/edge/deploy.py [--skip-central]
"""
import argparse
import base64
import os
import sys

from e2e.edge import helpers
from e2e.edge.topology import CENTRAL, EDGE_CLUSTERS

VOLUME_NAME = "edge-e2e-vol"
VOLUME_SIZE = 30 * 1024 ** 3

DEFAULT_BOOTSTRAP_CMD = (
    "git clone https://github.com/simplyblock/simplyblock-deploy.git || true; "
    "cd simplyblock-deploy && sudo ./bootstrap-cluster.sh --mode kubernetes")


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
    command = os.getenv("EDGE_E2E_BOOTSTRAP_CMD", DEFAULT_BOOTSTRAP_CMD)
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
