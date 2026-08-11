# coding=utf-8
"""Deploy simplyblock onto the provisioned e2e environment (= test 1).

Steps:
1. Wait for every k3s cluster to be Ready (cloud-init installed them).
2. Install the simplyblock stack on the CENTRAL cluster with the operator's
   Helm chart (control plane + operator + cert-manager + CSI), wait for the
   ControlPlane CR to report Ready, then declare the 3-node hyperscale
   storage cluster as StorageCluster/StorageNode CRs. Override the install
   with EDGE_E2E_BOOTSTRAP_CMD if your flow differs; after this step the
   state file carries central.api_url / cluster_id / cluster_secret.
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
import sys

# Allow running as a script (`python edge_e2e/x.py`) as well as `-m`:
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from edge_e2e import helpers
from edge_e2e.topology import CENTRAL, EDGE_CLUSTERS

VOLUME_NAME = "edge-e2e-vol"
VOLUME_SIZE = 30 * 1024 ** 3
CENTRAL_POOL = "edge-e2e-pool"
CENTRAL_VOLUME = "edge-e2e-central-vol"

# --- Central control-plane install (k8s-native) ------------------------------
#
# simplyblock is installed on kubernetes as a whole via the operator's Helm
# chart (control plane + operator + cert-manager + CSI), per
# https://docs.simplyblock.io/latest/deployments/kubernetes/ . The operator
# sits ON TOP of the control plane: its CRDs (ControlPlane, StorageCluster,
# StorageNode, Pool, ...) are thin mirrors of the sbcli API, which stays the
# source of truth. So the campaign installs the stack with helm, declares the
# central storage cluster + nodes as CRs, and then drives EDGE clusters
# through the v2 API (the operator has no edge CRs yet — that is follow-up
# work that will consume these same APIs).
#
# NB: the bare-metal bootstrap-cluster.sh path is deliberately NOT used: it
# assumes a terraform/bastion topology with root SSH and a docker daemon on
# the management host, none of which belong in a kubernetes-only deployment.
# --- Which BUILD of simplyblock to deploy ------------------------------------
#
# Every push to any branch is built and published by .github/workflows/
# docker-image.yml as simplyblock/simplyblock:<branch> and
# public.ecr.aws/simply-block/simplyblock:<branch>-<sha8> (the soak scripts in
# scripts/ pin exactly that, e.g. SB_TAG = "md-journal-05ed69d6"). The chart
# otherwise installs the RELEASED image, which does not contain the edge API —
# POST /clusters/edge would 404. Pin the branch build instead.
SB_REGISTRY = os.getenv("EDGE_E2E_REGISTRY", "public.ecr.aws/simply-block/simplyblock")


def _git(*args) -> str:
    import subprocess
    return subprocess.run(["git", *args], cwd=pathlib.Path(__file__).parent.parent,
                          capture_output=True, text=True).stdout.strip()


def sb_image() -> str:
    """<registry>:<branch>-<sha8> for the checked-out commit, or an explicit
    EDGE_E2E_SB_IMAGE override."""
    override = os.getenv("EDGE_E2E_SB_IMAGE")
    if override:
        return override
    branch = (os.getenv("EDGE_E2E_BRANCH")
              or _git("rev-parse", "--abbrev-ref", "HEAD")).replace("/", "-")
    sha8 = _git("rev-parse", "HEAD")[:8]
    return f"{SB_REGISTRY}:{branch}-{sha8}"


SB_BRANCH = os.getenv("EDGE_E2E_BRANCH") or _git("rev-parse", "--abbrev-ref", "HEAD")

HELM_REPO_NAME = "simplyblock"
HELM_REPO_URL = os.getenv(
    "EDGE_E2E_HELM_REPO", "https://simplyblock.github.io/helm-charts/charts")
HELM_RELEASE = "simplyblock-operator"
HELM_CHART = f"{HELM_REPO_NAME}/simplyblock-operator"
K8S_NAMESPACE = os.getenv("EDGE_E2E_NAMESPACE", "simplyblock")
CENTRAL_CLUSTER_CR = "edge-e2e-central"
# The CRD validator requires maxLogicalVolumeCount, workerNodes and
# mgmtIfname whenever `action` is not set. ens5 is the nitro primary NIC.
CENTRAL_MGMT_IFNAME = os.getenv("EDGE_E2E_MGMT_IFNAME", "ens5")
CENTRAL_MAX_LVOLS = int(os.getenv("EDGE_E2E_MAX_LVOLS", "10"))

INSTALL_HELM = (
    "command -v helm >/dev/null 2>&1 || "
    "curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 "
    "| sudo bash")

# Install the CLI from the SAME branch as the image (scripts/setup_lblk_*.py
# use `pip install git+https://github.com/simplyblock-io/sbcli@<branch>`).
INSTALL_SBCTL = (
    "sudo apt-get update -y && sudo apt-get install -y python3-pip git && "
    f"sudo pip3 install -q --upgrade 'git+https://github.com/simplyblock/sbcli@{SB_BRANCH}'")


# k3s writes its admin kubeconfig here; helm run under sudo has no
# ~/.kube/config and would otherwise fall back to localhost:8080.
KUBECONFIG = "/etc/rancher/k3s/k3s.yaml"


def helm_install_cmd() -> str:
    helm = f"sudo KUBECONFIG={KUBECONFIG} helm"
    image = sb_image()
    repository, tag = image.rsplit(":", 1)
    return (
        f"{helm} repo add {HELM_REPO_NAME} {HELM_REPO_URL} && "
        f"{helm} repo update && "
        f"{helm} upgrade --install {HELM_RELEASE} {HELM_CHART} "
        f"--namespace {K8S_NAMESPACE} --create-namespace "
        f"--set image.repository={repository} --set image.tag={tag} "
        f"--wait --timeout 20m")


def storage_cluster_manifest(worker_names) -> str:
    """Central hyperscale cluster as CRs, matching the schema of the CHART
    THAT IS INSTALLED (26.2.8), read from the live CRD via `kubectl explain`
    — not from the operator's main-branch Go types, which describe a newer
    API (a StorageNodeSet layer that this chart does not ship, and a
    StorageNode keyed by storageNodeSetRef).

    Here a single StorageNode CR carries `clusterName` plus the `workerNodes`
    list.
    """
    workers = "".join(f"\n    - {name}" for name in worker_names)
    return f"""apiVersion: storage.simplyblock.io/v1alpha1
kind: StorageCluster
metadata:
  name: {CENTRAL_CLUSTER_CR}
  namespace: {K8S_NAMESPACE}
spec:
  haType: ha
  blockSize: 512
---
apiVersion: storage.simplyblock.io/v1alpha1
kind: StorageNode
metadata:
  name: {CENTRAL_CLUSTER_CR}-nodes
  namespace: {K8S_NAMESPACE}
spec:
  clusterName: {CENTRAL_CLUSTER_CR}
  maxLogicalVolumeCount: {CENTRAL_MAX_LVOLS}
  mgmtIfname: {CENTRAL_MGMT_IFNAME}
  workerNodes:{workers}
"""


def wait_k3s_ready(state, server_name, expected_nodes):
    helpers.wait_for(
        f"k3s on {server_name}: {expected_nodes} Ready nodes",
        lambda: helpers.kubectl(
            state, server_name, "get nodes --no-headers", check=False
        ).count(" Ready") >= expected_nodes,
        timeout=900, interval=15)


def bootstrap_central(state):
    """Install the simplyblock stack on the central k3s cluster via the
    operator Helm chart, then declare the hyperscale storage cluster as CRs
    and record the API endpoint + credentials for the campaign."""
    server = f"{CENTRAL.name}-mgmt"
    wait_k3s_ready(state, server, expected_nodes=1 + CENTRAL.workers)

    print(f"Installing helm + sbctl on {server}...")
    helpers.ssh(state, server, INSTALL_HELM, timeout=900)
    helpers.ssh(state, server, INSTALL_SBCTL, timeout=1800)

    command = os.getenv("EDGE_E2E_BOOTSTRAP_CMD") or helm_install_cmd()
    print(f"Installing simplyblock via helm on {server}...")
    print(helpers.ssh(state, server, command, timeout=3600)[-2000:])

    # Poll from here with SHORT ssh calls rather than holding one session open
    # for a 10-minute `kubectl wait`: a dropped session failed the whole deploy
    # even though the control plane was still converging.
    print("Waiting for the ControlPlane to report Ready...")
    helpers.wait_for(
        "ControlPlane phase=Ready",
        lambda: "Ready" in helpers.ssh(
            state, server,
            f"sudo kubectl -n {K8S_NAMESPACE} get controlplane "
            "-o jsonpath='{.items[*].status.phase}'",
            check=False, timeout=90),
        timeout=1800, interval=20)

    print("Declaring the central StorageCluster + StorageNodes...")
    manifest = storage_cluster_manifest(state["central"]["workers"])
    helpers.ssh(state, server,
                f"cat <<'EOF' | sudo kubectl apply -f -\n{manifest}\nEOF",
                timeout=300)

    cluster_id = helpers.wait_for(
        "central StorageCluster to report its backend UUID",
        lambda: helpers.ssh(
            state, server,
            f"sudo kubectl -n {K8S_NAMESPACE} get storagecluster "
            f"{CENTRAL_CLUSTER_CR} -o jsonpath='{{.status.uuid}}'",
            check=False, timeout=60).strip() or False,
        timeout=2400, interval=20)

    # The operator publishes the cluster credentials as a k8s Secret
    # (simplyblock-cluster-<cr name>, keys: uuid + secret). Read them from
    # there rather than via `sbctl cluster get-secret`: sbctl on the admin
    # host has no FDB client configured ("kv_store is required for reading
    # from DB"), and the Secret is the k8s-native source anyway.
    secret = helpers.ssh(
        state, server,
        f"sudo kubectl -n {K8S_NAMESPACE} get secret "
        f"simplyblock-cluster-{CENTRAL_CLUSTER_CR} "
        "-o jsonpath='{.data.secret}' | base64 -d").strip()
    # The management API is a ClusterIP service (simplyblock-webappapi:5000)
    # with no ingress — nothing listens on port 80 of the node. Expose it as a
    # NodePort so the campaign (which drives the v2 API from outside the
    # cluster) can reach it.
    helpers.ssh(state, server,
                f"sudo kubectl -n {K8S_NAMESPACE} patch svc simplyblock-webappapi "
                "-p '{\"spec\":{\"type\":\"NodePort\"}}'", check=False, timeout=120)
    node_port = helpers.wait_for(
        "webappapi NodePort",
        lambda: helpers.ssh(
            state, server,
            f"sudo kubectl -n {K8S_NAMESPACE} get svc simplyblock-webappapi "
            "-o jsonpath='{.spec.ports[0].nodePort}'",
            check=False, timeout=60).strip() or False,
        timeout=300, interval=10)

    state["central"].update({
        "api_url": f"http://{helpers.instance(state, server)['public_ip']}:{node_port}",
        "cluster_id": cluster_id,
        "cluster_secret": secret,
        "namespace": K8S_NAMESPACE,
    })
    helpers.save_state(state)
    print(f"central: control plane up, cluster {cluster_id}")


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
    # The central fio leg is a NICE-TO-HAVE for test 2; the campaign's purpose
    # is the EDGE clusters. sbctl on the admin host has no FDB client in a k8s
    # deployment, so pool/volume creation via sbctl fails there — and in k8s
    # the native path is a Pool CR + a PVC through the CSI driver (there is no
    # Volume CRD). Until that is wired, don't let it block the edge run: test 2
    # already skips the central leg when fio_connect is absent.
    try:
        prepare_central_workload(state)
    except Exception as e:
        print(f"WARNING: central workload not prepared ({e}); "
              "test 2 will run on the edge clusters only")

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
