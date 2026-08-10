# coding=utf-8
"""Re-run the node bootstrap (packages + k3s) on an already-provisioned fleet.

Cloud-init runs once at first boot; if its user-data script failed (e.g. a bad
package name aborting `set -e` before the k3s install), the instances are up
but empty. Rather than pay for a re-provision, this replays the corrected
bootstrap over SSH using the tokens/roles recorded in state.json.

Idempotent: skips a node whose k3s is already serving.

    python edge_e2e/repair_bootstrap.py
"""
import concurrent.futures
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from edge_e2e import helpers
from edge_e2e.topology import CENTRAL

PACKAGES = ("sudo apt-get update -y && "
            "sudo apt-get install -y curl nvme-cli fio gdisk jq")

SERVER = ("curl -sfL https://get.k3s.io | sudo K3S_TOKEN={token} sh -s - server "
          "--write-kubeconfig-mode 644 --disable traefik --node-name {node_name}")

AGENT = ("until curl -sk https://{server_ip}:6443 >/dev/null 2>&1; do sleep 5; done; "
         "curl -sfL https://get.k3s.io | sudo K3S_URL=https://{server_ip}:6443 "
         "K3S_TOKEN={token} sh -s - agent --node-name {node_name}")


def _already_up(state, node_name) -> bool:
    out = helpers.ssh(state, node_name, "which kubectl k3s 2>/dev/null | head -1",
                      check=False, timeout=60)
    return bool(out.strip())


def bootstrap(state, node_name, role, token, server_ip=None):
    if _already_up(state, node_name):
        return f"{node_name}: already bootstrapped, skipped"
    helpers.ssh(state, node_name, PACKAGES, timeout=900)
    command = (SERVER.format(token=token, node_name=node_name) if role == "server"
               else AGENT.format(server_ip=server_ip, token=token, node_name=node_name))
    helpers.ssh(state, node_name, command, timeout=900)
    return f"{node_name}: {role} installed"


def main():
    state = helpers.load_state()
    jobs = []

    # central: server first (agents need its API up), then workers.
    central_server = state["central"]["server"]
    central_token = state["central"]["token"]
    print(bootstrap(state, central_server, "server", central_token))
    central_ip = helpers.instance(state, central_server)["private_ip"]
    for worker in state["central"]["workers"]:
        jobs.append((worker, "agent", central_token, central_ip))

    for name, entry in state["edge"].items():
        server_name = entry["nodes"][0]
        print(bootstrap(state, server_name, "server", entry["token"]))
        server_ip = helpers.instance(state, server_name)["private_ip"]
        for agent in entry["nodes"][1:]:
            jobs.append((agent, "agent", entry["token"], server_ip))

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(bootstrap, state, *job): job[0] for job in jobs}
        for future in concurrent.futures.as_completed(futures):
            try:
                print(future.result())
            except Exception as e:
                print(f"{futures[future]}: FAILED {e}")

    print("\n--- cluster readiness")
    for server_name, expected in [(central_server, 1 + CENTRAL.workers)] + [
            (entry["nodes"][0], len(entry["nodes"]))
            for entry in state["edge"].values()]:
        out = helpers.ssh(state, server_name, "sudo kubectl get nodes --no-headers",
                          check=False, timeout=60)
        ready = out.count(" Ready")
        print(f"{server_name}: {ready}/{expected} Ready")


if __name__ == "__main__":
    main()
