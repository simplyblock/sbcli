"""Refresh the cluster-specific blocks of a lab metadata JSON file.

After a redeploy, the lab metadata file (e.g. cluster_metadata_base.json)
still references the previous cluster's UUID, per-node UUIDs, and LVS
names. The soak scripts read those fields verbatim and fail on the very
first `sbctl cluster get <stale-uuid>`.

This script rewrites only the cluster-specific fields:
  - top-level ``cluster_uuid``
  - the ``topology`` block (cluster_uuid, cluster_nqn, nodes, lvstores)

Everything else (mgmt/storage_nodes/clients, user, password hints, etc.)
is preserved.

Run from the jump host, same environment as ``setup_lab_perf_test1.py``:

  python3 refresh_lab_metadata.py            # interactive password prompt
  python3 refresh_lab_metadata.py --password '...'
  python3 refresh_lab_metadata.py --metadata /root/cluster_metadata_base.json

Requires: sshpass + python3 on the runner; FoundationDB readable via the
mgmt node's installed simplyblock packages (same as setup_lab_perf_test1).
"""

import argparse
import getpass
import json
import os
import subprocess
import sys


SSH_OPTS = [
    "-o", "StrictHostKeyChecking=no",
    "-o", "UserKnownHostsFile=/dev/null",
    "-o", "LogLevel=ERROR",
    "-o", "ConnectTimeout=10",
    "-o", "ServerAliveInterval=30",
]


def ssh_run(ip, password, user, cmd, timeout=120):
    env = os.environ.copy()
    env["SSHPASS"] = password
    proc = subprocess.run(
        ["sshpass", "-e", "ssh", *SSH_OPTS, f"{user}@{ip}", cmd],
        env=env,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            f"SSH command failed on {ip} (rc={proc.returncode}): {cmd}\n"
            f"stderr: {proc.stderr.strip()}"
        )
    return proc.stdout


def resolve_cluster_uuid(mgmt_ip, password, user, override):
    if override:
        return override
    raw = ssh_run(mgmt_ip, password, user, "/usr/local/bin/sbctl cluster list --json")
    clusters = json.loads(raw)
    if not clusters:
        raise RuntimeError("`sbctl cluster list --json` returned no clusters")
    if len(clusters) > 1:
        print(
            f"WARNING: {len(clusters)} clusters present; using the first "
            f"({clusters[0]['UUID']}). Pass --cluster-uuid to disambiguate.",
            file=sys.stderr,
        )
    return clusters[0]["UUID"]


def fetch_cluster_topology(mgmt_ip, password, user, cluster_uuid):
    """Run the same FDB-querying snippet as setup_lab_perf_test1.fetch_cluster_topology."""
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
    out = ssh_run(mgmt_ip, password, user, script, timeout=180)
    try:
        return json.loads(out)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Failed to parse topology JSON from mgmt; got:\n{out}") from exc


def parse_args():
    here = os.path.dirname(os.path.abspath(__file__))
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--metadata",
        default=os.path.join(here, "cluster_metadata_base.json"),
        help="Path to the metadata JSON to refresh (default: ./cluster_metadata_base.json).",
    )
    parser.add_argument(
        "--password",
        help="Root password for SSH to the mgmt node. Falls back to SBCLI_ROOT_PASSWORD or interactive prompt.",
    )
    parser.add_argument(
        "--cluster-uuid",
        help="Override the cluster UUID to use. By default, the first cluster from `sbctl cluster list` is used.",
    )
    return parser.parse_args()


def resolve_password(cli_value):
    if cli_value:
        return cli_value
    env_value = os.environ.get("SBCLI_ROOT_PASSWORD")
    if env_value:
        return env_value
    return getpass.getpass("Root password for lab nodes: ")


def main():
    args = parse_args()

    with open(args.metadata, "r", encoding="utf-8") as handle:
        metadata = json.load(handle)

    mgmt_entry = metadata.get("mgmt") or {}
    mgmt_ip = mgmt_entry.get("public_ip") or mgmt_entry.get("private_ip")
    if not mgmt_ip:
        raise SystemExit(f"Metadata at {args.metadata} has no mgmt.public_ip/private_ip")
    user = metadata.get("user") or "root"

    password = resolve_password(args.password)

    cluster_uuid = resolve_cluster_uuid(mgmt_ip, password, user, args.cluster_uuid)
    print(f"Refreshing metadata for cluster {cluster_uuid}")

    topology = fetch_cluster_topology(mgmt_ip, password, user, cluster_uuid)

    old_uuid = metadata.get("cluster_uuid")
    metadata["cluster_uuid"] = cluster_uuid
    metadata["topology"] = topology

    with open(args.metadata, "w", encoding="utf-8") as handle:
        json.dump(metadata, handle, indent=2)
        handle.write("\n")

    if old_uuid and old_uuid != cluster_uuid:
        print(f"cluster_uuid: {old_uuid} -> {cluster_uuid}")
    node_count = len(topology.get("nodes", []))
    lvs_count = len(topology.get("lvstores", {}))
    print(f"topology: {node_count} storage nodes, {lvs_count} lvstores")
    print(f"Wrote {args.metadata}")


if __name__ == "__main__":
    main()
