#!/usr/bin/env python3
"""Deploy the bed of the replication service-class benchmark.

Two equal clusters on one control plane (source and target), three storage
nodes each, plus one fio client per source storage node. It is
setup_repl_test_2clusters.py with this benchmark's topology, so everything that
script documents (AMI, key, subnet, the replication target it registers, the
cluster_metadata_repl.json it writes) holds here too.

    SPDK_IMAGE=<ultra image with the transfer-class spdk change> \\
    python scripts/svcclass_bench/deploy.py

Environment:
    SPDK_IMAGE         storage-node image (required: the default pin predates
                       the per-transfer read class)
    SBCLI_BRANCH       sbcli installed on the nodes (default main); the control
                       plane gets this branch's code from hotfix_repl_lab.py
    BENCH_SN_TYPE      storage-node instance type (default i3en.6xlarge)
    BENCH_CLIENT_TYPE  client instance type (default m6in.8xlarge)
    BENCH_NODES        storage nodes per cluster (default 3)
    BENCH_NDCS/NPCS    erasure coding of both clusters (default 1+1)
"""
import os
import subprocess
import sys
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(SCRIPTS))
os.environ.setdefault("SBCLI_BRANCH", "main")

if not os.environ.get("SPDK_IMAGE"):
    sys.exit("SPDK_IMAGE must name the ultra image built with the transfer-class spdk change")

import setup_repl_test_2clusters as base  # noqa: E402  (reads the environment at import)

nodes = int(os.environ.get("BENCH_NODES", "3"))
ndcs = int(os.environ.get("BENCH_NDCS", "1"))
npcs = int(os.environ.get("BENCH_NPCS", "1"))

base.SN_TYPE = os.environ.get("BENCH_SN_TYPE", "i3en.6xlarge")
base.CLIENT_TYPE = os.environ.get("BENCH_CLIENT_TYPE", "m6in.8xlarge")
base.CLUSTERS = [
    {"name": "src", "nodes": nodes, "ndcs": ndcs, "npcs": npcs, "ha_jm_count": 3,
     "bootstrap": True, "pool": "pool_src"},
    {"name": "tgt", "nodes": nodes, "ndcs": ndcs, "npcs": npcs, "ha_jm_count": 3,
     "bootstrap": False, "pool": "pool_tgt"},
]
base.SN_COUNT = 2 * nodes
base.CLIENT_COUNT = nodes          # one volume per source node, one client per volume

# A private image: the instances carry no IAM profile, so each storage node is
# logged in to the registry and pulls the image before it is added. The node
# API starts the SPDK container with `containers.run`, which pulls only what is
# missing, so a pre-pulled image is all it needs.
_launched = []
_launch = base.launch_instances


def _launch_and_remember(*a, **kw):
    # The mgmt node hosts FoundationDB AND the monitoring stack (OpenSearch,
    # Graylog), which collects every service's and SPDK's logs: over a
    # multi-hour benchmark that outgrew the 30 GB default, the disk filled,
    # and FoundationDB stalled the whole control plane.
    if a and a[0] == "SB-Repl-Mgmt":
        kw["root_gb"] = 100
    insts = _launch(*a, **kw)
    _launched.extend(insts)
    return insts


_add_nodes = base.add_nodes_to_cluster
_pulled = set()


def _pull_then_add(mgmt_ip, cluster_uuid, priv_ips, ha_jm_count):
    image = base.SPDK_IMAGE
    registry = image.split("/")[0]
    if ".dkr.ecr." in registry:
        region = registry.split(".")[3]
        password = subprocess.run(["aws", "ecr", "get-login-password", "--region", region],
                                  check=True, capture_output=True, text=True).stdout.strip()
        by_priv = {i.private_ip_address: i.public_ip_address for i in _launched}
        for priv in priv_ips:
            if priv in _pulled:
                continue
            _login(by_priv[priv], registry, password)
            base.ssh_exec(by_priv[priv], [f"sudo docker pull -q {image}"], check=True)
            _pulled.add(priv)
    return _add_nodes(mgmt_ip, cluster_uuid, priv_ips, ha_jm_count)


def _login(ip, registry, password):
    """docker login with the token on stdin: base.ssh_exec echoes every
    command it runs, and a registry token must not reach the deploy log."""
    import paramiko
    c = paramiko.SSHClient()
    c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    c.connect(ip, username=base.USER, key_filename=base.KEY_PATH, timeout=30)
    try:
        stdin, out, err = c.exec_command(
            f"sudo docker login --username AWS --password-stdin {registry}")
        stdin.write(password + "\n")
        stdin.channel.shutdown_write()
        if out.channel.recv_exit_status() != 0:
            raise RuntimeError(f"{ip}: docker login failed: {err.read().decode(errors='replace')}")
    finally:
        c.close()


base.launch_instances = _launch_and_remember
base.add_nodes_to_cluster = _pull_then_add

def _patch_mgmt_host(mgmt_ip, rel_paths):
    """Copy this tree's versions of *rel_paths* over the mgmt host's installed
    sbcli, for fixes the deploy itself needs before hotfix_repl_lab.py runs."""
    import paramiko
    c = paramiko.SSHClient()
    c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    c.connect(mgmt_ip, username=base.USER, key_filename=base.KEY_PATH, timeout=30)
    try:
        _, out, _ = c.exec_command("sudo $(head -1 /usr/local/bin/sbctl | sed 's/^#!//') -c "
                                   "'import os, simplyblock_core as m; print(os.path.dirname(m.__file__))'")
        site = out.read().decode().strip().splitlines()[-1]
        with c.open_sftp() as s:
            for rel in rel_paths:
                local = SCRIPTS.parent / rel
                s.put(str(local), f"/tmp/{local.name}")
                target = f"{site}/{Path(rel).relative_to('simplyblock_core').as_posix()}"
                _, o, e = c.exec_command(f"sudo cp /tmp/{local.name} {target} && "
                                         f"sudo find {site} -name __pycache__ -prune -exec rm -rf {{}} +")
                if o.channel.recv_exit_status() != 0:
                    raise RuntimeError(f"patching {target}: {e.read().decode()}")
                print(f"patched {target}")
    finally:
        c.close()


def resume():
    """Continue a deploy that failed after the clusters were created.

    Instances are found by their purpose=svcclass-bench tag and Name, the
    clusters by `cluster list`: the one that already has nodes is the source.
    Storage nodes in no cluster yet are added to the target, then everything
    after phase 4 of the base script runs as it would have.
    """
    import json
    import time
    by_name = {"SB-Repl-Mgmt": [], "SB-Repl-Storage": [], "SB-Repl-Client": []}
    for inst in base.ec2.instances.filter(Filters=[
            {"Name": "tag:purpose", "Values": ["svcclass-bench"]},
            {"Name": "instance-state-name", "Values": ["running"]}]):
        name = next(t["Value"] for t in inst.tags if t["Key"] == "Name")
        by_name.setdefault(name, []).append(inst)
    (mgmt,) = by_name["SB-Repl-Mgmt"]
    sns, clients = by_name["SB-Repl-Storage"], by_name["SB-Repl-Client"]
    _launched.extend(sns)
    mgmt_ip = mgmt.public_ip_address
    _patch_mgmt_host(mgmt_ip, ["simplyblock_core/utils/__init__.py"])

    def sbctl_json(args):
        out = base.ssh_exec(mgmt_ip, [f"{base.SBCTL} {args} --json"], get_output=True)[0]
        return json.loads(out[out.index("["):])

    clusters = [c["UUID"] for c in sbctl_json("cluster list")]
    members = {cid: sbctl_json(f"sn list --cluster-id {cid}") for cid in clusters}
    src_uuid = next(cid for cid, ns in members.items() if any(n["Status"] == "online" for n in ns))
    (tgt_uuid,) = [cid for cid in clusters if cid != src_uuid]
    placed = {n["Management IP"] for ns in members.values() for n in ns if n["Status"] == "online"}
    src_priv = [n["Management IP"] for n in members[src_uuid]]
    tgt_priv = [i.private_ip_address for i in sns if i.private_ip_address not in src_priv]
    missing = [ip for ip in tgt_priv if ip not in placed]
    print(f"src {src_uuid} {src_priv}\ntgt {tgt_uuid} {tgt_priv} (adding {missing})")
    if missing:
        _pull_then_add(mgmt_ip, tgt_uuid, missing, base.CLUSTERS[1]["ha_jm_count"])

    online = sum(n["Status"] == "online" for cid in clusters for n in sbctl_json(f"sn list --cluster-id {cid}"))
    if online < base.SN_COUNT:
        raise RuntimeError(f"Only {online}/{base.SN_COUNT} nodes online")

    uuids = {"src": src_uuid, "tgt": tgt_uuid}
    status = {c["UUID"]: c["Status"].lower() for c in sbctl_json("cluster list")}
    for cfg in base.CLUSTERS:
        uuid = uuids[cfg["name"]]
        if status.get(uuid) != "active":
            print(f"Activating cluster '{cfg['name']}' ({uuid})...")
            time.sleep(10)
            base.ssh_exec(mgmt_ip, [f"{base.SBCTL} -d cluster activate {uuid}"], check=True)
        base.ssh_exec(mgmt_ip, [f"{base.SBCTL} -d pool add {cfg['pool']} {uuid}"])
    tgt_pool_uuid = base.get_pool_uuid(mgmt_ip, "pool_tgt")
    base.ssh_exec(mgmt_ip, [
        f"{base.SBCTL} -d cluster replication-target-add {src_uuid} tgt_{tgt_uuid[:8]}"
        f" {tgt_uuid} --target-pool {tgt_pool_uuid} --timeout {base.REPLICATION['timeout']}"])

    for c in clients:
        base.wait_for_ssh(c.public_ip_address)
        base.ssh_exec(c.public_ip_address, base.CLIENT_PREP_CMDS, check=True)

    by_priv = {i.private_ip_address: i.public_ip_address for i in sns}
    meta = {
        "mgmt": {"public_ip": mgmt_ip, "private_ip": mgmt.private_ip_address},
        "clusters": {
            name: {"cluster_uuid": uuid, "pool": f"pool_{name}", "nodes": len(priv),
                   "storage_public_ips": [by_priv[p] for p in priv],
                   "storage_private_ips": priv}
            for name, uuid, priv in (("src", src_uuid, src_priv), ("tgt", tgt_uuid, tgt_priv))},
        "replication": {"source_cluster": src_uuid, "target_cluster": tgt_uuid,
                        "target_pool": "pool_tgt", "timeout": base.REPLICATION["timeout"]},
        "clients": [{"public_ip": c.public_ip_address, "private_ip": c.private_ip_address}
                    for c in clients],
        "user": base.USER, "key_path": base.KEY_PATH, "branch": base.BRANCH,
    }
    (SCRIPTS / "cluster_metadata_repl.json").write_text(json.dumps(meta, indent=4))
    print("bed ready; metadata in cluster_metadata_repl.json")


if __name__ == "__main__":
    os.chdir(SCRIPTS)              # the metadata file lands where the hotfix reads it
    if "--resume" in sys.argv:
        resume()
    else:
        base.main()
