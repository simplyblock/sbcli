"""The OpenShift core-isolation job reaches a master node through the master
pool.

The job isolates a worker's cores with a pool of its own, worker-isolated-<host>.
MCO keeps a node carrying the master pool's label in the master pool even when a
custom pool selects it, so that pool never reaches a master, and on a compact
cluster every storage node is one. A master gets the isolation from one
MachineConfig and one KubeletConfig for the whole master pool, and a master
whose cores differ from the ones already applied stops the job rather than run
with another host's isolation.

The scenarios run the job's script under bash, which accepts the `[[` tests the
image's busybox shell does, against a fake ``kubectl`` that records what was
applied.
"""

import json
import os
import shutil
import subprocess
import sys

import pytest
import yaml
from jinja2 import Environment, PackageLoader

needs_tools = pytest.mark.skipif(
    os.name != "posix" or not shutil.which("bash") or not shutil.which("jq"), reason="bash or jq is not installed")

MASTER_MC = "simplyblock-master-isolated"
MASTER_KC = "simplyblock-master-static-cpu-manager"

FAKE_KUBECTL = r'''
import json
import os
import sys

import yaml

STATE_DIR = os.environ["FAKE_STATE"]
args = sys.argv[1:]
path = os.path.join(STATE_DIR, "state.json")
with open(path) as f:
    s = json.load(f)


def opt(name):
    for i, a in enumerate(args):
        if a == name:
            return args[i + 1]
    return ""


verb, kind = args[0], args[1]
if verb == "get" and kind == "nodes":
    host = opt("--field-selector").split("=", 1)[1]
    if host in s["masters"]:
        print("node/" + host)
elif verb == "get" and kind == "machineconfig":
    mc = s["applied"].get("MachineConfig/" + args[2])
    if mc is None:
        sys.stderr.write("Error from server (NotFound)\n")
        sys.exit(1)
    if "annotations" in opt("-o"):
        print(mc["metadata"].get("annotations", {}).get("simplyblock.io/cluster-id", ""))
    else:
        print(json.dumps(mc["spec"]["kernelArguments"]))
elif verb == "apply":
    manifest = yaml.safe_load(sys.stdin.read())
    s["applied"]["%s/%s" % (manifest["kind"], manifest["metadata"]["name"])] = manifest
    print("%s/%s configured" % (manifest["kind"].lower(), manifest["metadata"]["name"]))
with open(path, "w") as f:
    json.dump(s, f)
'''


def _script(hostname):
    env = Environment(loader=PackageLoader("simplyblock_web", "templates"), trim_blocks=True, lstrip_blocks=True)
    job = yaml.safe_load(env.get_template("oc_storage_core_isolation.yaml.j2").render(
        CORE_JOBNAME="snode-core-" + hostname, NAMESPACE="simplyblock", HOSTNAME=hostname, CLUSTER_ID="ac85c3",
        OPENSHIFT_MCP="worker"))
    return job["spec"]["template"]["spec"]["containers"][0]["args"][0]


def _run(tmp_path, hostname, cores, masters=(), applied=None):
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    (state_dir / "state.json").write_text(json.dumps({"masters": list(masters), "applied": applied or {}}))
    fake = tmp_path / "fake_kubectl.py"
    fake.write_text(FAKE_KUBECTL)
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    for name, body in (("kubectl", 'exec "%s" -I "%s" "$@"' % (sys.executable, fake)),
                       ("apk", "exit 0"), ("sleep", "exit 0")):
        tool = bin_dir / name
        tool.write_text("#!/bin/sh\n" + body + "\n")
        tool.chmod(0o755)
    config = tmp_path / "sn_config_file"
    config.write_text(json.dumps({"isolated_cores": cores}))
    script = (_script(hostname)
              .replace("/var/simplyblock/.cpu_isolation_applied", str(tmp_path / "marker"))
              .replace("/var/simplyblock/sn_config_file", str(config)))
    env = dict(os.environ, PATH=str(bin_dir) + os.pathsep + os.environ["PATH"], FAKE_STATE=str(state_dir))
    proc = subprocess.run(["bash", "-c", script], capture_output=True, text=True, env=env)
    return proc, json.loads((state_dir / "state.json").read_text())["applied"]


@needs_tools
def test_a_worker_gets_a_pool_of_its_own(tmp_path):
    proc, applied = _run(tmp_path, "worker-1", [2, 3])
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "MachineConfigPool/worker-isolated-worker-1" in applied
    assert applied["MachineConfig/worker-isolated-worker-1"]["metadata"]["labels"] == {
        "machineconfiguration.openshift.io/role": "worker-isolated-worker-1"}
    assert MASTER_MC not in json.dumps(sorted(applied))


@needs_tools
def test_a_master_is_isolated_through_the_master_pool(tmp_path):
    proc, applied = _run(tmp_path, "master-3", [2, 3], masters=["master-3"])
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert not any(k.startswith("MachineConfigPool/") for k in applied), "a custom pool never reaches a master"
    mc = applied["MachineConfig/" + MASTER_MC]
    assert mc["metadata"]["labels"] == {"machineconfiguration.openshift.io/role": "master"}
    assert "isolcpus=2,3" in mc["spec"]["kernelArguments"]
    assert mc["metadata"]["annotations"] == {"simplyblock.io/cluster-id": "ac85c3"}
    kc = applied["KubeletConfig/" + MASTER_KC]
    assert kc["spec"]["machineConfigPoolSelector"]["matchLabels"] == {
        "pools.operator.machineconfiguration.openshift.io/master": ""}
    assert kc["spec"]["kubeletConfig"]["cpuManagerPolicy"] == "static"


def _master_isolation(cores, cluster_id="ac85c3"):
    return {"MachineConfig/" + MASTER_MC: {
        "metadata": {"annotations": {"simplyblock.io/cluster-id": cluster_id}},
        "spec": {"kernelArguments": [
            "nohz_full=" + cores, "rcu_nocbs=" + cores, "isolcpus=" + cores, "intel_iommu=on", "iommu=pt"]}}}


@needs_tools
def test_a_second_master_with_the_same_cores_reuses_the_isolation(tmp_path):
    proc, _ = _run(tmp_path, "master-2", [2, 3], masters=["master-1", "master-2"],
                   applied=_master_isolation("2,3"))
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "already isolates 2,3" in proc.stdout


@needs_tools
def test_a_master_whose_cores_differ_stops_the_job(tmp_path):
    """One isolation applies to every master. Rewriting it for this host
    would reboot every master into another host's cores."""
    existing = _master_isolation("2,3")
    proc, applied = _run(tmp_path, "master-2", [4, 5], masters=["master-1", "master-2"], applied=existing)
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert "4,5" in proc.stdout and "isolcpus=2,3" in proc.stdout
    assert applied == existing
    assert not (tmp_path / "marker").exists()


@needs_tools
def test_a_rerun_replaces_a_previous_clusters_isolation(tmp_path):
    """A torn-down cluster leaves its isolation on the masters. It is no
    other master's of this cluster, so this cluster's replaces it."""
    proc, applied = _run(tmp_path, "master-1", [4, 5], masters=["master-1"],
                         applied=_master_isolation("2,3", cluster_id="old111"))
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "previous simplyblock cluster" in proc.stdout
    mc = applied["MachineConfig/" + MASTER_MC]
    assert "isolcpus=4,5" in mc["spec"]["kernelArguments"]
    assert mc["metadata"]["annotations"] == {"simplyblock.io/cluster-id": "ac85c3"}
