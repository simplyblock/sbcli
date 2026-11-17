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
    if "resourceVersion" in opt("-o"):
        print(mc["metadata"].get("resourceVersion", "1"))
    elif "annotations" in opt("-o"):
        print(mc["metadata"].get("annotations", {}).get("simplyblock.io/cluster-id", ""))
    else:
        print(json.dumps(mc["spec"]["kernelArguments"]))
elif verb == "patch":
    key = "MachineConfig/" + args[2]
    if s.get("race_patch"):
        # Another master's job replaces it between this job's read and patch.
        s["applied"][key] = s.pop("race_patch")
    mc = s["applied"][key]
    mc.setdefault("metadata", {}).setdefault("resourceVersion", "1")
    for op in json.loads(opt("-p")):
        parts = [x.replace("~1", "/") for x in op["path"].strip("/").split("/")]
        parent = mc
        for part in parts[:-1]:
            parent = parent.setdefault(part, {})
        if op["op"] == "test" and parent.get(parts[-1]) != op["value"]:
            with open(path, "w") as f:
                json.dump(s, f)
            sys.stderr.write("Error from server: testing value %s failed\n" % op["path"])
            sys.exit(1)
        if op["op"] in ("replace", "add"):
            parent[parts[-1]] = op["value"]
    mc["metadata"]["resourceVersion"] = str(int(mc["metadata"].get("resourceVersion", "1")) + 1)
    print("machineconfig.machineconfiguration.openshift.io/%s patched" % args[2])
elif verb == "create":
    # A concurrent writer: another master's job creates the MachineConfig
    # between this job's lookup and its create.
    src = opt("-f")
    manifest = yaml.safe_load(sys.stdin.read() if src in ("", "-") else open(src).read())
    key = "%s/%s" % (manifest["kind"], manifest["metadata"]["name"])
    if s.get("race") and key == "MachineConfig/" + s["race"]["metadata"]["name"]:
        s["applied"][key] = s.pop("race")
    if key in s["applied"]:
        with open(path, "w") as f:
            json.dump(s, f)
        sys.stderr.write('Error from server (AlreadyExists): machineconfigs "%s" already exists\n' % key)
        sys.exit(1)
    s["applied"][key] = manifest
    print("%s created" % key.lower())
elif verb == "apply":
    src = opt("-f")
    manifest = yaml.safe_load(sys.stdin.read() if src in ("", "-") else open(src).read())
    s["applied"]["%s/%s" % (manifest["kind"], manifest["metadata"]["name"])] = manifest
    print("%s/%s configured" % (manifest["kind"].lower(), manifest["metadata"]["name"]))
with open(path, "w") as f:
    json.dump(s, f)
'''


def _job(hostname):
    env = Environment(loader=PackageLoader("simplyblock_web", "templates"), trim_blocks=True, lstrip_blocks=True)
    return yaml.safe_load(env.get_template("oc_storage_core_isolation.yaml.j2").render(
        CORE_JOBNAME="snode-core-" + hostname, NAMESPACE="simplyblock", HOSTNAME=hostname, CLUSTER_ID="ac85c3",
        OPENSHIFT_MCP="worker"))


def _script(hostname):
    return _job(hostname)["spec"]["template"]["spec"]["containers"][0]["args"][0]


def _run(tmp_path, hostname, cores, masters=(), applied=None, marker=False, race=None, race_patch=None):
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    (state_dir / "state.json").write_text(json.dumps(
        {"masters": list(masters), "applied": applied or {}, "race": race, "race_patch": race_patch}))
    if marker:
        (tmp_path / "marker").touch()
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
              .replace("/tmp/master-isolated.yaml", str(tmp_path / "master-isolated.yaml"))
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


def _mc(cores, cluster_id="ac85c3"):
    return _master_isolation(cores, cluster_id)["MachineConfig/" + MASTER_MC] | {
        "kind": "MachineConfig"} | {"metadata": {"name": MASTER_MC, "annotations": {
            "simplyblock.io/cluster-id": cluster_id}}}


@needs_tools
def test_a_concurrent_master_with_other_cores_stops_the_job(tmp_path):
    """Two masters' jobs can both find no isolation and both write one. The
    create lets one win; the other re-reads it and refuses, rather than
    replace it and reboot every master into its own cores."""
    proc, applied = _run(tmp_path, "master-2", [4, 5], masters=["master-1", "master-2"], race=_mc("2,3"))
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert "isolcpus=2,3" in applied["MachineConfig/" + MASTER_MC]["spec"]["kernelArguments"]
    assert "4,5" in proc.stdout


@needs_tools
def test_a_concurrent_master_with_the_same_cores_reuses_the_isolation(tmp_path):
    proc, applied = _run(tmp_path, "master-2", [2, 3], masters=["master-1", "master-2"], race=_mc("2,3"))
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "already isolates 2,3" in proc.stdout


@needs_tools
def test_a_master_marked_by_an_earlier_version_still_gets_the_master_isolation(tmp_path):
    """An earlier version isolated a master through a pool of its own, which
    MCO ignores, and marked the node configured. The marker must not keep the
    master off the master pool's isolation."""
    proc, applied = _run(tmp_path, "master-1", [2, 3], masters=["master-1"], marker=True)
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "isolcpus=2,3" in applied["MachineConfig/" + MASTER_MC]["spec"]["kernelArguments"]


@needs_tools
def test_a_marked_master_with_its_isolation_in_place_skips_the_reboot_wait(tmp_path):
    proc, applied = _run(tmp_path, "master-1", [2, 3], masters=["master-1"], marker=True,
                         applied=_master_isolation("2,3"))
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "Skipping sleep" in proc.stdout
    assert "Node is rebooting" not in proc.stdout


def test_a_refusal_fails_the_job_at_once():
    """A refusal is final: retried in place it only crash-loops until the
    agent's wait times out, with the reason in a dead pod's log."""
    assert _job("master-1")["spec"]["backoffLimit"] == 0


@needs_tools
def test_an_unrecorded_isolation_is_replaced_by_one_writer_only(tmp_path):
    """An isolation written before the cluster-id annotation existed is
    replaced like any previous cluster's: the write tests the version it
    read, so a master that replaced it first wins, and this one compares."""
    unrecorded = {"MachineConfig/" + MASTER_MC: {"metadata": {"name": MASTER_MC, "resourceVersion": "7"},
                                                  "spec": {"kernelArguments": ["isolcpus=9"]}}}
    proc, applied = _run(tmp_path, "master-2", [4, 5], masters=["master-1", "master-2"],
                         applied=unrecorded, race_patch=_mc("2,3") | {"metadata": {
                             "name": MASTER_MC, "resourceVersion": "8",
                             "annotations": {"simplyblock.io/cluster-id": "ac85c3"}}})
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert "isolcpus=2,3" in applied["MachineConfig/" + MASTER_MC]["spec"]["kernelArguments"]
