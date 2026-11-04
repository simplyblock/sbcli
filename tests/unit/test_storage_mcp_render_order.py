"""The OpenShift CPU-topology job joins a node to its cluster's storage
MachineConfigPool only once the pool's rendered config is complete.

A pool renders from the MachineConfigs that exist when it is evaluated. Joined
to a pool created a moment earlier, before its MachineConfig and KubeletConfig,
a node is sent to a render without the kubelet settings, which differs from
the render it runs, and reboots into it; the complete render then arrives,
usually identical to the one the node started on, and it reboots again. The
job therefore creates the pool selecting no host, adds the MachineConfig and
the KubeletConfig, waits for the generated kubelet config to be part of the
pool's render, and only then moves the host in, so a node whose config is
already the same moves without a reboot.

The ordering tests read the rendered script. The scenario tests run it under
dash against a fake ``kubectl`` that keeps the pools, MachineConfigs and
KubeletConfigs in a JSON file and plays the render controller, so they cover
what the script does to a live pool and not only the order of its text.
"""

import json
import os
import shutil
import subprocess
import sys

import pytest
import yaml
from jinja2 import Environment, PackageLoader

POOL = "storage-ac85c3"
MC = "storage-mc-ac85c3"
KC = "storage-kubelet-ac85c3"
PENDING = "pending-storage-pool"
RETIRED = "retired-empty-storage-pool"

needs_dash = pytest.mark.skipif(
    os.name != "posix" or not shutil.which("dash"), reason="dash is not installed")


def _script(hostname="worker-1"):
    env = Environment(loader=PackageLoader("simplyblock_web", "templates"), trim_blocks=True, lstrip_blocks=True)
    job = yaml.safe_load(env.get_template("oc_storage_cpu_topology.yaml.j2").render(
        CORE_JOBNAME="snode-core-" + hostname, NAMESPACE="simplyblock", CLUSTER_ID="ac85c3",
        HOSTNAME=hostname, MCP_MAX_UNAVAILABLE=4, RESERVED_SYSTEM_CPUS="0-1", NSOCKET=0,
        OPENSHIFT_MCP="worker"))
    container = job["spec"]["template"]["spec"]["containers"][0]
    assert container["command"] == ["/bin/sh", "-c"]
    return container["args"][0]


def _pool_manifest(script):
    start = script.index("kind: MachineConfigPool")
    return script[start:script.index("MCPEOF", start)]


def test_a_new_pool_selects_no_host():
    pool = _pool_manifest(_script())
    selector = pool[pool.index("nodeSelector:"):]
    assert "$HOSTNAME" not in selector, selector


def test_the_host_joins_after_the_pool_renders_the_kubelet_config():
    script = _script()
    kubelet_config = script.index("kind: KubeletConfig")
    wait = script.index("generated-kubelet")
    join = script.index("Added $HOSTNAME to MCP")
    assert kubelet_config < wait < join, (kubelet_config, wait, join)


def test_the_host_leaves_the_other_pools_only_after_it_joined():
    script = _script()
    join = script.index("Added $HOSTNAME to MCP")
    leave = script.index("Removing $HOSTNAME from stale storage pool")
    assert join < leave, (join, leave)


def test_the_pool_is_created_not_applied():
    """`kubectl apply` of a pool that exists would replace its nodeSelector
    values with the placeholder and evict every joined host."""
    script = _script()
    pool_section = script[script.index("# ---- MachineConfigPool"):script.index("# ---- MachineConfig -")]
    commands = [line for line in pool_section.splitlines() if not line.lstrip().startswith("#")]
    assert any("kubectl create -f" in line for line in commands), commands
    assert not any("kubectl apply" in line for line in commands), commands


@needs_dash
def test_parses_under_dash():
    subprocess.run(["dash", "-n", "-c", _script()], check=True)


# ---- Scenarios against a fake kubectl ---------------------------------------

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


def log(line):
    with open(os.path.join(STATE_DIR, "calls.log"), "a") as f:
        f.write(line + "\n")


def save():
    with open(path, "w") as f:
        json.dump(s, f)


def fail(msg):
    save()
    sys.stderr.write(msg + "\n")
    sys.exit(1)


def opt(name):
    for i, a in enumerate(args):
        if a == name:
            return args[i + 1]
        if a.startswith(name + "="):
            return a.split("=", 1)[1]
    return ""


log(" ".join(args))
verb, kind = args[0], args[1]
if verb == "get":
    if kind == "mcp":
        for name in s["pools"]:
            print("machineconfigpool.machineconfiguration.openshift.io/" + name)
        sys.exit(0)
    if kind == "nodes":
        # Only the master-pool label lookup lists nodes.
        assert opt("-l") == "node-role.kubernetes.io/master", args
        host = opt("--field-selector").split("=", 1)[1]
        if host in s["masters"]:
            print("node/" + host)
        sys.exit(0)
    name = args[2]
    jsonpath = opt("-o")
    if kind == "machineconfigpool":
        if s["fail_get_pool"] > 0:
            s["fail_get_pool"] -= 1
            fail('Error from server (InternalError): an error on the server ("") has prevented the request')
        if "nodeSelector" in jsonpath and s["fail_get_hosts"] > 0:
            s["fail_get_hosts"] -= 1
            fail('Error from server (InternalError): an error on the server ("") has prevented the request')
        pool = s["pools"].get(name)
        if pool is None:
            fail('Error from server (NotFound): machineconfigpools.machineconfiguration.openshift.io "%s" not found' % name)
        if "metadata.labels" in jsonpath:
            print(pool.get("labels", {}).get("custom-kubelet", ""))
        elif "configuration.source" in jsonpath and name == "master":
            # MCO's KubeletConfig controller evaluates a KubeletConfig when it is
            # created or its spec changes, never when a pool's labels change, so
            # the master pool renders only a KubeletConfig whose selector matched
            # it at creation. Its generated config may take the very name of
            # another KubeletConfig's: MCO names it after the pool and a suffix.
            for kc in s["master_kcs"]:
                if s["reject_kubeletconfig"] or any(g[1] == kc for g in pool.get("generated", [])):
                    continue
                if s["render_after"] > 0:
                    s["render_after"] -= 1
                else:
                    pool["generated"] = [g for g in pool.get("generated", []) if g[0] != s["master_ours_name"]]
                    pool["generated"].append([s["master_ours_name"], kc])
            print("\n".join(["00-master"] + [g[0] for g in pool.get("generated", [])]))
        elif "configuration.source" in jsonpath:
            # The render controller: the MachineConfigs selected by the pool
            # render at once; the generated kubelet config joins the sources
            # `render_after` evaluations later, never for a rejected KubeletConfig.
            sources = list(s["mcs"])
            if s["kcs"] and not s["reject_kubeletconfig"]:
                if s["render_after"] > 0:
                    s["render_after"] -= 1
                else:
                    sources.append("99-%s-generated-kubelet%s" % (name, s["generated_suffix"]))
            print("\n".join(sources))
        elif "nodeSelector" in jsonpath:
            print("\n".join(pool["values"]))
        elif "RenderDegraded" in jsonpath:
            print("False")
        elif "conditions" in jsonpath:
            print(json.dumps([{"type": "Updated", "status": "True"}]))
    elif kind == "machineconfig" and name == "--no-headers":
        # Generated configs by owner, as custom-columns prints them.
        for pool_name, pool in s["pools"].items():
            if pool_name == "master":
                for generated, owner in pool.get("generated", []):
                    print("%s   %s" % (generated, owner))
            else:
                print("99-%s-generated-kubelet%s   %s" % (pool_name, s["generated_suffix"], s["kc_name"]))
    elif kind == "machineconfig":
        if name not in s["mcs"]:
            fail('Error from server (NotFound): machineconfigs.machineconfiguration.openshift.io "%s" not found' % name)
    elif kind == "kubeletconfig" and name == "-o":
        for kc in s["kcs"]:
            print("kubeletconfig.machineconfiguration.openshift.io/" + kc)
    elif kind == "kubeletconfig":
        if name not in s["kcs"]:
            fail('Error from server (NotFound): kubeletconfigs.machineconfiguration.openshift.io "%s" not found' % name)
        failed = s["reject_kubeletconfig"]
        if "Failure" in jsonpath:
            print("True" if failed else "")
        elif "conditions" in jsonpath:
            print(json.dumps([{"type": "Failure" if failed else "Success", "status": "True",
                               "message": "Error: reservedSystemCPUs is invalid" if failed else "Success"}]))
    elif kind == "pod":
        print("true")
    elif kind == "node":
        print("100")
elif verb in ("create", "apply"):
    src = opt("-f")
    text = sys.stdin.read() if src == "-" else open(src).read()
    manifest = yaml.safe_load(text)
    kind, name = manifest["kind"], manifest["metadata"]["name"]
    log("%s %s/%s" % (verb, kind, name))
    if kind == "MachineConfigPool":
        if name in s["pools"] and verb == "create":
            fail('Error from server (AlreadyExists): error when creating "%s": '
                 'machineconfigpools.machineconfiguration.openshift.io "%s" already exists' % (src, name))
        s["pools"][name] = {"values": manifest["spec"]["nodeSelector"]["matchExpressions"][0]["values"]}
    elif kind == "MachineConfig":
        if name not in s["mcs"]:
            s["mcs"].append(name)
    elif kind == "KubeletConfig":
        if name in s["kcs"] and verb == "create":
            fail('Error from server (AlreadyExists): kubeletconfigs.machineconfiguration.openshift.io "%s" already exists' % name)
        new = name not in s["kcs"]
        s["kcs"][name] = manifest["spec"]["kubeletConfig"]
        selector = manifest["spec"]["machineConfigPoolSelector"]["matchLabels"]
        master = s["pools"].get("master")
        if new and master is not None and all(
                master.get("labels", {}).get(k) == v for k, v in selector.items()):
            s["master_kcs"].append(name)
    print("%s/%s %sd" % (kind.lower(), name, verb))
elif verb == "delete":
    assert kind == "kubeletconfig", args
    for name in args[2:]:
        s["kcs"].pop(name, None)
        if name in s["master_kcs"]:
            s["master_kcs"].remove(name)
        master = s["pools"].get("master")
        if master is not None:
            master["generated"] = [g for g in master.get("generated", []) if g[1] != name]
        print('kubeletconfig.machineconfiguration.openshift.io "%s" deleted' % name)
elif verb == "label":
    key, value = args[3].split("=", 1)
    labels = s["pools"][args[2]].setdefault("labels", {})
    if key in labels and "--overwrite" not in args:
        fail("error: '%s' already has a value (%s), and --overwrite is false" % (key, labels[key]))
    labels[key] = value
    print("machineconfigpool.machineconfiguration.openshift.io/%s labeled" % args[2])
elif verb == "patch":
    pool = s["pools"][args[2]]
    patch = json.loads(opt("-p"))
    if opt("--type") == "json":
        for op in patch:
            index = op["path"].rsplit("/", 1)[1]
            if op["op"] == "test":
                if pool["values"][int(index)] != op["value"]:
                    fail("Error from server: testing value %s failed" % op["path"])
            elif op["op"] == "replace":
                pool["values"][int(index)] = op["value"]
            elif op["op"] == "add":
                pool["values"].append(op["value"])
    else:
        pool["values"] = patch["spec"]["nodeSelector"]["matchExpressions"][0]["values"]
    print("machineconfigpool.machineconfiguration.openshift.io/%s patched" % args[2])
save()
'''


def _state(**overrides):
    state = {"pools": {}, "mcs": [], "kcs": {}, "render_after": 2, "generated_suffix": "", "masters": [],
             "kc_label": "storage-cpumanager-ac85c3", "kc_name": "storage-kubelet-ac85c3",
             "master_ours_name": "99-master-generated-kubelet", "master_kcs": [],
             "reject_kubeletconfig": False, "fail_get_pool": 0, "fail_get_hosts": 0}
    state.update(overrides)
    return state


def _run(tmp_path, state, hostname="worker-1", marker=False):
    """Run the job's script under dash against the fake kubectl. Returns the
    completed process, the final state and the kubectl calls in order."""
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    (state_dir / "state.json").write_text(json.dumps(state))
    fake = tmp_path / "fake_kubectl.py"
    fake.write_text(FAKE_KUBECTL)
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    kubectl = bin_dir / "kubectl"
    kubectl.write_text('#!/bin/sh\nexec "%s" -I "%s" "$@"\n' % (sys.executable, fake))
    sleep = bin_dir / "sleep"
    sleep.write_text("#!/bin/sh\nexit 0\n")
    for tool in (kubectl, sleep):
        tool.chmod(0o755)
    marker_path = tmp_path / "cpu_topology_applied"
    if marker:
        marker_path.touch()
    script = (_script(hostname)
              .replace("/var/simplyblock/.cpu_topology_applied", str(marker_path))
              .replace("/tmp/mcp.yaml", str(tmp_path / "mcp.yaml")))
    env = dict(os.environ, PATH=str(bin_dir) + os.pathsep + os.environ["PATH"], FAKE_STATE=str(state_dir))
    proc = subprocess.run(["dash", "-c", script], capture_output=True, text=True, env=env)
    final = json.loads((state_dir / "state.json").read_text())
    calls = (state_dir / "calls.log").read_text().splitlines()
    return proc, final, calls


@needs_dash
def test_first_node_creates_the_pool_waits_for_the_render_and_takes_the_placeholder_slot(tmp_path):
    proc, state, calls = _run(tmp_path, _state())
    assert proc.returncode == 0, proc.stdout + proc.stderr
    create_pool = calls.index("create MachineConfigPool/" + POOL)
    create_mc = calls.index("apply MachineConfig/" + MC)
    create_kc = calls.index("apply KubeletConfig/" + KC)
    join = next(i for i, c in enumerate(calls) if c.startswith("patch machineconfigpool " + POOL))
    renders = [i for i, c in enumerate(calls) if "configuration.source" in c]
    assert create_pool < create_mc < create_kc < renders[0] < renders[-1] < join, calls
    assert "has rendered" in proc.stdout
    assert state["pools"][POOL]["values"] == ["worker-1"], "a joined pool lists only real hosts"
    assert (tmp_path / "cpu_topology_applied").exists()


@needs_dash
def test_a_pool_that_already_exists_is_never_overwritten(tmp_path):
    """The pool keeps its hosts whatever a lookup says: the script creates
    and tolerates AlreadyExists instead of applying over a live pool. A
    transient API error on the first pool lookup used to take the create
    branch and apply the placeholder over the joined hosts."""
    joined = ["worker-1", "worker-2", "worker-3"]
    proc, state, calls = _run(tmp_path, _state(
        pools={POOL: {"values": list(joined)}}, mcs=[MC], kcs={KC: {}}, render_after=0, fail_get_pool=1),
        hostname="worker-4")
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert not any(c.startswith("apply MachineConfigPool") for c in calls), calls
    assert "already exists" in proc.stdout
    assert state["pools"][POOL]["values"] == joined + ["worker-4"]


@needs_dash
def test_a_later_host_appends_when_the_placeholder_is_gone(tmp_path):
    proc, state, calls = _run(tmp_path, _state(
        pools={POOL: {"values": ["worker-1"]}}, mcs=[MC], kcs={KC: {}}), hostname="worker-2")
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert state["pools"][POOL]["values"] == ["worker-1", "worker-2"]


@needs_dash
def test_a_transient_failure_of_the_join_lookup_is_retried(tmp_path):
    proc, state, calls = _run(tmp_path, _state(
        pools={POOL: {"values": [PENDING]}}, mcs=[MC], kcs={KC: {}}, fail_get_hosts=2))
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "retry 2/15" in proc.stdout
    assert state["pools"][POOL]["values"] == ["worker-1"]


@needs_dash
def test_a_rejected_kubeletconfig_stops_the_job_with_the_reason(tmp_path):
    proc, state, calls = _run(tmp_path, _state(reject_kubeletconfig=True))
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert "MCO rejected the kubelet config" in proc.stdout
    assert "reservedSystemCPUs is invalid" in proc.stdout, "the KubeletConfig conditions are reported"
    assert not any(c.startswith("patch") for c in calls), "the host is not joined to the incomplete render"
    assert state["pools"][POOL]["values"] == [PENDING]


@needs_dash
def test_a_slow_render_still_gets_the_host(tmp_path):
    proc, state, calls = _run(tmp_path, _state(render_after=10 ** 6))
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "Joining worker-1 anyway" in proc.stdout
    assert sum("configuration.source" in c for c in calls) == 60
    assert state["pools"][POOL]["values"] == ["worker-1"]


@needs_dash
def test_the_generated_kubelet_config_may_carry_a_suffix(tmp_path):
    proc, state, calls = _run(tmp_path, _state(generated_suffix="-1"))
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "has rendered" in proc.stdout
    assert "[WARN]" not in proc.stdout


@needs_dash
def test_the_host_leaves_stale_pools_and_retires_an_emptied_one(tmp_path):
    proc, state, calls = _run(tmp_path, _state(pools={
        "storage-old1": {"values": ["worker-1", "worker-9"]},
        "storage-old2": {"values": ["worker-1"]},
    }))
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert state["pools"][POOL]["values"] == ["worker-1"]
    assert state["pools"]["storage-old1"]["values"] == ["worker-9"]
    assert state["pools"]["storage-old2"]["values"] == [RETIRED]


@needs_dash
def test_a_configured_node_only_confirms_numa_readiness(tmp_path):
    proc, state, calls = _run(tmp_path, _state(
        pools={POOL: {"values": ["worker-1"]}}, mcs=[MC], kcs={KC: {}}, render_after=0), marker=True)
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "skipping patch" in proc.stdout
    assert "numa-align/numa-0 is available (100)" in proc.stdout


# ---- Master nodes -----------------------------------------------------------
# MCO keeps a node carrying the master pool's label in the master pool even
# when a custom pool selects it, and ignores the custom pool. On a compact
# cluster every storage node is a master, so it gets the settings through a
# KubeletConfig of its own for the master pool and joins no storage pool.

MASTER_KC = KC + "-master"
MASTER_POOL_LABEL = "pools.operator.machineconfiguration.openshift.io/master"


def _master_pool(**fields):
    pool = {"values": ["master-placeholder"], "labels": {MASTER_POOL_LABEL: ""}}
    pool.update(fields)
    return pool


@needs_dash
def test_a_master_gets_a_kubelet_config_of_its_own_for_the_master_pool(tmp_path):
    """MCO evaluates a KubeletConfig when it is created, never when a pool's
    labels change: labeling the master pool for the storage KubeletConfig
    after it exists renders nothing there. The master KubeletConfig selects
    the master pool by its own label from the start."""
    proc, state, calls = _run(tmp_path, _state(masters=["master-3"], pools={"master": _master_pool()}),
                              hostname="master-3")
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert MASTER_KC in state["kcs"]
    assert "MCP master has rendered 99-master-generated-kubelet" in proc.stdout
    assert state["pools"]["master"]["generated"] == [["99-master-generated-kubelet", MASTER_KC]]
    assert state["pools"]["master"]["labels"] == {MASTER_POOL_LABEL: ""}, "the master pool is not relabeled"
    assert state["pools"][POOL]["values"] == [PENDING], "a master joins no storage pool"
    assert not any(c.startswith(("patch machineconfigpool master", "label")) for c in calls), calls
    assert (tmp_path / "cpu_topology_applied").exists()


@needs_dash
def test_the_master_kubelet_config_carries_the_storage_settings(tmp_path):
    proc, state, calls = _run(tmp_path, _state(masters=["master-3"], pools={"master": _master_pool()}),
                              hostname="master-3")
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert state["kcs"][MASTER_KC] == state["kcs"][KC]


@needs_dash
def test_a_master_waits_for_its_own_generated_kubelet_config(tmp_path):
    """Another KubeletConfig's generated config in the master pool is not this
    one's: the wait is for the one this cluster's KubeletConfig owns."""
    proc, state, calls = _run(tmp_path, _state(
        masters=["master-3"], master_ours_name="99-master-generated-kubelet-1",
        pools={"master": _master_pool(generated=[["99-master-generated-kubelet", "site-tuning"]])}),
        hostname="master-3")
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "MCP master has rendered 99-master-generated-kubelet-1" in proc.stdout
    assert proc.stdout.count("Waiting for MCP master") == 2, proc.stdout


@needs_dash
def test_a_master_leaves_every_storage_pool_including_the_cluster_one(tmp_path):
    """An earlier attempt may have listed the master in a storage pool, where
    MCO ignores it and raises MCCPoolAlert."""
    proc, state, calls = _run(tmp_path, _state(masters=["master-3"], pools={
        "master": _master_pool(),
        POOL: {"values": ["master-3", "worker-1"]},
        "storage-old1": {"values": ["master-3"]},
    }), hostname="master-3")
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert state["pools"][POOL]["values"] == ["worker-1"]
    assert state["pools"]["storage-old1"]["values"] == [RETIRED]


@needs_dash
def test_a_rerun_replaces_a_previous_clusters_master_kubelet_config(tmp_path):
    """A torn-down cluster leaves its master KubeletConfig behind, and its
    generated config under the name the new one's takes. Only one cluster's
    settings can apply to the masters: the previous one is deleted, and the
    wait is for the config the new KubeletConfig owns."""
    old = "storage-kubelet-old111-master"
    proc, state, calls = _run(tmp_path, _state(masters=["master-3"], kcs={old: {}}, master_kcs=[old], pools={
        "master": _master_pool(generated=[["99-master-generated-kubelet", old]])}), hostname="master-3")
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert old not in state["kcs"]
    assert "previous simplyblock cluster" in proc.stdout
    assert state["pools"]["master"]["generated"] == [["99-master-generated-kubelet", MASTER_KC]]
    assert proc.stdout.count("Waiting for MCP master") == 2, proc.stdout


@needs_dash
def test_a_foreign_kubelet_config_on_the_masters_is_left_alone(tmp_path):
    proc, state, calls = _run(tmp_path, _state(
        masters=["master-3"], kcs={"site-tuning": {}}, master_kcs=["site-tuning"],
        master_ours_name="99-master-generated-kubelet-1",
        pools={"master": _master_pool(labels={MASTER_POOL_LABEL: "", "custom-kubelet": "site-tuning"},
                                      generated=[["99-master-generated-kubelet", "site-tuning"]])}),
        hostname="master-3")
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "site-tuning" in state["kcs"]
    assert state["pools"]["master"]["labels"]["custom-kubelet"] == "site-tuning"
    assert ["99-master-generated-kubelet", "site-tuning"] in state["pools"]["master"]["generated"]


@needs_dash
def test_a_rerun_on_a_master_finds_its_kubelet_config_and_the_render(tmp_path):
    proc, state, calls = _run(tmp_path, _state(
        masters=["master-3"], kcs={KC: {}, MASTER_KC: {}}, master_kcs=[MASTER_KC], mcs=[MC], render_after=0,
        pools={"master": _master_pool(generated=[["99-master-generated-kubelet", MASTER_KC]])}),
        hostname="master-3")
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert not any(c.startswith(("create KubeletConfig", "delete")) for c in calls), calls
    assert "MCP master has rendered 99-master-generated-kubelet" in proc.stdout


@needs_dash
def test_a_worker_leaves_the_master_pool_alone(tmp_path):
    proc, state, calls = _run(tmp_path, _state(masters=["master-3"], pools={"master": _master_pool()}))
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert MASTER_KC not in state["kcs"]
    assert state["pools"]["master"]["labels"] == {MASTER_POOL_LABEL: ""}
    assert state["pools"][POOL]["values"] == ["worker-1"]
