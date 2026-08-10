# coding=utf-8
"""Edge-clusters e2e suite (tests 2-6). Requires a provisioned + deployed
environment (provision.py, deploy.py — deploy success IS test 1).

Run ordered:  pytest edge_e2e/test_edge_e2e.py -v -x

Test map (from the test plan):
  2. parallel fio on central + every edge cluster
  3. reboot failovers (1-node: interrupt + suspension + unreachable->offline->
     online; 2-node: no interrupt, degraded, node cycles — repeated for the
     second node after rebuild)
  4. graceful device removal + restart (IO unaffected wherever >1 device)
  5. device error via EBS force-detach -> unavailable, IO unaffected;
     reattach + device restart -> online; then permanent replacement with a
     brand-new EBS volume
  6. flaky and broken CP<->edge connections: nodes/cluster unreachable, IO
     never interrupted, full recovery after healing
"""
import random
import time

import pytest

from edge_e2e import helpers, workload
from edge_e2e.topology import EDGE_CLUSTERS, has_device_redundancy

pytestmark = pytest.mark.edge_e2e


@pytest.fixture(scope="session")
def state():
    return helpers.load_state()


@pytest.fixture(scope="session")
def apis(state):
    """cluster name -> EdgeApi for every deployed edge cluster."""
    base = state["central"]["api_url"]
    return {name: helpers.EdgeApi(base, entry["cluster_id"], entry["secret"])
            for name, entry in state["edge"].items()}


def _fio_everywhere(state, apis, runtime=0, suffix="run"):
    """Start the standard fio pod on the central cluster and on every edge
    cluster; returns [(server_name, pod_name)]."""
    pods = []
    # central: against a hyperscale lvol prepared by deploy/bootstrap
    central = state["central"]
    if central.get("fio_connect"):
        server = f"{central['server']}"
        pod = f"fio-central-{suffix}"
        workload.start_fio_pod(state, server, pod, central["fio_connect"],
                               runtime=runtime)
        pods.append((server, pod))
    for name, entry in state["edge"].items():
        api = apis[name]
        connect = api.connect_info(entry["volume_id"])
        server = entry["nodes"][0]
        pod = f"fio-{name}-{suffix}"
        workload.start_fio_pod(state, server, pod, connect, runtime=runtime)
        pods.append((server, pod))
    return pods


def _collect_fio(state, pods, timeout=5400):
    results = {}
    for server, pod in pods:
        results[pod] = workload.wait_fio_result(state, server, pod, timeout=timeout)
        workload.delete_fio_pod(state, server, pod)
    return results


# --------------------------------------------------------------- test 2: fio

def test_02_parallel_fio_all_clusters(state, apis):
    pods = _fio_everywhere(state, apis, runtime=0, suffix="t2")
    results = _collect_fio(state, pods)
    failed = {pod: r["log"][-2000:] for pod, r in results.items()
              if workload.fio_interrupted(r)}
    assert not failed, f"fio failed on: {list(failed)}\n{failed}"


# --------------------------------------------------- test 3: reboot failover

def _reboot_and_watch(state, api, node_name, expect_interrupt, fio_ctx):
    helpers.reboot_instance(state, node_name)
    # Status must walk unreachable -> offline -> online (spec §6.1: the
    # k8s API dies first, then the pod probe fails, then reassembly).
    helpers.observe_node_transitions(
        api, node_name, ["unreachable", "offline", "online"], timeout=1500)
    helpers.wait_cluster_status(api, "active", timeout=600)


@pytest.mark.parametrize("spec", [s for s in EDGE_CLUSTERS if s.nodes == 1],
                         ids=lambda s: s.name)
def test_03a_reboot_single_node(state, apis, spec):
    entry = state["edge"][spec.name]
    api = apis[spec.name]
    node_name = entry["nodes"][0]
    server = node_name

    connect = api.connect_info(entry["volume_id"])
    pod = f"fio-{spec.name}-t3"
    workload.start_fio_pod(state, server, pod, connect, runtime=1200)

    helpers.reboot_instance(state, node_name)
    # 1-node: cluster must suspend while the node is out.
    helpers.wait_for(f"{spec.name} suspended",
                     lambda: api.cluster_status() == "suspended", timeout=600)
    helpers.observe_node_transitions(
        api, node_name, ["unreachable", "offline", "online"], timeout=1500)
    helpers.wait_cluster_status(api, "active", timeout=600)

    result = workload.wait_fio_result(state, server, pod, timeout=1800)
    workload.delete_fio_pod(state, server, pod)
    # 1-node: the interruption MUST be visible.
    assert workload.fio_interrupted(result), \
        f"{spec.name}: expected IO interruption on single-node reboot"


@pytest.mark.parametrize("spec", [s for s in EDGE_CLUSTERS if s.nodes == 2],
                         ids=lambda s: s.name)
def test_03b_reboot_two_node_both_nodes(state, apis, spec):
    entry = state["edge"][spec.name]
    api = apis[spec.name]
    primary_name, secondary_name = entry["nodes"]
    server = primary_name

    for reboot_target in (secondary_name, primary_name):
        connect = api.connect_info(entry["volume_id"])
        pod = f"fio-{spec.name}-t3-{reboot_target[-2:]}"
        workload.start_fio_pod(state, server, pod, connect, runtime=1500)
        time.sleep(30)  # let IO settle before the fault

        rebooted = api.node_by_hostname(reboot_target)
        owned_stores = [lvs for lvs in rebooted["leader_of"]]

        helpers.reboot_instance(state, reboot_target)
        # 2-node: degraded only — NEVER suspended.
        helpers.wait_for(f"{spec.name} degraded",
                         lambda: api.cluster_status() == "degraded", timeout=600)
        assert api.cluster_status() != "suspended"

        if owned_stores:
            # Its store(s) must fail over to the survivor (secondary lvstore
            # promotion: update + set_leader + ANA flip).
            survivor = [n for n in entry["nodes"] if n != reboot_target][0]
            helpers.wait_for(
                f"{spec.name} stores {owned_stores} failed over to {survivor}",
                lambda: all(lvs in api.node_by_hostname(survivor)["leader_of"]
                            for lvs in owned_stores), timeout=900)

        helpers.observe_node_transitions(
            api, reboot_target, ["unreachable", "offline", "online"], timeout=1500)
        # rebuild done, cluster back to active before the second round
        helpers.wait_cluster_status(api, "active", timeout=900)

        if owned_stores:
            # Fail-back: the returning node leads its own store(s) again
            # (port-fenced handover after resync).
            helpers.wait_for(
                f"{spec.name} stores failed back to {reboot_target}",
                lambda: all(lvs in api.node_by_hostname(reboot_target)["leader_of"]
                            for lvs in owned_stores), timeout=1800)

        result = workload.wait_fio_result(state, server, pod, timeout=2400)
        workload.delete_fio_pod(state, server, pod)
        assert not workload.fio_interrupted(result), \
            f"{spec.name}: IO interrupted during {reboot_target} reboot:\n" \
            f"{result['log'][-2000:]}"


# ------------------------------------------- test 4: device remove + restart

@pytest.mark.parametrize("spec", [s for s in EDGE_CLUSTERS if has_device_redundancy(s)],
                         ids=lambda s: s.name)
def test_04_device_remove_and_restart(state, apis, spec):
    entry = state["edge"][spec.name]
    api = apis[spec.name]
    node_name = entry["nodes"][0]
    node = api.node_by_hostname(node_name)
    device = entry["device_paths"][0]

    connect = api.connect_info(entry["volume_id"])
    pod = f"fio-{spec.name}-t4"
    workload.start_fio_pod(state, entry["nodes"][0], pod, connect, runtime=600)
    time.sleep(15)

    api.remove_device(node["uuid"], device)
    helpers.wait_for(
        f"{spec.name} {device} offline",
        lambda: _device_status(api, node_name, device) == "offline", timeout=120)

    api.restart_device(node["uuid"], device)
    helpers.wait_for(
        f"{spec.name} {device} online",
        lambda: _device_status(api, node_name, device) == "online", timeout=300)

    result = workload.wait_fio_result(state, entry["nodes"][0], pod, timeout=1200)
    workload.delete_fio_pod(state, entry["nodes"][0], pod)
    assert not workload.fio_interrupted(result), \
        f"{spec.name}: IO interrupted by device remove/restart"


def _device_status(api, hostname, device_path):
    node = api.node_by_hostname(hostname)
    part = next((p for p in node["partitions"] if p["device_path"] == device_path), None)
    return part["status"] if part else "missing"


# ----------------------- test 5: EBS force-detach (error) + replace flows

def _detachable_volume(state, spec):
    """(node_name, volume_id, device_path) of the LAST data volume — its
    device path only backs one partition entry even on the -2p variants'
    single big disk... so skip -2p there (partitioned drives cannot be
    detached independently)."""
    entry = state["edge"][spec.name]
    node_name = entry["nodes"][0]
    volumes = helpers.instance(state, node_name)["data_volumes"]
    device = f"/dev/nvme{len(volumes)}n1"
    return node_name, volumes[-1], device


DETACH_SPECS = [s for s in EDGE_CLUSTERS
                if has_device_redundancy(s) and s.drives[0].partitions == 1]


@pytest.mark.parametrize("spec", DETACH_SPECS, ids=lambda s: s.name)
def test_05a_device_error_detach_reattach(state, apis, spec):
    entry = state["edge"][spec.name]
    api = apis[spec.name]
    node_name, volume_id, device = _detachable_volume(state, spec)
    node = api.node_by_hostname(node_name)

    connect = api.connect_info(entry["volume_id"])
    pod = f"fio-{spec.name}-t5a"
    workload.start_fio_pod(state, entry["nodes"][0], pod, connect, runtime=900)
    time.sleep(15)

    helpers.force_detach_volume(state, volume_id)
    helpers.wait_for(
        f"{spec.name} {device} unavailable",
        lambda: _device_status(api, node_name, device) == "unavailable", timeout=300)

    helpers.attach_volume(state, volume_id, node_name,
                          device=f"/dev/sd{chr(ord('f') + len(entry['device_paths']) - 1)}")
    time.sleep(20)  # nvme re-enumeration on the node
    api.restart_device(node["uuid"], device)
    helpers.wait_for(
        f"{spec.name} {device} online again",
        lambda: _device_status(api, node_name, device) == "online", timeout=300)

    result = workload.wait_fio_result(state, entry["nodes"][0], pod, timeout=1800)
    workload.delete_fio_pod(state, entry["nodes"][0], pod)
    assert not workload.fio_interrupted(result), \
        f"{spec.name}: IO interrupted by EBS detach/reattach"


@pytest.mark.parametrize("spec", DETACH_SPECS, ids=lambda s: s.name)
def test_05b_permanent_replacement_with_new_volume(state, apis, spec):
    api = apis[spec.name]
    node_name, volume_id, device = _detachable_volume(state, spec)
    node = api.node_by_hostname(node_name)

    helpers.force_detach_volume(state, volume_id)
    helpers.wait_for(
        f"{spec.name} {device} unavailable",
        lambda: _device_status(api, node_name, device) == "unavailable", timeout=300)

    # A brand-new EBS volume lands one nvme slot further.
    new_index = len(helpers.instance(state, node_name)["data_volumes"]) + 1
    device_letter = chr(ord('f') + new_index - 1)
    new_volume = helpers.create_and_attach_volume(
        state, node_name, size_gb=spec.drives[-1].size_gb, device=f"/dev/sd{device_letter}")
    helpers.instance(state, node_name)["data_volumes"].append(new_volume)
    helpers.save_state(state)
    time.sleep(20)
    new_device = f"/dev/nvme{new_index}n1"

    api.replace_device(node["uuid"], device, new_device)
    helpers.wait_for(
        f"{spec.name} replacement {new_device} online",
        lambda: _device_status(api, node_name, new_device) == "online", timeout=600)


# --------------------------- test 6: flaky / broken CP<->edge connections

def _central_ips(state):
    names = [state["central"]["server"], *state["central"]["workers"]]
    return [helpers.instance(state, n)["private_ip"] for n in names]


@pytest.mark.parametrize("mode", ["flaky", "broken"])
def test_06_cp_edge_connection_faults(state, apis, mode):
    victims = random.sample(list(state["edge"]), k=3)
    central_ips = _central_ips(state)
    pods = []
    try:
        for name in victims:
            entry = state["edge"][name]
            api = apis[name]
            connect = api.connect_info(entry["volume_id"])
            pod = f"fio-{name}-t6-{mode}"
            workload.start_fio_pod(state, entry["nodes"][0], pod, connect, runtime=600)
            pods.append((name, entry["nodes"][0], pod))
        time.sleep(15)

        for name in victims:
            for node_name in state["edge"][name]["nodes"]:
                if mode == "broken":
                    helpers.break_connection(state, node_name, central_ips)
                else:
                    helpers.make_connection_flaky(state, node_name)

        if mode == "broken":
            for name in victims:
                api = apis[name]
                for node_name in state["edge"][name]["nodes"]:
                    helpers.wait_node_status(api, node_name, "unreachable", timeout=600)
                helpers.wait_for(
                    f"{name} suspended/degraded on partition",
                    lambda: apis[name].cluster_status() in ("suspended", "degraded"),
                    timeout=600)
        else:
            time.sleep(180)  # flakiness soak: statuses may flap, IO must not

    finally:
        for name in victims:
            for node_name in state["edge"][name]["nodes"]:
                helpers.heal_connection(state, node_name)

    # After healing: nodes online, clusters active.
    for name in victims:
        api = apis[name]
        for node_name in state["edge"][name]["nodes"]:
            helpers.wait_node_status(api, node_name, "online", timeout=900)
        helpers.wait_cluster_status(api, "active", timeout=600)

    # IO on the edge clusters must have run through unharmed in BOTH modes.
    for name, server, pod in pods:
        result = workload.wait_fio_result(state, server, pod, timeout=1200)
        workload.delete_fio_pod(state, server, pod)
        assert not workload.fio_interrupted(result), \
            f"{name}: local IO interrupted during {mode} CP link:\n{result['log'][-2000:]}"
