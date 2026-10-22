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
already the same moves without a reboot."""

import os

from jinja2 import Environment, FileSystemLoader

TEMPLATES = os.path.join(os.path.dirname(__file__), "..", "..", "simplyblock_web", "templates")


def _script():
    env = Environment(loader=FileSystemLoader(TEMPLATES), trim_blocks=True, lstrip_blocks=True)
    return env.get_template("oc_storage_cpu_topology.yaml.j2").render(
        CLUSTER_ID="ac85c3", HOSTNAME="worker-1", MCP_MAX_UNAVAILABLE=4,
        RESERVED_SYSTEM_CPUS="0-1", NSOCKET=0, OPENSHIFT_MCP="worker")


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
