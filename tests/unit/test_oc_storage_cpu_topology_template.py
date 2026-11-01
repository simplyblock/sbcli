import unittest
from pathlib import Path

from jinja2 import Environment, FileSystemLoader

TEMPLATE_DIR = Path(__file__).resolve().parents[2] / "simplyblock_web" / "templates"


def _render() -> str:
    env = Environment(
        loader=FileSystemLoader(str(TEMPLATE_DIR)),
        trim_blocks=True,
        lstrip_blocks=True,
    )
    return env.get_template("oc_storage_cpu_topology.yaml.j2").render(
        CLUSTER_ID="cluster1",
        CORE_JOBNAME="core-job",
        HOSTNAME="node-a",
        MCP_MAX_UNAVAILABLE=1,
        NAMESPACE="simplyblock",
        NSOCKET=0,
        OPENSHIFT_MCP="worker",
        RESERVED_SYSTEM_CPUS="0-1",
    )


class TestOcStorageCpuTopologyTemplate(unittest.TestCase):

    def test_numa_readiness_does_not_look_up_the_plugin_pod(self):
        """The operator runs the NUMA plugin in a namespace this template does
        not know. The node's allocatable numa-align count is the readiness
        signal: the kubelet reports it only while the plugin is registered."""
        rendered = _render()
        self.assertNotIn("kubectl get pod", rendered)
        self.assertNotIn("simplyblock-numa-resource-plugin", rendered)
        self.assertIn("{.status.allocatable.numa-align/numa-0}", rendered)
        self.assertIn('[ "$NUMA_ALLOC" = "100" ]', rendered)


if __name__ == "__main__":
    unittest.main()
