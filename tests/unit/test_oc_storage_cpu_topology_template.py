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

    def test_plugin_pod_is_looked_up_in_every_namespace(self):
        """The NUMA device plugin is run by the operator in its own namespace,
        owned by the ControlPlane, so the readiness wait cannot assume
        kube-system. The service account reads pods cluster-wide through its
        ClusterRoleBinding, so the lookup spans all namespaces."""
        lookup = [line for line in _render().splitlines() if "kubectl get pod" in line]
        self.assertEqual(len(lookup), 1, "the plugin pod lookup is missing from the template")
        self.assertIn(" -A", lookup[0])
        self.assertNotIn("kube-system", lookup[0])


if __name__ == "__main__":
    unittest.main()
