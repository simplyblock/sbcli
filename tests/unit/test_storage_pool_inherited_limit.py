"""The storage pool does not lift a limit its workers were already under.

Before simplyblock is deployed the storage workers sit in a MachineConfigPool
of the cluster's own, and that pool may state a maxUnavailable. Moving the
workers into the storage pool must not let more of them go at once than that
pool let go. A pool that states no maxUnavailable states no limit: one at a
time is then only what OpenShift does by default, and nobody asked for it.
"""

import os
import re
import shutil
import subprocess
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import yaml
from jinja2 import Environment, FileSystemLoader

from simplyblock_core import utils
from simplyblock_core.utils import storage_mco_names

TEMPLATE_DIR = Path(__file__).resolve().parents[2] / "simplyblock_web" / "templates"
NAMES = storage_mco_names("cluster1")
RENDER_HASH = "0123456789abcdef0123456789abcdef"


def _script(max_unavailable=8) -> str:
    env = Environment(loader=FileSystemLoader(str(TEMPLATE_DIR)), trim_blocks=True, lstrip_blocks=True)
    job = yaml.safe_load(env.get_template("oc_storage_cpu_topology.yaml.j2").render(
        CLUSTER_ID="cluster1", CORE_JOBNAME="core-job", HOSTNAME="node-a",
        MCP_MAX_UNAVAILABLE=max_unavailable, NAMESPACE="simplyblock", NSOCKET=0,
        OPENSHIFT_MCP="worker", RESERVED_SYSTEM_CPUS="0-1", STORAGE_MCO=NAMES._asdict()))
    return job["spec"]["template"]["spec"]["containers"][0]["args"][0]


@unittest.skipUnless(shutil.which("sh"), "needs a POSIX shell")
class TestInheritedLimit(unittest.TestCase):
    """The Job's inherited_limit, run against a kubectl that answers from the
    environment."""

    def _inherited(self, *, config, stated, machines=""):
        function = re.search(r"^inherited_limit\(\) \{\n.*?^\}\n", _script(), re.DOTALL | re.MULTILINE)
        self.assertIsNotNone(function, "the Job defines no inherited_limit")
        stub = (
            'kubectl() { case "$*" in\n'
            '  *"get node"*) printf %s "$NODE_CONFIG" ;;\n'
            '  *spec.maxUnavailable*) [ "$3" = "$EXPECTED_POOL" ] && printf %s "$POOL_STATED" ;;\n'
            '  *status.machineCount*) printf %s "$POOL_MACHINES" ;;\n'
            'esac; }\n'
        )
        script = (f'set -e\nHOSTNAME=node-a\nMCP_NAME={NAMES.pool}\n{stub}'
                  f'{function.group(0)}inherited_limit\n')
        result = subprocess.run(
            ["sh", "-c", script], capture_output=True, text=True, check=True,
            env={"PATH": os.environ["PATH"], "NODE_CONFIG": config,
                 "POOL_STATED": stated, "POOL_MACHINES": machines,
                 "EXPECTED_POOL": self.expected_pool})
        return result.stdout.strip()

    def setUp(self):
        self.expected_pool = "worker"

    def test_a_pool_that_states_no_limit_hands_none_on(self):
        self.assertEqual(self._inherited(config=f"rendered-worker-{RENDER_HASH}", stated=""), "")

    def test_a_stated_limit_is_handed_on(self):
        self.assertEqual(self._inherited(config=f"rendered-worker-{RENDER_HASH}", stated="5"), "5")

    def test_a_stated_limit_of_one_is_a_limit(self):
        """One somebody wrote down is a decision, unlike the default of one."""
        self.assertEqual(self._inherited(config=f"rendered-worker-{RENDER_HASH}", stated="1"), "1")

    def test_a_percentage_is_of_the_pool_it_was_stated_on(self):
        value = self._inherited(config=f"rendered-worker-{RENDER_HASH}", stated="10%", machines="40")
        self.assertEqual(value, "4")

    def test_a_percentage_is_never_below_one(self):
        value = self._inherited(config=f"rendered-worker-{RENDER_HASH}", stated="10%", machines="4")
        self.assertEqual(value, "1")

    def test_the_limit_is_read_from_the_pool_the_host_is_in(self):
        """A host of a custom pool is under that pool's limit, whatever the
        worker pool says."""
        self.expected_pool = "infra-storage"
        value = self._inherited(config=f"rendered-infra-storage-{RENDER_HASH}", stated="2")
        self.assertEqual(value, "2")

    def test_a_host_with_no_rendered_config_is_read_as_a_worker(self):
        self.assertEqual(self._inherited(config="", stated="3"), "3")

    def test_a_host_already_in_the_storage_pool_inherits_nothing_from_it(self):
        """Its limit is the one this code wrote, which is not the customer's."""
        self.expected_pool = NAMES.pool
        value = self._inherited(config=f"rendered-{NAMES.pool}-{RENDER_HASH}", stated="8")
        self.assertEqual(value, "")


class TestThePoolIsCreatedUnderTheInheritedLimit(unittest.TestCase):

    def test_the_pool_records_what_it_inherited(self):
        self.assertIn(f'{NAMES.inherited_limit_annotation}: "$INHERITED"', _script())

    def test_the_bring_up_value_is_narrowed_to_it(self):
        script = _script(max_unavailable=8)
        self.assertIn("MAX_UNAVAILABLE=8", script)
        self.assertIn('[ "$INHERITED" -lt "$MAX_UNAVAILABLE" ]', script)
        self.assertIn("maxUnavailable: $MAX_UNAVAILABLE", script)


class TestEveryLaterValueStaysUnderIt(unittest.TestCase):
    """set_storage_mcp_max_unavailable is the one writer after the pool exists."""

    def _set(self, wanted, annotations):
        api = MagicMock()
        api.get_cluster_custom_object.return_value = {"metadata": {"annotations": annotations}}
        with patch.object(utils, "load_kube_config_with_fallback"), \
                patch.object(utils.client, "CustomObjectsApi", return_value=api):
            self.assertTrue(utils.set_storage_mcp_max_unavailable("cluster1", wanted))
        return api.patch_cluster_custom_object.call_args.kwargs["body"]["spec"]["maxUnavailable"]

    def test_a_wider_value_is_narrowed_to_the_inherited_limit(self):
        self.assertEqual(self._set(10, {NAMES.inherited_limit_annotation: "5"}), 5)

    def test_a_narrower_value_is_kept(self):
        self.assertEqual(self._set(3, {NAMES.inherited_limit_annotation: "5"}), 3)

    def test_a_pool_that_inherited_nothing_takes_the_value(self):
        self.assertEqual(self._set(10, {NAMES.inherited_limit_annotation: ""}), 10)
        self.assertEqual(self._set(10, {}), 10)
        self.assertEqual(self._set(10, None), 10)


if __name__ == "__main__":
    unittest.main()
