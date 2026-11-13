"""The kubelet restart of the storage node's init Job (kubernetes mode).

The Job allocates huge pages on the host and then restarts the kubelet, which
reads huge-page capacity only when it starts. The SPDK pod requests those pages,
so a kubelet that is not restarted keeps the pod unschedulable.

Regression: 2026-11-13, EKS 1.31 with Canonical's Ubuntu 22.04 node image. The
image runs the kubelet as a snap, under the unit
``snap.kubelet-eks.daemon.service``. The Job knew ``kubelet.service`` and
``k3s-agent.service`` only, printed "Neither kubelet nor k3s-agent service
found, skipping restart" and succeeded. The host had 3584 huge pages, the node
reported ``hugepages-2Mi: 0``, and the SPDK pod stayed Pending with
"Insufficient hugepages-2Mi" while the other nodes queued behind it.
"""
import os
import re
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

import yaml
from jinja2 import Environment, FileSystemLoader

TEMPLATE_DIR = Path(__file__).resolve().parents[2] / "simplyblock_web" / "templates"

_FAKE_SYSTEMCTL = """#!/bin/sh
# `systemctl cat <unit>` succeeds for the units this host has; everything is logged.
echo "$*" >> "$CALLS"
if [ "$1" = "cat" ]; then
  for unit in $HOST_UNITS; do
    [ "$unit" = "$2" ] && exit 0
  done
  exit 1
fi
exit 0
"""


def _restart_script() -> str:
    """The script the Job runs in the host's namespaces to restart the kubelet."""
    env = Environment(loader=FileSystemLoader(str(TEMPLATE_DIR)), trim_blocks=True, lstrip_blocks=True)
    job = yaml.safe_load(env.get_template("storage_init_job.yaml.j2").render(
        JOBNAME="snode-spdk-job-worker-1", NAMESPACE="simplyblock", HOSTNAME="worker-1",
        TLS_CONNECT="disabled"))
    script = job["spec"]["template"]["spec"]["containers"][0]["args"][0]
    found = re.search(r"nsenter --target 1 [^\n]*-- /bin/sh -c '\n(.*?)\n\s*'\n", script, re.DOTALL)
    assert found, "the init Job no longer restarts the kubelet through nsenter"
    return found.group(1)


@unittest.skipUnless(os.name == "posix" and shutil.which("dash"), "dash is not installed")
class TestInitJobRestartsTheKubelet(unittest.TestCase):

    def _run(self, host_units: str):
        """Run the restart script on a host that has ``host_units``; return
        what it printed and the systemctl calls it made."""
        with tempfile.TemporaryDirectory() as d:
            systemctl = os.path.join(d, "systemctl")
            with open(systemctl, "w") as f:
                f.write(_FAKE_SYSTEMCTL)
            os.chmod(systemctl, 0o755)
            calls = os.path.join(d, "calls")
            env = dict(os.environ, PATH=d + os.pathsep + os.environ["PATH"], CALLS=calls, HOST_UNITS=host_units)
            out = subprocess.run(
                ["dash", "-c", _restart_script()], check=True, capture_output=True, text=True, env=env).stdout
            with open(calls) as f:
                return out, f.read().splitlines()

    def test_restarts_the_snap_kubelet_of_an_ubuntu_eks_node(self):
        _, calls = self._run("snap.kubelet-eks.daemon.service")
        self.assertIn("restart snap.kubelet-eks.daemon.service", calls)

    def test_restarts_a_systemd_kubelet(self):
        _, calls = self._run("kubelet.service")
        self.assertEqual([c for c in calls if c.startswith("restart")], ["restart kubelet.service"])

    def test_restarts_the_k3s_agent(self):
        _, calls = self._run("k3s-agent.service")
        self.assertEqual([c for c in calls if c.startswith("restart")], ["restart k3s-agent.service"])

    def test_restarts_one_unit_only(self):
        """A host with more than one of them has one kubelet: the first match
        is the one restarted."""
        _, calls = self._run("kubelet.service snap.kubelet-eks.daemon.service")
        self.assertEqual([c for c in calls if c.startswith("restart")], ["restart kubelet.service"])

    def test_a_host_with_no_known_unit_says_what_was_looked_for(self):
        """Nothing is restarted, and the Job's log names every unit it looked
        for, so the next distribution that packages the kubelet differently is
        found from the log instead of from a Pending pod."""
        out, calls = self._run("")
        self.assertEqual([c for c in calls if c.startswith("restart")], [])
        for unit in ("kubelet.service", "snap.kubelet-eks.daemon.service", "k3s-agent.service"):
            self.assertIn(unit, out)
        self.assertIn("not restarted", out)


if __name__ == "__main__":
    unittest.main()
