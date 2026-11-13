"""The kubelet CPU-topology Job of a storage node (kubernetes mode, not OpenShift).

The Job writes the static CPU manager settings into the kubelet's configuration
and restarts the kubelet. It was written for a kubeadm host: the configuration
at ``/var/lib/kubelet/config.yaml`` as YAML, and ``kubelet.service``.

Regression: 2026-11-13, EKS 1.31 with Canonical's Ubuntu 22.04 node image. The
kubelet there is started with ``--config=/etc/kubernetes/kubelet/kubelet-config.json``,
a JSON document, and runs as the snap unit ``snap.kubelet-eks.daemon.service``.
The Job stopped at "kubelet config not found at /host/var/lib/kubelet/config.yaml",
so a deployment that asked for the kubelet configuration could not add a node.
The same settings applied by hand to that file gave the SPDK pod exclusive CPUs.

The script runs for real here, against a fake host under a temporary directory:
``/host``, ``/proc`` and ``/var/simplyblock`` are pointed into it, and
``nsenter``, ``systemctl``, ``pgrep``, ``apk`` and ``sleep`` are stand-ins.
"""
import json
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

import yaml
from jinja2 import Environment, FileSystemLoader

TEMPLATE_DIR = Path(__file__).resolve().parents[2] / "simplyblock_web" / "templates"

EKS_CONFIG = "/etc/kubernetes/kubelet/kubelet-config.json"
KUBEADM_CONFIG = "/var/lib/kubelet/config.yaml"
SNAP_UNIT = "snap.kubelet-eks.daemon.service"

_FAKES = {
    # Run what follows `--` on this machine instead of in the host's namespaces.
    "nsenter": '#!/bin/sh\nwhile [ "$1" != "--" ]; do shift; done\nshift\nexec "$@"\n',
    "systemctl": (
        '#!/bin/sh\n'
        'echo "$*" >> "$CALLS"\n'
        'case "$1" in\n'
        '  cat) for unit in $HOST_UNITS; do [ "$unit" = "$2" ] && exit 0; done; exit 1 ;;\n'
        '  is-active) [ "$KUBELET_COMES_BACK" = "yes" ] && exit 0; exit 3 ;;\n'
        'esac\n'
        'exit 0\n'
    ),
    "pgrep": '#!/bin/sh\n[ -n "$KUBELET_PID" ] && echo "$KUBELET_PID"\nexit 0\n',
    "apk": "#!/bin/sh\nexit 0\n",
    "sleep": "#!/bin/sh\nexit 0\n",
}


def _script() -> str:
    env = Environment(loader=FileSystemLoader(str(TEMPLATE_DIR)), trim_blocks=True, lstrip_blocks=True)
    job = yaml.safe_load(env.get_template("storage_cpu_topology.yaml.j2").render(
        CORE_JOBNAME="snode-spdk-enable-cpu-topology-worker-1", NAMESPACE="simplyblock",
        HOSTNAME="worker-1", RESERVED_SYSTEM_CPUS="0,8"))
    return job["spec"]["template"]["spec"]["containers"][0]["args"][0]


class _Host:
    """A fake worker under a temporary directory."""

    def __init__(self, root, units, kubelet_args=None, comes_back=True):
        self.root = root
        self.units = units
        self.kubelet_args = kubelet_args
        self.comes_back = comes_back
        self.calls = os.path.join(root, "calls")
        for d in ("host/var/lib/kubelet", "host/etc/kubernetes/kubelet", "host/etc/systemd/system",
                  "var/simplyblock", "proc/4242", "bin"):
            os.makedirs(os.path.join(root, d))
        for name, body in _FAKES.items():
            path = os.path.join(root, "bin", name)
            with open(path, "w") as f:
                f.write(body)
            os.chmod(path, 0o755)
        if kubelet_args is not None:
            with open(os.path.join(root, "proc/4242/cmdline"), "wb") as f:
                f.write(b"\0".join(a.encode() for a in ["/snap/kubelet-eks/current/kubelet"] + kubelet_args) + b"\0")
        self.write("/var/lib/kubelet/cpu_manager_state", '{"policyName":"none"}')

    def path(self, host_path):
        return os.path.join(self.root, "host", host_path.lstrip("/"))

    def write(self, host_path, content):
        with open(self.path(host_path), "w") as f:
            f.write(content)

    def read(self, host_path):
        with open(self.path(host_path)) as f:
            return f.read()

    @property
    def marker(self):
        return os.path.join(self.root, "var/simplyblock/.cpu_topology_applied")

    def run(self):
        # The Job's container is busybox ash, which knows pipefail; dash, which
        # stands in for it here, does not in every version.
        script = (_script()
                  .replace("set -euo pipefail", "set -eu")
                  .replace("/host", os.path.join(self.root, "host"))
                  .replace("/var/simplyblock/", os.path.join(self.root, "var/simplyblock") + "/")
                  .replace("/proc/", os.path.join(self.root, "proc") + "/"))
        env = dict(
            os.environ,
            PATH=os.path.join(self.root, "bin") + os.pathsep + os.environ["PATH"],
            CALLS=self.calls,
            HOST_UNITS=self.units,
            KUBELET_PID="4242" if self.kubelet_args is not None else "",
            KUBELET_COMES_BACK="yes" if self.comes_back else "no",
        )
        done = subprocess.run(["dash", "-c", script], capture_output=True, text=True, env=env)
        calls = []
        if os.path.exists(self.calls):
            with open(self.calls) as f:
                calls = f.read().splitlines()
        return done, calls


def _restarts(calls):
    return [c for c in calls if c.startswith("restart")]


@unittest.skipUnless(os.name == "posix" and shutil.which("dash") and shutil.which("jq"),
                     "dash and jq are not installed")
class TestCpuTopologyJob(unittest.TestCase):

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)

    def _eks_host(self, **kw):
        host = _Host(self._tmp.name, SNAP_UNIT, kubelet_args=["--config=" + EKS_CONFIG, "--v=2"], **kw)
        host.write(EKS_CONFIG, json.dumps({
            "kind": "KubeletConfiguration",
            "cgroupDriver": "systemd",
            "kubeReserved": {"cpu": "110m", "memory": "2829Mi"},
            "kubeReservedCgroup": "/runtime",
            "systemReservedCgroup": "/system",
        }, indent=2))
        return host

    def test_configures_the_json_kubelet_of_an_ubuntu_eks_node(self):
        host = self._eks_host()
        before = host.read(EKS_CONFIG)

        done, calls = host.run()

        self.assertEqual(done.returncode, 0, done.stdout + done.stderr)
        config = json.loads(host.read(EKS_CONFIG))
        self.assertEqual(config["cpuManagerPolicy"], "static")
        self.assertEqual(config["cpuManagerReconcilePeriod"], "5s")
        self.assertEqual(config["topologyManagerPolicy"], "single-numa-node")
        self.assertEqual(config["topologyManagerScope"], "pod")
        self.assertEqual(config["reservedSystemCPUs"], "0,8")
        # What the image set stays.
        self.assertEqual(config["cgroupDriver"], "systemd")
        self.assertEqual(config["kubeReserved"], {"cpu": "110m", "memory": "2829Mi"})
        self.assertEqual(host.read(EKS_CONFIG + ".pre-simplyblock.bak"), before)
        self.assertEqual(_restarts(calls), ["restart " + SNAP_UNIT])
        self.assertFalse(os.path.exists(host.path("/var/lib/kubelet/cpu_manager_state")),
                         "the CPU manager state of the old policy is still there, and the kubelet refuses it")
        self.assertTrue(os.path.exists(host.marker))

    def test_clears_the_reserved_cgroups_the_kubelet_refuses_beside_reserved_cpus(self):
        """Amazon Linux 2023 sets both, and the kubelet does not start with
        reservedSystemCPUs and either of them."""
        host = self._eks_host()
        done, _ = host.run()
        self.assertEqual(done.returncode, 0, done.stdout + done.stderr)
        config = json.loads(host.read(EKS_CONFIG))
        self.assertNotIn("kubeReservedCgroup", config)
        self.assertNotIn("systemReservedCgroup", config)

    def test_leaves_the_kubeadm_default_alone_when_the_kubelet_reads_another_file(self):
        host = self._eks_host()
        done, _ = host.run()
        self.assertEqual(done.returncode, 0, done.stdout + done.stderr)
        self.assertFalse(os.path.exists(host.path(KUBEADM_CONFIG)))
        self.assertEqual(os.listdir(host.path("/etc/systemd/system")), [],
                         "a drop-in pointing the kubelet at a file it does not read was written")

    def test_still_configures_a_kubeadm_host(self):
        host = _Host(self._tmp.name, "kubelet.service", kubelet_args=["--config=" + KUBEADM_CONFIG])
        host.write(KUBEADM_CONFIG, "kind: KubeletConfiguration\ncgroupDriver: systemd\ncpuManagerPolicy: none\n")

        done, calls = host.run()

        self.assertEqual(done.returncode, 0, done.stdout + done.stderr)
        config = yaml.safe_load(host.read(KUBEADM_CONFIG))
        self.assertEqual(config["cpuManagerPolicy"], "static")
        self.assertEqual(config["topologyManagerPolicy"], "single-numa-node")
        self.assertEqual(config["cgroupDriver"], "systemd")
        self.assertEqual(_restarts(calls), ["restart kubelet.service"])
        self.assertTrue(os.path.exists(host.marker))

    def test_a_kubelet_that_does_not_come_back_gets_its_configuration_back(self):
        """A configuration the kubelet refuses takes the node out of the
        cluster. The Job puts the old file back, restarts the kubelet with it,
        and fails, instead of marking the node configured."""
        host = self._eks_host(comes_back=False)
        before = host.read(EKS_CONFIG)

        done, calls = host.run()

        self.assertNotEqual(done.returncode, 0)
        self.assertEqual(host.read(EKS_CONFIG), before)
        self.assertEqual(_restarts(calls), ["restart " + SNAP_UNIT] * 2)
        self.assertFalse(os.path.exists(host.marker))

    def test_a_host_with_no_known_kubelet_unit_fails_and_keeps_its_configuration(self):
        host = self._eks_host()
        host.units = ""
        before = host.read(EKS_CONFIG)

        done, calls = host.run()

        self.assertNotEqual(done.returncode, 0)
        self.assertEqual(host.read(EKS_CONFIG), before)
        self.assertEqual(_restarts(calls), [])
        self.assertFalse(os.path.exists(host.marker))

    def test_a_configured_node_is_left_alone(self):
        host = self._eks_host()
        open(host.marker, "w").close()
        before = host.read(EKS_CONFIG)

        done, calls = host.run()

        self.assertEqual(done.returncode, 0, done.stdout + done.stderr)
        self.assertEqual(host.read(EKS_CONFIG), before)
        self.assertEqual(calls, [])


if __name__ == "__main__":
    unittest.main()
