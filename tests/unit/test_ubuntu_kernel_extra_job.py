"""The Ubuntu node-preparation Job of spdk_process_start (kubernetes mode).

Regression: 2026-09-29, k3s on Ubuntu 24.04 (simplyblock-dr real-storage
test bed). Every storage node's add failed, and kept failing on every retry:

* The Job's script used bash syntax (``[[ ... ]]``, ``source``) under
  ``/bin/sh -c``. The Job mounts the host's root over the container's, so it
  runs the host's /bin/sh, which on Ubuntu is dash: the ``[[`` test failed,
  the script took its "Unable to detect OS" branch and exited 1, always.
* A Job left by a failed attempt was never deleted, so every retry of
  spdk_process_start failed at create with 409 AlreadyExists
  ("jobs.batch snode-spdk-ubuntu-extra-<node> already exists") until the
  node-add task ran out of retries.
"""
import os
import shutil
import subprocess
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import yaml
from jinja2 import Environment, PackageLoader
from kubernetes.client import ApiException

from simplyblock_web.api.internal.storage_node import kubernetes as k8s_node


def _rendered_script():
    env = Environment(loader=PackageLoader('simplyblock_web', 'templates'), trim_blocks=True, lstrip_blocks=True)
    job = yaml.safe_load(env.get_template('ubuntu_kernel_extra.yaml.j2').render(
        UBUNTU_JOBNAME='snode-spdk-ubuntu-extra-worker-1', NAMESPACE='simplyblock', HOSTNAME='worker-1'))
    container = job['spec']['template']['spec']['containers'][0]
    assert container['command'] == ['chroot', '/host', '/bin/sh', '-c']
    return container['args'][0]


class TestUbuntuJobScriptIsPosix(unittest.TestCase):

    def test_no_bash_only_syntax(self):
        script = _rendered_script()
        self.assertNotIn('[[', script)
        self.assertNotIn('source ', script)

    @unittest.skipUnless(os.name == 'posix' and shutil.which('dash'), 'dash is not installed')
    def test_parses_under_dash(self):
        subprocess.run(['dash', '-n', '-c', _rendered_script()], check=True)

    @unittest.skipUnless(os.name == 'posix' and shutil.which('dash'), 'dash is not installed')
    def test_detects_the_os_under_dash(self):
        """The OS detection alone, against a fake os-release of a non-Ubuntu
        host: dash takes the warning branch and succeeds, instead of failing
        on the test syntax."""
        with tempfile.TemporaryDirectory() as d:
            release = os.path.join(d, 'os-release')
            with open(release, 'w') as f:
                f.write('ID=testos\n')
            script = _rendered_script().replace('/etc/os-release', release)
            out = subprocess.run(['dash', '-c', script], check=True, capture_output=True, text=True).stdout
        self.assertIn('Detected OS: testos', out)
        self.assertIn('Init setup complete', out)


    @unittest.skipUnless(os.name == 'posix' and shutil.which('dash'), 'dash is not installed')
    def test_ubuntu_with_the_module_installs_nothing(self):
        """On an Ubuntu host whose kernel already provides nvme-tcp the Job
        succeeds without apt: Ubuntu's AWS kernels have no
        linux-modules-extra package at all, so installing it would fail."""
        with tempfile.TemporaryDirectory() as d:
            release = os.path.join(d, 'os-release')
            with open(release, 'w') as f:
                f.write('ID=ubuntu\n')
            for tool, code in (('modprobe', 0), ('apt-get', 1)):
                path = os.path.join(d, tool)
                with open(path, 'w') as f:
                    f.write('#!/bin/sh\necho "%s $*" >> %s/calls\nexit %d\n' % (tool, d, code))
                os.chmod(path, 0o755)
            script = _rendered_script().replace('/etc/os-release', release)
            env = dict(os.environ, PATH=d + os.pathsep + os.environ['PATH'])
            out = subprocess.run(['dash', '-c', script], check=True, capture_output=True, text=True, env=env).stdout
            with open(os.path.join(d, 'calls')) as f:
                calls = f.read()
        self.assertIn('nvme-tcp is available', out)
        self.assertIn('modprobe -n nvme-tcp', calls)
        self.assertNotIn('apt-get', calls)


def _api_exception(status):
    e = ApiException(status=status)
    e.status = status
    return e


class TestCreateJobReplacingStale(unittest.TestCase):

    def setUp(self):
        self.batch = MagicMock()
        self.job = {'metadata': {'name': 'snode-spdk-ubuntu-extra-worker-1'}}

    def test_creates_when_there_is_no_stale_job(self):
        self.batch.delete_namespaced_job.side_effect = _api_exception(404)
        k8s_node._create_job_replacing_stale(self.batch, 'simplyblock', self.job)
        self.batch.create_namespaced_job.assert_called_once_with(namespace='simplyblock', body=self.job)
        self.batch.read_namespaced_job.assert_not_called()

    def test_waits_for_a_stale_job_to_go_before_creating(self):
        calls = []
        self.batch.delete_namespaced_job.side_effect = lambda **kw: calls.append('delete')

        def read(**kw):
            calls.append('read')
            if calls.count('read') < 3:
                return MagicMock()  # still terminating
            raise _api_exception(404)
        self.batch.read_namespaced_job.side_effect = read
        self.batch.create_namespaced_job.side_effect = lambda **kw: calls.append('create')
        with patch.object(k8s_node.time, 'sleep'):
            k8s_node._create_job_replacing_stale(self.batch, 'simplyblock', self.job)
        self.assertEqual(calls, ['delete', 'read', 'read', 'read', 'create'])

    def test_gives_up_when_the_stale_job_does_not_go(self):
        self.batch.read_namespaced_job.return_value = MagicMock()
        clock = iter(range(0, 10_000, 30))
        with patch.object(k8s_node.time, 'sleep'), patch.object(k8s_node.time, 'monotonic', side_effect=lambda: next(clock)):
            with self.assertRaises(RuntimeError):
                k8s_node._create_job_replacing_stale(self.batch, 'simplyblock', self.job)
        self.batch.create_namespaced_job.assert_not_called()

    def test_other_delete_errors_propagate(self):
        self.batch.delete_namespaced_job.side_effect = _api_exception(403)
        with self.assertRaises(ApiException):
            k8s_node._create_job_replacing_stale(self.batch, 'simplyblock', self.job)
        self.batch.create_namespaced_job.assert_not_called()


if __name__ == '__main__':
    unittest.main()


class TestUbuntuJobMountsTheHostBesideTheRoot(unittest.TestCase):
    """runc refuses a mount over the container's own / ("mountpoint ... is on
    the top of rootfs"); the Job never started and the storage node waited on
    it until the cluster's deadline (2026-10-03). The host's root goes to
    /host and the script runs under chroot of it."""

    def test_host_root_is_mounted_at_host_and_chrooted(self):
        env = Environment(loader=PackageLoader('simplyblock_web', 'templates'), trim_blocks=True, lstrip_blocks=True)
        job = yaml.safe_load(env.get_template('ubuntu_kernel_extra.yaml.j2').render(
            UBUNTU_JOBNAME='snode-spdk-ubuntu-extra-worker-1', NAMESPACE='simplyblock', HOSTNAME='worker-1'))
        spec = job['spec']['template']['spec']
        container = spec['containers'][0]
        mounts = {m['name']: m['mountPath'] for m in container['volumeMounts']}
        self.assertEqual(mounts['rootfs'], '/host')
        self.assertNotIn('/', mounts.values())
        self.assertEqual({v['name']: v['hostPath']['path'] for v in spec['volumes']}['rootfs'], '/')
        self.assertEqual(container['command'][:2], ['chroot', '/host'])
