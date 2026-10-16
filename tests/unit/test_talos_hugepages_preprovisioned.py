"""Talos: huge pages are a prerequisite the node verifies, not a setting it applies.

On Talos the host is immutable and has no shell, so the node agent cannot write
``nr_hugepages`` or restart the kubelet; the operator provisions the pool through
the machine config (``machine.sysctls`` ``vm.nr_hugepages``) before installing.
The storage node used to discover this by sniffing ``/proc/version`` inside the
init Job. It is now stated the way OpenShift is -- ``TALOS_CLUSTER`` on the node
agent, set by the operator from the StorageCluster's environment -- and a stated
Talos node checks the pool against what the SPDK pod is about to request instead
of letting the pod sit Pending on ``Insufficient hugepages-2Mi``.
"""
import os
import shutil
import subprocess
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import yaml
from jinja2 import Environment, PackageLoader

from simplyblock_web.api.internal.storage_node import kubernetes as k8s_node

MIB = 1024 * 1024


def _rendered_script(**overrides):
    values = {
        'JOBNAME': 'snode-spdk-job-worker-1', 'NAMESPACE': 'simplyblock', 'HOSTNAME': 'worker-1',
        'TLS_CONNECT': 'disabled', 'TALOS_CLUSTER': False,
    }
    values.update(overrides)
    env = Environment(loader=PackageLoader('simplyblock_web', 'templates'), trim_blocks=True, lstrip_blocks=True)
    job = yaml.safe_load(env.get_template('storage_init_job.yaml.j2').render(values))
    return job['spec']['template']['spec']['containers'][0]['args'][0]


class TestHugepageShortfall(unittest.TestCase):

    def test_enough_pages_is_no_shortfall(self):
        self.assertIsNone(k8s_node._hugepage_shortfall(13 * 1024 * MIB, 12800))

    def test_exact_fit_is_no_shortfall(self):
        self.assertIsNone(k8s_node._hugepage_shortfall(12800 * MIB, 12800))

    def test_shortfall_names_both_sides_and_the_sysctl_to_set(self):
        msg = k8s_node._hugepage_shortfall(2048 * MIB, 12801)
        self.assertIsNotNone(msg)
        self.assertIn('2048 MiB', msg)
        self.assertIn('12801 MiB', msg)
        # 12801 MiB of 2 MiB pages, rounded up.
        self.assertIn('vm.nr_hugepages to at least 6401', msg)

    def test_no_pool_at_all(self):
        self.assertIn('host provides 0 MiB', k8s_node._hugepage_shortfall(0, 1024))


class TestEnvFlag(unittest.TestCase):

    def test_unset_is_false(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertFalse(k8s_node._env_flag('TALOS_CLUSTER'))

    def test_true_in_any_case_with_whitespace(self):
        with patch.dict(os.environ, {'TALOS_CLUSTER': ' True '}, clear=True):
            self.assertTrue(k8s_node._env_flag('TALOS_CLUSTER'))

    def test_false_and_junk_are_false(self):
        for v in ('false', '1', 'yes', ''):
            with self.subTest(v=v), patch.dict(os.environ, {'TALOS_CLUSTER': v}, clear=True):
                self.assertFalse(k8s_node._env_flag('TALOS_CLUSTER'))


def _body(spdk_mem):
    return k8s_node.SPDKParams(
        server_ip='10.0.0.5', rpc_port=4420, rpc_username='u', rpc_password='p', l_cores='2-5',
        namespace='simplyblock', spdk_mem=spdk_mem, cluster_ip='10.0.0.1', cluster_mode='cluster',
        cluster_id='0123456789abcdef',
    )


class TestSpdkProcessStartOnTalos(unittest.TestCase):
    """The handler under TALOS_CLUSTER=true: it fails the add before creating
    any Job when the host's pool is short, and launches nothing that reaches
    into the host."""

    def setUp(self):
        patches = [
            patch.object(k8s_node, '_is_pod_up', return_value=False),
            patch.object(k8s_node, '_is_pod_present', return_value=False),
            patch.object(k8s_node, 'set_namespace'),
            patch.object(k8s_node.node_utils_k8s, 'get_namespace', return_value='simplyblock'),
            patch.object(k8s_node.utils, 'get_response',
                         side_effect=lambda data, error=None, http_code=None: (data, error)),
            patch.object(k8s_node.core_utils, 'get_k8s_batch_client'),
            patch.object(k8s_node.core_utils, 'get_k8s_core_client'),
            patch.object(k8s_node, 'Settings', return_value=MagicMock(
                tls_serve=False, tls_connect='disabled', tls_provider='',
                model_dump=lambda: {'tls_client_auth': 'none'})),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)
        self.batch = k8s_node.core_utils.get_k8s_batch_client.return_value

    def test_short_pool_fails_before_any_job(self):
        env = {'TALOS_CLUSTER': 'true', 'HOSTNAME': 'worker-1'}
        with patch.dict(os.environ, env, clear=True), \
                patch.object(k8s_node.node_utils, 'get_huge_memory', return_value=2048 * MIB):
            data, error = k8s_node.spdk_process_start(_body(spdk_mem=12 * 1024 * MIB))
        self.assertFalse(data)
        self.assertIn('host provides 2048 MiB', error)
        self.assertIn('vm.nr_hugepages', error)
        self.batch.create_namespaced_job.assert_not_called()

    def test_enough_pool_proceeds_and_skips_the_host_configuration_job(self):
        """With the pool in place the add goes on; with core isolation
        requested the grubby Job is skipped rather than run against a host
        with no shell, and the SPDK pod is created."""
        env = {'TALOS_CLUSTER': 'true', 'HOSTNAME': 'worker-1', 'CORE_ISOLATION': 'true'}
        node = MagicMock()
        node.spec.unschedulable = False
        core = k8s_node.core_utils.get_k8s_core_client.return_value
        core.read_node.return_value = node
        with patch.dict(os.environ, env, clear=True), \
                patch.object(k8s_node.node_utils, 'get_huge_memory', return_value=16 * 1024 * MIB), \
                patch.object(k8s_node, '_create_job_replacing_stale') as create_job, \
                patch.object(k8s_node.node_utils_k8s, 'wait_for_job_completion'):
            data, error = k8s_node.spdk_process_start(_body(spdk_mem=12 * 1024 * MIB))
        self.assertIsNone(error, error)
        self.assertIn('Pod created', data)
        # Exactly the init Job (baseline capture, no host write) -- not the
        # core-isolation Job, which is a create_namespaced_job on the batch
        # client and must not have happened.
        self.assertEqual(create_job.call_count, 1)
        self.batch.create_namespaced_job.assert_not_called()
        rendered = create_job.call_args.args[2]
        self.assertIn('TALOS_CLUSTER is set', rendered['spec']['template']['spec']['containers'][0]['args'][0])

    def test_without_the_flag_no_pool_check_is_made(self):
        """A managed host reserves the pages in the init Job; the agent must
        not pre-judge it against the pool it is about to create."""
        env = {'HOSTNAME': 'worker-1'}
        node = MagicMock()
        node.spec.unschedulable = False
        k8s_node.core_utils.get_k8s_core_client.return_value.read_node.return_value = node
        with patch.dict(os.environ, env, clear=True), \
                patch.object(k8s_node.node_utils, 'get_huge_memory') as huge, \
                patch.object(k8s_node, '_create_job_replacing_stale'), \
                patch.object(k8s_node.node_utils_k8s, 'wait_for_job_completion'):
            data, error = k8s_node.spdk_process_start(_body(spdk_mem=12 * 1024 * MIB))
        self.assertIsNone(error, error)
        huge.assert_not_called()


class TestInitJobScript(unittest.TestCase):

    def test_flag_renders_into_the_script(self):
        self.assertIn('if [ "true" = "true" ]; then', _rendered_script(TALOS_CLUSTER=True))
        self.assertIn('if [ "false" = "true" ]; then', _rendered_script(TALOS_CLUSTER=False))

    def test_host_writes_are_gated_on_the_one_variable(self):
        """Every host-touching step keys off MANAGE_HUGEPAGES; the kernel
        sniff only feeds that variable, so stating Talos and detecting it
        take the same path."""
        script = _rendered_script()
        self.assertNotIn('if [ "$OS_ID" != "talos" ]', script)
        self.assertEqual(script.count('if [ "$MANAGE_HUGEPAGES" = "true" ]; then'), 2)

    @unittest.skipUnless(os.name == 'posix' and shutil.which('dash'), 'dash is not installed')
    def test_parses_under_dash(self):
        for flag in (True, False):
            subprocess.run(['dash', '-n', '-c', _rendered_script(TALOS_CLUSTER=flag)], check=True)

    @unittest.skipUnless(os.name == 'posix' and shutil.which('dash'), 'dash is not installed')
    def test_stated_talos_on_a_non_talos_kernel_sends_no_config(self):
        """The flag alone, with a kernel string that does not say talos: the
        script never POSTs apply_config and never restarts anything."""
        with tempfile.TemporaryDirectory() as d:
            proc = os.path.join(d, 'proc')
            os.makedirs(proc)
            with open(os.path.join(proc, 'version'), 'w') as f:
                f.write('Linux version 6.8.0-generic (build@host) #1 SMP\n')
            with open(os.path.join(proc, 'meminfo'), 'w') as f:
                f.write('HugePages_Total:    1024\n')
            for tool in ('curl', 'nsenter'):
                path = os.path.join(d, tool)
                with open(path, 'w') as f:
                    f.write('#!/bin/sh\necho "%s $*" >> %s/calls\necho 200\n' % (tool, d))
                os.chmod(path, 0o755)
            script = (_rendered_script(TALOS_CLUSTER=True)
                      .replace('/proc/version', proc + '/version')
                      .replace('/proc/meminfo', proc + '/meminfo')
                      .replace('/sys/devices/system/node', d + '/no-such-sysfs')
                      .replace('/var/run/simplyblock', d))
            env = dict(os.environ, PATH=d + os.pathsep + os.environ['PATH'], NODE_IP='10.0.0.5', NODE_NAME='w1')
            out = subprocess.run(['dash', '-c', script], check=True, capture_output=True, text=True, env=env).stdout
            self.assertIn('TALOS_CLUSTER is set', out)
            self.assertIn('Init setup complete', out)
            self.assertFalse(os.path.exists(os.path.join(d, 'calls')), 'curl/nsenter were invoked')
