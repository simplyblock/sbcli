"""Talos: huge pages reserved through the Talos API, not written to the host.

On Talos the host is immutable and has no shell, so the node agent cannot
write ``nr_hugepages`` or restart the kubelet the way the init Job does
elsewhere. The storage node used to discover Talos by sniffing
``/proc/version`` inside the init Job and then simply skip -- leaving the SPDK
pod Pending on ``Insufficient hugepages-2Mi`` whenever the operator's
pre-provisioned pool was short. It is now stated the way OpenShift is
(``TALOS_CLUSTER`` on the node agent, set by the operator from the
StorageCluster's environment), and a stated Talos node reserves the pages
itself through ``talosctl`` run as Jobs against the node's own apid, in two
steps: patch the per-NUMA counts into ``machine.sysfs`` and wait for the pool
to appear on the host, then restart the kubelet and wait for it to advertise
the pool. Two steps because Talos writes the kernel parameter asynchronously:
one Job that restarted the kubelet right after the patch booted it seeing
zero pages (lab cluster, worker-1, 2026-10-16).
"""
import json
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

MIB = 1024 * 1024
GIB = 1024 * MIB


def _env():
    return Environment(loader=PackageLoader('simplyblock_web', 'templates'), trim_blocks=True, lstrip_blocks=True)


def _rendered_init_script(**overrides):
    values = {
        'JOBNAME': 'snode-spdk-job-worker-1', 'NAMESPACE': 'simplyblock', 'HOSTNAME': 'worker-1',
        'TLS_CONNECT': 'disabled', 'TALOS_CLUSTER': False,
    }
    values.update(overrides)
    job = yaml.safe_load(_env().get_template('storage_init_job.yaml.j2').render(values))
    return job['spec']['template']['spec']['containers'][0]['args'][0]


class TestHugepageShortfall(unittest.TestCase):

    def test_enough_pages_is_no_shortfall(self):
        self.assertIsNone(k8s_node._hugepage_shortfall(13 * GIB, 12800))

    def test_exact_fit_is_no_shortfall(self):
        self.assertIsNone(k8s_node._hugepage_shortfall(12800 * MIB, 12800))

    def test_shortfall_names_both_sides_and_the_page_count(self):
        msg = k8s_node._hugepage_shortfall(2048 * MIB, 12801)
        self.assertIsNotNone(msg)
        self.assertIn('2048 MiB', msg)
        self.assertIn('12801 MiB', msg)
        self.assertIn('6401 2 MiB pages', msg)  # 12801 MiB of 2 MiB pages, rounded up

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


class TestSizingHelpers(unittest.TestCase):

    def test_pages_per_numa_is_the_apply_config_sizing(self):
        """Two node configs on NUMA 0, one on NUMA 1, max_size overriding
        where larger: the figures apply_config has always written."""
        nodes = [
            {"huge_page_memory": 4_000_000_000, "max_size": 0, "socket": 0},
            {"huge_page_memory": 4_000_000_000, "max_size": 6_000_000_000, "socket": 0},
            {"huge_page_memory": 2_000_000_000, "max_size": 0, "socket": 1},
        ]
        self.assertEqual(k8s_node._hugepages_per_numa(nodes), {
            0: (4_000_000_000 + 1_000_000_000 + 6_000_000_000 + 1_000_000_000) // 2_000_000,
            1: (2_000_000_000 + 1_000_000_000) // 2_000_000,
        })

    def test_sysfs_patch_is_one_dotted_key_per_numa_node(self):
        patch_ = k8s_node._talos_sysfs_patch({1: 1500, 0: 6000})
        self.assertEqual(patch_, {"machine": {"sysfs": {
            "devices.system.node.node0.hugepages.hugepages-2048kB.nr_hugepages": "6000",
            "devices.system.node.node1.hugepages.hugepages-2048kB.nr_hugepages": "1500",
        }}})
        # A merge patch, not a JSON-6902 array: it adds to machine.sysfs
        # instead of replacing whatever else the operator keeps there, and it
        # is the format a multi-document machine config accepts.
        self.assertIsInstance(patch_, dict)

    def test_quantity_to_mib(self):
        for q, mib in (('13Gi', 13 * 1024), ('12800Mi', 12800), ('0', 0), (None, 0),
                       (str(2 * GIB), 2048), ('1048576Ki', 1024)):
            with self.subTest(q=q):
                self.assertEqual(k8s_node._quantity_to_mib(q), mib)

    def test_job_name_keeps_the_tail_under_63(self):
        long_node = 'worker-' + 'x' * 80
        name = k8s_node._job_name('snode-talos-hugepages-', long_node)
        self.assertEqual(len(name), 63)
        self.assertTrue(name.endswith('x' * 20))
        self.assertEqual(k8s_node._job_name('p-', 'w1'), 'p-w1')


class TestTalosJobTemplate(unittest.TestCase):

    def _job(self, step):
        values = {
            'TALOS_JOBNAME': f'snode-talos-{step}-worker-1', 'NAMESPACE': 'simplyblock', 'HOSTNAME': 'worker-1',
            'TALOSCTL_IMAGE': 'ghcr.io/siderolabs/talosctl:v1.12.7', 'TALOS_API_SECRET': 'talos-api-access-talos-secrets',
            'TALOS_PATCH': json.dumps(k8s_node._talos_sysfs_patch({0: 6000}), separators=(',', ':')),
            'TALOS_STEP': step,
        }
        return yaml.safe_load(_env().get_template('storage_talos_hugepages_job.yaml.j2').render(values))

    def test_patch_step_is_one_talosctl_patch_without_a_shell(self):
        spec = self._job('patch')['spec']['template']['spec']
        self.assertEqual(spec['nodeSelector'], {'kubernetes.io/hostname': 'worker-1'})
        self.assertNotIn('initContainers', spec)
        [c] = spec['containers']
        self.assertEqual(c['name'], 'patch-machine-config')
        self.assertEqual(c['args'][:4], ['--nodes', '$(NODE_IP)', '--endpoints', '$(NODE_IP)'])
        self.assertEqual(c['args'][4:8], ['patch', 'machineconfig', '--mode=no-reboot', '--patch'])
        self.assertEqual(json.loads(c['args'][8]), k8s_node._talos_sysfs_patch({0: 6000}))
        self.assertEqual(c['image'], 'ghcr.io/siderolabs/talosctl:v1.12.7')
        self.assertNotIn('command', c)
        self.assertIn({'name': 'TALOSCONFIG', 'value': '/var/run/secrets/talos.dev/config'}, c['env'])
        self.assertEqual(c['volumeMounts'][0]['mountPath'], '/var/run/secrets/talos.dev')
        self.assertEqual(spec['volumes'][0]['secret']['secretName'], 'talos-api-access-talos-secrets')

    def test_kubelet_step_is_one_service_restart(self):
        spec = self._job('kubelet')['spec']['template']['spec']
        [c] = spec['containers']
        self.assertEqual(c['name'], 'restart-kubelet')
        self.assertEqual(c['args'][4:], ['service', 'kubelet', 'restart'])
        self.assertNotIn('--patch', c['args'])

    def test_node_ip_is_the_pods_own_host(self):
        spec = self._job('patch')['spec']['template']['spec']
        env = {e['name']: e for e in spec['containers'][0]['env']}
        self.assertEqual(env['NODE_IP']['valueFrom']['fieldRef']['fieldPath'], 'status.hostIP')


def _body(spdk_mem):
    return k8s_node.SPDKParams(
        server_ip='10.0.0.5', rpc_port=4420, rpc_username='u', rpc_password='p', l_cores='2-5',
        namespace='simplyblock', spdk_mem=spdk_mem, cluster_ip='10.0.0.1', cluster_mode='cluster',
        cluster_id='0123456789abcdef',
    )


def _node(hugepages_2mi):
    node = MagicMock()
    node.spec.unschedulable = False
    node.status.capacity = {'hugepages-2Mi': hugepages_2mi}
    return node


class TestSpdkProcessStartOnTalos(unittest.TestCase):
    """The handler under TALOS_CLUSTER=true. The host pool is what
    ``get_huge_memory`` returns on each call; the kubelet's view is what
    ``read_node`` returns on each call."""

    def setUp(self):
        patches = [
            patch.object(k8s_node, '_is_pod_up', return_value=False),
            patch.object(k8s_node, '_is_pod_present', return_value=False),
            patch.object(k8s_node, 'set_namespace'),
            patch.object(k8s_node.node_utils_k8s, 'get_namespace', return_value='simplyblock'),
            patch.object(k8s_node.node_utils_k8s, 'wait_for_job_completion'),
            patch.object(k8s_node.utils, 'get_response',
                         side_effect=lambda data, error=None, http_code=None: (data, error)),
            patch.object(k8s_node.core_utils, 'get_k8s_batch_client'),
            patch.object(k8s_node.core_utils, 'get_k8s_core_client'),
            patch.object(k8s_node.core_utils, 'load_config', return_value={"nodes": [
                {"huge_page_memory": 12 * GIB, "max_size": 0, "socket": 0}]}),
            patch.object(k8s_node, '_create_job_replacing_stale'),
            patch.object(k8s_node, 'Settings', return_value=MagicMock(
                tls_serve=False, tls_connect='disabled', tls_provider='',
                model_dump=lambda: {'tls_client_auth': 'none'})),
            patch.object(k8s_node.time, 'sleep'),
            patch.object(k8s_node, '_TALOS_HUGEPAGE_WAIT_S', 0),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)
        self.batch = k8s_node.core_utils.get_k8s_batch_client.return_value
        self.core = k8s_node.core_utils.get_k8s_core_client.return_value
        self.core.list_namespaced_pod.return_value.items = []
        self.create_job = k8s_node._create_job_replacing_stale

        def created(batch_v1, namespace, job_yaml):
            resp = MagicMock()
            resp.metadata.name = job_yaml['metadata']['name']
            return resp
        self.create_job.side_effect = created
        self.env = {'TALOS_CLUSTER': 'true', 'HOSTNAME': 'worker-1'}

    def _jobs_created(self):
        return [call.args[2] for call in self.create_job.call_args_list]

    def _talos_jobs(self):
        return [j['metadata']['name'] for j in self._jobs_created()
                if j['metadata']['name'].startswith('snode-talos-')]

    def _start(self, host_pool, kubelet_capacity, spdk_mem=12 * GIB, env=None):
        """Run the handler with the host pool and kubelet capacity sequences
        the agent will observe on successive reads (the last value repeats)."""
        host = list(host_pool)
        kube = list(kubelet_capacity)
        host_iter = (host[min(i, len(host) - 1)] for i in range(10_000))
        kube_iter = (_node(kube[min(i, len(kube) - 1)]) for i in range(10_000))
        self.core.read_node.side_effect = lambda name: next(kube_iter)
        with patch.dict(os.environ, env or self.env, clear=True), \
                patch.object(k8s_node.node_utils, 'get_huge_memory', side_effect=lambda: next(host_iter)):
            return k8s_node.spdk_process_start(_body(spdk_mem=spdk_mem))

    def test_short_pool_is_patched_then_the_kubelet_restarted_then_the_add_proceeds(self):
        """Host pool short, kubelet at zero: the patch Job runs, the agent
        waits for the host pool, then the kubelet Job runs, the agent waits
        for the kubelet, and the SPDK pod is created."""
        data, error = self._start(host_pool=[2 * GIB, 16 * GIB], kubelet_capacity=['0', '16Gi'])
        self.assertIsNone(error, error)
        self.assertIn('Pod created', data)
        self.assertEqual(self._talos_jobs(), ['snode-talos-hugepages-worker-1', 'snode-talos-kubelet-worker-1'])
        [patch_job, kubelet_job] = [j for j in self._jobs_created() if j['metadata']['name'].startswith('snode-talos-')]
        patch_arg = patch_job['spec']['template']['spec']['containers'][0]['args'][-1]
        self.assertEqual(json.loads(patch_arg), k8s_node._talos_sysfs_patch({0: (12 * GIB + 10**9) // 2_000_000}))
        self.assertEqual(kubelet_job['spec']['template']['spec']['containers'][0]['args'][-3:],
                         ['service', 'kubelet', 'restart'])
        self.core.create_namespaced_pod.assert_called()

    def test_kubelet_is_not_restarted_before_the_host_pool_exists(self):
        """The patch Job completes but Talos has not written the pages yet:
        the agent keeps waiting on the host and does not create the kubelet
        Job; when the wait runs out the add fails on the host side."""
        data, error = self._start(host_pool=[2 * GIB], kubelet_capacity=['0'])
        self.assertFalse(data)
        self.assertIn('after applying the machine config patch', error)
        self.assertIn('host provides 2048 MiB', error)
        self.assertEqual(self._talos_jobs(), ['snode-talos-hugepages-worker-1'])
        self.core.create_namespaced_pod.assert_not_called()

    def test_pool_present_but_kubelet_lagging_restarts_only_the_kubelet(self):
        """The host already has the pool (a previous attempt patched it) but
        the kubelet booted before it existed: no patch, one kubelet Job."""
        data, error = self._start(host_pool=[16 * GIB], kubelet_capacity=['0', '16Gi'])
        self.assertIsNone(error, error)
        self.assertEqual(self._talos_jobs(), ['snode-talos-kubelet-worker-1'])
        self.core.create_namespaced_pod.assert_called()

    def test_pool_and_kubelet_already_sufficient_applies_nothing(self):
        """No Talos Job -- and so no kubelet restart -- as on a plain node
        restart with the pool in place."""
        data, error = self._start(host_pool=[16 * GIB], kubelet_capacity=['16Gi'])
        self.assertIsNone(error, error)
        self.assertEqual(self._talos_jobs(), [])

    def test_kubelet_never_following_fails_the_add(self):
        data, error = self._start(host_pool=[16 * GIB], kubelet_capacity=['2Gi'])
        self.assertFalse(data)
        self.assertIn('kubelet', error)
        self.assertIn('2048 MiB', error)
        self.assertEqual(self._talos_jobs(), ['snode-talos-kubelet-worker-1'])

    def test_missing_credentials_secret_fails_before_any_job(self):
        self.core.read_namespaced_secret.side_effect = ApiException(status=404)
        data, error = self._start(host_pool=[2 * GIB], kubelet_capacity=['0'])
        self.assertFalse(data)
        self.assertIn("Secret 'talos-api-access-talos-secrets' not found", error)
        self.assertIn('os:admin', error)
        self.create_job.assert_not_called()

    def test_unreadable_secret_does_not_block(self):
        """No RBAC to read Secrets: the check is skipped and the Job tells."""
        self.core.read_namespaced_secret.side_effect = ApiException(status=403)
        data, error = self._start(host_pool=[2 * GIB, 16 * GIB], kubelet_capacity=['0', '16Gi'])
        self.assertIsNone(error, error)
        self.assertEqual(len(self._talos_jobs()), 2)

    def test_job_failure_is_reported_with_the_step_and_job_name(self):
        k8s_node.node_utils_k8s.wait_for_job_completion.side_effect = RuntimeError("Job 'x' failed")
        data, error = self._start(host_pool=[2 * GIB], kubelet_capacity=['0'])
        self.assertFalse(data)
        self.assertIn('Talos patch Job', error)
        self.assertIn('snode-talos-hugepages-worker-1', error)
        self.core.create_namespaced_pod.assert_not_called()

    def test_completed_job_log_is_copied_before_the_job_is_deleted(self):
        pod = MagicMock()
        pod.metadata.name = 'snode-talos-hugepages-worker-1-abcde'
        self.core.list_namespaced_pod.return_value.items = [pod]
        self.core.read_namespaced_pod_log.return_value = 'patched MachineConfigs\nApplied configuration without a reboot\n'
        with self.assertLogs(k8s_node.logger, level='INFO') as logs:
            data, error = self._start(host_pool=[2 * GIB, 16 * GIB], kubelet_capacity=['0', '16Gi'])
        self.assertIsNone(error, error)
        self.assertTrue(any('Applied configuration without a reboot' in line for line in logs.output), logs.output)
        self.core.read_namespaced_pod_log.assert_any_call('snode-talos-hugepages-worker-1-abcde', 'simplyblock')
        deleted = [c.kwargs['name'] for c in self.batch.delete_namespaced_job.call_args_list]
        self.assertEqual([n for n in deleted if n.startswith('snode-talos-')],
                         ['snode-talos-hugepages-worker-1', 'snode-talos-kubelet-worker-1'])

    def test_image_and_secret_come_from_the_environment(self):
        env = dict(self.env, TALOSCTL_IMAGE='mirror.local/talosctl:v1.11.5', TALOS_API_SECRET='sb-talos')
        self._start(host_pool=[2 * GIB, 16 * GIB], kubelet_capacity=['0', '16Gi'], env=env)
        [patch_job, _] = [j for j in self._jobs_created() if j['metadata']['name'].startswith('snode-talos-')]
        spec = patch_job['spec']['template']['spec']
        self.assertEqual(spec['containers'][0]['image'], 'mirror.local/talosctl:v1.11.5')
        self.assertEqual(spec['volumes'][0]['secret']['secretName'], 'sb-talos')
        self.core.read_namespaced_secret.assert_called_with('sb-talos', 'simplyblock')

    def test_host_configuration_job_is_skipped_on_talos(self):
        """Core isolation requested: the grubby Job is skipped rather than
        run against a host with no shell."""
        data, error = self._start(host_pool=[16 * GIB], kubelet_capacity=['16Gi'],
                                  env=dict(self.env, CORE_ISOLATION='true'))
        self.assertIsNone(error, error)
        self.batch.create_namespaced_job.assert_not_called()

    def test_without_the_flag_nothing_talos_happens(self):
        """A managed host reserves the pages in the init Job; the agent must
        not pre-judge it against the pool it is about to create."""
        self.core.read_node.return_value = _node('0')
        with patch.dict(os.environ, {'HOSTNAME': 'worker-1'}, clear=True), \
                patch.object(k8s_node.node_utils, 'get_huge_memory') as huge:
            data, error = k8s_node.spdk_process_start(_body(spdk_mem=12 * GIB))
        self.assertIsNone(error, error)
        huge.assert_not_called()
        self.assertEqual(self._talos_jobs(), [])


class TestApplyConfigSizingUnchanged(unittest.TestCase):

    def test_apply_config_writes_the_shared_sizing(self):
        nodes = [{"huge_page_memory": 12 * GIB, "max_size": 0, "socket": 0},
                 {"huge_page_memory": 4 * GIB, "max_size": 0, "socket": 1}]
        with patch.object(k8s_node.core_utils, 'load_config', return_value={"nodes": nodes}), \
                patch.object(k8s_node.core_utils, 'validate_config', return_value=True), \
                patch.object(k8s_node.core_utils, 'set_hugepages_if_needed') as set_hp, \
                patch.object(k8s_node.utils, 'get_response', side_effect=lambda d, error=None, http_code=None: (d, error)):
            k8s_node.apply_config()
        self.assertEqual([c.args for c in set_hp.call_args_list],
                         [(numa, pages) for numa, pages in k8s_node._hugepages_per_numa(nodes).items()])


class TestInitJobScript(unittest.TestCase):

    def test_flag_renders_into_the_script(self):
        self.assertIn('if [ "true" = "true" ]; then', _rendered_init_script(TALOS_CLUSTER=True))
        self.assertIn('if [ "false" = "true" ]; then', _rendered_init_script(TALOS_CLUSTER=False))

    def test_host_writes_are_gated_on_the_one_variable(self):
        """Every host-touching step keys off MANAGE_HUGEPAGES; the kernel
        sniff only feeds that variable, so stating Talos and detecting it
        take the same path."""
        script = _rendered_init_script()
        self.assertNotIn('if [ "$OS_ID" != "talos" ]', script)
        self.assertEqual(script.count('if [ "$MANAGE_HUGEPAGES" = "true" ]; then'), 2)

    @unittest.skipUnless(os.name == 'posix' and shutil.which('dash'), 'dash is not installed')
    def test_parses_under_dash(self):
        for flag in (True, False):
            subprocess.run(['dash', '-n', '-c', _rendered_init_script(TALOS_CLUSTER=flag)], check=True)

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
            script = (_rendered_init_script(TALOS_CLUSTER=True)
                      .replace('/proc/version', proc + '/version')
                      .replace('/proc/meminfo', proc + '/meminfo')
                      .replace('/sys/devices/system/node', d + '/no-such-sysfs')
                      .replace('/var/run/simplyblock', d))
            env = dict(os.environ, PATH=d + os.pathsep + os.environ['PATH'], NODE_IP='10.0.0.5', NODE_NAME='w1')
            out = subprocess.run(['dash', '-c', script], check=True, capture_output=True, text=True, env=env).stdout
            self.assertIn('TALOS_CLUSTER is set', out)
            self.assertIn('Init setup complete', out)
            self.assertFalse(os.path.exists(os.path.join(d, 'calls')), 'curl/nsenter were invoked')
