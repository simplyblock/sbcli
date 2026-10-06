"""The Kubernetes Jobs the storage-node API renders are marked for the log collector.

The operator's chart runs fluent-bit, which ships only pods annotated
``log-collector/enabled: "true"`` to Graylog. The SPDK pod template carries the
annotation; these Jobs did not, so a node preparation that failed left nothing
in Graylog once the Job was cleaned up.

Regression: 2026-10-06-graylog-receives-nothing.
"""
import unittest

import yaml
from jinja2 import Environment, PackageLoader

JOB_TEMPLATES = (
    'storage_init_job.yaml.j2',
    'storage_core_isolation.yaml.j2',
    'oc_storage_core_isolation.yaml.j2',
    'storage_cpu_topology.yaml.j2',
    'oc_storage_cpu_topology.yaml.j2',
    'ubuntu_kernel_extra.yaml.j2',
)


class TestJobTemplatesAreShippedToTheLogCollector(unittest.TestCase):

    def test_every_job_pod_is_marked(self):
        env = Environment(loader=PackageLoader('simplyblock_web', 'templates'),
                          trim_blocks=True, lstrip_blocks=True)
        for name in JOB_TEMPLATES:
            with self.subTest(template=name):
                job = yaml.safe_load(env.get_template(name).render())
                self.assertEqual(job['kind'], 'Job')
                annotations = (job['spec']['template'].get('metadata') or {}).get('annotations') or {}
                self.assertEqual(annotations.get('log-collector/enabled'), 'true')
