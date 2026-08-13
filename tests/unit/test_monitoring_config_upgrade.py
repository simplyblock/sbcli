# coding=utf-8
"""
test_monitoring_config_upgrade.py — the monitoring-config upgrade path of
``sbctl cluster update`` (docker mode).

``scripts/prometheus.yml`` and ``scripts/alerting/alert_resources.yaml`` are
rendered artifacts: they are not tracked by git and not recorded by pip, so
upgrading the package leaves the previously rendered files untouched. Before
this, only ``create_cluster`` and ``add_mgmt_node`` ever rendered them, so a
cluster upgraded in place kept scraping and alerting on the deploy-time
templates — the v2 ``simplyblock_metrics`` scrape job shipped with this release
would never have appeared. Both consumers additionally read their config only at
startup, so the re-render has to be followed by a service restart.
"""

import os
import subprocess
from unittest.mock import MagicMock, patch

import pytest
import yaml
from docker.errors import APIError, NotFound
from pydantic import SecretStr

from simplyblock_core import cluster_ops, utils
from simplyblock_core.models.cluster import Cluster


@pytest.fixture
def captured_render():
    """Run the renderers without root, capturing what they would have installed.

    Both renderers write into a temporary directory and then ``sudo mv`` the
    result next to their template, which a test can neither do nor should do.
    """
    captured = {}

    def fake_check_call(args, *rest, **kwargs):
        if args[:2] == ['sudo', 'mv']:
            source, destination = args[2], args[3]
            with open(source) as file:
                captured[os.path.basename(destination)] = file.read()
            os.remove(source)
        return 0

    with patch('simplyblock_core.utils.subprocess.check_call', fake_check_call):
        yield captured


def _scrape_jobs(rendered_config):
    return {job['job_name']: job for job in yaml.safe_load(rendered_config)['scrape_configs']}


def test_rendered_prometheus_config_scrapes_both_exporters(captured_render):
    utils.render_prometheus_config('cluster-uuid', 'topsecret')

    jobs = _scrape_jobs(captured_render['prometheus.yml'])
    assert 'cluster_metrics' in jobs
    assert 'simplyblock_metrics' in jobs


def test_rendered_prometheus_config_authenticates_v2_job_with_bearer_token(captured_render):
    utils.render_prometheus_config('cluster-uuid', 'topsecret')

    job = _scrape_jobs(captured_render['prometheus.yml'])['simplyblock_metrics']
    assert job['metrics_path'] == '/api/v2/metrics'
    assert job['static_configs'] == [{'targets': ['HAProxy:80']}]
    assert job['authorization'] == {'credentials': 'topsecret'}


def test_rendered_prometheus_config_keeps_v1_basic_auth(captured_render):
    utils.render_prometheus_config('cluster-uuid', 'topsecret')

    job = _scrape_jobs(captured_render['prometheus.yml'])['cluster_metrics']
    assert job['basic_auth'] == {'username': 'cluster-uuid', 'password': 'topsecret'}


def test_alerting_render_also_renders_prometheus_config(captured_render):
    """The two are rendered together, so callers refresh both at once."""
    utils.render_and_deploy_alerting_configs(
        'https://hooks.slack.com/services/T00/B00/XXX', 'http://1.2.3.4/grafana',
        'cluster-uuid', 'topsecret')

    assert set(captured_render) == {'prometheus.yml', 'alert_resources.yaml'}


def _docker_client_with(*present_service_names):
    services = {name: MagicMock(name=name) for name in present_service_names}

    def get(name):
        if name not in services:
            raise NotFound(f"service {name} not found")
        return services[name]

    client = MagicMock()
    client.services.get.side_effect = get
    return client, services


def test_restart_monitoring_services_force_updates_config_consumers():
    client, services = _docker_client_with('monitoring_prometheus', 'monitoring_grafana')

    utils.restart_monitoring_services(client)

    for service in services.values():
        service.update.assert_called_once_with(force_update=True)


def test_restart_monitoring_services_skips_absent_services():
    """Monitoring is optional, so its services may not exist at all."""
    client, _ = _docker_client_with()

    utils.restart_monitoring_services(client)  # no NotFound escapes


def _cluster(disable_monitoring=False):
    cluster = MagicMock(spec=Cluster)
    cluster.uuid = 'cluster-uuid'
    cluster.mode = 'docker'
    cluster.secret = SecretStr('topsecret')
    cluster.contact_point = 'https://hooks.slack.com/services/T00/B00/XXX'
    cluster.grafana_endpoint = 'http://1.2.3.4/grafana'
    cluster.disable_monitoring = disable_monitoring
    return cluster


def test_refresh_monitoring_config_renders_and_restarts():
    cluster = _cluster()
    client = MagicMock()

    with (
        patch.object(cluster_ops.utils, 'render_and_deploy_alerting_configs') as render,
        patch.object(cluster_ops.utils, 'restart_monitoring_services') as restart,
    ):
        cluster_ops._refresh_monitoring_config(cluster, client)

    render.assert_called_once_with(
        'https://hooks.slack.com/services/T00/B00/XXX', 'http://1.2.3.4/grafana',
        'cluster-uuid', 'topsecret')
    restart.assert_called_once_with(client)


def test_refresh_monitoring_config_skipped_when_monitoring_disabled():
    with (
        patch.object(cluster_ops.utils, 'render_and_deploy_alerting_configs') as render,
        patch.object(cluster_ops.utils, 'restart_monitoring_services') as restart,
    ):
        cluster_ops._refresh_monitoring_config(_cluster(disable_monitoring=True), MagicMock())

    render.assert_not_called()
    restart.assert_not_called()


@pytest.mark.parametrize('failure', [
    subprocess.CalledProcessError(1, ['sudo', 'mv']),
    APIError('swarm unreachable'),
])
def test_refresh_monitoring_config_failure_does_not_abort_update(failure):
    """Monitoring is not on the data path, so it must not fail the upgrade."""
    with (
        patch.object(cluster_ops.utils, 'render_and_deploy_alerting_configs', side_effect=failure),
        patch.object(cluster_ops.utils, 'restart_monitoring_services') as restart,
    ):
        cluster_ops._refresh_monitoring_config(_cluster(), MagicMock())

    restart.assert_not_called()


def test_update_cluster_refreshes_monitoring_even_with_cp_only():
    """Prometheus and Grafana are control plane, so ``--cp-only`` includes them."""
    cluster = _cluster()
    client = MagicMock()
    client.services.list.return_value = []

    with (
        patch.object(cluster_ops.db_controller, 'get_cluster_by_id', return_value=cluster),
        patch.object(cluster_ops.utils, 'get_docker_client', return_value=client),
        patch.object(cluster_ops.utils, 'create_docker_service'),
        patch.object(cluster_ops, 'pull_docker_image_with_retry'),
        patch.object(cluster_ops, '_refresh_monitoring_config') as refresh,
    ):
        cluster_ops.update_cluster('cluster-uuid', mgmt_only=True)

    refresh.assert_called_once_with(cluster, client)
