"""``cluster update-secret`` updates every copy of the secret, or none.

The original bug: ``set_secret`` always looked the cluster's user up in Grafana,
so on a cluster created with monitoring disabled the command failed on a DNS
error for ``simplyblock-grafana`` and the new secret was never stored. With
monitoring enabled it reset only the Grafana user and left Prometheus scraping
with the old secret.
"""
import uuid
from unittest.mock import patch

import pytest
from pydantic import SecretStr

from simplyblock_core import cluster_ops, utils
from simplyblock_core.db_controller import DBController
from simplyblock_core.exceptions import MonitoringConfigError
from simplyblock_core.models.cluster import Cluster

OLD_SECRET = "oldsupersecret1234567891012"
NEW_SECRET = "newsupersecret1234567891012"


def _seed_cluster(disable_monitoring: bool, mode: str = "docker") -> Cluster:
    db = DBController()
    cluster = Cluster()
    cluster.uuid = str(uuid.uuid4())
    cluster.secret = SecretStr(OLD_SECRET)
    cluster.grafana_endpoint = "http://simplyblock-grafana:3000"
    cluster.grafana_secret = SecretStr("grafana-admin")
    cluster.disable_monitoring = disable_monitoring
    cluster.mode = mode
    cluster.write_to_db(db.kv_store)
    return cluster


def _stored_secret(cluster: Cluster) -> str:
    return DBController().get_cluster_by_id(cluster.uuid).secret.get_secret_value()


@pytest.fixture()
def grafana():
    with patch.object(cluster_ops, "_sync_grafana_user") as mock:
        yield mock


@pytest.fixture()
def docker_rotation():
    with patch.object(cluster_ops, "_rotate_docker_monitoring_secret") as mock:
        yield mock


@pytest.fixture()
def configmap_rotation():
    with patch.object(utils, "rotate_prometheus_configmap_secret", return_value=True) as mock:
        yield mock


def test_monitoring_disabled_touches_no_monitoring_component(grafana, docker_rotation, configmap_rotation):
    cluster = _seed_cluster(disable_monitoring=True)

    cluster_ops.set_secret(cluster.uuid, SecretStr(NEW_SECRET))

    grafana.assert_not_called()
    docker_rotation.assert_not_called()
    configmap_rotation.assert_not_called()
    assert _stored_secret(cluster) == NEW_SECRET


def test_docker_cluster_rotates_grafana_and_the_stack_configs(grafana, docker_rotation, configmap_rotation):
    cluster = _seed_cluster(disable_monitoring=False, mode="docker")

    cluster_ops.set_secret(cluster.uuid, SecretStr(NEW_SECRET))

    grafana.assert_called_once()
    assert grafana.call_args.args[0] == cluster.uuid
    assert grafana.call_args.args[3].get_secret_value() == NEW_SECRET
    docker_rotation.assert_called_once_with(cluster.uuid, OLD_SECRET, NEW_SECRET)
    configmap_rotation.assert_not_called()
    assert _stored_secret(cluster) == NEW_SECRET


def test_kubernetes_cluster_rotates_grafana_and_the_configmap(grafana, docker_rotation, configmap_rotation):
    cluster = _seed_cluster(disable_monitoring=False, mode="kubernetes")

    cluster_ops.set_secret(cluster.uuid, SecretStr(NEW_SECRET))

    grafana.assert_called_once()
    configmap_rotation.assert_called_once_with(OLD_SECRET, NEW_SECRET)
    docker_rotation.assert_not_called()
    assert _stored_secret(cluster) == NEW_SECRET


def test_a_grafana_failure_keeps_the_old_secret(grafana, docker_rotation, configmap_rotation):
    cluster = _seed_cluster(disable_monitoring=False)
    grafana.side_effect = MonitoringConfigError("Grafana is unreachable")

    with pytest.raises(MonitoringConfigError):
        cluster_ops.set_secret(cluster.uuid, SecretStr(NEW_SECRET))

    docker_rotation.assert_not_called()
    assert _stored_secret(cluster) == OLD_SECRET


def test_a_prometheus_failure_keeps_the_old_secret(grafana, docker_rotation, configmap_rotation):
    cluster = _seed_cluster(disable_monitoring=False)
    docker_rotation.side_effect = MonitoringConfigError("monitoring_prometheus not found")

    with pytest.raises(MonitoringConfigError):
        cluster_ops.set_secret(cluster.uuid, SecretStr(NEW_SECRET))

    assert _stored_secret(cluster) == OLD_SECRET


@pytest.mark.parametrize("secret", [
    "short",
    "has a space in it 1234567890",
    "has'aquote1234567890123456",
])
def test_an_unusable_secret_is_refused_before_anything_changes(grafana, docker_rotation, secret):
    cluster = _seed_cluster(disable_monitoring=False)

    with pytest.raises(ValueError):
        cluster_ops.set_secret(cluster.uuid, SecretStr(secret))

    grafana.assert_not_called()
    assert _stored_secret(cluster) == OLD_SECRET
