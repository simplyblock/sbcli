"""``cluster update-secret`` only talks to Grafana when the cluster has one.

The original bug: ``set_secret`` always looked the cluster's user up in Grafana,
so on a cluster created with monitoring disabled the command failed on a DNS
error for ``simplyblock-grafana`` and the new secret was never stored.
"""
import uuid
from unittest.mock import patch

import pytest
from pydantic import SecretStr

from simplyblock_core import cluster_ops
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.cluster import Cluster

NEW_SECRET = "newsupersecret1234567891012"


def _seed_cluster(disable_monitoring: bool) -> Cluster:
    db = DBController()
    cluster = Cluster()
    cluster.uuid = str(uuid.uuid4())
    cluster.secret = SecretStr("oldsupersecret1234567891012")
    cluster.grafana_endpoint = "http://simplyblock-grafana:3000"
    cluster.grafana_secret = SecretStr("grafana-admin")
    cluster.disable_monitoring = disable_monitoring
    cluster.write_to_db(db.kv_store)
    return cluster


@pytest.fixture()
def grafana():
    with patch.object(cluster_ops, "_create_update_user") as mock:
        yield mock


def test_monitoring_disabled_skips_grafana_and_stores_secret(grafana):
    cluster = _seed_cluster(disable_monitoring=True)

    cluster_ops.set_secret(cluster.uuid, SecretStr(NEW_SECRET))

    grafana.assert_not_called()
    stored = DBController().get_cluster_by_id(cluster.uuid)
    assert stored.secret.get_secret_value() == NEW_SECRET


def test_monitoring_enabled_updates_grafana_user(grafana):
    cluster = _seed_cluster(disable_monitoring=False)

    cluster_ops.set_secret(cluster.uuid, SecretStr(NEW_SECRET))

    grafana.assert_called_once()
    args, kwargs = grafana.call_args
    assert args[0] == cluster.uuid
    assert args[3].get_secret_value() == NEW_SECRET
    assert kwargs == {"update_secret": True}
    stored = DBController().get_cluster_by_id(cluster.uuid)
    assert stored.secret.get_secret_value() == NEW_SECRET
