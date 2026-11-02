"""utils.get_docker_client for a cluster attached with `cluster add`.

Only the first cluster's bootstrap records a management node, so a second
cluster on the same control plane has none of its own; adding a storage node
to it must still reach the swarm through the deployment's management node.
"""
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from simplyblock_core import utils
from simplyblock_core.models.mgmt_node import MgmtNode

MGMT = SimpleNamespace(status=MgmtNode.STATUS_ONLINE, docker_ip_port="10.0.0.1:2375",
                       cluster_id="first")


def _db(nodes_by_cluster):
    db = MagicMock()
    db.get_mgmt_nodes.side_effect = lambda cluster_id=None: (
        nodes_by_cluster.get(cluster_id, []) if cluster_id else
        [n for ns in nodes_by_cluster.values() for n in ns])
    return db


def test_a_cluster_without_its_own_mgmt_node_uses_the_deployments():
    with patch("simplyblock_core.db_controller.DBController", return_value=_db({"first": [MGMT]})), \
            patch.object(utils.docker, "DockerClient") as client:
        utils.get_docker_client("second")
    client.assert_called_once_with(base_url="tcp://10.0.0.1:2375", version="auto")


def test_no_mgmt_node_at_all_is_still_an_error():
    with patch("simplyblock_core.db_controller.DBController", return_value=_db({})):
        with pytest.raises(RuntimeError, match="No mgmt nodes"):
            utils.get_docker_client("second")
