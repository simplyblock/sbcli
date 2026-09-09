import unittest
from copy import deepcopy
from unittest.mock import MagicMock, mock_open, patch

from simplyblock_core import cluster_ops
from simplyblock_core.models.cluster import Cluster


def _compose(*args):
    return {"services": {"node-exporter": {"command": list(args)}}}


class _Service:
    def __init__(self, args):
        self.id = "svc-1"
        self.name = "monitoring_node-exporter"
        self.attrs = {
            "Version": {"Index": 7},
            "Spec": {
                "Name": self.name,
                "Labels": {"com.docker.stack.namespace": "monitoring"},
                "Mode": {"Global": {}},
                "TaskTemplate": {
                    "ContainerSpec": {
                        "Image": "prom/node-exporter:v1.7.0",
                        "Args": list(args),
                        "Mounts": [{"Source": "/", "Target": "/rootfs"}],
                    },
                    "RestartPolicy": {"Condition": "any"},
                },
                "Networks": [{"Target": "monitoring-net"}],
            },
        }


class TestMonitoringArgsReconcile(unittest.TestCase):

    def _run(self, service, compose):
        docker_client = MagicMock()
        docker_client.services.get.return_value = service
        with patch("builtins.open", mock_open(read_data="compose")), \
                patch.object(cluster_ops.yaml, "safe_load", return_value=compose):
            cluster_ops._reconcile_monitoring_args(docker_client)
        return docker_client

    def test_drift_updates_service_args_from_compose(self):
        service = _Service(["--old"])
        desired = ["--path.rootfs=/rootfs", "--no-collector.ipvs"]

        docker_client = self._run(service, _compose(*desired))

        docker_client.api.update_service.assert_called_once()
        args, kwargs = docker_client.api.update_service.call_args
        self.assertEqual(args, ("svc-1", 7))
        self.assertEqual(kwargs["task_template"]["ContainerSpec"]["Args"], desired)
        self.assertTrue(kwargs["fetch_current_spec"])
        self.assertEqual(
            kwargs["task_template"]["ContainerSpec"]["Mounts"],
            [{"Source": "/", "Target": "/rootfs"}])
        self.assertEqual(
            kwargs["task_template"]["RestartPolicy"], {"Condition": "any"})

    def test_update_uses_copy_of_task_template(self):
        service = _Service(["--old"])
        original = deepcopy(service.attrs["Spec"]["TaskTemplate"])

        self._run(service, _compose("--new"))

        self.assertEqual(service.attrs["Spec"]["TaskTemplate"], original)

    def test_current_args_are_idempotent(self):
        service = _Service(["--path.rootfs=/rootfs"])

        docker_client = self._run(service, _compose("--path.rootfs=/rootfs"))

        docker_client.api.update_service.assert_not_called()

    def test_missing_service_does_not_raise(self):
        docker_client = MagicMock()
        docker_client.services.get.side_effect = cluster_ops.docker.errors.NotFound(
            "missing")

        with patch("builtins.open", mock_open(read_data="compose")), \
                patch.object(cluster_ops.yaml, "safe_load",
                             return_value=_compose("--path.rootfs=/rootfs")):
            cluster_ops._reconcile_monitoring_args(docker_client)

        docker_client.api.update_service.assert_not_called()

    def test_compose_dollar_escape_is_unescaped_before_compare(self):
        service = _Service(['--collector.filesystem.ignored-mount-points=^($|/)'])

        docker_client = self._run(
            service,
            _compose('--collector.filesystem.ignored-mount-points=^($$|/)'))

        docker_client.api.update_service.assert_not_called()

    def test_literal_quotes_survive_round_trip(self):
        service = _Service(["--old"])
        desired = ['--collector.filesystem.ignored-mount-points="^($|/)"']

        docker_client = self._run(
            service,
            _compose('--collector.filesystem.ignored-mount-points="^($$|/)"'))

        task_template = docker_client.api.update_service.call_args.kwargs[
            "task_template"]
        self.assertEqual(task_template["ContainerSpec"]["Args"], desired)


class TestUpdateClusterMonitoringArgsWiring(unittest.TestCase):

    def test_disable_monitoring_skips_args_reconcile(self):
        cluster = Cluster()
        cluster.uuid = "cl-1"
        cluster.mode = "docker"
        cluster.disable_monitoring = True
        docker_client = MagicMock()
        docker_client.services.list.return_value = []

        with patch.object(cluster_ops, "db_controller") as db, \
                patch.object(cluster_ops, "utils") as utils, \
                patch.object(cluster_ops, "release_upgrades") as upgrades, \
                patch.object(cluster_ops, "pull_docker_image_with_retry"), \
                patch.object(cluster_ops, "_reconcile_monitoring_args") as reconcile:
            db.get_cluster_by_id.return_value = cluster
            utils.get_docker_client.return_value = docker_client

            cluster_ops.update_cluster("cl-1", mgmt_only=True)

        upgrades.run_pre_update.assert_called_once_with(cluster)
        reconcile.assert_not_called()


if __name__ == "__main__":
    unittest.main()
