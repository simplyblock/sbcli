"""Grafana user sync and secret rotation in the monitoring configs.

``cluster update-secret`` used to fail silently against Grafana -- no timeout,
no status check on the user lookup, and a retry loop that returned ``None``
once it gave up -- and never touched the Prometheus config, which kept
scraping with the old secret. These cover the pieces that now carry it.
"""
from unittest.mock import MagicMock, patch

import pytest
import requests
from pydantic import SecretStr

from simplyblock_core import cluster_ops, utils
from simplyblock_core.exceptions import MonitoringConfigError
from simplyblock_core.models.cluster import Cluster

OLD = "oldsupersecret1234567891012"
NEW = "newsupersecret1234567891012"
URL = "http://grafana:3000"


class TestReplaceSecretToken:

    @pytest.mark.parametrize("before, after", [
        (f"password: '{OLD}'", f"password: '{NEW}'"),
        (f'password: "{OLD}"', f'password: "{NEW}"'),
        (f"credentials: {OLD}\n", f"credentials: {NEW}\n"),
        (f'httpHeaderValue1: "Bearer {OLD}"', f'httpHeaderValue1: "Bearer {NEW}"'),
    ])
    def test_replaces_the_token_and_keeps_its_quoting(self, before, after):
        assert utils.replace_secret_token(before, OLD, NEW) == (after, 1)

    def test_a_longer_token_containing_the_secret_is_left_alone(self):
        text = f"password: '{OLD}x'\nusername: 'x{OLD}'"
        assert utils.replace_secret_token(text, OLD, NEW) == (text, 0)

    def test_every_occurrence_is_counted(self):
        text = f"password: '{OLD}'\ncredentials: '{OLD}'"
        rotated, count = utils.replace_secret_token(text, OLD, NEW)
        assert count == 2
        assert OLD not in rotated

    def test_an_empty_old_secret_matches_nothing(self):
        assert utils.replace_secret_token("password: ''", "", NEW) == ("password: ''", 0)


class TestRotatePrometheusConfigmap:

    def _v1(self, prometheus_yml):
        v1 = MagicMock()
        v1.read_namespaced_config_map.return_value = MagicMock(data={"prometheus.yml": prometheus_yml})
        return v1

    def test_patches_the_configmap_carrying_the_old_secret(self):
        v1 = self._v1(f"basic_auth:\n  username: 'c1'\n  password: '{OLD}'\n")
        with patch.object(utils, "load_kube_config_with_fallback"), \
                patch.object(utils.client, "CoreV1Api", return_value=v1):
            assert utils.rotate_prometheus_configmap_secret(OLD, NEW) is True
        body = v1.patch_namespaced_config_map.call_args.kwargs["body"]
        assert body == {"data": {"prometheus.yml": f"basic_auth:\n  username: 'c1'\n  password: '{NEW}'\n"}}

    def test_a_configmap_for_another_cluster_is_not_patched(self):
        v1 = self._v1("basic_auth:\n  username: 'c2'\n  password: 'someothersecret12345678'\n")
        with patch.object(utils, "load_kube_config_with_fallback"), \
                patch.object(utils.client, "CoreV1Api", return_value=v1):
            assert utils.rotate_prometheus_configmap_secret(OLD, NEW) is False
        v1.patch_namespaced_config_map.assert_not_called()


def _response(status_code, body=None):
    response = MagicMock(status_code=status_code, ok=status_code < 400, text="")
    response.json.return_value = body or {}
    return response


@pytest.fixture()
def session():
    session = MagicMock()
    with patch.object(cluster_ops.requests, "Session", return_value=session), \
            patch.object(cluster_ops, "GRAFANA_READY_TIMEOUT_SEC", 0):
        yield session


class TestSyncGrafanaUser:

    def _sync(self):
        cluster_ops._sync_grafana_user("c1", URL, SecretStr("admin-pass"), SecretStr(NEW))

    def test_existing_user_gets_its_password_reset(self, session):
        session.request.side_effect = [_response(200, {"id": 7}), _response(200)]
        self._sync()
        method, url = session.request.call_args.args
        assert (method, url) == ("PUT", f"{URL}/api/admin/users/7/password")
        assert session.request.call_args.kwargs["json"] == {"password": NEW}

    def test_missing_user_is_created(self, session):
        session.request.side_effect = [_response(404), _response(200)]
        self._sync()
        method, url = session.request.call_args.args
        assert (method, url) == ("POST", f"{URL}/api/admin/users")
        assert session.request.call_args.kwargs["json"] == {"name": "c1", "login": "c1", "password": NEW}

    def test_every_request_is_bounded(self, session):
        session.request.side_effect = [_response(200, {"id": 7}), _response(200)]
        self._sync()
        for call in session.request.call_args_list:
            assert call.kwargs["timeout"] == cluster_ops.GRAFANA_REQUEST_TIMEOUT_SEC

    def test_a_refused_lookup_raises_without_retrying(self, session):
        session.request.side_effect = [_response(401)]
        with pytest.raises(MonitoringConfigError, match="refused to look up user c1: HTTP 401"):
            self._sync()
        assert session.request.call_count == 1

    def test_a_refused_password_reset_raises(self, session):
        session.request.side_effect = [_response(200, {"id": 7}), _response(400)]
        with pytest.raises(MonitoringConfigError, match="HTTP 400"):
            self._sync()

    def test_unreachable_grafana_raises(self, session):
        session.request.side_effect = requests.ConnectionError("Name or service not known")
        with pytest.raises(MonitoringConfigError, match="unreachable"):
            self._sync()

    def test_a_transient_failure_is_retried(self, session):
        session.request.side_effect = [_response(502), _response(200, {"id": 7}), _response(200)]
        with patch.object(cluster_ops, "GRAFANA_READY_TIMEOUT_SEC", 60), \
                patch.object(cluster_ops, "wait_fixed", return_value=lambda _: 0):
            self._sync()
        assert session.request.call_count == 3


class TestRotateOnAllManagers:

    def test_only_copies_carrying_the_old_secret_are_rewritten(self):
        nodes = [MagicMock(mgmt_ip="10.0.0.1"), MagicMock(mgmt_ip="10.0.0.2")]
        files = {"10.0.0.1": f"password: '{OLD}'", "10.0.0.2": "password: 'already-rotated-123456'"}
        writes = []

        def run(node, command, directory, image):
            if command[0] == "cat":
                return files[node.mgmt_ip].encode()
            writes.append(node.mgmt_ip)
            return b""

        with patch.object(cluster_ops, "_run_on_mgmt_node", side_effect=run):
            assert cluster_ops._rotate_secret_on_all_managers(nodes, "/x/prometheus.yml", OLD, NEW, "img") is True
        assert writes == ["10.0.0.1"]

    def test_an_unreachable_manager_raises(self):
        nodes = [MagicMock(mgmt_ip="10.0.0.1")]
        with patch.object(cluster_ops, "_run_on_mgmt_node",
                          side_effect=cluster_ops.DockerException("connection refused")):
            with pytest.raises(MonitoringConfigError, match="10.0.0.1"):
                cluster_ops._rotate_secret_on_all_managers(nodes, "/x/prometheus.yml", OLD, NEW, "img")


class TestAddGrafanaUserOrDisableMonitoring:
    """A new cluster is created even when Grafana cannot take its user."""

    def _cluster(self):
        cluster = Cluster()
        cluster.uuid = "c1"
        cluster.grafana_endpoint = URL
        cluster.secret = SecretStr(NEW)
        return cluster

    def test_success_keeps_monitoring_enabled(self):
        cluster = self._cluster()
        with patch.object(cluster_ops, "_sync_grafana_user") as sync:
            cluster_ops._add_grafana_user_or_disable_monitoring(cluster, SecretStr("admin-pass"))
        sync.assert_called_once()
        assert cluster.disable_monitoring is False

    def test_failure_disables_monitoring_instead_of_raising(self):
        cluster = self._cluster()
        with patch.object(cluster_ops, "_sync_grafana_user",
                          side_effect=MonitoringConfigError("Grafana is unreachable")):
            cluster_ops._add_grafana_user_or_disable_monitoring(cluster, SecretStr("admin-pass"))
        assert cluster.disable_monitoring is True
