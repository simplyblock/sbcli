"""Tests for the FDB backup agent execution backends.

``fdbbackup``/``fdbrestore``/``fdbcli`` run inside the FDB backup agent, which
is a docker container on swarm deployments and a pod on kubernetes ones. Both
backends must behave identically from the caller's point of view:

  * a command line (string or argv) goes in, its combined stdout/stderr comes
    back as a utf-8 ``str``,
  * a non-zero exit raises ``FdbCommandError`` instead of being swallowed,
  * a missing agent raises ``FdbAgentNotFoundError``,
  * nothing that is logged or put into an exception message carries the S3
    credentials embedded in a blobstore backup path.
"""

from unittest.mock import MagicMock, patch

import pytest

from simplyblock_core import fdb_agent_client
from simplyblock_core.fdb_agent_client import (
    DockerFdbAgent,
    FdbAgentNotFoundError,
    FdbCommandError,
    FdbCommandTimeoutError,
    KubernetesFdbAgent,
    get_fdb_agent,
    redact,
)
from simplyblock_core.models.mgmt_node import MgmtNode

BLOBSTORE_PATH = "blobstore://AKIAKEY:s3cr3t@s3.eu-west-1.amazonaws.com/backup-1?bucket=b&region=eu-west-1"


def _docker_client(container):
    client = MagicMock()
    client.containers.list.return_value = [container] if container else []
    return client


def _container(name="app_fdb-backup-agent.1.abc", exit_code=0, output=b"ok"):
    container = MagicMock()
    container.name = name
    container.exec_run.return_value = MagicMock(exit_code=exit_code, output=output)
    return container


def _pod(name="fdb-backup-agent-0", phase="Running"):
    pod = MagicMock()
    pod.metadata.name = name
    pod.status.phase = phase
    return pod


def _core_api(pods_by_selector):
    api = MagicMock()

    def list_namespaced_pod(namespace, label_selector):
        return MagicMock(items=pods_by_selector.get(label_selector, []))

    api.list_namespaced_pod.side_effect = list_namespaced_pod
    return api


def _ws_response(output="ok", returncode=0, still_open=False):
    response = MagicMock()
    response.is_open.return_value = still_open
    response.returncode = returncode
    response.read_all.return_value = output
    return response


# --- docker backend --------------------------------------------------------

def test_docker_exec_returns_decoded_output():
    container = _container(output="héllo".encode("utf-8"))
    with patch.object(fdb_agent_client.docker, "DockerClient",
                      return_value=_docker_client(container)):
        assert DockerFdbAgent("1.2.3.4:2375").exec("fdbbackup status") == "héllo"

    container.exec_run.assert_called_once_with(cmd=["fdbbackup", "status"])


def test_docker_exec_raises_on_non_zero_exit():
    container = _container(exit_code=1, output=b"backup not found")
    with patch.object(fdb_agent_client.docker, "DockerClient",
                      return_value=_docker_client(container)):
        with pytest.raises(FdbCommandError) as exc_info:
            DockerFdbAgent("1.2.3.4:2375").exec("fdbbackup list -b file:///backup")

    assert exc_info.value.exit_code == 1
    assert exc_info.value.output == "backup not found"


def test_docker_missing_container_raises():
    with patch.object(fdb_agent_client.docker, "DockerClient",
                      return_value=_docker_client(_container(name="app_WebAppAPI.1.xyz"))):
        with pytest.raises(FdbAgentNotFoundError):
            DockerFdbAgent("1.2.3.4:2375").exec("fdbbackup status")


def test_docker_read_timeout_becomes_command_timeout():
    from requests.exceptions import ReadTimeout

    container = _container()
    container.exec_run.side_effect = ReadTimeout("read timed out")
    with patch.object(fdb_agent_client.docker, "DockerClient",
                      return_value=_docker_client(container)):
        with pytest.raises(FdbCommandTimeoutError):
            DockerFdbAgent("1.2.3.4:2375", timeout=5).exec("fdbbackup start -d file:///b -w")


def test_quoted_command_is_split_like_the_docker_api_did():
    """The fdbcli restore command relies on shell-style splitting.

    It used to be handed to ``exec_run`` as a string, which docker split with
    ``shlex.split``. Pre-splitting must produce the exact same argv.
    """
    from docker.utils import split_command

    command = "fdbcli --exec \"writemode on; clearrange \\\"\\\" \\xff\""
    assert DockerFdbAgent._argv(command) == split_command(command)


# --- kubernetes backend ----------------------------------------------------

def test_k8s_exec_returns_output_and_uses_argv():
    api = _core_api({"app=fdb-backup-agent": [_pod()]})
    response = _ws_response(output="backup started")
    with patch.object(fdb_agent_client.utils, "load_kube_config_with_fallback"), \
            patch.object(fdb_agent_client.k8s_client, "CoreV1Api", return_value=api), \
            patch.object(fdb_agent_client, "k8s_stream", return_value=response) as stream:
        assert KubernetesFdbAgent(namespace="simplyblock").exec("fdbbackup status") == "backup started"

    assert stream.call_args.kwargs["command"] == ["fdbbackup", "status"]
    assert stream.call_args.kwargs["name"] == "fdb-backup-agent-0"
    assert stream.call_args.kwargs["namespace"] == "simplyblock"
    assert stream.call_args.kwargs["_preload_content"] is False
    response.close.assert_called_once()


def test_k8s_exec_reads_exit_code_before_draining_output():
    """``read_all()`` drops the channel buffers the exit code is parsed from."""
    api = _core_api({"app=fdb-backup-agent": [_pod()]})
    response = MagicMock()
    response.is_open.return_value = False
    order = []
    type(response).returncode = property(lambda self: order.append("returncode") or 0)
    response.read_all.side_effect = lambda: order.append("read_all") or "out"

    with patch.object(fdb_agent_client.utils, "load_kube_config_with_fallback"), \
            patch.object(fdb_agent_client.k8s_client, "CoreV1Api", return_value=api), \
            patch.object(fdb_agent_client, "k8s_stream", return_value=response):
        KubernetesFdbAgent().exec("fdbbackup status")

    assert order == ["returncode", "read_all"]


def test_k8s_exec_raises_on_non_zero_exit():
    api = _core_api({"app=fdb-backup-agent": [_pod()]})
    response = _ws_response(output="ERROR: no backup", returncode=3)
    with patch.object(fdb_agent_client.utils, "load_kube_config_with_fallback"), \
            patch.object(fdb_agent_client.k8s_client, "CoreV1Api", return_value=api), \
            patch.object(fdb_agent_client, "k8s_stream", return_value=response):
        with pytest.raises(FdbCommandError) as exc_info:
            KubernetesFdbAgent().exec("fdbbackup list -b file:///backup")

    assert exc_info.value.exit_code == 3
    assert exc_info.value.output == "ERROR: no backup"


def test_k8s_exec_raises_when_stream_does_not_finish():
    api = _core_api({"app=fdb-backup-agent": [_pod()]})
    response = _ws_response(still_open=True)
    with patch.object(fdb_agent_client.utils, "load_kube_config_with_fallback"), \
            patch.object(fdb_agent_client.k8s_client, "CoreV1Api", return_value=api), \
            patch.object(fdb_agent_client, "k8s_stream", return_value=response):
        with pytest.raises(FdbCommandTimeoutError):
            KubernetesFdbAgent(timeout=1).exec("fdbbackup status")

    response.close.assert_called_once()


def test_k8s_falls_back_to_the_fdb_cluster_pods():
    fallback_selector = fdb_agent_client.K8S_POD_SELECTORS[1]
    api = _core_api({fallback_selector: [_pod(name="simplyblock-fdb-cluster-storage-1")]})
    with patch.object(fdb_agent_client.utils, "load_kube_config_with_fallback"), \
            patch.object(fdb_agent_client.k8s_client, "CoreV1Api", return_value=api), \
            patch.object(fdb_agent_client, "k8s_stream", return_value=_ws_response()) as stream:
        KubernetesFdbAgent().exec("fdbbackup status")

    assert stream.call_args.kwargs["name"] == "simplyblock-fdb-cluster-storage-1"


def test_k8s_ignores_pods_that_are_not_running():
    api = _core_api({"app=fdb-backup-agent": [_pod(phase="Pending")]})
    with patch.object(fdb_agent_client.utils, "load_kube_config_with_fallback"), \
            patch.object(fdb_agent_client.k8s_client, "CoreV1Api", return_value=api):
        with pytest.raises(FdbAgentNotFoundError):
            KubernetesFdbAgent().exec("fdbbackup status")


# --- backend selection & redaction ----------------------------------------

def test_kubernetes_mgmt_node_gets_the_kubernetes_backend():
    node = MgmtNode()
    node.mode = "kubernetes"
    assert isinstance(get_fdb_agent(node), KubernetesFdbAgent)


@pytest.mark.parametrize("mode", ["docker", ""])
def test_non_kubernetes_mgmt_node_gets_the_docker_backend(mode):
    node = MgmtNode()
    node.mode = mode
    node.docker_ip_port = "10.0.0.1:2375"
    agent = get_fdb_agent(node)
    assert isinstance(agent, DockerFdbAgent)
    assert agent._docker_ip_port == "10.0.0.1:2375"


def test_fdb_backup_mode_env_selects_the_kubernetes_backend():
    """``FDB_BACKUP_MODE=kubernetes`` wins without consulting the mgmt node.

    The docker backend needs a mgmt node for its daemon address; the
    kubernetes one does not, so the lookup must not even happen.
    """
    from simplyblock_core.controllers import fdb_backup_controller

    with patch.object(fdb_backup_controller.constants, "FDB_BACKUP_MODE", "kubernetes"), \
            patch.object(fdb_backup_controller.db_controller, "get_mgmt_nodes") as get_mgmt_nodes:
        agent = fdb_backup_controller.__get_fdb_agent()

    assert isinstance(agent, KubernetesFdbAgent)
    get_mgmt_nodes.assert_not_called()


def test_without_fdb_backup_mode_the_mgmt_node_decides():
    from simplyblock_core.controllers import fdb_backup_controller

    node = MgmtNode()
    node.mode = "docker"
    node.docker_ip_port = "10.0.0.1:2375"
    with patch.object(fdb_backup_controller.constants, "FDB_BACKUP_MODE", ""), \
            patch.object(fdb_backup_controller.db_controller, "get_mgmt_nodes", return_value=[node]):
        assert isinstance(fdb_backup_controller.__get_fdb_agent(), DockerFdbAgent)


def test_no_mgmt_node_raises_agent_not_found():
    from simplyblock_core.controllers import fdb_backup_controller

    with patch.object(fdb_backup_controller.constants, "FDB_BACKUP_MODE", ""), \
            patch.object(fdb_backup_controller.db_controller, "get_mgmt_nodes", return_value=[]):
        with pytest.raises(FdbAgentNotFoundError):
            fdb_backup_controller.__get_fdb_agent()


def test_blobstore_credentials_are_masked_in_command_rendering():
    masked = redact(["fdbbackup", "list", "-b", BLOBSTORE_PATH])
    assert "s3cr3t" not in masked
    assert "AKIAKEY" not in masked
    assert "blobstore://**********@s3.eu-west-1.amazonaws.com" in masked


def test_command_error_message_does_not_leak_credentials():
    error = FdbCommandError(["fdbbackup", "list", "-b", BLOBSTORE_PATH], 1, "denied")
    assert "s3cr3t" not in str(error)
    assert "denied" in str(error)
