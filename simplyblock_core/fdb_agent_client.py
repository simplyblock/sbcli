# coding=utf-8
"""Command execution inside the FoundationDB backup agent.

``fdbbackup``, ``fdbrestore`` and ``fdbcli`` only exist inside the FDB backup
agent image, so every backup operation has to be executed there. How that
agent is reached depends on the deployment mode of the management node:

* ``docker`` — a container on the mgmt node's docker daemon, reached over the
  docker remote API.
* ``kubernetes`` — a pod in the cluster namespace, reached through the
  kubernetes API (``kubectl`` is not available in the control plane image).

:class:`FdbAgent` hides that difference behind a single ``exec()`` that takes a
command line and returns its combined stdout/stderr as a utf-8 string, or
raises :class:`FdbCommandError` when the command exits non-zero.
"""

import abc
import re
import shlex
from typing import List, Sequence, Tuple, Union

import docker
from kubernetes import client as k8s_client
from kubernetes.stream import stream as k8s_stream
from requests.exceptions import ReadTimeout

from simplyblock_core import constants, utils
from simplyblock_core.models.mgmt_node import MgmtNode

logger = utils.get_logger()


MODE_KUBERNETES = "kubernetes"

# Docker (swarm) deployments: the swarm task container of the
# ``fdb-backup-agent`` service, see scripts/docker-compose-swarm.yml.
DOCKER_CONTAINER_NAME_PREFIX = "app_fdb-backup-agent"

# Kubernetes deployments: label selectors tried in order. The first selector
# matching a running pod wins, so a chart that ships a dedicated backup agent
# takes precedence over the FDB cluster pods (which carry the same binaries).
K8S_POD_SELECTORS: Tuple[str, ...] = (
    "app=fdb-backup-agent",
    f"foundationdb.org/fdb-cluster-name={constants.FDB_SERVICE_NAME}",
)

Command = Union[str, Sequence[str]]

# Backup paths are blobstore URLs carrying the S3 credentials inline
# (blobstore://<key>:<secret>@s3...), so a command line must never be logged
# or put into an error message verbatim.
_BLOBSTORE_CRED_RE = re.compile(r"(blobstore://)[^@\s]+@", re.IGNORECASE)


def redact(command: Sequence[str]) -> str:
    """Render ``command`` as a single line with any inline credentials masked."""
    return _BLOBSTORE_CRED_RE.sub(r"\1**********@", " ".join(command))


class FdbAgentError(Exception):
    """Base error for FDB backup agent command execution."""


class FdbAgentNotFoundError(FdbAgentError):
    """No FDB backup agent container/pod could be located."""


class FdbCommandError(FdbAgentError):
    """A command executed inside the FDB backup agent exited non-zero."""

    def __init__(self, command: Sequence[str], exit_code: int, output: str):
        super().__init__(
            f"Command '{redact(command)}' failed with exit code {exit_code}: {output.strip()}")
        self.command = list(command)
        self.exit_code = exit_code
        self.output = output


class FdbCommandTimeoutError(FdbAgentError):
    """A command executed inside the FDB backup agent did not finish in time."""

    def __init__(self, command: Sequence[str], timeout: int):
        super().__init__(f"Command '{redact(command)}' timed out after {timeout}s")
        self.command = list(command)
        self.timeout = timeout


class FdbAgent(abc.ABC):
    """Runs commands inside the cluster's FDB backup agent."""

    def __init__(self, timeout: int = constants.FDB_AGENT_EXEC_TIMEOUT_SEC):
        self._timeout = timeout

    @abc.abstractmethod
    def exec(self, command: Command) -> str:
        """Run ``command`` inside the agent and return its combined
        stdout/stderr, decoded as utf-8.

        ``command`` is either an argument list or a shell-style string, which
        is split with :func:`shlex.split` (the same splitting the docker API
        applies to string commands).

        :raises FdbAgentNotFoundError: the agent container/pod does not exist
        :raises FdbCommandError: the command exited non-zero
        :raises FdbCommandTimeoutError: the command exceeded the exec timeout
        """

    @staticmethod
    def _argv(command: Command) -> List[str]:
        return shlex.split(command) if isinstance(command, str) else list(command)


class DockerFdbAgent(FdbAgent):
    """Executes in the ``fdb-backup-agent`` container of a docker mgmt node."""

    def __init__(self, docker_ip_port: str, timeout: int = constants.FDB_AGENT_EXEC_TIMEOUT_SEC):
        super().__init__(timeout)
        self._docker_ip_port = docker_ip_port

    def _get_container(self):
        node_docker = docker.DockerClient(
            base_url=f"tcp://{self._docker_ip_port}", version="auto", timeout=self._timeout)
        for container in node_docker.containers.list():
            if (container.name or "").startswith(DOCKER_CONTAINER_NAME_PREFIX):
                return container
        raise FdbAgentNotFoundError(
            f"No '{DOCKER_CONTAINER_NAME_PREFIX}*' container on docker host {self._docker_ip_port}")

    def exec(self, command: Command) -> str:
        argv = self._argv(command)
        container = self._get_container()
        logger.debug(f"Running '{redact(argv)}' in container {container.name}")
        try:
            result = container.exec_run(cmd=argv)
        except ReadTimeout as e:
            raise FdbCommandTimeoutError(argv, self._timeout) from e

        output = (result.output or b"").decode("utf-8", errors="replace")
        if result.exit_code != 0:
            raise FdbCommandError(argv, result.exit_code, output)
        return output


class KubernetesFdbAgent(FdbAgent):
    """Executes in the FDB backup agent pod through the kubernetes API."""

    def __init__(self, namespace: str = constants.K8S_NAMESPACE,
                 timeout: int = constants.FDB_AGENT_EXEC_TIMEOUT_SEC):
        super().__init__(timeout)
        self._namespace = namespace

    def _get_core_api(self) -> k8s_client.CoreV1Api:
        utils.load_kube_config_with_fallback()
        return k8s_client.CoreV1Api()

    def _get_pod_name(self, core_api: k8s_client.CoreV1Api) -> str:
        for selector in K8S_POD_SELECTORS:
            for pod in core_api.list_namespaced_pod(
                    namespace=self._namespace, label_selector=selector).items:
                if pod.status.phase == "Running":
                    return pod.metadata.name
        raise FdbAgentNotFoundError(
            f"No running FDB backup agent pod in namespace {self._namespace} "
            f"matching any of {list(K8S_POD_SELECTORS)}")

    @staticmethod
    def _get_exit_code(response) -> int:
        """Exit code of a finished exec stream.

        Must be read *before* ``read_all()``, which drops the channel buffers
        the return code is parsed from. A stream that carries no error channel
        payload is treated as success, which is what the API server sends for
        a command that exited zero.
        """
        try:
            return response.returncode or 0
        except (TypeError, KeyError, ValueError, IndexError) as e:
            logger.debug(f"Could not read exit code from exec stream, assuming success: {e}")
            return 0

    def exec(self, command: Command) -> str:
        argv = self._argv(command)
        core_api = self._get_core_api()
        pod_name = self._get_pod_name(core_api)
        logger.debug(f"Running '{redact(argv)}' in pod {self._namespace}/{pod_name}")

        response = k8s_stream(
            core_api.connect_get_namespaced_pod_exec,
            name=pod_name, namespace=self._namespace, command=argv,
            stderr=True, stdin=False, stdout=True, tty=False,
            _preload_content=False)
        try:
            response.run_forever(timeout=self._timeout)
            if response.is_open():
                raise FdbCommandTimeoutError(argv, self._timeout)
            exit_code = self._get_exit_code(response)
            output = response.read_all()
        finally:
            response.close()

        if exit_code != 0:
            raise FdbCommandError(argv, exit_code, output)
        return output


def get_fdb_agent(mgmt_node: MgmtNode) -> FdbAgent:
    """Return the :class:`FdbAgent` matching ``mgmt_node``'s deployment mode."""
    if mgmt_node.mode == MODE_KUBERNETES:
        return KubernetesFdbAgent()
    return DockerFdbAgent(mgmt_node.docker_ip_port)
