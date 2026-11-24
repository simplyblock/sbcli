"""The Kubernetes node agent's answer to "does this node's SPDK run?".

Regression: 2026-11-16, K3s with three servers, storage cluster active. One
server's ``k3s`` unit was restarted, which takes its API server down for about
ten seconds. The agents' pod lookup failed with ``API error: (503)``, and the
agent answered that as "SPDK container is not running". The monitor reads that
answer as a fact: one second later it set a storage node offline, two seconds
after that the cluster was suspended, and the suspend recovery restarted all
three storage nodes. No SPDK pod had stopped. On a single-server cluster the
same restart took all three nodes offline at once.

"The Kubernetes API did not answer" is not "the pod does not run". The agent
now fails the request instead, which every caller already reads as unknown.
"""
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
import urllib3
from flask import Flask
from kubernetes.client import ApiException

from simplyblock_web.api.internal.storage_node import kubernetes as agent

PORT = 4420
CLUSTER = "5f50e0"
POD = f"snode-spdk-pod-{PORT}-{CLUSTER}"


def _pod(name=POD, phase="Running", spdk_running=True, statuses=True):
    container = SimpleNamespace(name="spdk-container",
                                state=SimpleNamespace(running=object() if spdk_running else None))
    proxy = SimpleNamespace(name="spdk-proxy-container", state=SimpleNamespace(running=object()))
    return SimpleNamespace(
        metadata=SimpleNamespace(name=name),
        status=SimpleNamespace(phase=phase, container_statuses=[proxy, container] if statuses else None),
    )


@pytest.fixture
def api():
    """The Kubernetes client the agent lists pods with. Retries do not wait."""
    client = MagicMock()
    client.list_namespaced_pod.return_value = SimpleNamespace(items=[])
    with patch.object(agent.core_utils, "get_k8s_core_client", return_value=client), \
            patch.object(agent.node_utils_k8s, "get_namespace", return_value="simplyblock"), \
            patch.object(agent, "_POD_LOOKUP_WAIT_SEC", 0):
        yield client


def _answers(api, *pods):
    api.list_namespaced_pod.return_value = SimpleNamespace(items=list(pods))


class TestWhatTheApiSays:

    def test_a_running_spdk_container_is_up(self, api):
        _answers(api, _pod("some-other-pod"), _pod())
        assert agent._is_pod_up(PORT, CLUSTER) is True

    def test_no_pod_is_not_up(self, api):
        _answers(api, _pod("snode-spdk-pod-4422-5f50e0"))
        assert agent._is_pod_up(PORT, CLUSTER) is False
        assert agent._is_pod_present(PORT, CLUSTER) is False

    @pytest.mark.parametrize("pod", [
        _pod(phase="Pending"),
        _pod(spdk_running=False),
        _pod(statuses=False),      # a pod the kubelet has not reported on yet
    ])
    def test_a_pod_whose_spdk_does_not_run_is_present_and_not_up(self, api, pod):
        _answers(api, pod)
        assert agent._is_pod_up(PORT, CLUSTER) is False
        assert agent._is_pod_present(PORT, CLUSTER) is True


#: What the lookup failed with while the K3s server restarted, and what a
#: stopped API server looks like from a pod.
API_DOWN = [
    ApiException(status=503, reason="Service Unavailable"),
    urllib3.exceptions.MaxRetryError(None, "/api/v1/namespaces/simplyblock/pods", "connection refused"),
    ConnectionResetError("connection reset by peer"),
    TimeoutError("timed out"),
]


class TestWhenTheApiCannotBeAsked:

    @pytest.mark.parametrize("error", API_DOWN, ids=lambda e: type(e).__name__)
    def test_it_is_not_reported_as_a_pod_that_does_not_run(self, api, error):
        api.list_namespaced_pod.side_effect = error

        with pytest.raises(agent.SPDKPodLookupError):
            agent._is_pod_up(PORT, CLUSTER)
        with pytest.raises(agent.SPDKPodLookupError):
            agent._is_pod_present(PORT, CLUSTER)

    def test_the_lookup_is_tried_again_before_it_is_given_up(self, api):
        api.list_namespaced_pod.side_effect = ApiException(status=503)

        with pytest.raises(agent.SPDKPodLookupError):
            agent._is_pod_up(PORT, CLUSTER)

        assert api.list_namespaced_pod.call_count == agent._POD_LOOKUP_ATTEMPTS

    def test_an_api_that_answers_on_the_second_try_is_believed(self, api):
        api.list_namespaced_pod.side_effect = [
            ApiException(status=503), SimpleNamespace(items=[_pod()])]

        assert agent._is_pod_up(PORT, CLUSTER) is True

    def test_every_call_is_bounded_so_the_lookup_ends_before_the_monitor_stops_waiting(self, api):
        """The monitor gives the agent ten seconds. A lookup that outlasts it
        is a timeout to the monitor whatever the agent would have said."""
        _answers(api, _pod())
        agent._is_pod_up(PORT, CLUSTER)

        assert api.list_namespaced_pod.call_args.kwargs["_request_timeout"] == agent._POD_LOOKUP_TIMEOUT_SEC
        worst = (agent._POD_LOOKUP_ATTEMPTS * agent._POD_LOOKUP_TIMEOUT_SEC
                 + (agent._POD_LOOKUP_ATTEMPTS - 1) * 0.5)
        assert worst < 10


def _ask(api_client):
    """Call the endpoint as the control plane does and return (status code, body)."""
    query = SimpleNamespace(rpc_port=PORT, cluster_id="5f50e00d-715c-4a1b-9d36-000000000000")
    with Flask(__name__).app_context():
        answer = agent.spdk_process_is_up(query)
    if isinstance(answer, tuple):
        response, code = answer
        return code, response.get_json()
    return answer.status_code, answer.get_json()


class TestTheEndpoint:

    def test_a_running_spdk_is_answered_true(self, api):
        _answers(api, _pod())
        code, body = _ask(api)
        assert code == 200 and body["status"] is True and body["results"] is True

    def test_a_pod_that_is_gone_is_answered_not_running(self, api):
        """This is the answer the monitor sets a node offline on, and it stays
        an answer: HTTP 200 with the reason."""
        code, body = _ask(api)
        assert code == 200
        assert body["status"] is False
        assert body["error"] == "SPDK container is not running"

    def test_an_api_that_cannot_be_asked_fails_the_request(self, api):
        """A failed request is what the control plane's client raises on, and
        every caller reads that as unknown, as it reads an unreachable agent."""
        api.list_namespaced_pod.side_effect = ApiException(status=503)

        code, body = _ask(api)

        assert code == 503
        assert body["status"] is False
        assert "unknown" in body["error"]
        assert "not running" not in body["error"]


def test_the_control_planes_client_raises_on_that_answer():
    """The other half of the contract: the client turns the agent's 503 into an
    exception rather than into `(False, ...)`."""
    from simplyblock_core.snode_client import SNodeClient, SNodeClientException

    client = SNodeClient.__new__(SNodeClient)
    client.url = "https://10.0.0.1:5000/snode/"
    client.timeout = 5
    client.session = MagicMock()
    client.session.request.return_value = SimpleNamespace(
        status_code=503, content=b'{"status": false, "error": "SPDK liveness is unknown"}',
        json=lambda: {"status": False, "error": "SPDK liveness is unknown"})

    with pytest.raises(SNodeClientException):
        client.spdk_process_is_up(PORT, "5f50e00d")
