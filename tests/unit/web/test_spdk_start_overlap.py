"""Two requests to start one SPDK pod do not both create it.

A start request prepares the host before it creates the pod, and on OpenShift
that preparation waits for a reboot. Its caller gives up before that and asks
again, so two requests for one pod wait side by side and both arrive at the
create (OpenShift, 2026-11-25: pods created three seconds apart for one node).
The later one is the caller's; the earlier one has nobody left to answer.
"""

from types import SimpleNamespace
from unittest import mock

from kubernetes.client import ApiException

from simplyblock_core import constants, storage_node_ops
from simplyblock_web.api.internal.storage_node import kubernetes as agent


def _pods(*names):
    return SimpleNamespace(items=[SimpleNamespace(metadata=SimpleNamespace(name=n)) for n in names])


class TestTheNewestStartRequestWins:

    def test_a_request_alone_is_not_superseded(self):
        ticket = agent._claim_spdk_start("cluster-a", 4420)
        assert not agent._spdk_start_superseded("cluster-a", 4420, ticket)

    def test_a_later_request_for_the_same_pod_supersedes_the_earlier_one(self):
        first = agent._claim_spdk_start("cluster-b", 4420)
        second = agent._claim_spdk_start("cluster-b", 4420)
        assert agent._spdk_start_superseded("cluster-b", 4420, first)
        assert not agent._spdk_start_superseded("cluster-b", 4420, second)

    def test_requests_for_other_pods_do_not_supersede_each_other(self):
        """Another port is a sibling node on the host; another cluster is
        another deployment."""
        mine = agent._claim_spdk_start("cluster-c", 4420)
        agent._claim_spdk_start("cluster-c", 4421)
        agent._claim_spdk_start("cluster-d", 4420)
        assert not agent._spdk_start_superseded("cluster-c", 4420, mine)

    def test_the_port_is_compared_as_a_number(self):
        """The request body and the query string do not agree on its type."""
        ticket = agent._claim_spdk_start("cluster-e", "4420")
        assert not agent._spdk_start_superseded("cluster-e", 4420, ticket)


class TestRemovingAnSpdkPodReportsWhetherItIsGone:

    def _remove(self, core):
        with mock.patch.object(agent.core_utils, "get_k8s_core_client", return_value=core), \
                mock.patch.object(agent.node_utils_k8s, "get_namespace", return_value="simplyblock"), \
                mock.patch.object(agent.time, "sleep"):
            return agent._remove_spdk_pod(4420, "338f1821-85e2-4fef-89c0-c95432b62371")

    def test_a_pod_that_goes_away_is_reported_gone(self):
        core = mock.Mock()
        core.read_namespaced_pod.side_effect = ApiException(status=404)
        core.list_namespaced_pod.return_value = _pods("snode-spdk-pod-4421-338f18")
        assert self._remove(core) == (True, "")
        core.delete_namespaced_pod.assert_called_once_with("snode-spdk-pod-4420-338f18", "simplyblock")

    def test_a_pod_already_gone_is_reported_gone(self):
        core = mock.Mock()
        core.delete_namespaced_pod.side_effect = ApiException(status=404)
        core.read_namespaced_pod.side_effect = ApiException(status=404)
        core.list_namespaced_pod.return_value = _pods()
        assert self._remove(core) == (True, "")

    def test_a_delete_that_fails_is_reported(self):
        core = mock.Mock()
        core.delete_namespaced_pod.side_effect = ApiException(status=500, reason="boom")
        removed, reason = self._remove(core)
        assert removed is False
        assert "snode-spdk-pod-4420-338f18" in reason

    def test_a_pod_still_terminating_is_not_reported_gone(self):
        """It still holds its name, so a create would conflict, and the host's
        huge pages, so a pod under another name would not schedule."""
        core = mock.Mock()
        core.read_namespaced_pod.side_effect = ApiException(status=404)
        core.list_namespaced_pod.return_value = _pods("snode-spdk-pod-4420-338f18")
        removed, reason = self._remove(core)
        assert removed is False
        assert "did not terminate" in reason


def test_the_slot_name_treats_no_mask_and_an_empty_mask_alike():
    """The node record stores an absent mask as "", and the slot has to name
    the same node the record does."""
    assert (storage_node_ops._rpc_port_slot("10.0.0.12:5000", 0, None)
            == storage_node_ops._rpc_port_slot("10.0.0.12:5000", 0, ""))


def test_the_superseded_answer_is_one_the_caller_can_recognise():
    assert constants.SPDK_START_SUPERSEDED
    assert constants.SPDK_START_SUPERSEDED in f"{constants.SPDK_START_SUPERSEDED}: a later request arrived"
