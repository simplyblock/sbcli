# coding=utf-8
"""add_node must not leave an SPDK pod behind when it bails out.

Between ``spdk_process_start`` and the StorageNode record being written there
is no DB row pointing at the pod. It has no owner reference, so Kubernetes
will not reap it, and no node record, so the control plane cannot reconcile
it. Left behind it holds the host's hugepages and every later add-node
attempt on that host stays Pending with "Insufficient hugepages-2Mi", which
stalls the operator's serialised queue and wedges the deployment.

Seen on fresh 12-node deploys on 2026-09-08 and 2026-09-11; both needed a
manual delete of the unowned pod to unblock.
"""
import ast
import functools
import inspect
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from kubernetes.client import ApiException
from tenacity import Retrying

from simplyblock_core import storage_node_ops

# The confirm-gone poll waits 3s between attempts; a test that exercises the
# 'pod never disappears' path must not actually sleep 30s for it.
_NoWaitRetrying = functools.partial(Retrying, sleep=lambda _: None)


class TestAbortStartedSpdk(unittest.TestCase):
    """The cleanup helper itself, on the node-agent path (docker mode)."""

    def test_kills_the_pod(self):
        api = MagicMock()
        api.spdk_process_kill = MagicMock(return_value=(True, ""))
        storage_node_ops._abort_started_spdk(api, 4420, "cluster-1", "boom")
        api.spdk_process_kill.assert_called_once_with(4420, "cluster-1")

    def test_reports_but_does_not_raise_when_the_kill_fails(self):
        api = MagicMock()
        api.spdk_process_kill = MagicMock(return_value=(False, "pod stuck"))
        storage_node_ops._abort_started_spdk(api, 4420, "cluster-1", "boom")
        api.spdk_process_kill.assert_called_once()

    def test_swallows_an_exception_from_the_kill(self):
        """The caller is already on a failure path; cleanup must not mask it
        with a second exception."""
        api = MagicMock()
        api.spdk_process_kill = MagicMock(side_effect=RuntimeError("api down"))
        storage_node_ops._abort_started_spdk(api, 4420, "cluster-1", "boom")
        api.spdk_process_kill.assert_called_once()


class TestAbortStartedSpdkInKubernetes(unittest.TestCase):
    """In kubernetes mode the teardown must not depend on the node agent.

    The agent runs ON the host being abandoned, and the commonest reason
    add_node aborts after starting SPDK is that the host went away -- an
    agent crash, or the one-off CPU-topology reboot. On 2026-09-11 the abort
    handler fired correctly for worker gddnr / rpc_port 4436 and then got
    "Connection refused" from that node's agent, so the kill was never
    delivered. The delete is a plain namespaced pod delete with nothing
    node-local about it, so it belongs on the API server path.
    """

    def setUp(self):
        self.k8s = MagicMock()
        self.k8s.list_namespaced_pod = MagicMock(
            return_value=SimpleNamespace(items=[]))
        self.api = MagicMock()
        self.api.spdk_process_kill = MagicMock(return_value=(True, ""))

    def _run(self, cluster_id="cluster-1abcdef", rpc_port=4436):
        with patch.object(storage_node_ops.utils, "get_k8s_core_client",
                          return_value=self.k8s):
            storage_node_ops._abort_started_spdk(
                self.api, rpc_port, cluster_id, "boom", cluster_mode="kubernetes")

    def test_deletes_the_pod_through_the_api_server_not_the_agent(self):
        self._run()
        deleted = [c.args[0] for c in self.k8s.delete_namespaced_pod.call_args_list]
        self.assertIn("snode-spdk-pod-4436-cluste", deleted)
        self.api.spdk_process_kill.assert_not_called()

    def test_also_removes_the_fluentd_companion(self):
        self._run()
        deleted = [c.args[0] for c in self.k8s.delete_namespaced_pod.call_args_list]
        self.assertIn("simplyblock-fluentd-4436-cluste", deleted)

    def test_a_pod_that_is_already_gone_counts_as_cleaned_up(self):
        self.k8s.delete_namespaced_pod = MagicMock(
            side_effect=ApiException(status=404))
        self._run()
        self.api.spdk_process_kill.assert_not_called()

    def test_falls_back_to_the_agent_when_the_api_delete_fails(self):
        self.k8s.delete_namespaced_pod = MagicMock(
            side_effect=ApiException(status=500))
        self._run()
        self.api.spdk_process_kill.assert_called_once_with(4436, "cluster-1abcdef")

    def test_falls_back_to_the_agent_when_there_is_no_api_client(self):
        with patch.object(storage_node_ops.utils, "get_k8s_core_client",
                          side_effect=RuntimeError("not in cluster")):
            storage_node_ops._abort_started_spdk(
                self.api, 4436, "cluster-1abcdef", "boom", cluster_mode="kubernetes")
        self.api.spdk_process_kill.assert_called_once()

    def test_a_pod_that_never_disappears_is_not_reported_as_cleaned_up(self):
        """A pod still Terminating still holds the host's hugepages, so the
        API delete has not achieved anything yet -- try the agent too."""
        still_there = SimpleNamespace(
            items=[SimpleNamespace(
                metadata=SimpleNamespace(name="snode-spdk-pod-4436-cluste"))])
        self.k8s.list_namespaced_pod = MagicMock(return_value=still_there)
        with patch.object(storage_node_ops, "Retrying", _NoWaitRetrying):
            self._run()
        self.api.spdk_process_kill.assert_called_once()

    def test_docker_mode_never_touches_the_api_server(self):
        with patch.object(storage_node_ops.utils, "get_k8s_core_client",
                          return_value=self.k8s):
            storage_node_ops._abort_started_spdk(
                self.api, 4436, "cluster-1abcdef", "boom", cluster_mode="docker")
        self.k8s.delete_namespaced_pod.assert_not_called()
        self.api.spdk_process_kill.assert_called_once()


class TestNoLeakingExitInAddNode(unittest.TestCase):
    """Structural guard: once the pod exists, every bail-out must clean up.

    Enforced on the source rather than by driving add_node, which needs far
    too much of the world mocked to be a useful regression test. This catches
    the case the manual fix cannot: someone adding a FOURTH early return to
    that window later on.
    """

    def _add_node_body(self):
        src = inspect.getsource(storage_node_ops.add_node)
        # normalise the leading indentation so ast can parse the function alone
        return ast.parse(inspect.cleandoc("\n".join(src.splitlines())))

    def test_every_return_false_after_pod_start_cleans_up(self):
        src_lines = inspect.getsource(storage_node_ops.add_node).splitlines()

        start = next((i for i, l in enumerate(src_lines)
                      if "spdk_process_start(" in l), None)
        self.assertIsNotNone(start, "spdk_process_start call not found in add_node")

        persisted = next((i for i, l in enumerate(src_lines)
                          if "snode.rpc_port = rpc_port" in l), None)
        self.assertIsNotNone(persisted, "StorageNode persistence point not found")
        self.assertGreater(persisted, start)

        offenders = []
        for i in range(start, persisted):
            line = src_lines[i].strip()
            if line in ("return False", "return None"):
                # the preceding few lines must hand off to the cleanup helper
                window = " ".join(x.strip() for x in src_lines[max(start, i - 4):i])
                if "_abort_started_spdk" not in window:
                    offenders.append((i - start, src_lines[i].strip()))

        self.assertEqual(
            offenders, [],
            "add_node bails out after the SPDK pod exists without calling "
            "_abort_started_spdk, which leaks an unowned pod holding the "
            f"host's hugepages. Offending exits (offset from pod start): {offenders}")


if __name__ == "__main__":
    unittest.main()
