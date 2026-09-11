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
import inspect
import unittest
from unittest.mock import MagicMock

from simplyblock_core import storage_node_ops


class TestAbortStartedSpdk(unittest.TestCase):
    """The cleanup helper itself."""

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
