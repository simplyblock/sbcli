"""The node agent keeps the log tail of an SPDK container that died on its own
before a restart removes it (2026-11-20 FTT2 bed: three SPDKs died in a
cascade and every trace went with their containers)."""

import json
import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from simplyblock_web.api.internal.storage_node import docker as agent


def _client(status, logs=b"line1\nline2\n"):
    c = MagicMock()
    c.attrs = {"State": {"Status": status, "ExitCode": 134, "OOMKilled": False,
                         "Error": "", "StartedAt": "s", "FinishedAt": "f"}}
    c.logs.return_value = logs
    client = MagicMock()
    client.containers.get.return_value = c
    return client


class TestPreserveExitedSpdkLogs(unittest.TestCase):

    def setUp(self):
        self.dir = tempfile.mkdtemp()
        p = patch.object(agent, "SPDK_CRASH_LOG_DIR", self.dir)
        p.start()
        self.addCleanup(p.stop)

    def test_exited_container_log_is_kept(self):
        path = agent._preserve_exited_spdk_logs(_client("exited"), "/spdk_4420")
        self.assertTrue(path and os.path.exists(path))
        with open(path, "rb") as f:
            head, rest = f.read().split(b"\n", 1)
        self.assertEqual(json.loads(head)["ExitCode"], 134)
        self.assertIn(b"line2", rest)

    def test_running_container_is_left_alone(self):
        self.assertIsNone(agent._preserve_exited_spdk_logs(_client("running"), "/spdk_4420"))
        self.assertEqual(os.listdir(self.dir), [])

    def test_only_the_newest_are_kept(self):
        with patch.object(agent, "SPDK_CRASH_LOG_KEEP", 2):
            for i in range(4):
                with patch.object(agent.time, "strftime", return_value=f"t{i}"):
                    agent._preserve_exited_spdk_logs(_client("exited"), "/spdk_4420")
        self.assertEqual(len(os.listdir(self.dir)), 2)

    def test_never_raises(self):
        client = MagicMock()
        client.containers.get.side_effect = RuntimeError("dockerd wedged")
        self.assertIsNone(agent._preserve_exited_spdk_logs(client, "/spdk_4420"))


if __name__ == "__main__":
    unittest.main()
