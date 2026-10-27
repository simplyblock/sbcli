"""The cluster's metadata journal and the integrity flags it sits beside.

The data plane journals blobstore metadata pages so that a write torn by a
power loss on a device that is not 4K-write-atomic cannot leave a half-written
page behind. It is a creation-time option of the lvstore (the super block
records it, and a load reads it back from there), so it is a cluster property
frozen at create, carried into every lvstore the cluster builds, and replayed
from the persisted stack exactly as it was created.

These tests pin:

  1. a new cluster journals unless it is created without (the API, the CLI,
     and both create paths default on), while a record written before the
     flag existed still reads as off: its lvstores were created without the
     journal, and a data plane of that age rejects the key;
  2. the RPC sends ``md_journal`` only when it is on, because a data plane
     built before the option rejects a parameter it does not know;
  3. the lvstore stack entry carries the cluster's flag, so a restart replays
     the decision the cluster was created with;
  4. ``atomic_4k`` without ``inline_checksum`` is refused at create: the data
     plane reads it only when checksums are on, so the combination is a
     setting that silently does nothing.
"""

import inspect
import unittest
from unittest.mock import patch

from pydantic import SecretStr

from simplyblock_core import cluster_ops, storage_node_ops
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.rpc_client import RPCClient


class TestClusterFlag(unittest.TestCase):

    def test_an_old_record_reads_as_off(self):
        self.assertFalse(Cluster({'inline_checksum': True}).md_journal)

    def test_both_create_paths_default_on(self):
        for create in (cluster_ops.create_cluster, cluster_ops.add_cluster, cluster_ops._add_cluster_impl):
            with self.subTest(create=create.__name__):
                self.assertIs(inspect.signature(create).parameters['md_journal'].default, True)


class TestCLI(unittest.TestCase):

    def _parse(self, *argv):
        from simplyblock_cli import cli as cli_module
        wrapper = cli_module.CLIWrapper.__new__(cli_module.CLIWrapper)
        cli_module.CLIWrapper.__init__(wrapper)
        return wrapper.parser.parse_args(list(argv))

    def test_create_and_add_journal_by_default(self):
        for command in ("create", "add"):
            with self.subTest(command=command):
                self.assertIs(self._parse("cluster", command).md_journal, True)

    def test_disable_turns_it_off(self):
        for command in ("create", "add"):
            with self.subTest(command=command):
                self.assertIs(self._parse("cluster", command, "--disable-md-journal").md_journal, False)


class TestCreateLvstoreRPC(unittest.TestCase):

    def _params_for(self, **kw):
        c = RPCClient("1.2.3.4", 8080, "u", SecretStr("p"))
        captured = {}
        with patch.object(c, "_request",
                          side_effect=lambda m, **p: captured.update(p) or True):
            c.create_lvstore("LVS_1", "raid0_1", 4096, "none", **kw)
        return captured

    def test_on_sends_the_key(self):
        self.assertIs(self._params_for(md_journal=True)["md_journal"], True)

    def test_off_sends_nothing(self):
        self.assertNotIn("md_journal", self._params_for(md_journal=False))

    def test_absent_sends_nothing(self):
        self.assertNotIn("md_journal", self._params_for())


class TestLvstoreStackEntry(unittest.TestCase):

    def test_carries_the_clusters_flag(self):
        for flag in (True, False):
            with self.subTest(md_journal=flag):
                entry = storage_node_ops.lvstore_stack_entry(
                    Cluster({'md_journal': flag}), "LVS_1", "raid0_1", 8192)
                self.assertEqual(entry["type"], "bdev_lvstore")
                self.assertEqual(entry["name"], "LVS_1")
                self.assertIs(entry["params"]["md_journal"], flag)

    def test_replays_through_the_rpc(self):
        """The stored params are what a restart passes to create_lvstore, so
        every key in them has to be one the RPC wrapper accepts."""
        entry = storage_node_ops.lvstore_stack_entry(
            Cluster({'md_journal': True}), "LVS_1", "raid0_1", 8192)
        c = RPCClient("1.2.3.4", 8080, "u", SecretStr("p"))
        captured = {}
        with patch.object(c, "_request",
                          side_effect=lambda m, **p: captured.update(p) or True):
            c.create_lvstore(**entry["params"])
        self.assertEqual(captured["lvs_name"], "LVS_1")
        self.assertEqual(captured["bdev_name"], "raid0_1")
        self.assertIs(captured["md_journal"], True)


class TestAtomic4kNeedsInlineChecksum(unittest.TestCase):

    def test_add_cluster_refuses_atomic_4k_alone(self):
        with self.assertRaisesRegex(ValueError, "atomic_4k requires inline_checksum"):
            cluster_ops.add_cluster(
                4096, 2097152, 0, 0, 0, 0, 1, 1, 4096, 4096, "ha", False, 256,
                128, 4, False, False, "",
                inline_checksum=False, atomic_4k=True)

    def test_create_cluster_refuses_atomic_4k_alone(self):
        with self.assertRaisesRegex(ValueError, "atomic_4k requires inline_checksum"):
            cluster_ops.create_cluster(
                4096, 2097152, "", 0, 0, 0, 0, "eth0", "", 0, "", "", "", 1, 1,
                4096, 4096, "ha", "kubernetes", False, 256, 256, 128, 4, False,
                False, "", None, "", "", "tcp", False, "",
                inline_checksum=False, atomic_4k=True)


class TestClusterDTO(unittest.TestCase):

    def test_reports_the_flag(self):
        from simplyblock_web.api.v2._dtos import ClusterDTO
        for flag in (True, False):
            with self.subTest(md_journal=flag):
                dto = ClusterDTO.from_model(Cluster({'uuid': '9b1c4f0e-3a43-4c39-9d4e-1f2a3b4c5d6e',
                                                     'status': Cluster.STATUS_ACTIVE,
                                                     'md_journal': flag}))
                self.assertIs(dto.md_journal, flag)
