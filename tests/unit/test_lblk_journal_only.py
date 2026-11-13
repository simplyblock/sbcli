"""``sn configure --lblk-journal-only``: on a multi-attach cluster whose array
plugin creates each node's data LUN at add-node, the node's one local disk is
its journal and nothing else (never partitioned, no local data device)."""
import unittest

from simplyblock_core import utils


def _disk(name="sdb", **kw):
    return dict({"name": name, "serial": f"S-{name}", "size": 50 * 2**30, "type": "disk"}, **kw)


class TestJournalOnlyLayout(unittest.TestCase):

    def test_single_disk_becomes_flagged_journal(self):
        out = utils.lblk_journal_only_layout({"sdb": _disk()})
        self.assertEqual(list(out), ["sdb"])
        self.assertTrue(out["sdb"]["journal"])
        self.assertEqual(out["sdb"]["serial"], "S-sdb")

    def test_input_is_not_modified(self):
        entries = {"sdb": _disk()}
        utils.lblk_journal_only_layout(entries)
        self.assertNotIn("journal", entries["sdb"])

    def test_two_disks_refused(self):
        with self.assertRaisesRegex(ValueError, "exactly one disk"):
            utils.lblk_journal_only_layout({"sdb": _disk(), "sdc": _disk("sdc")}, socket=1)

    def test_no_disk_refused(self):
        with self.assertRaisesRegex(ValueError, "exactly one disk"):
            utils.lblk_journal_only_layout({})

    def test_partition_refused(self):
        with self.assertRaisesRegex(ValueError, "whole disk"):
            utils.lblk_journal_only_layout({"sdb1": _disk("sdb1", type="part")})


class TestConfigureArgs(unittest.TestCase):
    """The flag is in the CLI reference and the generated CLI."""

    def test_flag_is_known_to_the_cli_reference(self):
        import os

        import yaml
        path = os.path.join(os.path.dirname(utils.__file__), "..", "..", "simplyblock_cli", "cli-reference.yaml")
        with open(path, encoding="utf-8") as f:
            text = f.read()
        self.assertIn("--lblk-journal-only", text)
        ref = yaml.safe_load(text)
        self.assertTrue(ref)

    def test_generated_cli_has_the_flag(self):
        from simplyblock_cli import cli
        with open(cli.__file__, encoding="utf-8") as f:
            self.assertIn("'--lblk-journal-only'", f.read())


if __name__ == "__main__":
    unittest.main()
