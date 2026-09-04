"""Legacy lvstore_stack_secondary/_tertiary values must load as ''.

The fields were a secondary bdev STACK (List[dict], default []) until the
2026-04-16 restart refactor repurposed them as the UUID of the primary this
node peers for — without migrating existing FDB records. 26.3.0 then fixed
the annotation to ``str``, and BaseModel.from_dict's generic coercion turned
the stored legacy ``[]`` into the TRUTHY string ``"[]"``: the first
post-upgrade node restart of a pre-2026-04 cluster aborted with
``KeyError: 'StorageNode [] not found'`` (customer incident 2026-09-04).

StorageNode.from_dict now normalizes legacy values to '' on every read, so
stored records heal the moment they are loaded — no one-shot migration to
run or miss. The JM-connect path additionally degrades a dangling primary
reference to a skip instead of aborting the restart.
"""

import unittest

from simplyblock_core.models.storage_node import StorageNode

UUID = "9735c655-4b4d-4731-94dd-051947bcebe5"


def _load(**overrides):
    data = {"uuid": "node-1", **overrides}
    return StorageNode().from_dict(data)


class TestLegacyStackFieldNormalization(unittest.TestCase):

    def test_legacy_empty_list_loads_as_empty_string(self):
        """The pre-2026-04 default, still present in old FDB records."""
        node = _load(lvstore_stack_secondary=[], lvstore_stack_tertiary=[])
        self.assertEqual(node.lvstore_stack_secondary, "")
        self.assertEqual(node.lvstore_stack_tertiary, "")

    def test_poisoned_string_loads_as_empty_string(self):
        """A 26.3.0 control plane re-persisted the coerced '[]' string on its
        first full-object write; those records must heal too."""
        node = _load(lvstore_stack_secondary="[]", lvstore_stack_tertiary="[]")
        self.assertEqual(node.lvstore_stack_secondary, "")
        self.assertEqual(node.lvstore_stack_tertiary, "")

    def test_legacy_nonempty_stack_loads_as_empty_string(self):
        """A real pre-refactor bdev stack cannot be mapped to a primary UUID;
        '' (unlinked) is the only safe reading. The JM mesh verifier
        re-establishes the link once both peers are up."""
        node = _load(lvstore_stack_secondary=[{"type": "bdev_distr",
                                               "name": "distr_1"}])
        self.assertEqual(node.lvstore_stack_secondary, "")

    def test_valid_uuid_is_preserved(self):
        node = _load(lvstore_stack_secondary=UUID)
        self.assertEqual(node.lvstore_stack_secondary, UUID)

    def test_mixed_legacy_and_valid(self):
        """Normalization of one field must not clobber a valid sibling."""
        node = _load(lvstore_stack_secondary=UUID, lvstore_stack_tertiary=[])
        self.assertEqual(node.lvstore_stack_secondary, UUID)
        self.assertEqual(node.lvstore_stack_tertiary, "")

    def test_missing_keys_default_to_empty_string(self):
        node = _load()
        self.assertEqual(node.lvstore_stack_secondary, "")
        self.assertEqual(node.lvstore_stack_tertiary, "")

    def test_normalized_value_round_trips(self):
        """After a load-heal, persistence writes '' — the poison never
        survives a read-modify-write cycle."""
        node = _load(lvstore_stack_secondary=[])
        self.assertEqual(node.to_dict()["lvstore_stack_secondary"], "")

    def test_caller_dict_is_not_mutated(self):
        data = {"uuid": "node-1", "lvstore_stack_secondary": []}
        StorageNode().from_dict(data)
        self.assertEqual(data["lvstore_stack_secondary"], [])


if __name__ == "__main__":
    unittest.main()
