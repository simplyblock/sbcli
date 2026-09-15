"""test_lblk_persist_node_config.py — persist_node_config must be able to find
an lblk node slot.

Background (2026-09-15, docker lab): bringing up an lblk cluster failed at
`sn add-node` with

    POST persist_node_config {'numa_node': 0, 'ssd_list': [], ...}
    -> {"error":"No matching node found for given numa_node and ssd_list"}
    ERROR: Failed to size hugepages for the cluster before reserving them

The endpoint selected a node-config slot with

    ssd_set = set(body.ssd_list) if body.ssd_list is not None else None
    ...
    if ssd_set is not None and not ssd_set.intersection(...): continue

An empty ssd_list is `set()`, which `is not None`, so the filter ran -- and
`set().intersection(anything)` is empty, so EVERY slot was skipped. lblk
configs always carry `ssd_pcis == []` (generate_configs seeds it, and
validate_config demands exactly one of ssd_pcis/lblk_devices be non-empty),
so lblk could never match. Two things were wrong and both are pinned here:

  * empty must mean "no device filter", not "match nothing"; and
  * an lblk slot needs a device identity of its own, or a host with more than
    one slot on a socket (nodes_per_socket > 1) silently gets the FIRST one
    written -- the wrong slot, with no error.

It stayed latent because apply_cluster_hugepages only calls the endpoint when
the recomputed sizing differs from what `sn configure` wrote; a cluster that
changes neither max_subsys nor vcpu_count early-returns and never gets here.
"""

import unittest
from unittest.mock import MagicMock, patch

from flask import Flask

from simplyblock_core import storage_node_ops, utils
from simplyblock_web.api.internal.storage_node import docker as mod


def _lblk_entry(name, serial, size=512 * 1024 ** 3, journal=False):
    entry = {"name": name, "serial": serial, "by_id": f"/dev/disk/by-id/{serial}",
             "size": size, "numa": 0}
    if journal:
        entry["journal"] = True
    return entry


def _lblk_slot(socket=0, serials=("SN-A", "SN-B")):
    """A node-config entry as `sn configure --lblk` writes it: ssd_pcis
    present but empty, devices in lblk_devices."""
    return {
        "socket": socket,
        "ssd_pcis": [],
        "lblk_devices": [_lblk_entry(f"nvme{i}n1", s) for i, s in enumerate(serials)],
        "max_lvol": 75,
        "huge_page_memory": 4 * 1024 ** 3,
        "isolated": [1, 2, 3, 4],
        "cpu_mask": "0x1e",
        "l-cores": "1-4",
        "distribution": {},
        "core_to_index": {},
    }


def _nvme_slot(socket=0, pcis=("0000:00:01.0",)):
    return {
        "socket": socket,
        "ssd_pcis": list(pcis),
        "max_lvol": 75,
        "huge_page_memory": 4 * 1024 ** 3,
        "isolated": [1, 2, 3, 4],
        "cpu_mask": "0x1e",
        "l-cores": "1-4",
        "distribution": {},
        "core_to_index": {},
    }


def _persist(nodes, **body_fields):
    """Run the handler against ``nodes`` and return (payload, written_config).

    ``utils.get_response`` jsonifies, which needs an application context.
    """
    node_info = {"nodes": nodes}
    written = {}

    def _store(config, *args, **kwargs):
        written.update(config)

    with Flask(__name__).app_context(), \
            patch.object(mod.core_utils, "load_config", return_value=node_info), \
            patch.object(mod.core_utils, "store_config_file", side_effect=_store), \
            patch.object(mod.core_utils, "generate_mask", return_value="0x1e"):
        response = mod.persist_node_config(mod.PersistNodeConfigParams(**body_fields))
    return response.get_json(), written


class TestLblkSlotIsMatchable(unittest.TestCase):
    """The reported failure: an lblk slot must be found, not skipped."""

    def test_empty_ssd_list_is_no_filter_not_match_nothing(self):
        """The exact body add-node sent in the failing run. ssd_list=[] is
        what `node_config.get("ssd_pcis")` yields for every lblk slot, so if
        this does not match, lblk add-node can never complete.
        """
        nodes = [_lblk_slot()]
        payload, written = _persist(
            nodes, numa_node=0, ssd_list=[], huge_page_memory=6733168640,
            small_pool_count=32, large_pool_count=16)

        self.assertTrue(payload["status"], payload.get("error"))
        self.assertEqual(written["nodes"][0]["huge_page_memory"], 6733168640)
        self.assertEqual(written["nodes"][0]["small_pool_count"], 32)

    def test_omitted_ssd_list_still_matches(self):
        """None and [] must behave identically -- the distinction between
        "absent" and "empty" is exactly what made this a bug."""
        payload, written = _persist([_lblk_slot()], numa_node=0, max_lvol=45)
        self.assertTrue(payload["status"], payload.get("error"))
        self.assertEqual(written["nodes"][0]["max_lvol"], 45)

    def test_lblk_serials_select_the_right_slot_on_a_shared_socket(self):
        """nodes_per_socket > 1 puts several slots on one socket. Matching on
        numa_node alone would write the first one; the serials are what tells
        this host's slots apart."""
        nodes = [_lblk_slot(socket=0, serials=("SN-A", "SN-B")),
                 _lblk_slot(socket=0, serials=("SN-C", "SN-D"))]

        payload, written = _persist(
            nodes, numa_node=0, ssd_list=[], lblk_serials=["SN-C", "SN-D"],
            huge_page_memory=999)

        self.assertTrue(payload["status"], payload.get("error"))
        self.assertEqual(written["nodes"][1]["huge_page_memory"], 999)
        self.assertEqual(written["nodes"][0]["huge_page_memory"], 4 * 1024 ** 3,
                         "the sibling slot on the same socket must be untouched")

    def test_unknown_serials_do_not_match(self):
        """A non-empty device filter that matches nothing is still an error --
        writing the wrong slot would be worse than refusing."""
        payload, _ = _persist([_lblk_slot()], numa_node=0, lblk_serials=["SN-NOPE"])
        self.assertFalse(payload["status"])
        self.assertIn("SN-NOPE", payload["error"])


class TestNvmeMatchingUnchanged(unittest.TestCase):
    """The nvme path is the one that always worked; keep it that way."""

    def test_ssd_list_still_selects_among_slots(self):
        nodes = [_nvme_slot(socket=0, pcis=("0000:00:01.0",)),
                 _nvme_slot(socket=0, pcis=("0000:00:02.0",))]

        payload, written = _persist(
            nodes, numa_node=0, ssd_list=["0000:00:02.0"], max_lvol=45)

        self.assertTrue(payload["status"], payload.get("error"))
        self.assertEqual(written["nodes"][1]["max_lvol"], 45)
        self.assertEqual(written["nodes"][0]["max_lvol"], 75)

    def test_non_matching_pci_is_refused(self):
        payload, _ = _persist([_nvme_slot()], numa_node=0,
                              ssd_list=["0000:00:09.0"], max_lvol=45)
        self.assertFalse(payload["status"])


class TestDeviceIdentityHelpers(unittest.TestCase):

    def test_serials_of_an_lblk_list(self):
        self.assertEqual(
            utils.lblk_device_serials([_lblk_entry("nvme0n1", "SN-A"),
                                       _lblk_entry("nvme1n1", "SN-B")]),
            {"SN-A", "SN-B"})

    def test_missing_and_empty_serials_are_dropped(self):
        """A slot must never be identified by a blank serial -- that would
        match any other entry missing one."""
        self.assertEqual(
            utils.lblk_device_serials([{"name": "nvme0n1"},
                                       {"name": "nvme1n1", "serial": ""},
                                       _lblk_entry("nvme2n1", "SN-C")]),
            {"SN-C"})

    def test_none_is_empty(self):
        self.assertEqual(utils.lblk_device_serials(None), set())

    def test_slot_identity_covers_both_modes(self):
        self.assertEqual(mod._node_config_device_ids(_lblk_slot()), {"SN-A", "SN-B"})
        self.assertEqual(mod._node_config_device_ids(_nvme_slot()), {"0000:00:01.0"})


class TestCallersSendLblkIdentity(unittest.TestCase):
    """The endpoint can only disambiguate lblk slots if its callers actually
    send the serials."""

    def setUp(self):
        siblings = patch.object(utils, "parse_thread_siblings", return_value={})
        siblings.start()
        self.addCleanup(siblings.stop)

    def test_apply_cluster_hugepages_sends_serials(self):
        snode_api = MagicMock()
        snode_api.persist_node_config.return_value = (True, None)
        node_config = _lblk_slot()
        node_config["number_of_alcemls"] = 2
        node_config["number_of_distribs"] = 2
        node_config["distribution"] = {"poller_cpu_cores": [1, 2]}

        result = storage_node_ops.apply_cluster_hugepages(
            snode_api, node_config, req_cpu_count=8, max_prov=10 * 1024 ** 3)

        self.assertIsNotNone(result)
        kwargs = snode_api.persist_node_config.call_args.kwargs
        self.assertEqual(set(kwargs["lblk_serials"]), {"SN-A", "SN-B"})

    def test_apply_cluster_vcpu_count_sends_serials(self):
        snode_api = MagicMock()
        snode_api.persist_node_config.return_value = (True, None)
        nodes = [_lblk_slot()]
        node_info = {"cpu_topology": {"0": list(range(32))}}

        ok = storage_node_ops.apply_cluster_vcpu_count(snode_api, node_info, nodes, 8)

        self.assertTrue(ok)
        kwargs = snode_api.persist_node_config.call_args.kwargs
        self.assertEqual(set(kwargs["lblk_serials"]), {"SN-A", "SN-B"})


class TestRestartPathPersists(unittest.TestCase):
    """restart_storage_node's max_lvol write had the same defect AND threw the
    result away, so on lblk it failed silently and SPDK came back up against a
    stale config. _restart_storage_node_impl needs a live DB to call, so this
    is a source-level guard in the style of test_restart_cpu_fixes.py.
    """

    def test_persist_is_keyword_called_with_serials_and_checked(self):
        import inspect
        src = inspect.getsource(storage_node_ops._restart_storage_node_impl)
        self.assertNotIn(
            "snode_api.persist_node_config(snode.max_lvol,", src,
            "the positional call pairs numa_node/ssd_list by position only")
        self.assertIn("lblk_serials=utils.lblk_device_serials(snode.lblk_devices)", src)
        self.assertIn("ok, err = snode_api.persist_node_config(", src,
                      "a discarded result makes a failed persist invisible")


class TestClientPayload(unittest.TestCase):
    """SnodeClient must put the serials on the wire, and must not send an
    empty list (which the endpoint would read as a filter)."""

    @staticmethod
    def _payload(**kwargs):
        from simplyblock_core.snode_client import SNodeClient
        client = SNodeClient.__new__(SNodeClient)
        captured = {}
        with patch.object(SNodeClient, "_request",
                          side_effect=lambda m, p, payload=None, **_: captured.update(payload or {})):
            client.persist_node_config(
                max_lvol=45, huge_page_memory=None, numa_node=0, ssd_list=[], **kwargs)
        return captured

    def test_serials_are_sent_sorted(self):
        self.assertEqual(self._payload(lblk_serials={"SN-B", "SN-A"})["lblk_serials"],
                         ["SN-A", "SN-B"])

    def test_absent_serials_are_none_not_empty_list(self):
        self.assertIsNone(self._payload()["lblk_serials"])


if __name__ == "__main__":
    unittest.main()
