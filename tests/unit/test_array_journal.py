"""Where a node's journal is kept on a multi-attach cluster whose array plugin
creates the nodes' LUNs (Cluster.array_journal):

  - "lun": node add creates a journal LUN next to the data LUN, with the
    journal options (a faster pool) over the array's options and its own
    size, presented to that node only; the host config names no device
    (sn configure --lblk-array-only). The LUN is recorded on the node,
    detached with it and deleted with the cluster.
  - "local": the host config names one local disk or partition
    (sn configure --lblk-journal-only), used whole.
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core import utils
from simplyblock_core.array.fake import FakeArrayPlugin
from simplyblock_core.controllers import array_controller
from tests.unit import test_lblk_journal_disk_split as journal_split
from tests.unit.test_array_lifecycle import (
    CID,
    GIB,
    PLUGIN,
    FakeDB,
    _Case,
    _cluster,
    _lun_device,
    _node,
)


def _journal_cluster(journal="lun", lun_size=100 * GIB, journal_lun_size=0, journal_options=None):
    cl = _cluster(lun_size=lun_size)
    cl.array_journal = journal
    cl.array_journal_lun_size = journal_lun_size
    cl.array_journal_options = dict(journal_options or {})
    return cl


class TestRegistration(_Case):

    def test_journal_lun_with_its_own_pool(self):
        db = self.use_db(FakeDB(_cluster(plugin=""), []))
        view = array_controller.register_array(
            CID, PLUGIN, options={"pool": "capacity"}, lun_size=100 * GIB, journal="lun",
            journal_lun_size=8 * GIB, journal_options={"pool": "fast"})
        self.assertEqual((db.cluster.array_journal, db.cluster.array_journal_lun_size,
                          db.cluster.array_journal_options), ("lun", 8 * GIB, {"pool": "fast"}))
        self.assertEqual((view["journal"], view["journal_lun_size"], view["journal_options"]),
                         ("lun", 8 * GIB, {"pool": "fast"}))
        # The journal options were checked by the plugin, over the array's.
        self.assertIn({"pool": "fast"}, [c[2] for c in FakeArrayPlugin.connected])
        # The array's own options are unchanged.
        self.assertEqual(db.cluster.array_options, {"pool": "capacity"})

    def test_default_is_local(self):
        db = self.use_db(FakeDB(_cluster(plugin=""), []))
        view = array_controller.register_array(CID, PLUGIN, lun_size=GIB)
        self.assertEqual((db.cluster.array_journal, view["journal"], view["journal_lun_size"]),
                         ("local", "local", 0))

    def test_refusals(self):
        db = self.use_db(FakeDB(_cluster(plugin=""), []))
        for kw, msg in (({"journal": "nvram"}, "journal must be one of"),
                        ({"journal": "lun"}, "needs lun_size"),
                        ({"lun_size": GIB, "journal_options": {"pool": "f"}}, "'lun' only"),
                        ({"lun_size": GIB, "journal_lun_size": GIB}, "'lun' only"),
                        ({"lun_size": GIB, "journal": "lun", "journal_lun_size": -1}, "negative")):
            with self.subTest(kw=kw), self.assertRaisesRegex(ValueError, msg):
                array_controller.register_array(CID, PLUGIN, **kw)
        self.assertEqual(db.cluster.array_plugin, "")

    def test_bad_journal_options_save_nothing(self):
        db = self.use_db(FakeDB(_cluster(plugin=""), []))
        real_connect = FakeArrayPlugin.connect

        def connect(self, endpoint, credentials, options):
            if options.get("pool") == "missing":
                raise array_controller.array.ArrayError("no pool 'missing'")
            return real_connect(self, endpoint, credentials, options)
        with patch.object(FakeArrayPlugin, "connect", connect), \
                self.assertRaisesRegex(ValueError, "journal options: .*no pool"):
            array_controller.register_array(CID, PLUGIN, lun_size=GIB, journal="lun",
                                            journal_options={"pool": "missing"})
        self.assertEqual(db.cluster.array_plugin, "")

    def test_delete_resets_and_counts_journal_luns(self):
        a = _node("a")
        db = self.use_db(FakeDB(_cluster(plugin=""), [a]))
        array_controller.register_array(CID, PLUGIN, lun_size=GIB, journal="lun",
                                        journal_options={"pool": "fast"})
        a.array_journal_lun_id = "lun-j"
        with self.assertRaisesRegex(ValueError, "1 LUN"):
            array_controller.delete_array(CID, db.cluster.array_id)
        array_controller.delete_array(CID, db.cluster.array_id, force=True)
        self.assertEqual((db.cluster.array_journal, db.cluster.array_journal_options), ("local", {}))


class TestLocalKeyStoreOptIn(_Case):
    """The opt-in to the local key store travels with the registration, so an
    operator-managed cluster never needs the CLI for it."""

    def test_opt_in_with_the_registration(self):
        db = self.use_db(FakeDB(_cluster(plugin=""), []))
        db.cluster.allow_local_kms_credentials = False
        with self.assertRaisesRegex(ValueError, "could not be stored"):
            array_controller.register_array(CID, PLUGIN, credentials={"p": "x"})
        self.assertFalse(db.cluster.allow_local_kms_credentials)
        array_controller.register_array(CID, PLUGIN, credentials={"p": "x"}, allow_local_kms_credentials=True)
        self.assertTrue(db.cluster.allow_local_kms_credentials)
        self.assertEqual(db.cluster.array_credential_ref["kind"], "managed")
        # Omitted: unchanged.
        array_controller.register_array(CID, PLUGIN, credentials={"p": "y"})
        self.assertTrue(db.cluster.allow_local_kms_credentials)


class TestSize(unittest.TestCase):

    def test_default_is_3_percent_at_least_2_gib_whole_gib(self):
        self.assertEqual(array_controller.journal_lun_size(_journal_cluster(lun_size=1024 * GIB)),
                         31 * GIB)  # 30.72 GiB rounded up
        self.assertEqual(array_controller.journal_lun_size(_journal_cluster(lun_size=10 * GIB)), 2 * GIB)
        self.assertEqual(array_controller.journal_lun_size(
            _journal_cluster(journal_lun_size=5 * GIB + 1)), 5 * GIB + 1)

    def test_gate(self):
        self.assertTrue(array_controller.journal_on_lun(_journal_cluster()))
        self.assertFalse(array_controller.journal_on_lun(_journal_cluster(journal="local")))
        self.assertFalse(array_controller.journal_on_lun(_journal_cluster(lun_size=0)))
        cl = _journal_cluster()
        cl.array_journal = ""                      # a record from before the field
        self.assertFalse(array_controller.journal_on_lun(cl))


class TestProvisioning(_Case):

    def _api(self):
        """Every LUN the fake array holds is a disk of the host."""
        api = MagicMock()
        api.scan_new_luns.return_value = ({}, None)

        def inv():
            return [{"type": "disk", "name": f"sd{i}", "serial": lun["wwn"], "wwn": "",
                     "size": lun["size"], "by_id_path": f"/dev/disk/by-id/{lun['wwn']}", "numa_node": 0}
                    for i, lun in enumerate(FakeArrayPlugin.luns.values())], None
        api.get_blockdevices.side_effect = inv
        return api

    def test_journal_lun_then_data_lun(self):
        cl = _journal_cluster(journal_options={"pool": "fast"}, journal_lun_size=4 * GIB)
        cl.array_options = {"pool": "capacity", "thin": True}
        api, info = self._api(), {"hostname": "w1"}
        jentry, host = array_controller.provision_node_lun(cl, api, info, {"socket": 0}, [], journal=True)
        cfg = array_controller.add_lun_to_config([], jentry)
        dentry, _ = array_controller.provision_node_lun(cl, api, info, {"socket": 0}, cfg)
        cfg = array_controller.add_lun_to_config(cfg, dentry)

        jlun = FakeArrayPlugin.luns[jentry["array_lun_id"]]
        dlun = FakeArrayPlugin.luns[dentry["array_lun_id"]]
        self.assertEqual((jlun["name"], jlun["size"], jlun["hosts"]), ("simplyblock-w1-s0-journal", 4 * GIB, [host]))
        self.assertEqual(jlun["tags"]["simplyblock-role"], "journal")
        self.assertEqual((dlun["name"], dlun["size"]), ("simplyblock-w1-s0", 100 * GIB))
        # The journal LUN was created through a plugin with the journal pool,
        # the data LUN through one with the array's own options.
        pools = [c[2].get("pool") for c in FakeArrayPlugin.connected]
        self.assertEqual(pools, ["fast", "capacity"])
        self.assertEqual(FakeArrayPlugin.connected[0][2]["thin"], True)
        # The journal entry is flagged, the data entry is not, and both stay.
        self.assertEqual([bool(e.get("journal")) for e in cfg], [True, False])
        self.assertEqual(array_controller.journal_lun_id(cfg), jentry["array_lun_id"])

    def test_retried_add_reuses_each_kind(self):
        cl = _journal_cluster()
        api, info = self._api(), {"hostname": "w1"}
        jentry, _ = array_controller.provision_node_lun(cl, api, info, {"socket": 0}, [], journal=True)
        dentry, _ = array_controller.provision_node_lun(cl, api, info, {"socket": 0}, [jentry])
        cfg = [jentry, dentry]
        j2, _ = array_controller.provision_node_lun(cl, api, info, {"socket": 0}, cfg, journal=True)
        d2, _ = array_controller.provision_node_lun(cl, api, info, {"socket": 0}, cfg)
        self.assertEqual((j2["array_lun_id"], d2["array_lun_id"]),
                         (jentry["array_lun_id"], dentry["array_lun_id"]))
        self.assertEqual(len(FakeArrayPlugin.luns), 2)

    def test_journal_lun_is_not_multi_attached(self):
        from simplyblock_core import multi_attach
        from simplyblock_core.models.nvme_device import NVMeDevice
        cl = _journal_cluster()
        api, info = self._api(), {"hostname": "w1"}
        jentry, _ = array_controller.provision_node_lun(cl, api, info, {"socket": 0}, [], journal=True)
        dev = NVMeDevice()
        dev.serial_number = jentry["serial"]
        multi_attach.mark_lun_devices(cl, _node("a"), [dev], [jentry])
        self.assertFalse(dev.multi_attach)
        self.assertEqual(dev.array_lun_id, "")


class TestReleaseAndDelete(_Case):

    def test_node_release_detaches_its_journal_lun(self):
        a = _node("a")
        db = self.use_db(FakeDB(_journal_cluster(), [a]))
        p = self.plugin(db.cluster)
        data, journal = p.create_lun("d", GIB, {}), p.create_lun("j", GIB, {})
        for lun in (data, journal):
            p.attach(lun["id"], "host-a")
        _lun_device(a, data["id"], data["wwn"])
        a.array_journal_lun_id = journal["id"]
        array_controller.release_node_luns(a)
        self.assertEqual((p.get(data["id"])["hosts"], p.get(journal["id"])["hosts"]), ([], []))
        self.assertEqual(len(FakeArrayPlugin.luns), 2)   # released, not deleted

    def test_cluster_delete_deletes_recorded_journal_luns(self):
        a = _node("a")
        cl = _journal_cluster()
        self.use_db(FakeDB(cl, [a]))
        p = self.plugin(cl)
        journal = p.create_lun("j", GIB, {})
        a.array_journal_lun_id = journal["id"]
        with patch.object(FakeArrayPlugin, "list_luns", side_effect=NotImplementedError):
            array_controller.delete_cluster_luns(cl)
        self.assertEqual(FakeArrayPlugin.luns, {})


class TestHostConfig(unittest.TestCase):
    """sn configure: --lblk-array-only (no local device) and a local journal
    on a partition (--lblk-journal-only)."""

    def test_journal_only_accepts_a_partition(self):
        out = utils.lblk_journal_only_layout(
            {"nvme0n1p3": {"name": "nvme0n1p3", "serial": "P", "size": GIB, "type": "part"}})
        self.assertTrue(out["nvme0n1p3"]["journal"])

    def test_array_only_node_counts_its_two_luns(self):
        self.assertEqual(utils.node_config_device_count({"lblk_devices": [], "lblk_array_only": True}), 2)
        self.assertEqual(utils.node_config_device_count({"lblk_devices": [], "ssd_pcis": []}), 0)

    def test_array_only_sys_memory_counts_the_luns(self):
        node = {"lblk_devices": [], "lblk_array_only": True}
        self.assertEqual(utils.node_config_min_sys_memory(node, 10 * 1024 * GIB),
                         int(utils.sys_memory_for_capacity(10 * 1024 * GIB)))

    def _node(self, **kw):
        node = {"socket": 0, "cpu_mask": "0x2", "isolated": [1], "l-cores": "1@1", "ssd_pcis": [],
                "lblk_devices": [], "distribution": {}}
        node.update(kw)
        return node

    def test_validate_array_only(self):
        fields = {f: [] for f in ("app_thread_core", "jm_cpu_core", "poller_cpu_cores", "alceml_cpu_cores",
                                  "distrib_cpu_cores", "jc_singleton_core")}
        with patch.object(utils, "validate_node_config", wraps=utils.validate_node_config):
            base = self._node(distribution=fields)
            for k in ("max_lvol", "max_size", "huge_page_memory", "number_of_distribs", "nic_ports",
                      "number_of_alcemls", "small_pool_count", "large_pool_count", "sys_memory"):
                base.setdefault(k, 1)
            self.assertFalse(utils.validate_node_config(dict(base)))
            self.assertTrue(utils.validate_node_config(dict(base, lblk_array_only=True)))
            self.assertFalse(utils.validate_node_config(dict(
                base, lblk_array_only=True,
                lblk_devices=[{"name": "sdb", "serial": "S", "size": GIB}])))


class TestGenerateArrayOnly(unittest.TestCase):
    # The harness only; its tests stay in their module.
    _run = journal_split.TestGenerateConfigsPerNode._run

    def test_array_only_node_has_no_device(self):
        config, calls = self._run({}, nodes_per_socket=2, selection={"array_only": True})
        self.assertEqual(calls, [])
        self.assertEqual([(n["lblk_devices"], n["lblk_array_only"]) for n in config["nodes"]],
                         [([], True), ([], True)])


class TestConfigureArgs(unittest.TestCase):

    def _args(self, **kw):
        import argparse
        base = {"max_lvol": "10", "max_prov": "", "pci_allowed": "", "pci_blocked": "", "lblk": True,
                "blk_names": "", "blk_names_exclude": "", "blk_serials": "", "lblk_journal_only": False,
                "lblk_journal_device": False, "lblk_array_only": True, "device_model": "", "size_range": "",
                "nvme_names": "", "upgrade": False}
        base.update(kw)
        return argparse.Namespace(**base)

    def test_node_configure_validation(self):
        import argparse

        from simplyblock_web import node_configure
        node_configure.validate_arguments(self._args())
        for kw in ({"lblk": False}, {"blk_names": "sdb"}, {"lblk_journal_only": True},
                   {"lblk_journal_device": True}):
            with self.subTest(kw=kw), self.assertRaises(argparse.ArgumentError):
                node_configure.validate_arguments(self._args(**kw))

    def test_cli_has_the_flags(self):
        from simplyblock_cli import cli
        with open(cli.__file__, encoding="utf-8") as f:
            text = f.read()
        for flag in ("'--lblk-array-only'", "'--journal'", "'--journal-lun-size'", "'--journal-options'"):
            self.assertIn(flag, text)

    def test_cli_set_array_passes_the_journal(self):
        from simplyblock_cli.clibase import CLIWrapperBase
        args = MagicMock(cluster_id=CID, plugin="p", endpoint="", options=None, credentials_file=None,
                         credential_ref=None, lun_size=GIB, journal="lun", journal_lun_size=8 * GIB,
                         journal_options='{"pool": "fast"}', json=True)
        with patch.object(array_controller, "register_array", return_value={"id": "arr"}) as reg:
            CLIWrapperBase.cluster__set_array(MagicMock(), "set-array", args)
        kw = reg.call_args.kwargs
        self.assertEqual((kw["journal"], kw["journal_lun_size"], kw["journal_options"]),
                         ("lun", 8 * GIB, {"pool": "fast"}))


if __name__ == "__main__":
    unittest.main()
