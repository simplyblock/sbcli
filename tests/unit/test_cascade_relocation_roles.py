"""A cascading replica relocation builds each replica in the role it is moving
into, not the role the pre-move pointers give it.

Run 50, 2026-10-01 18:08. Removing v7ppl relocated LVS_2 (leader tt9v7) as a
cascade: the secondary moved v7ppl -> wzkz2, which held LVS_2 as tertiary, and
the tertiary moved wzkz2 -> htthx. _relocate_replica_between updates the
primary's pointer only after the build, and the build took its role from those
pointers, so:

* wzkz2, becoming the secondary, was still the recorded tertiary and was built
  ``role='tertiary'``;
* htthx, becoming the tertiary, was in neither pointer yet and was built
  ``role='secondary'`` -- with the secondary's hublvol.

The DB then said secondary=wzkz2, tertiary=htthx: the reverse of the data
plane. The health check wired htthx's second hublvol path to a "secondary"
hublvol on wzkz2 that had never been created (-5 Input/output error every
cycle). Covered here: the role is passed and wins over the pointers, the new
tertiary's cntlid window cannot collide with the one the old tertiary keeps,
and phase 3c reports a replica holding the wrong role.
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core import storage_node_ops as sno
from simplyblock_core.models.storage_node import StorageNode
from tests.unit.test_node_removal import FakeDB, _cluster, _lvol, _node


class TestNonLeaderRole(unittest.TestCase):

    def test_the_named_role_wins_over_the_pointers(self):
        wzkz2 = _node("wzkz2")
        tt9v7 = _node("tt9v7", lvstore="LVS_2", secondary_id="v7ppl", tertiary_id="wzkz2")
        self.assertEqual(sno._non_leader_role(wzkz2, tt9v7, "secondary"), "secondary")

    def test_without_a_role_the_pointers_decide(self):
        """A node restart: the topology is settled, so the pointers are right."""
        wzkz2 = _node("wzkz2")
        tt9v7 = _node("tt9v7", lvstore="LVS_2", secondary_id="htthx", tertiary_id="wzkz2")
        self.assertEqual(sno._non_leader_role(wzkz2, tt9v7), "tertiary")
        self.assertEqual(sno._non_leader_role(_node("htthx"), tt9v7), "secondary")

    def test_a_role_that_is_not_a_non_leader_role_is_refused(self):
        with self.assertRaises(ValueError):
            sno._non_leader_role(_node("a"), _node("p"), "primary")


class TestNonLeaderCntlidWindow(unittest.TestCase):

    def test_a_named_path_keeps_its_position_window(self):
        lvol = _lvol("tt9v7", ["tt9v7", "wzkz2", "htthx"])
        self.assertEqual(sno._non_leader_min_cntlid(lvol, _node("wzkz2")), 1000)
        self.assertEqual(sno._non_leader_min_cntlid(lvol, _node("htthx")), 2000)

    def test_the_new_tertiary_of_a_cascade_cannot_take_the_old_tertiarys_window(self):
        """lvol.nodes is re-pointed after the build. While htthx is being built
        the list still reads [tt9v7, v7ppl, wzkz2], and wzkz2 -- now the
        secondary -- keeps the subsystems it created as tertiary, at 2000.
        The role rule gave htthx 2000 as well."""
        lvol = _lvol("tt9v7", ["tt9v7", "v7ppl", "wzkz2"])
        window = sno._non_leader_min_cntlid(lvol, _node("htthx"))
        self.assertNotIn(window, (1, 1000, 2000))
        self.assertEqual(window, 3000)

    def test_a_node_listed_as_the_primary_is_not_given_the_primarys_window(self):
        lvol = _lvol("tt9v7", ["wzkz2", "tt9v7"])
        self.assertNotEqual(sno._non_leader_min_cntlid(lvol, _node("wzkz2")), 1)


class TestCascadeBuildsTheIncomingRole(unittest.TestCase):

    def _cascade(self):
        cl = _cluster(npcs=2, ndcs=2, ft=2)
        tt9v7 = _node("tt9v7", lvstore="LVS_2", secondary_id="v7ppl", tertiary_id="wzkz2")
        v7ppl = _node("v7ppl", status=StorageNode.STATUS_IN_REMOVAL, stack_secondary="tt9v7")
        wzkz2 = _node("wzkz2", stack_tertiary="tt9v7")
        htthx = _node("htthx")
        db = FakeDB(cl, [tt9v7, v7ppl, wzkz2, htthx],
                    lvols={"tt9v7": [_lvol("tt9v7", ["tt9v7", "v7ppl", "wzkz2"])]})
        built = []

        def _build(new_host, leader, primary, role=None, **_kw):
            built.append((new_host.get_id(), role))
            return True

        with patch.object(sno, "recreate_lvstore_on_non_leader", side_effect=_build), \
             patch.object(sno, "_delete_replica_on_peer"), \
             patch.object(sno, "_teardown_lvol_subsystems_on_vacated_peer"), \
             patch.object(sno, "_prune_stale_lvstore_ports"):
            ok1 = sno._relocate_replica_between("tt9v7", "v7ppl", "wzkz2", "secondary", db)
            ok2 = sno._relocate_replica_between("tt9v7", "wzkz2", "htthx", "tertiary", db)
        return ok1 and ok2, built, tt9v7

    def test_each_host_is_built_in_the_role_it_moves_into(self):
        ok, built, tt9v7 = self._cascade()
        self.assertTrue(ok)
        self.assertEqual(built, [("wzkz2", "secondary"), ("htthx", "tertiary")])
        self.assertEqual((tt9v7.secondary_node_id, tt9v7.tertiary_node_id), ("wzkz2", "htthx"),
                         "the recorded roles are the roles the data plane was given")


class TestTheBuildUsesTheNamedRole(unittest.TestCase):
    """Inside the build, the passed role -- not the pointers -- reaches SPDK.
    Driven through the activation-mode hublvol attach, the first role-bearing
    call after the stack is up; a sentinel stops the build right after it."""

    def test_the_hublvol_attach_takes_the_passed_role(self):
        class _Stop(Exception):
            pass

        snode = MagicMock(spec=StorageNode)
        snode.get_id = MagicMock(return_value="wzkz2")
        snode.cluster_id = "cluster-1"
        snode.rpc_client = MagicMock(return_value=MagicMock())
        primary = _node("tt9v7", lvstore="LVS_2", secondary_id="v7ppl", tertiary_id="wzkz2")
        db = MagicMock()
        db.get_storage_node_by_id = MagicMock(return_value=snode)
        db.get_lvols_by_node_id = MagicMock(return_value=[])

        with patch.object(sno, "DBController", return_value=db), \
             patch.object(sno, "_connect_to_remote_devs", return_value=[]), \
             patch.object(sno, "_connect_to_remote_jm_devs", return_value=[]), \
             patch.object(sno, "_derive_lvstore_ports", return_value={}), \
             patch.object(sno, "_rpc_bdev_exists", return_value=False), \
             patch.object(sno, "_set_restart_phase"), \
             patch.object(sno, "_create_bdev_stack", return_value=(True, None)), \
             patch.object(sno.jc_compression_upgrade, "resume_is_held", side_effect=_Stop):
            with self.assertRaises(_Stop):
                sno._recreate_lvstore_on_non_leader_impl(
                    snode, primary, primary, activation_mode=True, role="secondary")
        self.assertEqual(snode.connect_to_hublvol.call_args.kwargs["role"], "secondary")


class TestPhase3cReportsAReplicaHoldingTheWrongRole(unittest.TestCase):

    def _nodes(self):
        tt9v7 = _node("tt9v7", lvstore="LVS_2", secondary_id="wzkz2", tertiary_id="htthx")
        wzkz2 = _node("wzkz2", stack_secondary="tt9v7")
        htthx = _node("htthx", stack_tertiary="tt9v7")
        return [tt9v7, wzkz2, htthx]

    def test_the_run_50_reversal_is_reported_on_both_replicas(self):
        held = {"wzkz2": "tertiary", "htthx": "secondary"}
        wrong = sno.replica_role_violations(self._nodes(), lambda n, lvs: held[n.get_id()])
        self.assertEqual(sorted(wrong), [
            ("htthx", "LVS_2", "tt9v7", "tertiary", "secondary"),
            ("wzkz2", "LVS_2", "tt9v7", "secondary", "tertiary"),
        ])

    def test_matching_roles_are_not_reported(self):
        held = {"wzkz2": "secondary", "htthx": "tertiary"}
        self.assertEqual(
            sno.replica_role_violations(self._nodes(), lambda n, lvs: held[n.get_id()]), [])

    def test_an_unknown_role_is_not_a_mismatch(self):
        self.assertEqual(sno.replica_role_violations(self._nodes(), lambda n, lvs: None), [])

    def test_a_node_holding_both_roles_of_one_primary_is_skipped(self):
        p = _node("p", lvstore="LVS_9")
        both = _node("b", stack_secondary="p", stack_tertiary="p")
        self.assertEqual(sno.replica_role_violations([p, both], lambda n, lvs: "tertiary"), [])

    def test_the_verification_reads_the_role_from_spdk_and_logs_it(self):
        nodes = self._nodes()
        answers = {"wzkz2": {"name": "LVS_2", "lvs_tertiary": True, "lvs leadership": False},
                   "htthx": {"name": "LVS_2", "lvs_tertiary": False, "lvs leadership": False}}
        for n in nodes:
            rpc = MagicMock()
            rpc.bdev_lvol_get_lvstores.return_value = [answers.get(n.get_id(), {"name": "x"})]
            n.rpc_client = MagicMock(return_value=rpc)
        db = MagicMock()
        db.get_storage_nodes_by_cluster_id.return_value = nodes
        with self.assertLogs(sno.logger, level="ERROR") as logs:
            missing = sno._verify_replica_stacks("cluster-1", db)
        self.assertEqual(missing, [])
        self.assertEqual(sum("REPLICA ROLE MISMATCH" in m for m in logs.output), 2)


class TestReusedStackIsReWindowed(unittest.TestCase):
    """After the cascade, wzkz2 is LVS_2's secondary (position 1, window 1000)
    but kept the subsystems it created as tertiary, in 2000 -- the window
    htthx's position owns. htthx was built at 3000. Only wzkz2 is moved."""

    def _setup(self, *, htthx_status=StorageNode.STATUS_ONLINE, wzkz2_window=2000,
               shared=False):
        tt9v7 = _node("tt9v7", lvstore="LVS_2", secondary_id="wzkz2", tertiary_id="htthx")
        wzkz2 = _node("wzkz2")
        htthx = _node("htthx", status=htthx_status)
        lvols = [_lvol("tt9v7", ["tt9v7", "wzkz2", "htthx"], lvol_id="v1", nqn="nqn:a")]
        if shared:
            lvols.append(_lvol("tt9v7", ["tt9v7", "wzkz2", "htthx"], lvol_id="v2", nqn="nqn:a"))
        for lv in lvols:
            lv.status = "online"
            lv.allowed_hosts = []
            lv.max_namespace_per_subsys = 5 if shared else 1
        windows = {"wzkz2": wzkz2_window, "htthx": 3000}
        rpcs = {}
        for n in (tt9v7, wzkz2, htthx):
            rpc = MagicMock()
            rpc.subsystem_get.return_value = {"nqn": "nqn:a", "min_cntlid": windows.get(n.get_id(), 1)}
            n.rpc_client = MagicMock(return_value=rpc)
            rpcs[n.get_id()] = rpc
        db = FakeDB(_cluster(npcs=2, ndcs=2, ft=2), [tt9v7, wzkz2, htthx], lvols={"tt9v7": lvols})
        return db, rpcs, lvols

    def _run(self, db):
        with patch.object(sno, "add_lvol_thread", return_value=(True, None)) as add:
            sno._rewindow_moved_replica_subsystems(["tt9v7"], db)
        return add

    def test_the_old_tertiary_turned_secondary_moves_to_its_own_window(self):
        db, rpcs, _ = self._setup()
        add = self._run(db)
        rpcs["wzkz2"].subsystem_delete.assert_called_once_with("nqn:a")
        self.assertEqual(rpcs["wzkz2"].subsystem_create.call_args.args[3], 1000)
        add.assert_called_once()
        self.assertEqual(add.call_args.kwargs["lvol_ana_state"], "non_optimized")
        rpcs["htthx"].subsystem_delete.assert_not_called()

    def test_a_volume_without_two_other_live_paths_keeps_the_path(self):
        db, rpcs, _ = self._setup(htthx_status=StorageNode.STATUS_OFFLINE)
        self._run(db)
        rpcs["wzkz2"].subsystem_delete.assert_not_called()

    def test_a_subsystem_already_in_its_window_is_left_alone(self):
        db, rpcs, _ = self._setup(wzkz2_window=1000)
        self._run(db)
        rpcs["wzkz2"].subsystem_delete.assert_not_called()

    def test_a_shared_subsystem_is_recreated_once_with_every_volume_back(self):
        db, rpcs, lvols = self._setup(shared=True)
        add = self._run(db)
        rpcs["wzkz2"].subsystem_delete.assert_called_once_with("nqn:a")
        rpcs["wzkz2"].subsystem_create.assert_called_once()
        self.assertEqual(sorted(c.args[0].get_id() for c in add.call_args_list), ["v1", "v2"])

    def test_only_primaries_whose_replicas_moved_are_re_windowed(self):
        removed = _node("v7ppl", status=StorageNode.STATUS_IN_REMOVAL, stack_secondary="tt9v7")
        tt9v7 = _node("tt9v7", lvstore="LVS_2", secondary_id="v7ppl", tertiary_id="wzkz2")
        other = _node("jj7dr", lvstore="LVS_19", secondary_id="wzkz2", tertiary_id="htthx")
        db = FakeDB(_cluster(npcs=2, ndcs=2, ft=2), [removed, tt9v7, other])

        def _plan(_removed, _db):
            tt9v7.secondary_node_id = "wzkz2"
            return True

        with patch.object(sno, "DBController", return_value=db), \
             patch.object(sno, "_plan_driven_relocation", side_effect=_plan), \
             patch.object(sno, "_rewindow_moved_replica_subsystems") as rewindow:
            self.assertTrue(sno._relocate_replicas_hosted_on(removed))
        rewindow.assert_called_once_with(["tt9v7"], db)


if __name__ == "__main__":
    unittest.main()
