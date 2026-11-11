"""A spare landing volume per replicating volume (replication_spare).

bf13 (2026-11-05): creating the landing volume at transfer start took 5-47 s
on the loaded destination and sat on the critical path of every cycle's lag.
"""
import inspect
from unittest.mock import MagicMock, patch

from simplyblock_core import replication_spare as rs
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.services import snapshot_replication as sr


def _snapshot(lvol_id="LV1", size=10 * 2**30):
    snap = MagicMock()
    snap.lvol.get_id.return_value = lvol_id
    snap.size = size
    snap.get_id.return_value = "S1"
    return snap


def _spare(size=10 * 2**30, status=LVol.STATUS_ONLINE):
    lv = MagicMock()
    lv.get_id.return_value = "SPARE"
    lv.lvol_name = rs.spare_name("LV1")
    lv.size = size
    lv.status = status
    return lv


class TestAdopt:

    def test_a_fitting_spare_is_renamed_to_the_landing_name(self):
        spare = _spare()
        with patch.object(rs.db, "get_lvol_by_name", return_value=spare):
            got = rs.adopt(_snapshot(), "REP_repl_internal_LV1_1", "POOL")
        assert got is spare
        assert spare.lvol_name == "REP_repl_internal_LV1_1"
        spare.write_to_db.assert_called_once()

    def test_no_spare_means_no_adoption(self):
        with patch.object(rs.db, "get_lvol_by_name", side_effect=KeyError):
            assert rs.adopt(_snapshot(), "REP_x", "POOL") is None

    def test_a_spare_of_another_size_is_discarded_not_adopted(self):
        spare = _spare(size=20 * 2**30)
        with patch.object(rs.db, "get_lvol_by_name", return_value=spare), \
                patch.object(rs, "discard") as discard:
            assert rs.adopt(_snapshot(), "REP_x", "POOL") is None
        discard.assert_called_once_with("LV1", "POOL")
        spare.write_to_db.assert_not_called()

    def test_a_refused_rename_leaves_the_spare_alone(self):
        spare = _spare()
        spare.write_to_db.side_effect = RuntimeError("unique index")
        with patch.object(rs.db, "get_lvol_by_name", return_value=spare):
            assert rs.adopt(_snapshot(), "REP_x", "POOL") is None


class TestPrestage:

    def test_one_spare_is_created_when_none_exists(self):
        remote_lv = MagicMock(node_id="N1", pool_uuid="POOL")
        node = MagicMock()
        create = MagicMock(return_value=("NEW", None))
        with patch.object(rs.db, "get_lvol_by_name", side_effect=KeyError), \
                patch.object(rs, "_replicating", return_value=True), \
                patch.object(rs.db, "get_storage_node_by_id", return_value=node):
            assert rs.prestage(_snapshot(), remote_lv, create) == "NEW"
        create.assert_called_once()
        assert create.call_args.args[0] == rs.spare_name("LV1")
        assert create.call_args.args[2] is node and create.call_args.args[3] == "POOL"

    def test_an_existing_spare_is_not_duplicated(self):
        create = MagicMock()
        with patch.object(rs.db, "get_lvol_by_name", return_value=_spare()):
            assert rs.prestage(_snapshot(), MagicMock(pool_uuid="POOL"), create) is None
        create.assert_not_called()

    def test_a_failed_create_is_not_fatal(self):
        create = MagicMock(return_value=(None, "no space"))
        with patch.object(rs.db, "get_lvol_by_name", side_effect=KeyError), \
                patch.object(rs.db, "get_storage_node_by_id", return_value=MagicMock()):
            assert rs.prestage(_snapshot(), MagicMock(pool_uuid="POOL"), create) is None


class TestDiscard:

    def test_the_spare_is_deleted(self):
        spare = _spare()
        with patch.object(rs.db, "get_lvol_by_name", return_value=spare), \
                patch("simplyblock_core.controllers.lvol_controller.delete_lvol") as delete:
            assert rs.discard("LV1") is True
        delete.assert_called_once_with(spare, force_delete=True)

    def test_nothing_to_discard(self):
        with patch.object(rs.db, "get_lvol_by_name", side_effect=KeyError):
            assert rs.discard("LV1") is False


class TestRunnerUsesTheSpare:

    def test_the_start_adopts_a_spare_before_creating_a_landing_volume(self):
        src = inspect.getsource(sr.process_snap_replicate_start)
        adopt = src.index("replication_spare.adopt(snapshot, rep_name, remote_pool_uuid)")
        create = src.index("_create_landing_volume(\n            f\"REP_{snapshot.snap_name}\"")
        assert adopt < create

    def test_the_spare_is_prestaged_after_the_submit_outside_an_endgame(self):
        src = inspect.getsource(sr.process_snap_replicate_start)
        submit = src.index("task.status = JobSchedule.STATUS_RUNNING")
        prestage = src.index("replication_spare.prestage(snapshot, remote_lv, _create_landing_volume)")
        assert submit < prestage
        guard = src.rindex("if not _cutover_owns(", 0, prestage)
        assert guard < prestage

    def test_detach_and_cutover_end_discard_the_spare(self):
        from simplyblock_core.controllers import replication_policy_controller as rpc
        from simplyblock_core.services import tasks_runner_replication_final as final
        assert "replication_spare.discard(lvol_id)" in inspect.getsource(rpc.detach_policy)
        assert "replication_spare.discard(lvol_id)" in inspect.getsource(final._discard_spare_landing)
