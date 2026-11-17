"""Spare landing volumes (replication_spare) never outlive their volume.

EKS bed 2026-11-16: back-and-forth online cluster migrations (scope=cluster,
deleteSource=true) left 42-43 ``REP_spare_<lvol id>`` volumes of 10 GiB on each
cluster, every one keyed by a volume that no longer existed. Provisioned
capacity reached 193%, past prov_cap_crit, so every new landing volume was
refused and the migrations stalled for hours. New stale spares appeared within
minutes of each completed cutover.
"""
from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from simplyblock_core import replication_spare as rs
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.lvol_model import LVol

NOW = 1_800_000_000.0
OLD = str(datetime.fromtimestamp(NOW) - timedelta(hours=1))
YOUNG = str(datetime.fromtimestamp(NOW) - timedelta(seconds=30))


def _vol(uuid, name="", status=LVol.STATUS_ONLINE, do_replicate=True, create_dt=OLD):
    lv = SimpleNamespace(uuid=uuid, lvol_name=name or uuid, status=status,
                         do_replicate=do_replicate, create_dt=create_dt, pool_uuid="POOL",
                         node_id="N1")
    lv.get_id = lambda: lv.uuid
    return lv


def _task(name, lvol_id=None, status=JobSchedule.STATUS_RUNNING, canceled=False, **params):
    if lvol_id:
        params["lvol_id"] = lvol_id
    return SimpleNamespace(function_name=name, status=status, canceled=canceled,
                           function_params=params)


class _DB:
    """The few look-ups replication_spare makes, over in-memory records."""

    def __init__(self, lvols=(), tasks=(), snapshots=None):
        self.lvols = {lv.get_id(): lv for lv in lvols}
        self.tasks = list(tasks)
        self.snapshots = snapshots or {}

    def get_lvol_by_id(self, uuid):
        if uuid not in self.lvols:
            raise KeyError(uuid)
        return self.lvols[uuid]

    def get_lvol_by_name(self, name, pool_uuid=None, include_deleted=False):
        found = [lv for lv in self.lvols.values() if lv.lvol_name == name
                 and (pool_uuid is None or lv.pool_uuid == pool_uuid)]
        if len(found) > 1:
            raise ValueError("Multiple values present")
        if not found:
            raise KeyError(name)
        return found[0]

    def query(self, model, index, name):
        return [lv for lv in self.lvols.values() if lv.lvol_name == name]

    def get_lvols(self, cluster_id=None):
        return list(self.lvols.values())

    def get_job_tasks(self, cluster_id):
        return self.tasks

    def get_clusters(self):
        return [SimpleNamespace(get_id=lambda: "C1")]

    def get_snapshot_by_id(self, sid):
        if sid not in self.snapshots:
            raise KeyError(sid)
        return self.snapshots[sid]

    def get_pool_by_id(self, pool_uuid):
        return SimpleNamespace(cluster_id="C1")

    def get_storage_node_by_id(self, nid):
        return SimpleNamespace(get_id=lambda: nid, cluster_id="C2")


def _snapshot(lvol_id="LV1"):
    return SimpleNamespace(lvol=_vol(lvol_id), size=10 * 2**30, get_id=lambda: "S1")


def _deleted_by(db):
    """Patch delete_lvol: removes the record, returns the list of deleted ids."""
    deleted: list = []

    def _delete(lvol, **_kw):
        deleted.append(lvol.get_id())
        db.lvols.pop(lvol.get_id(), None)
    return deleted, patch("simplyblock_core.controllers.lvol_controller.delete_lvol",
                          side_effect=_delete)


# --------------------------------------------------------------------------- #
# Leak path 1: a transfer that runs after the volume stopped replicating      #
# (after a cutover with deleteSource the source's queued transfers drain; a   #
# stopped or detached volume's transfer can still be inside the runner).      #
# --------------------------------------------------------------------------- #

class TestPrestageOnlyForAReplicatingVolume:

    def _prestage(self, db):
        create = MagicMock(return_value=("NEW", None))
        with patch.object(rs, "db", db):
            got = rs.prestage(_snapshot(), SimpleNamespace(node_id="N2", pool_uuid="POOL"),
                              create)
        return got, create

    def test_a_replicating_volume_gets_its_spare(self):
        got, create = self._prestage(_DB([_vol("LV1")]))
        assert got == "NEW"
        assert create.call_args.args[0] == rs.spare_name("LV1")

    def test_no_spare_after_the_volume_stopped_replicating(self):
        # _stop_source_replication (cutover done), replication_stop and a policy
        # detach all clear do_replicate; a draining transfer still prestaged.
        got, create = self._prestage(_DB([_vol("LV1", do_replicate=False)]))
        assert got is None
        create.assert_not_called()

    def test_no_spare_for_a_deleted_volume(self):
        # --delete-source: the snapshot's embedded lvol outlives the volume
        got, create = self._prestage(_DB([]))
        assert got is None
        create.assert_not_called()

    def test_no_spare_for_a_volume_being_deleted(self):
        got, create = self._prestage(_DB([_vol("LV1", status=LVol.STATUS_IN_DELETION)]))
        assert got is None
        create.assert_not_called()

    @pytest.mark.parametrize("params", [{"cutover_lvs": "LVS_1"}, {"shrink_snap_id": "S9"}])
    def test_no_spare_while_the_cutover_endgame_runs(self, params):
        db = _DB([_vol("LV1")], tasks=[_task(JobSchedule.FN_REPLICATION_FINAL, "LV1", **params)])
        got, create = self._prestage(db)
        assert got is None
        create.assert_not_called()

    def test_a_finished_or_queued_cutover_does_not_block_the_spare(self):
        db = _DB([_vol("LV1")], tasks=[
            _task(JobSchedule.FN_REPLICATION_FINAL, "LV1", status=JobSchedule.STATUS_DONE,
                  cutover_lvs="LVS_1"),
            _task(JobSchedule.FN_REPLICATION_FINAL, "LV1")])
        got, create = self._prestage(db)
        assert got == "NEW"

    def test_unreadable_volume_creates_nothing(self):
        db = _DB([])
        db.get_lvol_by_id = MagicMock(side_effect=RuntimeError("fdb"))  # type: ignore[method-assign]
        got, create = self._prestage(db)
        assert got is None
        create.assert_not_called()


# --------------------------------------------------------------------------- #
# Leak path 2: the create races with the end of the replication. The create   #
# takes 5-47 s on a loaded destination; the endgame's (or the stop's) discard #
# ran meanwhile, found nothing, and the spare landed afterwards.              #
# --------------------------------------------------------------------------- #

class TestPrestageRacingWithTheEnd:

    def _run(self, end):
        owner = _vol("LV1")
        db = _DB([owner])

        def _create(name, snapshot, node, pool):
            db.lvols["SPARE"] = _vol("SPARE", name=name)
            end(db, owner)              # happens while the create runs
            return "SPARE", None

        deleted, delete_patch = _deleted_by(db)
        with patch.object(rs, "db", db), delete_patch:
            got = rs.prestage(_snapshot(), SimpleNamespace(node_id="N2", pool_uuid="POOL"),
                              _create)
        return got, deleted

    def test_cutover_endgame_started_during_the_create(self):
        got, deleted = self._run(lambda db, owner: db.tasks.append(
            _task(JobSchedule.FN_REPLICATION_FINAL, "LV1", cutover_lvs="LVS_1")))
        assert got is None
        assert deleted == ["SPARE"]

    def test_replication_stopped_during_the_create(self):
        got, deleted = self._run(lambda db, owner: setattr(owner, "do_replicate", False))
        assert got is None
        assert deleted == ["SPARE"]

    def test_source_deleted_during_the_create(self):
        got, deleted = self._run(lambda db, owner: db.lvols.pop("LV1"))
        assert got is None
        assert deleted == ["SPARE"]

    def test_nothing_ended_keeps_the_spare(self):
        got, deleted = self._run(lambda db, owner: None)
        assert got == "SPARE"
        assert deleted == []


# --------------------------------------------------------------------------- #
# Leak path 3: terminal paths that never discarded.                           #
# --------------------------------------------------------------------------- #

class TestDiscardOnEveryTerminalPath:

    def test_discard_removes_spares_in_every_pool(self):
        # The unscoped name look-up raised "Multiple values present" and the
        # discard deleted nothing.
        a = _vol("SA", name=rs.spare_name("LV1"))
        b = _vol("SB", name=rs.spare_name("LV1"))
        b.pool_uuid = "POOL2"
        db = _DB([a, b])
        deleted, delete_patch = _deleted_by(db)
        with patch.object(rs, "db", db), delete_patch:
            assert rs.discard("LV1") is True
        assert sorted(deleted) == ["SA", "SB"]

    def test_cutover_done_discards_the_spare_after_the_source_stops(self, monkeypatch):
        from simplyblock_core.services import tasks_runner_replication_final as trf
        events: list = []
        src = _vol("SRC1")
        src.replication_interval_min = 1
        src.replication_policy_id = ""
        src.write_to_db = lambda *a: events.append(("stopped", src.do_replicate))

        class _FinalDB:
            kv_store = None

            def get_lvol_replication_by_id(self, rid):
                raise KeyError(rid)

            def get_lvol_by_id(self, uuid):
                if uuid != "SRC1":
                    raise KeyError(uuid)
                return src

        monkeypatch.setattr(trf, "db", _FinalDB())
        task = JobSchedule()
        task.uuid = "cutover-1"
        task.status = JobSchedule.STATUS_RUNNING
        task.function_params = {"lvol_id": "SRC1"}
        with patch.object(rs, "discard",
                          side_effect=lambda lvol_id, *a: events.append(("discard", lvol_id))):
            trf._record_cutover_done(task.frozen_view())
        assert ("discard", "SRC1") in events
        assert events.index(("stopped", False)) < events.index(("discard", "SRC1"))

    def test_replication_stop_discards_the_spare(self):
        from simplyblock_core.controllers import lvol_controller
        lvol = MagicMock(uuid="LV1", replication_policy_id="", node_id="N1")
        db = MagicMock()
        db.get_lvol_by_id.return_value = lvol
        db.get_storage_node_by_id.return_value = SimpleNamespace(cluster_id="C1")
        db.get_job_tasks.return_value = []
        with patch.object(lvol_controller, "DBController", return_value=db), \
                patch.object(rs, "discard") as discard:
            assert lvol_controller.replication_stop("LV1", from_policy=True)
        discard.assert_called_once_with("LV1")
        assert lvol.do_replicate is False

    @pytest.mark.parametrize("replicating,expected", [(True, 1), (False, 0)])
    def test_deleting_a_replicating_volume_discards_its_spare(self, replicating, expected):
        from simplyblock_core.controllers import lvol_controller

        class _Stop(Exception):
            pass

        lvol = MagicMock(uuid="LV1", status=LVol.STATUS_ONLINE, do_replicate=replicating,
                         replication_policy_id="", replication_node_id="", group_id="G1",
                         node_id="N1", pool_uuid="POOL")
        lvol.get_id.return_value = "LV1"
        db = MagicMock()
        db.get_storage_node_by_id.return_value = SimpleNamespace(
            lvstore_status="ready", cluster_id="C1")
        db.get_job_tasks.return_value = []
        # stop delete_lvol right after the replication cleanup
        db.get_consistency_group_by_id.side_effect = _Stop
        with patch.object(lvol_controller, "DBController", return_value=db), \
                patch.object(lvol_controller.ops_gate, "assert_object_ops_allowed"), \
                patch("simplyblock_core.controllers.migration_controller."
                      "get_active_migration_for_lvol", return_value=None), \
                patch.object(rs, "discard") as discard:
            with pytest.raises(_Stop):
                lvol_controller.delete_lvol(lvol)
        assert discard.call_count == expected
        if expected:
            discard.assert_called_with("LV1")


# --------------------------------------------------------------------------- #
# The sweep: the safety net for whatever still escapes.                       #
# --------------------------------------------------------------------------- #

class TestSweep:

    def _sweep(self, db):
        deleted, delete_patch = _deleted_by(db)
        with patch.object(rs, "db", db), delete_patch:
            n = rs.sweep("C2", now=NOW)
        assert n == len(deleted)
        return deleted

    def test_a_spare_of_a_gone_volume_is_deleted(self):
        db = _DB([_vol("SP", name=rs.spare_name("GONE"))])
        assert self._sweep(db) == ["SP"]

    def test_a_spare_of_a_volume_that_stopped_replicating_is_deleted(self):
        db = _DB([_vol("SP", name=rs.spare_name("LV1")), _vol("LV1", do_replicate=False)])
        assert self._sweep(db) == ["SP"]

    def test_a_spare_of_a_live_replicating_volume_is_kept(self):
        db = _DB([_vol("SP", name=rs.spare_name("LV1")), _vol("LV1")])
        assert self._sweep(db) == []

    def test_a_young_spare_is_kept(self):
        db = _DB([_vol("SP", name=rs.spare_name("GONE"), create_dt=YOUNG)])
        assert self._sweep(db) == []

    def test_a_spare_being_adopted_is_kept(self):
        # a transfer of the volume that has not settled on a landing volume
        # yet may be renaming the spare right now
        snap = SimpleNamespace(lvol=_vol("GONE"))
        db = _DB([_vol("SP", name=rs.spare_name("GONE"))],
                 tasks=[_task(JobSchedule.FN_SNAPSHOT_REPLICATION, snapshot_id="S1")],
                 snapshots={"S1": snap})
        assert self._sweep(db) == []

    def test_a_settled_transfer_does_not_keep_the_spare(self):
        snap = SimpleNamespace(lvol=_vol("GONE"))
        db = _DB([_vol("SP", name=rs.spare_name("GONE"))],
                 tasks=[_task(JobSchedule.FN_SNAPSHOT_REPLICATION, snapshot_id="S1",
                              remote_lvol_id="OTHER"),
                        _task(JobSchedule.FN_SNAPSHOT_REPLICATION, snapshot_id="S1",
                              status=JobSchedule.STATUS_DONE)],
                 snapshots={"S1": snap})
        assert self._sweep(db) == ["SP"]

    def test_a_spare_renamed_meanwhile_is_kept(self):
        spare = _vol("SP", name=rs.spare_name("GONE"))
        db = _DB([spare])
        real_get = db.get_lvol_by_id

        def _get(uuid):
            got = real_get(uuid)
            if uuid == "SP":
                got = _vol("SP", name="REP_repl_internal_x")   # adopted after the listing
            return got
        db.get_lvol_by_id = _get  # type: ignore[method-assign]
        assert self._sweep(db) == []

    def test_unreadable_tasks_delete_nothing(self):
        db = _DB([_vol("SP", name=rs.spare_name("GONE"))])
        db.get_job_tasks = MagicMock(side_effect=RuntimeError("fdb"))  # type: ignore[method-assign]
        assert self._sweep(db) == []

    def test_unreadable_owner_deletes_nothing(self):
        db = _DB([_vol("SP", name=rs.spare_name("LV1"))])
        db.get_lvol_by_id = MagicMock(side_effect=RuntimeError("fdb"))  # type: ignore[method-assign]
        assert self._sweep(db) == []

    def test_other_volumes_are_never_touched(self):
        db = _DB([_vol("A", name="REP_repl_internal_x"), _vol("B", name="pvc-1"),
                  _vol("SP", name=rs.spare_name("GONE"), status=LVol.STATUS_IN_DELETION)])
        assert self._sweep(db) == []

    def test_the_bed_state_is_cleaned_up(self):
        # 42 spares keyed by volumes that exist in neither cluster, next to
        # one live replicating volume with its own spare
        spares = [_vol(f"SP{i}", name=rs.spare_name(f"OLD{i}")) for i in range(42)]
        db = _DB(spares + [_vol("LIVE"), _vol("SPLIVE", name=rs.spare_name("LIVE"))])
        assert len(self._sweep(db)) == 42
        assert "SPLIVE" in db.lvols


class TestMonitorRunsTheSweep:

    def test_the_sweep_is_throttled_per_cluster(self, monkeypatch):
        from simplyblock_core.services import snapshot_monitor as sm
        monkeypatch.setattr(sm, "_last_spare_sweep", {})
        with patch.object(rs, "sweep", return_value=0) as sweep:
            sm.sweep_spare_landing_volumes("C1", 1000)
            sm.sweep_spare_landing_volumes("C1", 1001)
            sm.sweep_spare_landing_volumes("C2", 1001)
            sm.sweep_spare_landing_volumes("C1", 1000 + rs.SWEEP_INTERVAL_SEC)
        assert [c.args[0] for c in sweep.call_args_list] == ["C1", "C2", "C1"]
