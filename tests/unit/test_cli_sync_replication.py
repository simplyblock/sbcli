"""The sync-replication CLI commands: ``volume sync-promote / sync-demote /
sync-status`` and ``cluster sync-status``, and the async replication commands
a sync-replication cluster refuses.

Driven through the generated ``CLIWrapper.run()`` so the argument parsing and
the dispatch are the real ones; the controllers are mocked (their behaviour is
tested in the sync-replication controller and integration tests).
"""
import json
import sys
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

from simplyblock_cli import cli as cli_module
from simplyblock_cli import clibase
from simplyblock_core.controllers.sync_replication_controller import (
    ClusterSyncStatus, LvsSyncStatus, SyncPromoteResult, VolumeSyncStatus,
)
from simplyblock_core.exceptions import SyncGateError, SyncReplicationUnsupportedError
from simplyblock_core.utils.nvme import NvmeConnectEntry

CLUSTER_ID = "2c1e0a3b-77b2-4a5e-9d0e-6f3b8c2a1d55"
VOLUME_ID = "7d4f0c2e-1b3a-4c5d-8e6f-0a1b2c3d4e5f"
TASK_ID = "5e6f7a8b-9c0d-4e1f-a2b3-c4d5e6f7a8b9"
NOW = datetime(2026, 9, 30, 12, 0, tzinfo=UTC)


def _run(argv, module_attr="sync_replication_controller", controller=None):
    """Run ``sbctl <argv>`` with ``clibase.<module_attr>`` mocked; returns the
    controller mock and what was printed (exit code 0 or SystemExit)."""
    controller = controller or MagicMock()
    printed = MagicMock()
    with patch.object(sys, "argv", ["sbctl", *argv]), \
            patch.object(clibase, module_attr, controller), \
            patch("builtins.print", printed):
        try:
            cli_module.CLIWrapper().run()
            code = 0
        except SystemExit as e:
            code = e.code
    output = "\n".join(" ".join(str(a) for a in c.args) for c in printed.call_args_list)
    return controller, output, code


def _entry(ip):
    return NvmeConnectEntry(
        transport="tcp", ip=ip, port=4420, nqn="nqn.2023-02.io.simplyblock:v", reconnect_delay=2,
        ctrl_loss_tmo=60, fast_io_fail_tmo=10, nr_io_queues=4, keep_alive_tmo=5,
        connect=f"sudo nvme connect -a {ip}")


def _lvs(**overrides):
    fields = dict(
        lvs_name="LVS_1", owner_id="owner-1", state="degraded",
        distribs={"distrib_1": "replica_unsynced"}, worst_status="replica_unsynced", mode_full=True,
        unsynced_pages=3, bytes_behind=3 * 4096, remote_journal_in_sync=True, jc_leader_id="n1",
        lag_seconds=90, answering=("n1", "n2"), resync_running=False,
        gate_problems=("LVS_1: distrib_1 on n1: replica_unsynced",), last_replicated_at=NOW)
    fields.update(overrides)
    return LvsSyncStatus(**fields)


def _cluster_status(lvs=()):
    return ClusterSyncStatus(
        state="degraded", degraded=True, resyncing=False, completed=True, peer_ready=False,
        diverged=True, last_replicated_at=NOW, lag_seconds=90, bytes_behind=3 * 4096,
        computed_at=NOW, lvs=tuple(lvs))


class TestVolumeSyncPromote:

    def test_done_prints_the_connection_strings(self):
        controller = MagicMock()
        controller.sync_promote_lvol.return_value = SyncPromoteResult(
            in_progress=False, connection_strings={VOLUME_ID: [_entry("10.0.1.10"), _entry("10.0.1.11")]})

        controller, output, code = _run(["volume", "sync-promote", VOLUME_ID, "--site", "site-b"],
                                        controller=controller)

        assert code == 0
        controller.sync_promote_lvol.assert_called_once_with(VOLUME_ID, "site-b", force=False)
        assert output == "sudo nvme connect -a 10.0.1.10\nsudo nvme connect -a 10.0.1.11"

    def test_force(self):
        controller = MagicMock()
        controller.sync_promote_lvol.return_value = SyncPromoteResult(in_progress=True, task_id=TASK_ID)

        controller, output, code = _run(
            ["volume", "sync-promote", VOLUME_ID, "--site", "site-b", "--force"], controller=controller)

        assert code == 0
        controller.sync_promote_lvol.assert_called_once_with(VOLUME_ID, "site-b", force=True)
        assert "in progress" in output
        assert TASK_ID in output

    def test_site_is_required(self):
        controller, _, code = _run(["volume", "sync-promote", VOLUME_ID])

        assert code == 2
        controller.sync_promote_lvol.assert_not_called()

    def test_refusal_fails_with_its_message(self):
        controller = MagicMock()
        controller.sync_promote_lvol.side_effect = SyncGateError(
            "sync-replication planned", ["LVS_1: distrib_1 on n1: replica_unsynced"])

        _, output, code = _run(["volume", "sync-promote", VOLUME_ID, "--site", "site-b"],
                               controller=controller)

        assert code == 1
        assert "replica_unsynced" in output


class TestVolumeSyncDemote:

    def test_demoted(self):
        controller = MagicMock()
        controller.sync_demote_lvol.return_value = [VOLUME_ID]

        controller, output, code = _run(["volume", "sync-demote", VOLUME_ID, "--site", "site-a"],
                                        controller=controller)

        assert code == 0
        controller.sync_demote_lvol.assert_called_once_with(VOLUME_ID, "site-a")
        assert output == f"Volume {VOLUME_ID} demoted on site site-a"

    def test_not_served_there_is_a_no_op(self):
        controller = MagicMock()
        controller.sync_demote_lvol.return_value = []

        _, output, code = _run(["volume", "sync-demote", VOLUME_ID, "--site", "site-a"], controller=controller)

        assert code == 0
        assert "nothing to do" in output

    def test_site_is_required(self):
        controller, _, code = _run(["volume", "sync-demote", VOLUME_ID])

        assert code == 2
        controller.sync_demote_lvol.assert_not_called()


class TestVolumeSyncStatus:

    def test_json(self):
        controller = MagicMock()
        controller.volume_sync_status_by_id.return_value = VolumeSyncStatus(
            role="secondary", site="site-b", cluster=_cluster_status())

        controller, output, code = _run(["volume", "sync-status", VOLUME_ID, "--site", "site-b", "--json"],
                                        controller=controller)

        assert code == 0
        controller.volume_sync_status_by_id.assert_called_once_with(VOLUME_ID, "site-b")
        assert json.loads(output) == {
            "site": "site-b", "role": "secondary", "state": "degraded", "completed": True, "degraded": True,
            "resyncing": False, "peer_ready": False, "diverged": True,
            "last_replicated_at": NOW.isoformat(), "lag_seconds": 90, "bytes_behind": 3 * 4096}

    def test_table(self):
        controller = MagicMock()
        controller.volume_sync_status_by_id.return_value = VolumeSyncStatus(
            role="primary", site="site-a", cluster=_cluster_status())

        _, output, code = _run(["volume", "sync-status", VOLUME_ID, "--site", "site-a"], controller=controller)

        assert code == 0
        assert "primary" in output
        assert "peer_ready" in output

    def test_site_is_required(self):
        controller, _, code = _run(["volume", "sync-status", VOLUME_ID])

        assert code == 2
        controller.volume_sync_status_by_id.assert_not_called()


class TestClusterSyncStatus:

    def test_json_has_the_aggregate_and_every_lvs(self):
        controller = MagicMock()
        controller.cluster_sync_status.return_value = _cluster_status(
            [_lvs(), _lvs(lvs_name="LVS_2", state="healthy", worst_status="synced", bytes_behind=0,
                          gate_problems=(), remote_journal_in_sync=None, last_replicated_at=None)])

        controller, output, code = _run(["cluster", "sync-status", CLUSTER_ID, "--json"], controller=controller)

        assert code == 0
        controller.cluster_sync_status.assert_called_once_with(CLUSTER_ID)
        data = json.loads(output)
        assert (data["state"], data["peer_ready"], data["bytes_behind"]) == ("degraded", False, 3 * 4096)
        assert data["lvs"] == [
            {"lvs": "LVS_1", "owner": "owner-1", "state": "degraded", "worst_status": "replica_unsynced",
             "mode_full": True, "bytes_behind": 3 * 4096, "remote_journal_in_sync": True,
             "last_replicated_at": NOW.isoformat(), "answering": 2,
             "gate_problems": "LVS_1: distrib_1 on n1: replica_unsynced"},
            {"lvs": "LVS_2", "owner": "owner-1", "state": "healthy", "worst_status": "synced",
             "mode_full": True, "bytes_behind": 0, "remote_journal_in_sync": None,
             "last_replicated_at": None, "answering": 2, "gate_problems": ""},
        ]

    def test_table_lists_every_lvs(self):
        controller = MagicMock()
        controller.cluster_sync_status.return_value = _cluster_status([_lvs(), _lvs(lvs_name="LVS_2")])

        _, output, code = _run(["cluster", "sync-status", CLUSTER_ID], controller=controller)

        assert code == 0
        assert "LVS_1" in output and "LVS_2" in output
        assert "peer_ready" in output


class TestNotASyncCluster:
    """A cluster without sync replication: the controller refuses, the CLI
    fails with its message."""

    @pytest.mark.parametrize("argv, method", [
        (["volume", "sync-promote", VOLUME_ID, "--site", "a"], "sync_promote_lvol"),
        (["volume", "sync-demote", VOLUME_ID, "--site", "a"], "sync_demote_lvol"),
        (["volume", "sync-status", VOLUME_ID, "--site", "a"], "volume_sync_status_by_id"),
        (["cluster", "sync-status", CLUSTER_ID], "cluster_sync_status"),
    ])
    def test_fails_clearly(self, argv, method):
        controller = MagicMock()
        getattr(controller, method).side_effect = SyncReplicationUnsupportedError(
            f"cluster {CLUSTER_ID} is not a sync-replication cluster")

        _, output, code = _run(argv, controller=controller)

        assert code == 1
        assert "not a sync-replication cluster" in output


class TestAsyncCommandsOnASyncCluster:
    """The async replication commands: the controller refuses them on a sync
    cluster (tested against the real database), the CLI fails with that."""

    @pytest.mark.parametrize("argv, method", [
        (["volume", "replication-start", VOLUME_ID], "replication_start"),
        (["volume", "replication-stop", VOLUME_ID], "replication_stop"),
        (["volume", "replication-trigger", VOLUME_ID], "replication_trigger"),
        (["volume", "replication-commit", VOLUME_ID], "replication_commit"),
    ])
    def test_fails_clearly(self, argv, method):
        controller = MagicMock()
        getattr(controller, method).side_effect = SyncReplicationUnsupportedError(
            "Replication x is not supported on a sync-replication cluster")

        _, output, code = _run(argv, module_attr="lvol_controller", controller=controller)

        assert code == 1
        assert "not supported on a sync-replication cluster" in output
        getattr(controller, method).assert_called_once()
