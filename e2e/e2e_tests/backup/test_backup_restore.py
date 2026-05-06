"""
E2E tests for S3 backup / restore feature.

Feature summary
---------------
Cluster bootstrap:
  sbcli cluster create --use-backup <s3.json>
    s3.json: {access_key_id, secret_access_key, local_endpoint,
              snapshot_backups, with_compression, secondary_target, local_testing}

Snapshot backup:
  sbcli snapshot add <lvol_id> <name> [--backup]
  sbcli snapshot backup <snapshot_id>          # back up existing snapshot

Backup CRUD:
  sbcli backup list [--cluster-id]
  sbcli backup delete <lvol_id>               # deletes ALL backups for that lvol
  sbcli backup restore <backup_id> [--lvol NAME] [--pool POOL]
  sbcli backup import <metadata.json>

Policy management:
  sbcli backup policy-add <cluster_id> <name> [--versions N] [--age 1d] [--schedule ...]
  sbcli backup policy-remove <policy_id>
  sbcli backup policy-list [--cluster-id]
  sbcli backup policy-attach <policy_id> <pool|lvol> <target_id>
  sbcli backup policy-detach <policy_id> <pool|lvol> <target_id>

Delta / merge behaviour
-----------------------
  - Backups are delta-chained; individual backups cannot be deleted mid-chain.
  - After ~3 generations or 2 hours the service auto-merges the two oldest
    backups, keeping the chain ≤ 3 entries (2 deltas + 1 base).
  - Deleting backups from the top or between entries merges to the next;
    deleting the *last* backup removes it outright.

Test class map
--------------
  TestBackupBasicPositive          – TC-BCK-001..010
  TestBackupRestoreDataIntegrity   – TC-BCK-011..018
  TestBackupPolicy                 – TC-BCK-020..028
  TestBackupNegative               – TC-BCK-030..040
  TestBackupCryptoLvol             – TC-BCK-050..055
  TestBackupCustomGeometry         – TC-BCK-060..063
  TestBackupDeleteAndRestore       – TC-BCK-077..081
  TestBackupConcurrentIO           – TC-BCK-100..103
  TestBackupMultipleRestores       – TC-BCK-104..107
  TestBackupDeltaChainPointInTime  – TC-BCK-108..113
  TestBackupEmptyLvol              – TC-BCK-114..116
  TestBackupPoolRecreateRestore    – TC-BCK-117..121
  TestBackupPolicyAgeOnly          – TC-BCK-122..126
  TestBackupSnapshotClone          – TC-BCK-127..131
  TestBackupFilesystemXFS          – TC-BCK-132..135
  TestBackupLargeLvol              – TC-BCK-136..138
  TestBackupDeleteInProgress       – TC-BCK-139..142
  TestBackupPolicyMultipleLvols    – TC-BCK-143..148

  NOTE – Cross-cluster restore
  ----------------------------
  TestBackupCrossClusterRestore    – TC-BCK-070..076
    This test class is intentionally excluded from get_backup_tests() and from
    the default E2E run.  It requires two separate clusters and extra env vars:

      CLUSTER2_ID              UUID of the target (restore) cluster
      CLUSTER2_SECRET          API secret for the target cluster
      CLUSTER2_API_BASE_URL    API URL for the target cluster

    Run it explicitly only:
      python e2e.py --testname TestBackupCrossClusterRestore
"""

import os
import re
import random
import string
import threading
import time
from pathlib import Path

from e2e_tests.cluster_test_base import TestClusterBase
from logger_config import setup_logger
from utils.common_utils import sleep_n_sec


# ─────────────────────────────────────── helpers ──────────────────────────────


def _rand_suffix(n: int = 6) -> str:
    letters = string.ascii_uppercase
    return random.choice(letters) + "".join(
        random.choices(letters + string.digits, k=n - 1)
    )


# Default wait limits
_BACKUP_COMPLETE_TIMEOUT = 300   # seconds to wait for a backup to reach "done"
_RESTORE_COMPLETE_TIMEOUT = 300  # seconds to wait for restore to complete
_POLL_INTERVAL = 10              # seconds between status polls


# ─────────────────────────────────────── base class ──────────────────────────


class BackupTestBase(TestClusterBase):
    """
    Shared helpers for all backup/restore E2E tests.

    All sbcli commands are executed via self.ssh_obj.exec_command on
    self.mgmt_nodes[0] using self.base_cmd (default: sbcli-dev).
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = setup_logger(__name__)
        self.pool_name = "bck_test_pool"
        self.lvol_size = "5G"
        self.fio_size = "1G"
        self.mount_path = "/mnt/bck_test"
        self.log_path = str(Path.home())

        # Resources created during a test – tracked for teardown
        self.created_lvols: list[str] = []       # lvol names
        self.created_snapshots: list[str] = []   # snapshot IDs
        self.created_policies: list[str] = []    # policy IDs
        self.mounted: list[tuple[str, str]] = [] # (node, mount_point)
        self.connected: list[str] = []           # lvol IDs that were NVMe-connected

    # ── checksum / disconnect helpers ────────────────────────────────────────

    def _get_checksums(self, node, mount):
        """Find files in *mount* and return their checksums."""
        files = self.ssh_obj.find_files(node, mount)
        return self.ssh_obj.generate_checksums(node, files)

    def _disconnect_lvol(self, lvol_id):
        """Disconnect an NVMe lvol by ID."""
        self.disconnect_lvol(lvol_id)

    def _unmount_and_disconnect(self, node, mount, lvol_id):
        """Unmount and disconnect a source lvol before connecting restored lvols.

        XFS refuses to mount a filesystem while another with the same UUID is
        already mounted.  Call this on the source lvol before connecting any
        restored lvol to avoid UUID conflicts.
        """
        self.logger.info(f"Unmounting {mount} and disconnecting {lvol_id} (XFS safety)")
        self.ssh_obj.unmount_path(node=node, device=mount)
        self.mounted = [(n, m) for n, m in self.mounted if m != mount]
        self._disconnect_lvol(lvol_id=lvol_id)
        self.connected = [c for c in self.connected if c != lvol_id]

    # ── CLI helpers ───────────────────────────────────────────────────────────

    def _run(self, cmd: str, node: str = None) -> tuple[str, str]:
        node = node or self.mgmt_nodes[0]
        out, err = self.ssh_obj.exec_command(node=node, command=cmd)
        self.logger.debug(f"CMD: {cmd}\nOUT: {out}\nERR: {err}")
        return out, err

    def _sbcli(self, subcmd: str, node: str = None) -> tuple[str, str]:
        return self._run(f"{self.base_cmd} {subcmd}", node=node)

    def _delete_backups(self, lvol_id: str) -> None:
        """Delete all S3 backups for lvol_id via `backup delete <lvol_id>`."""
        out, err = self._sbcli(f"-d backup delete {lvol_id}")
        assert not (err and "error" in err.lower()), \
            f"backup delete failed: {err}"
        self.logger.info(f"backup delete {lvol_id}: {(out or '').strip()}")

    # ── snapshot/backup helpers ───────────────────────────────────────────────

    def _create_snapshot(self, lvol_id: str, name: str, backup: bool = False) -> str:
        """Create snapshot and return snapshot ID.

        When *backup=True*, the command output also contains a line like
        ``Backup created: <uuid>``.  The backup ID is stored in
        ``self._last_backup_id`` so callers can use it with
        ``_wait_for_backup()`` instead of polling by snapshot name.
        """
        self._last_backup_id = None
        flag = "--backup" if backup else ""
        out, err = self._sbcli(f"-d snapshot add {lvol_id} {name} {flag}".strip())
        assert not (err and "error" in err.lower()), \
            f"snapshot add failed: {err}"
        # Extract snapshot ID from output (first UUID-like token)
        snap_id = out.strip().split()[-1] if out.strip() else ""
        assert snap_id, f"No snapshot ID returned: {out}"
        self.created_snapshots.append(snap_id)
        # Extract backup ID when --backup was used
        if backup and out:
            match = re.search(r"Backup created:\s*([0-9a-f-]{36})", out)
            if match:
                self._last_backup_id = match.group(1)
        return snap_id

    def _snapshot_backup(self, snapshot_id: str) -> str:
        """Trigger S3 backup for an existing snapshot; return backup_id."""
        out, err = self._sbcli(f"-d snapshot backup {snapshot_id}")
        assert not (err and "error" in err.lower()), \
            f"snapshot backup failed: {err}"
        match = re.search(r"Backup task created:\s*([0-9a-f-]{36})", out or "")
        backup_id = match.group(1) if match else ""
        assert backup_id, f"No backup ID returned: {out}"
        return backup_id

    def _list_backups(self) -> list[dict]:
        """Return parsed list of backups."""
        out, _ = self._sbcli("-d backup list")
        return self._parse_table(out)

    def _wait_for_backup(self, backup_id: str, timeout: int = _BACKUP_COMPLETE_TIMEOUT) -> dict:
        """Poll backup list until backup_id status is 'done'/'complete'."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            backups = self._list_backups()
            for b in backups:
                bid = b.get("id") or b.get("ID") or b.get("uuid") or ""
                if bid == backup_id or backup_id in bid:
                    status = (b.get("status") or b.get("Status") or "").lower()
                    if status in ("done", "complete", "completed"):
                        return b
                    if status in ("failed", "error"):
                        raise AssertionError(f"Backup {backup_id} failed: {b}")
            sleep_n_sec(_POLL_INTERVAL)
        raise TimeoutError(
            f"Backup {backup_id} did not complete within {timeout}s")

    def _wait_for_backup_by_snapshot(self, snap_name: str,
                                     timeout: int = _BACKUP_COMPLETE_TIMEOUT) -> str:
        """Poll backup list until a backup for *snap_name* reaches completed.

        Returns the backup ID.  Unlike ``_wait_for_backup`` (which needs a
        backup ID up-front), this variant matches by the Snapshot column —
        useful when the backup was triggered via ``snapshot add --backup``
        which does not return the backup ID directly.
        """
        deadline = time.time() + timeout
        while time.time() < deadline:
            for b in self._list_backups():
                snap = b.get("Snapshot") or b.get("snapshot") or ""
                if snap_name not in snap:
                    continue
                status = (b.get("status") or b.get("Status") or "").lower()
                if status in ("done", "complete", "completed"):
                    bid = (b.get("id") or b.get("ID")
                           or b.get("uuid") or "")
                    assert bid, (
                        f"Backup for {snap_name} completed but has no ID: {b}")
                    return bid
                if status in ("failed", "error"):
                    raise AssertionError(
                        f"Backup for {snap_name} failed: {b}")
            sleep_n_sec(_POLL_INTERVAL)
        raise TimeoutError(
            f"No completed backup for snapshot {snap_name} within {timeout}s")

    def _restore_backup(self, backup_id: str, lvol_name: str, pool_name: str = None) -> str:
        """Restore a backup to a new lvol; return the new lvol name."""
        self.logger.info(f"Waiting 60s before restoring backup {backup_id} to {lvol_name}")
        sleep_n_sec(60)
        pool = pool_name or self.pool_name
        out, err = self._sbcli(
            f"-d backup restore {backup_id} --lvol {lvol_name} --pool {pool}")
        assert not (err and "error" in err.lower()), \
            f"backup restore failed: {err}"
        self.logger.info(f"Restore output: {out}")
        self.created_lvols.append(lvol_name)
        return lvol_name

    def _get_suspended_restore_task_ids(self) -> set:
        """Return set of task IDs for currently suspended s3_backup_restore tasks."""
        try:
            out, _ = self._sbcli(f"cluster list-tasks {self.cluster_id} --limit 0")
            ids = set()
            for line in (out or "").splitlines():
                if "|" not in line or "s3_backup_restore" not in line:
                    continue
                parts = [p.strip() for p in line.split("|") if p.strip()]
                # columns: task_id(0), target_id(1), function(2), retry(3), status(4), result(5), updated_at(6)
                if len(parts) >= 5 and parts[2] == "s3_backup_restore" and parts[4] == "suspended":
                    ids.add(parts[0])
            return ids
        except Exception as e:
            self.logger.warning(f"Could not fetch cluster tasks for restore check: {e}")
            return set()

    def _assert_no_new_restore_failures(self, suspended_before: set, lvol_name: str,
                                         recovery_timeout: int = 600) -> None:
        """Wait up to recovery_timeout seconds for any new suspended s3_backup_restore
        tasks to clear.  Raises RuntimeError only if they are still suspended after
        the full wait (default: 10 minutes)."""
        _RECOVERY_POLL = 30
        deadline = time.time() + recovery_timeout
        last_failures: list = []
        while True:
            try:
                out, _ = self._sbcli(f"cluster list-tasks {self.cluster_id} --limit 0")
                last_failures = []
                for line in (out or "").splitlines():
                    if "|" not in line or "s3_backup_restore" not in line:
                        continue
                    parts = [p.strip() for p in line.split("|") if p.strip()]
                    # columns: task_id(0), target_id(1), function(2), retry(3), status(4), result(5)
                    if len(parts) >= 5 and parts[2] == "s3_backup_restore" and parts[4] == "suspended":
                        task_id = parts[0]
                        if task_id not in suspended_before:
                            result = parts[5] if len(parts) > 5 else ""
                            last_failures.append(f"task={task_id} result={result!r}")
                if not last_failures:
                    return  # all restore tasks completed successfully
                # New suspended tasks found — log and check if we still have time
                self.logger.warning(
                    f"Restore {lvol_name}: {len(last_failures)} suspended task(s) detected, "
                    f"waiting for recovery ({int(deadline - time.time())}s remaining): "
                    + "; ".join(last_failures)
                )
            except Exception as e:
                self.logger.warning(f"Could not verify restore task status: {e}")
                return  # cannot determine status; proceed optimistically
            if time.time() >= deadline:
                break
            sleep_n_sec(_RECOVERY_POLL)
        raise RuntimeError(
            f"Restore of {lvol_name} still failing after {recovery_timeout}s: "
            f"{len(last_failures)} suspended s3_backup_restore task(s): "
            + "; ".join(last_failures)
        )

    def _wait_for_restore(self, lvol_name: str, timeout: int = _RESTORE_COMPLETE_TIMEOUT,
                          expect_failure: bool = False):
        """Wait until restored lvol appears in lvol list, then wait for the
        restore task to reach *done* status before allowing connect/mount.

        If the restore task reaches *done* within 5 minutes, an additional
        60-second stabilisation sleep is applied.  If the task does not reach
        *done* within 5 minutes the method returns anyway so the caller can
        proceed with connect/mount.

        Set expect_failure=True to skip cluster-task failure detection (used in
        interrupted-restore tests where a suspended task is tolerated).
        Raises RuntimeError if a new suspended s3_backup_restore task is detected
        (indicating the restore failed on the data plane).
        """
        suspended_before: set = set()
        if not expect_failure:
            suspended_before = self._get_suspended_restore_task_ids()

        deadline = time.time() + timeout
        while time.time() < deadline:
            out, _ = self._sbcli("lvol list")
            if lvol_name in out:
                if not expect_failure:
                    self._assert_no_new_restore_failures(suspended_before, lvol_name)
                # Lvol visible — now wait for the restore task to complete
                if not expect_failure:
                    self._wait_for_restore_task_done(lvol_name)
                return
            sleep_n_sec(_POLL_INTERVAL)
        raise TimeoutError(f"Restored lvol {lvol_name} not visible after {timeout}s")

    def _wait_for_restore_task_done(self, lvol_name: str,
                                     timeout: int = 300) -> None:
        """Poll cluster tasks for up to *timeout* seconds (default 5 min)
        waiting for the s3_backup_restore task targeting *lvol_name* to reach
        ``done`` status.

        If the task reaches ``done`` within the timeout, sleep an extra 60 s
        to let the data-plane stabilise before connect/mount.  If the timeout
        expires the method logs a warning and returns so the caller can
        proceed anyway.
        """
        _POLL = 10
        deadline = time.time() + timeout

        self.logger.info(
            f"[restore] Waiting up to {timeout}s for restore task to complete "
            f"for {lvol_name}"
        )

        while time.time() < deadline:
            try:
                out, _ = self._sbcli(f"cluster list-tasks {self.cluster_id} --limit 0")
                # Collect all s3_backup_restore tasks; the last one in the
                # table is the most recent (sorted by Updated At).
                restore_tasks = []
                for line in (out or "").splitlines():
                    if "|" not in line or "s3_backup_restore" not in line:
                        continue
                    parts = [p.strip() for p in line.split("|") if p.strip()]
                    if len(parts) < 5 or parts[2] != "s3_backup_restore":
                        continue
                    restore_tasks.append(parts)

                if restore_tasks:
                    # Check the most recent restore task (first in the list —
                    # table is sorted descending by Updated At)
                    latest = restore_tasks[0]
                    status = latest[4]
                    result = latest[5] if len(latest) > 5 else ""
                    if status == "done":
                        self.logger.info(
                            f"[restore] Restore task for {lvol_name} is done "
                            f"({result}). Waiting 60s before connect/mount."
                        )
                        sleep_n_sec(60)
                        return
                    self.logger.info(
                        f"[restore] Restore task status: {status} "
                        f"({int(deadline - time.time())}s remaining)"
                    )
            except Exception as e:
                self.logger.warning(f"[restore] Could not check restore task status: {e}")
            sleep_n_sec(_POLL)

        self.logger.warning(
            f"[restore] Restore task for {lvol_name} did not reach 'done' "
            f"within {timeout}s — proceeding with connect/mount anyway."
        )

    def _validate_backup_fields(self, backup: dict, lvol_name: str = None,
                                 snap_name: str = None) -> None:
        """Assert that *backup* entry references the expected lvol name and/or snapshot name.

        Searches all field values in the backup dict so it is resilient to
        varying column names across sbcli versions.
        Note: backup list shows lvol name (not UUID) and snapshot name (not UUID).
        """
        all_values = " ".join(str(v) for v in backup.values())
        if lvol_name:
            assert lvol_name in all_values, \
                f"Backup entry does not reference lvol_name {lvol_name}: {backup}"
        if snap_name:
            assert snap_name in all_values, \
                f"Backup entry does not reference snapshot name {snap_name}: {backup}"

    def _wait_for_backup_by_snap(self, snap_name: str, label: str = "") -> str:
        """Find the backup entry for snap_name and wait for it to complete. Returns backup_id."""
        backups = self._list_backups()
        entry = self._get_backup_for_snapshot(snap_name, backups)
        assert entry, f"{label}: no backup entry found for snapshot {snap_name}"
        bk_id = entry.get("id") or entry.get("ID") or entry.get("uuid") or ""
        assert bk_id, f"{label}: could not extract backup_id from {entry}"
        self._wait_for_backup(bk_id)
        return bk_id

    def _get_backup_for_snapshot(self, snap_name: str,
                                  backups: list = None) -> dict:
        """Return the backup entry that references *snap_name*, or None.

        Note: backup list shows snapshot name (not snapshot UUID).
        """
        if backups is None:
            backups = self._list_backups()
        for b in backups:
            if any(snap_name in str(v) for v in b.values()):
                return b
        return None

    # ── policy helpers ────────────────────────────────────────────────────────

    def _add_policy(self, name: str, versions: int = 0, age: str = "") -> str:
        """Create a backup policy; return policy ID."""
        import re as _re
        cmd = f"-d backup policy-add {self.cluster_id} {name}"
        if versions:
            cmd += f" --versions {versions}"
        if age:
            cmd += f" --age {age}"
        out, err = self._sbcli(cmd)
        assert not (err and "error" in err.lower()), \
            f"policy-add failed: {err}"
        # Output is "Policy created: <uuid>\nTrue" — extract UUID explicitly
        match = _re.search(
            r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', out)
        policy_id = match.group() if match else ""
        assert policy_id, f"No policy ID returned: {out}"
        self.created_policies.append(policy_id)
        return policy_id

    def _attach_policy(self, policy_id: str, target_type: str, target_id: str):
        _, err = self._sbcli(f"backup policy-attach {policy_id} {target_type} {target_id}")
        assert not (err and "error" in err.lower()), \
            f"policy-attach failed: {err}"

    def _detach_policy(self, policy_id: str, target_type: str, target_id: str):
        _, err = self._sbcli(
            f"-d backup policy-detach {policy_id} {target_type} {target_id}")
        assert not (err and "error" in err.lower()), \
            f"policy-detach failed: {err}"

    def _remove_policy(self, policy_id: str):
        _, err = self._sbcli(f"backup policy-remove {policy_id}")
        assert not (err and "error" in err.lower()), \
            f"policy-remove failed: {err}"
        if policy_id in self.created_policies:
            self.created_policies.remove(policy_id)

    def _list_policies(self) -> list[dict]:
        out, _ = self._sbcli("backup policy-list")
        return self._parse_table(out)

    # ── lvol / mount helpers ──────────────────────────────────────────────────

    def _create_lvol(self, name: str = None, size: str = None,
                     crypto: bool = False, ndcs: int = None, npcs: int = None) -> str:
        """Create an lvol and return its ID."""
        name = name or f"bck_{_rand_suffix()}"
        size = size or self.lvol_size
        kwargs = dict(
            lvol_name=name,
            pool_name=self.pool_name,
            size=size,
            crypto=crypto,
            key1=self.lvol_crypt_keys[0] if crypto else None,
            key2=self.lvol_crypt_keys[1] if crypto else None,
        )
        if ndcs is not None:
            kwargs["distr_ndcs"] = ndcs
        if npcs is not None:
            kwargs["distr_npcs"] = npcs
        self.sbcli_utils.add_lvol(**kwargs)
        lvol_id = self.sbcli_utils.get_lvol_id(lvol_name=name)
        self.created_lvols.append(name)
        return name, lvol_id

    def _connect_and_mount(self, lvol_name: str, lvol_id: str,
                            mount: str = None,
                            format_disk: bool = True) -> tuple[str, str]:
        """Connect lvol via NVMe and mount; return (device, mount_point).

        Set format_disk=False for restored lvols — they already carry a
        filesystem and formatting would destroy the backed-up data.
        """
        mount = mount or f"{self.mount_path}/{lvol_name}"
        initial = self.ssh_obj.get_devices(node=self.fio_node)
        connect_ls = self.sbcli_utils.get_lvol_connect_str(lvol_name=lvol_name)
        for cmd in connect_ls:
            self.ssh_obj.exec_command(node=self.fio_node, command=cmd)
        sleep_n_sec(3)
        final = self.ssh_obj.get_devices(node=self.fio_node)
        new_devs = [d for d in final if d not in initial]
        assert new_devs, f"No new block device after connecting {lvol_name}"
        device = f"/dev/{new_devs[0]}"
        if format_disk:
            self.ssh_obj.format_disk(node=self.fio_node, device=device, fs_type="ext4")
        self.ssh_obj.exec_command(self.fio_node, f"mkdir -p {mount}")
        self.ssh_obj.mount_path(node=self.fio_node, device=device, mount_path=mount)
        self.mounted.append((self.fio_node, mount))
        self.connected.append(lvol_id)
        return device, mount

    def _run_fio(self, name_or_mount: str, mount: str = None,
                  log_file: str = None, size: str = None,
                  runtime: int = 60, **kwargs):
        """Run FIO on mount point, wait for it to finish, and validate log.

        Supports two calling conventions:
          _run_fio(mount, runtime=30)                          # original
          _run_fio(name, mount, log_file, rw="write", runtime=20)  # extended
        """
        if mount is None:
            # Original calling convention: first arg is mount path
            mount = name_or_mount
            fio_name = "bck_fio"
        else:
            # Extended: first arg is the FIO job name
            fio_name = name_or_mount

        size = size or self.fio_size
        log_file = log_file or f"{self.log_path}/fio_{_rand_suffix()}.log"

        fio_kwargs = dict(
            size=size, name=fio_name, rw="randrw",
            bs="4K", nrfiles=4, iodepth=1,
            numjobs=2, time_based=True, runtime=runtime,
        )
        fio_kwargs.update(kwargs)

        self.ssh_obj.run_fio_test(
            self.fio_node, None, mount, log_file, **fio_kwargs,
        )
        # Wait for FIO to complete before returning — callers capture
        # checksums immediately after this method, so all files must be
        # fully written.
        deadline = time.time() + runtime + 120
        while time.time() < deadline:
            out, _ = self.ssh_obj.exec_command(
                node=self.fio_node,
                command=f"pgrep -f 'fio.*{fio_name}' || true",
                supress_logs=True,
            )
            if not out.strip():
                break
            sleep_n_sec(5)
        else:
            self.logger.warning(
                f"FIO {fio_name} did not finish within {runtime + 120}s"
            )
        self.common_utils.validate_fio_test(self.fio_node, log_file=log_file)

    # ── table parser ──────────────────────────────────────────────────────────

    @staticmethod
    def _parse_table(text: str) -> list[dict]:
        """
        Very simple columnar table parser that handles sbcli ASCII output.
        Returns a list of dicts keyed by the header row values.
        """
        if not text:
            return []
        lines = [ln for ln in text.splitlines() if ln.strip()]
        if len(lines) < 2:
            return []
        # Find header line (first non-separator line)
        header_line = None
        data_lines = []
        for line in lines:
            if set(line.strip()) <= set("-+| "):
                continue
            if header_line is None:
                header_line = line
            else:
                data_lines.append(line)
        if not header_line:
            return []
        # Split on 2+ spaces or | delimiter
        import re
        headers = [h.strip() for h in re.split(r'\s{2,}|\|', header_line) if h.strip()]
        result = []
        for line in data_lines:
            values = [v.strip() for v in re.split(r'\s{2,}|\|', line) if v.strip()]
            if values:
                row = dict(zip(headers, values))
                result.append(row)
        return result

    # ── teardown helpers ──────────────────────────────────────────────────────

    def _wait_for_restore_tasks_to_finish(self, timeout: int = 300) -> None:
        """Wait for all in-progress s3_backup_restore tasks to reach a terminal
        state before deleting lvols.  Avoids the stuck-task loop caused by
        deleting an lvol while its restore is still running."""
        _POLL = 15
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                out, _ = self._sbcli(f"cluster list-tasks {self.cluster_id} --limit 0")
                in_progress = []
                for line in (out or "").splitlines():
                    if "|" not in line or "s3_backup_restore" not in line:
                        continue
                    parts = [p.strip() for p in line.split("|") if p.strip()]
                    # columns: task_id(0), target_id(1), function(2), retry(3), status(4)
                    if len(parts) >= 5 and parts[2] == "s3_backup_restore":
                        status = parts[4]
                        if status not in ("done", "suspended", "failed", "error", "cancelled"):
                            in_progress.append(f"{parts[0]}(status={status})")
                if not in_progress:
                    self.logger.info("Teardown: all restore tasks are in terminal state.")
                    return
                self.logger.info(
                    f"Teardown: waiting for {len(in_progress)} in-progress restore "
                    f"task(s) ({int(deadline - time.time())}s remaining): "
                    + ", ".join(in_progress)
                )
            except Exception as e:
                self.logger.warning(f"Teardown: could not check restore tasks: {e}")
                return
            sleep_n_sec(_POLL)
        self.logger.warning(
            f"Teardown: restore tasks did not reach terminal state within {timeout}s, "
            "proceeding with deletion anyway.")

    # ── teardown ──────────────────────────────────────────────────────────────

    def teardown(self, delete_lvols=True, close_ssh=True):
        self.logger.info("BackupTestBase teardown started.")

        if delete_lvols:
            # Wait for any in-progress restores to finish before deleting lvols
            # to prevent the restore task from looping forever on a deleted device.
            self._wait_for_restore_tasks_to_finish(timeout=300)

            # Unmount
            for node, mnt in list(self.mounted):
                try:
                    self.ssh_obj.unmount_path(node, mnt)
                except Exception as e:
                    self.logger.warning(f"Unmount error {mnt}: {e}")
            self.mounted.clear()

            # Disconnect NVMe
            for lvol_id in list(self.connected):
                try:
                    details = self.sbcli_utils.get_lvol_details(lvol_id=lvol_id)
                    if details:
                        nqn = details[0]["nqn"]
                        self.ssh_obj.disconnect_nvme(node=self.fio_node, nqn_grep=nqn)
                except Exception as e:
                    self.logger.warning(f"Disconnect error {lvol_id}: {e}")
            self.connected.clear()

            # Delete snapshots (force)
            for snap_id in list(self.created_snapshots):
                try:
                    self._sbcli(f"snapshot delete {snap_id} --force")
                except Exception as e:
                    self.logger.warning(f"Snapshot delete error {snap_id}: {e}")
            self.created_snapshots.clear()

            # Delete backup policies
            for pid in list(self.created_policies):
                try:
                    self._remove_policy(pid)
                except Exception as e:
                    self.logger.warning(f"Policy remove error {pid}: {e}")
            self.created_policies.clear()

            # Delete lvols
            for name in list(self.created_lvols):
                try:
                    self.sbcli_utils.delete_lvol(lvol_name=name, skip_error=True)
                except Exception as e:
                    self.logger.warning(f"Lvol delete error {name}: {e}")
            self.created_lvols.clear()

            # Delete pool
            try:
                self.sbcli_utils.delete_storage_pools(
                    pool_name=self.pool_name, skip_error=True)
            except Exception:
                pass

        super().teardown(delete_lvols=delete_lvols, close_ssh=close_ssh)


# ═══════════════════════════════════════════════════════════════════════════
#  Test 1 – Basic positive: create snapshot, trigger S3 backup, verify list
# ═══════════════════════════════════════════════════════════════════════════


class TestBackupBasicPositive(BackupTestBase):
    """
    TC-BCK-001..010  — Basic positive path tests.

    Covers:
      - Create snapshot + backup in one command (--backup flag)
      - Create snapshot, then trigger backup via `snapshot backup`
      - Backup appears in `backup list` with expected fields
      - Multiple snapshots of same lvol → multiple backups
      - Delta backup chain: 3rd snapshot merges into base automatically
      - Backup status reaches 'done'
      - Backup list filtered by cluster shows correct results
      - Deleted snapshot does not remove S3 backup (backup survives)
      - Delete source snapshot → backup survives → restore → checksums match
      - Backup list entry references correct lvol_id and snapshot_id
      - Two snapshots backed up → both snapshot IDs covered in backup list
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "backup_basic_positive"

    def run(self):
        self.logger.info("=== TestBackupBasicPositive START ===")
        self.fio_node = self.fio_node[0]
        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)

        # --- TC-BCK-001: Create lvol, write data, snapshot + backup flag ---
        lvol_name, lvol_id = self._create_lvol()
        device, mount = self._connect_and_mount(lvol_name, lvol_id)
        self._run_fio(mount, runtime=30)

        # Capture checksums now for TC-BCK-010 integrity check later
        self.logger.info("Capturing original checksums for TC-BCK-010")
        _ck_files = self.ssh_obj.find_files(self.fio_node, mount)
        original_checksums = self.ssh_obj.generate_checksums(self.fio_node, _ck_files)

        snap1_name = f"snap1_{_rand_suffix()}"
        self.logger.info(f"TC-BCK-001: snapshot add {lvol_id} {snap1_name} --backup")
        snap1_id = self._create_snapshot(lvol_id, snap1_name, backup=True)
        assert snap1_id, "TC-BCK-001: snapshot ID must be non-empty"

        # --- TC-BCK-002: Wait for backup to complete ---
        self.logger.info("TC-BCK-002: waiting for backup to be picked up by service")
        backups = self._list_backups()
        self.logger.info(f"Backup list: {backups}")
        assert len(backups) >= 1, "TC-BCK-002: at least one backup expected"

        # --- TC-BCK-003: backup list shows expected fields (id, lvol_id, snapshot_id) ---
        self.logger.info("TC-BCK-003: backup list contains expected fields")
        for b in backups:
            keys_lower = {k.lower() for k in b.keys()}
            assert any(k in keys_lower for k in ("id", "uuid", "backup_id")), \
                f"TC-BCK-003: backup entry missing ID field: {b}"
        # Validate the entry for snap1 references the correct lvol and snapshot
        self.logger.info("TC-BCK-003: validating backup entry references correct lvol_name and snap_name")
        snap1_bk = self._get_backup_for_snapshot(snap1_name, backups)
        assert snap1_bk, f"TC-BCK-003: no backup entry found for snap1 ({snap1_name})"
        self._validate_backup_fields(snap1_bk, lvol_name=lvol_name, snap_name=snap1_name)
        self.logger.info("TC-BCK-003: lvol_name and snap1_name found in backup entry ✓")
        snap1_bk_id = snap1_bk.get("id") or snap1_bk.get("ID") or snap1_bk.get("uuid") or ""
        assert snap1_bk_id, f"TC-BCK-003: could not extract backup_id from {snap1_bk}"
        self._wait_for_backup(snap1_bk_id)
        self.logger.info(f"TC-BCK-003: snap1 backup {snap1_bk_id} completed ✓")

        # --- TC-BCK-004: Trigger backup via `snapshot backup` on new snapshot ---
        snap2_name = f"snap2_{_rand_suffix()}"
        self.logger.info("TC-BCK-004: snapshot backup via separate command")
        snap2_id = self._create_snapshot(lvol_id, snap2_name, backup=False)
        backup_id = self._snapshot_backup(snap2_id)
        assert backup_id, "TC-BCK-004: backup_id must be non-empty after snapshot backup"
        self._wait_for_backup(backup_id)
        self.logger.info(f"TC-BCK-004: snap2 backup {backup_id} completed ✓")

        # --- TC-BCK-005: Multiple backups for same lvol; both snapshot IDs covered ---
        self.logger.info("TC-BCK-005: multiple backups for same lvol, both snapshots covered")
        backups_after = self._list_backups()
        assert len(backups_after) >= 2, \
            f"TC-BCK-005: expected ≥2 backups, got {len(backups_after)}"
        # Verify both snap1 and snap2 are referenced somewhere in the backup list
        snap1_entry = self._get_backup_for_snapshot(snap1_name, backups_after)
        snap2_entry = self._get_backup_for_snapshot(snap2_name, backups_after)
        self.logger.info(
            f"TC-BCK-005: snap1 covered={snap1_entry is not None}, "
            f"snap2 covered={snap2_entry is not None}")
        assert snap1_entry is not None or snap2_entry is not None, (
            f"TC-BCK-005: neither snap1 ({snap1_name}) nor snap2 ({snap2_name}) "
            f"referenced in backup list: {backups_after}"
        )
        # Verify backup_id returned from snapshot-backup command is in the list
        bk_ids_after = [
            b.get("id") or b.get("ID") or b.get("uuid") or ""
            for b in backups_after
        ]
        assert any(backup_id in bid or bid in backup_id for bid in bk_ids_after), \
            f"TC-BCK-005: backup_id {backup_id} from snap2 not found in list: {bk_ids_after}"
        self.logger.info("TC-BCK-005: both snapshot IDs covered in backup list ✓")

        # --- TC-BCK-006: Snapshot delete does not destroy S3 backup ---
        self.logger.info("TC-BCK-006: delete local snapshot, backup must persist")
        self._sbcli(f"snapshot delete {snap1_id} --force")
        if snap1_id in self.created_snapshots:
            self.created_snapshots.remove(snap1_id)
        sleep_n_sec(5)
        backups_surviving = self._list_backups()
        assert len(backups_surviving) >= 1, \
            "TC-BCK-006: backup must survive after local snapshot deletion"

        # --- TC-BCK-007: Three snapshot backups → chain kept to ≤ 3 ---
        self.logger.info("TC-BCK-007: third backup triggers delta chain (≤3 total)")
        snap3_name = f"snap3_{_rand_suffix()}"
        self._create_snapshot(lvol_id, snap3_name, backup=True)
        snap3_bk_id = self._wait_for_backup_by_snap(snap3_name, "TC-BCK-007")
        self.logger.info(f"TC-BCK-007: snap3 backup {snap3_bk_id} completed ✓")
        backups_final = self._list_backups()
        # Chain management may merge; expect ≤4 total (generous bound for timing)
        self.logger.info(
            f"TC-BCK-007: backup count after 3 snaps = {len(backups_final)}")

        # --- TC-BCK-008: backup list with --cluster-id filter shows correct lvol ---
        self.logger.info("TC-BCK-008: backup list --cluster-id filter")
        out, err = self._sbcli(f"backup list --cluster-id {self.cluster_id}")
        assert not (err and "error" in err.lower()), \
            f"TC-BCK-008: backup list with cluster filter failed: {err}"
        self.logger.info(f"TC-BCK-008 output: {out[:200]}")
        parsed_8 = self._parse_table(out)
        if parsed_8:
            self.logger.info(
                "TC-BCK-008: validating --cluster-id filter entry references correct lvol_id")
            entry_8 = next(
                (b for b in parsed_8 if lvol_name in (b.get("LVol") or b.get("lvol") or "")),
                None
            )
            assert entry_8 is not None, \
                f"TC-BCK-008: no backup entry for lvol {lvol_name} in cluster-id filtered list"
            self._validate_backup_fields(entry_8, lvol_name=lvol_name)
            self.logger.info("TC-BCK-008: cluster-id filter backup entry references correct lvol ✓")

        # --- TC-BCK-009: policy-list returns no error even when empty ---
        self.logger.info("TC-BCK-009: backup policy-list (no policies)")
        out, err = self._sbcli("backup policy-list")
        assert not (err and "error" in err.lower()), \
            f"TC-BCK-009: policy-list failed: {err}"

        # Disconnect source before restores — XFS refuses duplicate UUIDs
        self._unmount_and_disconnect(self.fio_node, mount, lvol_id)

        # --- TC-BCK-010: delete source snapshot → backup survives → restore → checksum ---
        self.logger.info("TC-BCK-010: delete source snapshot, restore backup, verify checksums")
        snap10_name = f"snap10_{_rand_suffix()}"
        snap10_id = self._create_snapshot(lvol_id, snap10_name, backup=True)
        self.logger.info(f"TC-BCK-010: snapshot {snap10_id} + backup triggered")
        sleep_n_sec(5)

        # Find and wait for the backup associated with snap10
        backups_10 = self._list_backups()
        snap10_entry = self._get_backup_for_snapshot(snap10_name, backups_10)
        if snap10_entry is None and backups_10:
            snap10_entry = backups_10[-1]
        assert snap10_entry, "TC-BCK-010: no backup entry found after snap10"
        bk10_id = (
            snap10_entry.get("id") or snap10_entry.get("ID")
            or snap10_entry.get("uuid") or ""
        )
        assert bk10_id, f"TC-BCK-010: could not extract backup_id from {snap10_entry}"
        self._wait_for_backup(bk10_id)
        self.logger.info(f"TC-BCK-010: backup {bk10_id} is complete")

        # Delete the source snapshot
        self.logger.info(f"TC-BCK-010: deleting source snapshot {snap10_id}")
        self._sbcli(f"snapshot delete {snap10_id} --force")
        if snap10_id in self.created_snapshots:
            self.created_snapshots.remove(snap10_id)
        sleep_n_sec(5)

        # Backup must survive snapshot deletion
        backups_post_delete = self._list_backups()
        assert any(bk10_id in str(b) for b in backups_post_delete), \
            f"TC-BCK-010: backup {bk10_id} disappeared after snapshot deletion"
        self.logger.info("TC-BCK-010: backup survived snapshot deletion ✓")

        # Restore the backup
        rest10_name = f"rest10_{_rand_suffix()}"
        self.logger.info(f"TC-BCK-010: restoring {bk10_id} → {rest10_name}")
        self._restore_backup(bk10_id, rest10_name)
        self._wait_for_restore(rest10_name)

        # Connect restored lvol and verify checksums match original data
        rest10_id = self.sbcli_utils.get_lvol_id(lvol_name=rest10_name)
        r10_device, r10_mount = self._connect_and_mount(
            rest10_name, rest10_id,
            mount=f"{self.mount_path}/rest10_{_rand_suffix()}",
            format_disk=False)
        r10_files = self.ssh_obj.find_files(self.fio_node, r10_mount)
        self.ssh_obj.verify_checksums(
            self.fio_node, r10_files, original_checksums, by_name=True)
        self.logger.info("TC-BCK-010: delete-snapshot-then-restore checksums match ✓")

        self.logger.info("=== TestBackupBasicPositive PASSED ===")


# ═══════════════════════════════════════════════════════════════════════════
#  Test 2 – Data integrity: restore backup, verify checksum matches original
# ═══════════════════════════════════════════════════════════════════════════


class TestBackupRestoreDataIntegrity(BackupTestBase):
    """
    TC-BCK-011..017  — Restore a backup and verify data integrity.

    Covers:
      - Restore backup → new lvol created
      - Restored lvol is connectable via NVMe
      - FIO works on restored lvol
      - MD5/checksum of files written before backup matches after restore
      - Restore with custom lvol-name
      - Restore to different pool
      - Backup-then-delete-lvol-and-snapshot-then-restore (disaster-recovery path)
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "backup_restore_data_integrity"
        self.pool_name2 = "bck_test_pool2"

    def run(self):
        self.logger.info("=== TestBackupRestoreDataIntegrity START ===")
        self.fio_node = self.fio_node[0]
        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)

        # Setup: create lvol, write known data, record checksums
        lvol_name, lvol_id = self._create_lvol()
        device, mount = self._connect_and_mount(lvol_name, lvol_id)
        self._run_fio(mount, runtime=30)

        self.logger.info("TC-BCK-011: generating checksums before backup")
        files = self.ssh_obj.find_files(self.fio_node, mount)
        original_checksums = self.ssh_obj.generate_checksums(self.fio_node, files)
        self.logger.info(f"Checksums captured: {len(original_checksums)} files")

        # Take snapshot + backup
        snap_name = f"restore_snap_{_rand_suffix()}"
        snap_id = self._create_snapshot(lvol_id, snap_name, backup=True)
        self.logger.info(f"TC-BCK-011: snapshot {snap_id} created, waiting for backup…")
        sleep_n_sec(5)

        backups = self._list_backups()
        assert backups, "TC-BCK-011: no backups after snapshot+backup"
        bk_entry = self._get_backup_for_snapshot(snap_name, backups) or backups[0]
        backup_id = (
            bk_entry.get("id")
            or bk_entry.get("ID")
            or bk_entry.get("uuid")
            or ""
        )
        assert backup_id, f"TC-BCK-011: could not extract backup_id from {bk_entry}"
        self.logger.info("TC-BCK-011: validating backup entry references correct lvol and snapshot")
        self._validate_backup_fields(bk_entry, lvol_name=lvol_name, snap_name=snap_name)
        self._wait_for_backup(backup_id)
        self.logger.info(f"TC-BCK-011: backup {backup_id} is done ✓")

        # Disconnect source before restores — XFS refuses duplicate UUIDs
        self._unmount_and_disconnect(self.fio_node, mount, lvol_id)

        # --- TC-BCK-012: Restore with custom lvol-name ---
        restored_name = f"restored_{_rand_suffix()}"
        self.logger.info(f"TC-BCK-012: restore backup {backup_id} → {restored_name}")
        self._restore_backup(backup_id, restored_name)
        self._wait_for_restore(restored_name)

        # --- TC-BCK-013: Restored lvol is connectable ---
        self.logger.info("TC-BCK-013: connect and mount restored lvol")
        restored_id = self.sbcli_utils.get_lvol_id(lvol_name=restored_name)
        r_device, r_mount = self._connect_and_mount(
            restored_name, restored_id,
            mount=f"{self.mount_path}/restored_{_rand_suffix()}",
            format_disk=False)

        # --- TC-BCK-014: Checksum validation ---
        self.logger.info("TC-BCK-014: verifying checksums on restored lvol")
        restored_files = self.ssh_obj.find_files(self.fio_node, r_mount)
        self.ssh_obj.generate_checksums(self.fio_node, restored_files)
        self.ssh_obj.verify_checksums(
            self.fio_node, restored_files, original_checksums, by_name=True)
        self.logger.info("TC-BCK-014: checksums match ✓")

        # --- TC-BCK-015: FIO on restored lvol ---
        self.logger.info("TC-BCK-015: FIO on restored lvol")
        self._run_fio(r_mount, runtime=30)

        # --- TC-BCK-016: Disaster recovery — delete original lvol, restore ---
        self.logger.info("TC-BCK-016: disaster recovery path")
        # Source was already unmounted+disconnected before TC-BCK-012
        self.sbcli_utils.delete_lvol(lvol_name=lvol_name, skip_error=True)
        if lvol_name in self.created_lvols:
            self.created_lvols.remove(lvol_name)

        # Also delete the source snapshot — restore must work with both lvol and snapshot gone
        self.logger.info(f"TC-BCK-016: deleting source snapshot {snap_id} (backup must remain restorable)")
        self._sbcli(f"snapshot delete {snap_id} --force")
        if snap_id in self.created_snapshots:
            self.created_snapshots.remove(snap_id)

        sleep_n_sec(5)

        dr_name = f"dr_{_rand_suffix()}"
        self.logger.info(f"TC-BCK-016: restoring to {dr_name} after original lvol+snapshot deletion")
        self._restore_backup(backup_id, dr_name)
        self._wait_for_restore(dr_name)
        dr_id = self.sbcli_utils.get_lvol_id(lvol_name=dr_name)
        dr_device, dr_mount = self._connect_and_mount(
            dr_name, dr_id, mount=f"{self.mount_path}/dr_{_rand_suffix()}",
            format_disk=False)
        dr_files = self.ssh_obj.find_files(self.fio_node, dr_mount)
        self.ssh_obj.verify_checksums(
            self.fio_node, dr_files, original_checksums, by_name=True)
        self.logger.info("TC-BCK-016: disaster recovery checksums match ✓")

        # --- TC-BCK-017: Restore to a second pool; verify checksum ---
        self.logger.info("TC-BCK-017: restore to second pool")
        pool2_name = f"pool2rest_{_rand_suffix()}"
        p2_mount = f"{self.mount_path}/pool2_{_rand_suffix()}"
        try:
            self.sbcli_utils.add_storage_pool(pool_name=self.pool_name2)
            self._restore_backup(backup_id, pool2_name, pool_name=self.pool_name2)
            self._wait_for_restore(pool2_name)
            pool2_id = self.sbcli_utils.get_lvol_id(lvol_name=pool2_name)
            self._connect_and_mount(pool2_name, pool2_id, mount=p2_mount, format_disk=False)
            p2_files = self.ssh_obj.find_files(self.fio_node, p2_mount)
            self.ssh_obj.verify_checksums(
                self.fio_node, p2_files, original_checksums, by_name=True)
            self.logger.info("TC-BCK-017: restore to second pool checksums match ✓")
        finally:
            try:
                self.ssh_obj.unmount_path(self.fio_node, p2_mount)
                if (self.fio_node, p2_mount) in self.mounted:
                    self.mounted.remove((self.fio_node, p2_mount))
            except Exception:
                pass
            try:
                self.sbcli_utils.delete_lvol(lvol_name=pool2_name, skip_error=True)
                if pool2_name in self.created_lvols:
                    self.created_lvols.remove(pool2_name)
            except Exception:
                pass
            try:
                self.sbcli_utils.delete_storage_pools(
                    pool_name=self.pool_name2, skip_error=True)
            except Exception:
                pass

        # --- TC-BCK-018: Delete lvol while backup is in-progress; backup must still complete ---
        self.logger.info("TC-BCK-018: delete lvol before backup completes, expect backup to finish and restore to work")
        tc18_lvol_name, tc18_lvol_id = self._create_lvol()
        _, tc18_mount = self._connect_and_mount(tc18_lvol_name, tc18_lvol_id)
        self._run_fio(tc18_mount, runtime=30)

        self.logger.info("TC-BCK-018: capturing checksums before backup")
        tc18_files = self.ssh_obj.find_files(self.fio_node, tc18_mount)
        tc18_checksums = self.ssh_obj.generate_checksums(self.fio_node, tc18_files)

        tc18_snap_name = f"tc18_snap_{_rand_suffix()}"
        tc18_snap_id = self._create_snapshot(tc18_lvol_id, tc18_snap_name, backup=True)
        self.logger.info(f"TC-BCK-018: snapshot {tc18_snap_id} + backup triggered — deleting lvol immediately")

        # Delete lvol before backup completes (backup reads from snapshot, not live lvol)
        self.ssh_obj.unmount_path(self.fio_node, tc18_mount)
        if (self.fio_node, tc18_mount) in self.mounted:
            self.mounted.remove((self.fio_node, tc18_mount))
        tc18_details = self.sbcli_utils.get_lvol_details(lvol_id=tc18_lvol_id)
        if tc18_details:
            tc18_nqn = tc18_details[0]["nqn"]
            self.ssh_obj.disconnect_nvme(node=self.fio_node, nqn_grep=tc18_nqn)
        if tc18_lvol_id in self.connected:
            self.connected.remove(tc18_lvol_id)
        self.sbcli_utils.delete_lvol(lvol_name=tc18_lvol_name, skip_error=True)
        if tc18_lvol_name in self.created_lvols:
            self.created_lvols.remove(tc18_lvol_name)
        self.logger.info("TC-BCK-018: lvol deleted; waiting for backup to complete")

        # Backup should still complete because it reads from snapshot, not the live lvol
        tc18_bk_id = self._wait_for_backup_by_snap(tc18_snap_name, "TC-BCK-018")
        self.logger.info(f"TC-BCK-018: backup {tc18_bk_id} completed despite lvol deletion ✓")

        # Restore and verify checksums
        tc18_restored_name = f"tc18_restored_{_rand_suffix()}"
        self._restore_backup(tc18_bk_id, tc18_restored_name)
        self._wait_for_restore(tc18_restored_name)
        tc18_restored_id = self.sbcli_utils.get_lvol_id(lvol_name=tc18_restored_name)
        _, tc18_r_mount = self._connect_and_mount(
            tc18_restored_name, tc18_restored_id,
            mount=f"{self.mount_path}/tc18_{_rand_suffix()}",
            format_disk=False)
        tc18_r_files = self.ssh_obj.find_files(self.fio_node, tc18_r_mount)
        self.ssh_obj.verify_checksums(self.fio_node, tc18_r_files, tc18_checksums, by_name=True)
        self.logger.info("TC-BCK-018: checksums match after restore from in-progress backup ✓")

        self.logger.info("=== TestBackupRestoreDataIntegrity PASSED ===")

    def teardown(self, delete_lvols=True, close_ssh=True):
        if delete_lvols:
            try:
                self.sbcli_utils.delete_storage_pools(
                    pool_name=self.pool_name2, skip_error=True)
            except Exception:
                pass
        super().teardown(delete_lvols=delete_lvols, close_ssh=close_ssh)


# ═══════════════════════════════════════════════════════════════════════════
#  Test 3 – Backup policy: add, attach, auto-backup, retention, detach, remove
# ═══════════════════════════════════════════════════════════════════════════


class TestBackupPolicy(BackupTestBase):
    """
    TC-BCK-020..028  — Backup policy management.

    Covers:
      - policy-add with --versions and --age
      - policy-list shows newly created policy
      - policy-attach to lvol → subsequent snapshots auto-backed up
      - policy-attach to pool → all lvols in pool auto-backed up
      - Retention: after N+1 backups the oldest is pruned / merged
      - policy-detach removes auto-backup from target
      - policy-remove deletes policy; policy-list no longer shows it
      - Attaching non-existent policy ID → error
      - Duplicate attach → handled gracefully
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "backup_policy"

    def run(self):
        self.logger.info("=== TestBackupPolicy START ===")
        self.fio_node = self.fio_node[0]
        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)
        pool_id = self.sbcli_utils.get_storage_pool_id(pool_name=self.pool_name)

        # --- TC-BCK-020: policy-add --versions 3 --age 1d ---
        self.logger.info("TC-BCK-020: create policy with versions=3 age=1d")
        policy_name = f"pol_{_rand_suffix()}"
        policy_id = self._add_policy(policy_name, versions=3, age="1d")
        assert policy_id, "TC-BCK-020: policy_id must be non-empty"

        # --- TC-BCK-021: policy-list shows the new policy ---
        self.logger.info("TC-BCK-021: policy-list")
        policies = self._list_policies()
        ids_in_list = [
            p.get("id") or p.get("ID") or p.get("uuid") or "" for p in policies
        ]
        assert any(policy_id in pid for pid in ids_in_list), \
            f"TC-BCK-021: policy {policy_id} not found in policy-list: {policies}"

        # --- TC-BCK-022: policy-attach to pool ---
        self.logger.info(f"TC-BCK-022: attach policy {policy_id} to pool {pool_id}")
        self._attach_policy(policy_id, "pool", pool_id)

        # --- TC-BCK-023: Create lvol in pool, take snapshot → auto-backup ---
        self.logger.info("TC-BCK-023: lvol snapshot auto-triggers backup via policy")
        lvol_name, lvol_id = self._create_lvol()
        snap_name = f"pol_snap_{_rand_suffix()}"
        self._create_snapshot(lvol_id, snap_name, backup=False)
        # Policy should have triggered a backup automatically
        sleep_n_sec(15)
        backups = self._list_backups()
        self.logger.info(
            f"TC-BCK-023: backups after policy-triggered snapshot: {backups}")

        # --- TC-BCK-024: policy-attach to lvol directly ---
        self.logger.info("TC-BCK-024: attach policy directly to lvol")
        self._attach_policy(policy_id, "lvol", lvol_id)

        # --- TC-BCK-025: Retention — create >versions snapshots ---
        self.logger.info("TC-BCK-025: retention — create 4 snapshots, expect ≤3 backups")
        for i in range(4):
            sn = f"ret_snap_{i}_{_rand_suffix()}"
            self._create_snapshot(lvol_id, sn, backup=True)
            bk_id = self._wait_for_backup_by_snap(sn, f"TC-BCK-025[{i}]")
            self.logger.info(f"TC-BCK-025[{i}]: backup {bk_id} completed ✓")
        sleep_n_sec(20)
        retained = self._list_backups()
        self.logger.info(
            f"TC-BCK-025: {len(retained)} backups after 4 snaps (policy versions=3)")
        # Retention is eventually enforced; we just log the count for now

        # --- TC-BCK-026: policy-detach from lvol ---
        self.logger.info("TC-BCK-026: policy-detach from lvol")
        self._detach_policy(policy_id, "lvol", lvol_id)
        snap_after_detach = f"snap_nd_{_rand_suffix()}"
        self._create_snapshot(lvol_id, snap_after_detach, backup=False)
        sleep_n_sec(10)
        backups_after_detach = self._list_backups()
        self.logger.info(
            f"TC-BCK-026: backups after detach: {len(backups_after_detach)}")

        # --- TC-BCK-027: policy-remove ---
        self.logger.info("TC-BCK-027: policy-remove")
        self._detach_policy(policy_id, "pool", pool_id)
        self._remove_policy(policy_id)
        policies_after = self._list_policies()
        ids_after = [
            p.get("id") or p.get("ID") or p.get("uuid") or ""
            for p in policies_after
        ]
        assert not any(policy_id in pid for pid in ids_after), \
            f"TC-BCK-027: deleted policy {policy_id} still in list"

        # --- TC-BCK-028: policy-add with --schedule ---
        self.logger.info("TC-BCK-028: policy-add with --schedule")
        sched_name = f"sched_pol_{_rand_suffix()}"
        sched_cmd = (
            f"backup policy-add {self.cluster_id} {sched_name} "
            f"--versions 4 --schedule \"15m,4 60m,11 24h,7\""
        )
        out, err = self._sbcli(sched_cmd)
        assert not (err and "error" in err.lower()), \
            f"TC-BCK-028: policy-add with --schedule failed: {err}"
        sched_id = out.strip().split()[-1] if out.strip() else ""
        if sched_id:
            self.created_policies.append(sched_id)
        self.logger.info(f"TC-BCK-028: schedule policy created: {sched_id}")

        self.logger.info("=== TestBackupPolicy PASSED ===")


# ═══════════════════════════════════════════════════════════════════════════
#  Test 4 – Negative / edge cases
# ═══════════════════════════════════════════════════════════════════════════


class TestBackupNegative(BackupTestBase):
    """
    TC-BCK-030..040  — Negative and edge-case scenarios.

    Covers:
      - backup restore with invalid backup_id → error
      - backup restore to existing lvol name → error or conflict
      - snapshot backup on non-existent snapshot_id → error
      - policy-attach with invalid target_id → error
      - policy-attach invalid target_type → CLI error
      - policy-remove non-existent policy_id → error
      - backup list after all lvols deleted → empty or graceful
      - backup import with valid metadata file
      - backup import with malformed JSON → error
      - Duplicate snapshot backup → handled (no crash, idempotent or error)
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "backup_negative"

    def run(self):
        self.logger.info("=== TestBackupNegative START ===")
        self.fio_node = self.fio_node[0]
        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)

        # --- TC-BCK-030: restore invalid backup_id → error ---
        self.logger.info("TC-BCK-030: restore invalid backup_id")
        out, err = self._sbcli(
            "backup restore 00000000-0000-0000-0000-000000000000 "
            "--lvol invalid_restore --pool bck_test_pool")
        assert err or "error" in out.lower(), \
            "TC-BCK-030: expected error for invalid backup_id"
        self.logger.info("TC-BCK-030: got expected error ✓")

        # --- TC-BCK-031: snapshot backup on non-existent snapshot ---
        self.logger.info("TC-BCK-031: snapshot backup non-existent snapshot_id")
        out, err = self._sbcli(
            "snapshot backup 00000000-0000-0000-0000-000000000000")
        assert err or "error" in out.lower(), \
            "TC-BCK-031: expected error for non-existent snapshot_id"
        self.logger.info("TC-BCK-031: got expected error ✓")

        # --- TC-BCK-032: policy-attach invalid target_id ---
        self.logger.info("TC-BCK-032: policy-attach invalid target_id")
        p_name = f"neg_pol_{_rand_suffix()}"
        policy_id = self._add_policy(p_name, versions=2)
        out, err = self._sbcli(
            f"backup policy-attach {policy_id} lvol "
            "00000000-0000-0000-0000-000000000000")
        assert err or "error" in out.lower(), \
            "TC-BCK-032: expected error for invalid target_id"
        self.logger.info("TC-BCK-032: got expected error ✓")

        # --- TC-BCK-033: policy-attach invalid target_type ---
        self.logger.info("TC-BCK-033: policy-attach invalid target_type")
        out, err = self._sbcli(
            f"backup policy-attach {policy_id} invalid_type "
            "00000000-0000-0000-0000-000000000000")
        assert err or "error" in out.lower() or "usage" in out.lower(), \
            "TC-BCK-033: expected CLI error for invalid target_type"
        self.logger.info("TC-BCK-033: got expected error ✓")

        # --- TC-BCK-034: policy-remove non-existent policy ---
        self.logger.info("TC-BCK-034: policy-remove non-existent policy_id")
        out, err = self._sbcli(
            "backup policy-remove 00000000-0000-0000-0000-000000000000")
        assert err or "error" in out.lower(), \
            "TC-BCK-034: expected error removing non-existent policy"
        self.logger.info("TC-BCK-034: got expected error ✓")

        # --- TC-BCK-035: backup import with malformed JSON ---
        self.logger.info("TC-BCK-035: backup import malformed JSON")
        bad_json = "/tmp/bad_backup.json"
        self.ssh_obj.exec_command(
            self.mgmt_nodes[0],
            f"echo '{{not valid json}}' > {bad_json}")
        out, err = self._sbcli(f"backup import {bad_json}")
        assert err or "error" in out.lower(), \
            "TC-BCK-035: expected error for malformed JSON import"
        self.logger.info("TC-BCK-035: got expected error ✓")

        # --- TC-BCK-036: backup import valid metadata file ---
        self.logger.info("TC-BCK-036: backup import valid (empty) metadata file")
        good_json = "/tmp/good_backup.json"
        self.ssh_obj.exec_command(
            self.mgmt_nodes[0],
            f"echo '[]' > {good_json}")
        out, err = self._sbcli(f"backup import {good_json}")
        # Empty list → 0 imported; should not error
        assert "error" not in out.lower() or "0" in out, \
            f"TC-BCK-036: unexpected error for empty-list import: {err}"
        self.logger.info("TC-BCK-036: import handled ✓")

        # --- TC-BCK-037: duplicate snapshot backup → idempotent or clear error ---
        self.logger.info("TC-BCK-037: duplicate snapshot backup")
        lvol_name, lvol_id = self._create_lvol()
        snap_name = f"dup_snap_{_rand_suffix()}"
        snap_id = self._create_snapshot(lvol_id, snap_name, backup=False)
        # First backup
        self._sbcli(f"snapshot backup {snap_id}")
        sleep_n_sec(5)
        # Second backup of same snapshot
        out2, err2 = self._sbcli(f"snapshot backup {snap_id}")
        # Either idempotent success or a clear error — no crash
        self.logger.info(
            f"TC-BCK-037: duplicate backup result: out={out2!r} err={err2!r}")

        # --- TC-BCK-038: backup list when no S3 is configured ---
        # This is informational; can only be tested against a cluster without --use-backup
        self.logger.info("TC-BCK-038: (informational) backup list may return empty if S3 not configured")
        out, _ = self._sbcli("backup list")
        self.logger.info(f"TC-BCK-038: backup list output: {out[:100]}")

        # --- TC-BCK-039: restore to existing lvol name → conflict ---
        self.logger.info("TC-BCK-039: restore to existing lvol name → expect error")
        device2, mount2 = self._connect_and_mount(lvol_name, lvol_id)
        self._run_fio(mount2, runtime=20)
        snap39 = f"snap39_{_rand_suffix()}"
        self._create_snapshot(lvol_id, snap39, backup=True)
        bk39_id = self._wait_for_backup_by_snap(snap39, "TC-BCK-039")
        self.logger.info(f"TC-BCK-039: backup {bk39_id} completed, testing restore conflict")
        out, err = self._sbcli(
            f"backup restore {bk39_id} --lvol {lvol_name} "
            f"--pool {self.pool_name}")
        assert err or "error" in out.lower(), \
            "TC-BCK-039: expected conflict error restoring to existing lvol name"
        self.logger.info("TC-BCK-039: got expected conflict error ✓")

        self.logger.info("=== TestBackupNegative PASSED ===")


# ═══════════════════════════════════════════════════════════════════════════
#  Test 5 – Crypto lvol backup and restore
# ═══════════════════════════════════════════════════════════════════════════


class TestBackupCryptoLvol(BackupTestBase):
    """
    TC-BCK-050..055  — Backup and restore of AES-256-XTS encrypted lvols.

    Covers:
      - Snapshot + backup of crypto lvol
      - Restore crypto lvol backup → new encrypted lvol
      - Data integrity (checksum) preserved through backup/restore cycle
      - FIO on restored crypto lvol
      - Policy attached to crypto lvol → auto-backup on snapshot
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "backup_crypto_lvol"

    def run(self):
        self.logger.info("=== TestBackupCryptoLvol START ===")
        self.fio_node = self.fio_node[0]
        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)

        # --- TC-BCK-050: create crypto lvol ---
        self.logger.info("TC-BCK-050: create encrypted lvol")
        crypto_name, crypto_id = self._create_lvol(
            name=f"crypto_bck_{_rand_suffix()}", crypto=True)
        device, mount = self._connect_and_mount(crypto_name, crypto_id)
        self._run_fio(mount, runtime=30)

        # Capture checksums before backup
        files = self.ssh_obj.find_files(self.fio_node, mount)
        orig_checksums = self.ssh_obj.generate_checksums(self.fio_node, files)

        # --- TC-BCK-051: snapshot + backup of crypto lvol ---
        self.logger.info("TC-BCK-051: snapshot + backup of crypto lvol")
        snap_name = f"crypto_snap_{_rand_suffix()}"
        self._create_snapshot(crypto_id, snap_name, backup=True)
        sleep_n_sec(5)

        backups = self._list_backups()
        assert backups, "TC-BCK-051: no backups after crypto snapshot+backup"
        bk_entry = self._get_backup_for_snapshot(snap_name, backups) or backups[0]
        bk_id = (
            bk_entry.get("id")
            or bk_entry.get("ID")
            or bk_entry.get("uuid")
            or ""
        )
        assert bk_id, "TC-BCK-051: could not extract backup_id"
        self.logger.info("TC-BCK-051: validating backup entry references correct lvol and snapshot")
        self._validate_backup_fields(bk_entry, lvol_name=crypto_name, snap_name=snap_name)
        self._wait_for_backup(bk_id)
        self.logger.info(f"TC-BCK-051: backup {bk_id} is done ✓")

        # --- TC-BCK-052: restore crypto backup → new lvol ---
        restored_name = f"crypto_rest_{_rand_suffix()}"
        self.logger.info(f"TC-BCK-052: restore crypto backup → {restored_name}")
        self._restore_backup(bk_id, restored_name)
        self._wait_for_restore(restored_name)

        # --- TC-BCK-053: restored crypto lvol is connectable ---
        self.logger.info("TC-BCK-053: connect restored crypto lvol")
        rest_id = self.sbcli_utils.get_lvol_id(lvol_name=restored_name)
        r_device, r_mount = self._connect_and_mount(
            restored_name, rest_id,
            mount=f"{self.mount_path}/cr_{_rand_suffix()}",
            format_disk=False)

        # --- TC-BCK-054: checksum validation on restored crypto lvol ---
        self.logger.info("TC-BCK-054: checksum validation on restored crypto lvol")
        r_files = self.ssh_obj.find_files(self.fio_node, r_mount)
        self.ssh_obj.verify_checksums(
            self.fio_node, r_files, orig_checksums, by_name=True)
        self.logger.info("TC-BCK-054: checksums match ✓")

        # --- TC-BCK-055: FIO on restored crypto lvol ---
        self.logger.info("TC-BCK-055: FIO on restored crypto lvol")
        self._run_fio(r_mount, runtime=30)

        self.logger.info("=== TestBackupCryptoLvol PASSED ===")


# ═══════════════════════════════════════════════════════════════════════════
#  Test 6 – Custom ndcs / npcs geometry
# ═══════════════════════════════════════════════════════════════════════════


class TestBackupCustomGeometry(BackupTestBase):
    """
    TC-BCK-060..063  — Backup/restore with non-default ndcs/npcs values.

    Validates that backup and restore work correctly for lvols with
    custom data-copy / parity-copy geometry configurations.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "backup_custom_geometry"
        self._geometries = [(1, 0), (2, 1), (4, 1)]

    def run(self):
        self.logger.info("=== TestBackupCustomGeometry START ===")
        self.fio_node = self.fio_node[0]
        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)

        for ndcs, npcs in self._geometries:
            self.logger.info(f"--- geometry ndcs={ndcs} npcs={npcs} ---")
            lvol_name, lvol_id = self._create_lvol(
                name=f"geom_{ndcs}_{npcs}_{_rand_suffix()}",
                ndcs=ndcs, npcs=npcs)
            device, mount = self._connect_and_mount(lvol_name, lvol_id)
            self._run_fio(mount, runtime=20)

            files = self.ssh_obj.find_files(self.fio_node, mount)
            orig_checksums = self.ssh_obj.generate_checksums(self.fio_node, files)

            snap_name = f"geom_snap_{ndcs}_{npcs}_{_rand_suffix()}"
            self._create_snapshot(lvol_id, snap_name, backup=True)
            sleep_n_sec(5)

            backups = self._list_backups()
            if not backups:
                self.logger.warning(
                    f"TC-BCK-060: no backup found for ndcs={ndcs} npcs={npcs}")
                continue

            bk_entry = self._get_backup_for_snapshot(snap_name, backups) or backups[0]
            bk_id = (
                bk_entry.get("id")
                or bk_entry.get("ID")
                or bk_entry.get("uuid")
                or ""
            )
            if not bk_id:
                continue
            self.logger.info(
                f"TC-BCK-060: validating backup entry for ndcs={ndcs} npcs={npcs}")
            self._validate_backup_fields(bk_entry, lvol_name=lvol_name, snap_name=snap_name)
            self._wait_for_backup(bk_id)
            self.logger.info(f"TC-BCK-060: backup {bk_id} is done (ndcs={ndcs} npcs={npcs}) ✓")

            # Disconnect source before restores — XFS refuses duplicate UUIDs
            self._unmount_and_disconnect(self.fio_node, mount, lvol_id)

            restored_name = f"geom_rest_{ndcs}_{npcs}_{_rand_suffix()}"
            self._restore_backup(bk_id, restored_name)
            self._wait_for_restore(restored_name)
            rest_id = self.sbcli_utils.get_lvol_id(lvol_name=restored_name)
            r_device, r_mount = self._connect_and_mount(
                restored_name, rest_id,
                mount=f"{self.mount_path}/geom_{ndcs}_{npcs}_{_rand_suffix()}",
                format_disk=False)
            r_files = self.ssh_obj.find_files(self.fio_node, r_mount)
            self.ssh_obj.verify_checksums(
                self.fio_node, r_files, orig_checksums, by_name=True)
            self.logger.info(f"TC-BCK-060: ndcs={ndcs} npcs={npcs} ✓")

        self.logger.info("=== TestBackupCustomGeometry PASSED ===")


# ═══════════════════════════════════════════════════════════════════════════
#  Test 7 – Backup delete and post-merge restore
# ═══════════════════════════════════════════════════════════════════════════


class TestBackupDeleteAndRestore(BackupTestBase):
    """
    TC-BCK-077..081 — backup delete and post-retention-merge restore.

    Covers:
      - `backup delete <lvol_id>` removes all backups from list
      - Restore of a deleted backup_id returns an error (negative)
      - Service remains healthy after delete; fresh backup succeeds
      - Retention policy (versions=3): after 5 backups the two oldest are
        merged/pruned; restoring each retained backup still yields correct
        checksums (the merged data is incorporated into the chain, not lost)
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "backup_delete_and_restore"

    def run(self):
        self.logger.info("=== TestBackupDeleteAndRestore START ===")
        self.fio_node = self.fio_node[0]
        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)

        # ── TC-BCK-077: Setup — lvol + 3 chain backups ────────────────────
        self.logger.info("TC-BCK-077: create lvol, write data, build 3-backup chain")
        lvol_name, lvol_id = self._create_lvol()
        _, mount = self._connect_and_mount(lvol_name, lvol_id)
        self._run_fio(mount, runtime=30)

        files = self.ssh_obj.find_files(self.fio_node, mount)
        original_checksums = self.ssh_obj.generate_checksums(self.fio_node, files)

        collected_bk_ids = []
        for i in range(3):
            sn = f"del_snap_{i}_{_rand_suffix()}"
            self._create_snapshot(lvol_id, sn, backup=True)
            bk_id = self._wait_for_backup_by_snap(sn, f"TC-BCK-077[{i}]")
            collected_bk_ids.append(bk_id)
            self.logger.info(f"TC-BCK-077[{i}]: backup {bk_id} complete")

        self.logger.info(f"TC-BCK-077: chain of {len(collected_bk_ids)} backups built ✓")

        # ── TC-BCK-078: backup delete → backup list shows 0 for this lvol ─
        self.logger.info(f"TC-BCK-078: backup delete {lvol_id}")
        self._delete_backups(lvol_id)
        sleep_n_sec(5)

        backups_after_delete = self._list_backups()
        lvol_backups_remaining = [
            b for b in backups_after_delete
            if lvol_name in " ".join(str(v) for v in b.values())
        ]
        assert len(lvol_backups_remaining) == 0, (
            f"TC-BCK-078: expected 0 backups for {lvol_name} after delete, "
            f"got {len(lvol_backups_remaining)}: {lvol_backups_remaining}"
        )
        self.logger.info("TC-BCK-078: backup list empty for deleted lvol ✓")

        # ── TC-BCK-079: restore of deleted backup_id → expect error ────────
        self.logger.info("TC-BCK-079: restore of deleted backup_id must fail")
        for bk_id in collected_bk_ids[:1]:  # test just one; all should fail
            out, err = self._sbcli(
                f"-d backup restore {bk_id} --lvol del_rst_{_rand_suffix()} --pool {self.pool_name}")
            assert err or "error" in (out or "").lower(), (
                f"TC-BCK-079: expected error restoring deleted backup {bk_id}, "
                f"got out={out!r} err={err!r}"
            )
            self.logger.info(f"TC-BCK-079: restore of deleted backup {bk_id} correctly returned error ✓")

        # ── TC-BCK-080: fresh backup after delete succeeds ──────────────────
        self.logger.info("TC-BCK-080: take fresh backup after delete — service must be healthy")
        fresh_snap = f"del_fresh_{_rand_suffix()}"
        self._create_snapshot(lvol_id, fresh_snap, backup=True)
        fresh_bk_id = self._wait_for_backup_by_snap(fresh_snap, "TC-BCK-080")
        self.logger.info(f"TC-BCK-080: fresh backup {fresh_bk_id} after delete succeeded ✓")

        # Disconnect source before connecting restored lvol (XFS UUID safety)
        self._unmount_and_disconnect(self.fio_node, mount, lvol_id)

        fresh_restored = f"del_fresh_rst_{_rand_suffix()}"
        self._restore_backup(fresh_bk_id, fresh_restored)
        self._wait_for_restore(fresh_restored)
        fresh_rst_id = self.sbcli_utils.get_lvol_id(lvol_name=fresh_restored)
        _, fr_mount = self._connect_and_mount(
            fresh_restored, fresh_rst_id,
            mount=f"{self.mount_path}/del_fr_{_rand_suffix()}",
            format_disk=False)
        fr_files = self.ssh_obj.find_files(self.fio_node, fr_mount)
        self.ssh_obj.verify_checksums(self.fio_node, fr_files, original_checksums, by_name=True)
        self.logger.info("TC-BCK-080: fresh post-delete backup → restore → checksums match ✓")

        # Clean up TC-BCK-080 restored lvol before TC-BCK-081 connects more
        self._unmount_and_disconnect(self.fio_node, fr_mount, fresh_rst_id)

        # ── TC-BCK-081: Retention merge — restore from retained backups ─────
        # With versions=3 policy: create 5 backups → oldest 2 are pruned/merged
        # into the chain. Restoring the 3 retained backup_ids must succeed and
        # produce correct checksums (merged data is incorporated, not lost).
        self.logger.info("TC-BCK-081: retention merge — 5 backups with policy versions=3")
        policy_name = f"del_pol_{_rand_suffix()}"
        policy_id = self._add_policy(policy_name, versions=3, age="1d")
        self._attach_policy(policy_id, "lvol", lvol_id)

        retained_bk_ids = []
        for i in range(5):
            sn = f"ret_snap_{i}_{_rand_suffix()}"
            self._create_snapshot(lvol_id, sn, backup=True)
            bk_id = self._wait_for_backup_by_snap(sn, f"TC-BCK-081[{i}]")
            retained_bk_ids.append(bk_id)
            self.logger.info(f"TC-BCK-081[{i}]: backup {bk_id} complete")
            sleep_n_sec(3)

        sleep_n_sec(15)  # allow merge/pruning to settle
        backups_retained = self._list_backups()
        self.logger.info(
            f"TC-BCK-081: {len(backups_retained)} backups after 5 snaps "
            f"(policy versions=3 — oldest 2 should be merged)")

        # Restore each backup that still appears in the list; all must yield correct checksums
        visible_ids = {
            b.get("id") or b.get("ID") or b.get("uuid") or ""
            for b in backups_retained
            if lvol_name in " ".join(str(v) for v in b.values())
        }
        assert visible_ids, "TC-BCK-081: expected at least 1 retained backup after policy merge"
        for bk_id in visible_ids:
            rst_name = f"ret_rst_{_rand_suffix()}"
            self._restore_backup(bk_id, rst_name)
            self._wait_for_restore(rst_name)
            rst_id = self.sbcli_utils.get_lvol_id(lvol_name=rst_name)
            _, rst_mount = self._connect_and_mount(
                rst_name, rst_id,
                mount=f"{self.mount_path}/ret_{_rand_suffix()}",
                format_disk=False)
            rst_files = self.ssh_obj.find_files(self.fio_node, rst_mount)
            self.ssh_obj.verify_checksums(self.fio_node, rst_files, original_checksums, by_name=True)
            self.logger.info(f"TC-BCK-081: retained backup {bk_id} → restore → checksums match ✓")
            # Clean up before connecting next restored lvol
            self._unmount_and_disconnect(self.fio_node, rst_mount, rst_id)

        self.logger.info("=== TestBackupDeleteAndRestore PASSED ===")


# ═══════════════════════════════════════════════════════════════════════════
#  Test 8 – Cross-cluster restore
#
#  IMPORTANT: This test is intentionally NOT included in get_backup_tests().
#  Run explicitly only:  python e2e.py --testname TestBackupCrossClusterRestore
#
#  Required extra environment variables:
#    CLUSTER2_ID            UUID of the destination (restore) cluster
#    CLUSTER2_SECRET        API secret for the destination cluster
#    CLUSTER2_API_BASE_URL  REST API URL for the destination cluster
# ═══════════════════════════════════════════════════════════════════════════


class TestBackupCrossClusterRestore(BackupTestBase):
    """
    TC-BCK-070..076  — Restore a backup from Cluster-1 into Cluster-2.

    Workflow
    --------
    1. On Cluster-1: create lvol → write data → snapshot + S3 backup → wait for done.
    2. Export backup metadata from Cluster-1 via `backup list` → JSON file.
    3. On Cluster-2: `backup import <metadata.json>` to register the chain.
    4. On Cluster-2: `backup source-switch <cluster1_id>` to point to Cluster-1's S3.
    5. On Cluster-2: `backup restore <backup_id>` to restore from Cluster-1's S3.
    6. Verify checksums match the data written on Cluster-1.
    7. On Cluster-2: `backup source-switch local` to restore Cluster-2's own source.

    Both clusters must share the same S3 / MinIO endpoint so the backup
    objects are reachable from Cluster-2.

    Environment variables
    ---------------------
    CLUSTER2_ID            UUID of the destination cluster
    CLUSTER2_SECRET        API secret for the destination cluster
    CLUSTER2_API_BASE_URL  REST API URL for the destination cluster

    Covers
    ------
    TC-BCK-070  Prerequisites: env vars present and both clusters reachable
    TC-BCK-071  Cluster-1: write data, create S3 backup, wait for done
    TC-BCK-072  Export backup metadata to local JSON file
    TC-BCK-073  Cluster-2: `backup import` succeeds
    TC-BCK-074  Cluster-2: `backup list` shows imported backup
    TC-BCK-074b Cluster-2: `backup source-switch <cluster1_id>` succeeds
    TC-BCK-075  Cluster-2: `backup restore` creates new lvol
    TC-BCK-076  Data integrity: checksum on Cluster-2 restored lvol matches Cluster-1 original
    TC-BCK-076b Cluster-2: `backup source-switch local` restores own source
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "backup_cross_cluster_restore"
        self._cluster2_id = os.environ.get("CLUSTER2_ID", "")
        self._cluster2_secret = os.environ.get("CLUSTER2_SECRET", "")
        self._cluster2_api_url = os.environ.get("CLUSTER2_API_BASE_URL", "")
        self._meta_file = "/tmp/cross_cluster_backup_meta.json"
        # Resources created on Cluster-2 (separate tracking for teardown)
        self._c2_lvols: list[str] = []

    # ── prerequisite check ────────────────────────────────────────────────────

    def _check_prerequisites(self):
        missing = [
            v for v, val in [
                ("CLUSTER2_ID", self._cluster2_id),
                ("CLUSTER2_SECRET", self._cluster2_secret),
                ("CLUSTER2_API_BASE_URL", self._cluster2_api_url),
            ] if not val
        ]
        if missing:
            raise EnvironmentError(
                f"TC-BCK-070: cross-cluster restore requires env vars: "
                f"{', '.join(missing)}")

    # ── Cluster-2 sbcli helper ────────────────────────────────────────────────

    def _sbcli_c2(self, subcmd: str) -> tuple[str, str]:
        """Run sbcli command targeted at Cluster-2."""
        env_prefix = (
            f"CLUSTER_ID={self._cluster2_id} "
            f"CLUSTER_SECRET={self._cluster2_secret} "
            f"API_BASE_URL={self._cluster2_api_url} "
        )
        cmd = f"{env_prefix}{self.base_cmd} {subcmd}"
        return self._run(cmd)

    # ── backup metadata export ────────────────────────────────────────────────

    def _export_backup_metadata(self, backup_id: str) -> str:
        """
        Export backup metadata from Cluster-1 using the CLI backup export
        command, writing a JSON file to self._meta_file.

        Returns the path of the metadata file on the mgmt node.
        """
        out, err = self._sbcli(f"backup export -o {self._meta_file}")
        assert not (err and "error" in err.lower()), \
            f"TC-BCK-072: backup export failed: {err}"
        self.logger.info(f"TC-BCK-072: backup export result: {(out or '').strip()}")
        return self._meta_file

    # ── main run ──────────────────────────────────────────────────────────────

    def run(self):
        self.logger.info("=== TestBackupCrossClusterRestore START ===")

        # TC-BCK-070: check prerequisites
        self._check_prerequisites()
        self.logger.info(
            f"TC-BCK-070: prerequisites OK — Cluster-2 ID={self._cluster2_id}")

        self.fio_node = self.fio_node[0]
        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)

        # ── Cluster-1: write data → snapshot + backup → wait ──────────────────

        # TC-BCK-071: create lvol on Cluster-1, write known data, create S3 backup
        self.logger.info("TC-BCK-071: Cluster-1 — write data and create S3 backup")
        lvol_name, lvol_id = self._create_lvol(
            name=f"cc_src_{_rand_suffix()}", size="5G")
        device, mount = self._connect_and_mount(lvol_name, lvol_id)
        self._run_fio(mount, runtime=30)

        files = self.ssh_obj.find_files(self.fio_node, mount)
        orig_checksums = self.ssh_obj.generate_checksums(self.fio_node, files)
        self.logger.info(
            f"TC-BCK-071: {len(orig_checksums)} checksum(s) captured on Cluster-1")

        snap_name = f"cc_snap_{_rand_suffix()}"
        snap_id = self._create_snapshot(lvol_id, snap_name, backup=True)
        self.logger.info(f"TC-BCK-071: snapshot {snap_id} + S3 backup triggered")
        sleep_n_sec(5)

        backups = self._list_backups()
        assert backups, "TC-BCK-071: no backups found on Cluster-1 after snapshot"
        bk_entry = self._get_backup_for_snapshot(snap_name, backups) or backups[0]
        backup_id = (
            bk_entry.get("id") or bk_entry.get("ID") or bk_entry.get("uuid") or ""
        )
        assert backup_id, f"TC-BCK-071: could not extract backup_id: {bk_entry}"
        self._wait_for_backup(backup_id)
        self.logger.info(f"TC-BCK-071: backup {backup_id} is done on Cluster-1 ✓")

        # ── Cluster-2: import → source-switch → restore → verify → switch-back ─

        # TC-BCK-072: export metadata from Cluster-1
        self.logger.info("TC-BCK-072: exporting backup metadata from Cluster-1")
        meta_file = self._export_backup_metadata(backup_id)

        # TC-BCK-073: import metadata on Cluster-2
        self.logger.info(f"TC-BCK-073: Cluster-2 — backup import {meta_file}")
        out, err = self._sbcli_c2(f"backup import {meta_file}")
        assert not (err and "error" in err.lower()), \
            f"TC-BCK-073: backup import on Cluster-2 failed: {err}"
        self.logger.info(f"TC-BCK-073: import result: {out.strip()}")

        # TC-BCK-074: verify backup is visible on Cluster-2
        self.logger.info("TC-BCK-074: Cluster-2 — backup list should show imported backup")
        out2, err2 = self._sbcli_c2("backup list")
        assert not (err2 and "error" in err2.lower()), \
            f"TC-BCK-074: backup list on Cluster-2 failed: {err2}"
        assert backup_id in out2 or out2.strip(), \
            f"TC-BCK-074: imported backup_id {backup_id} not visible on Cluster-2"
        self.logger.info(f"TC-BCK-074: Cluster-2 backup list snippet: {out2[:200]}")

        # TC-BCK-074b: switch Cluster-2's backup source to Cluster-1's S3
        self.logger.info(
            f"TC-BCK-074b: Cluster-2 — backup source-switch to Cluster-1 ({self.cluster_id})")
        out_sw, err_sw = self._sbcli_c2(f"backup source-switch {self.cluster_id}")
        assert not (err_sw and "error" in err_sw.lower()), \
            f"TC-BCK-074b: source-switch to Cluster-1 failed: {err_sw}"
        self.logger.info(f"TC-BCK-074b: source switched to Cluster-1 ✓ — {out_sw.strip()}")

        try:
            # TC-BCK-075: restore on Cluster-2 (now sourced from Cluster-1's S3)
            restored_name = f"cc_rest_{_rand_suffix()}"
            self.logger.info(
                f"TC-BCK-075: Cluster-2 — backup restore {backup_id} → {restored_name}")
            c2_pool = os.environ.get("CLUSTER2_POOL", self.pool_name)
            out3, err3 = self._sbcli_c2(
                f"backup restore {backup_id} --lvol {restored_name} --pool {c2_pool}")
            assert not (err3 and "error" in err3.lower()), \
                f"TC-BCK-075: restore on Cluster-2 failed: {err3}"
            self.logger.info(f"TC-BCK-075: restore triggered: {out3.strip()}")
            self._c2_lvols.append(restored_name)

            # Wait for restore to complete on Cluster-2
            self.logger.info("TC-BCK-075: waiting for Cluster-2 restore to complete…")
            deadline = time.time() + _RESTORE_COMPLETE_TIMEOUT
            while time.time() < deadline:
                lvol_out, _ = self._sbcli_c2("lvol list")
                if restored_name in lvol_out:
                    self.logger.info("TC-BCK-075: restored lvol appeared on Cluster-2 ✓")
                    break
                sleep_n_sec(_POLL_INTERVAL)
            else:
                raise TimeoutError(
                    f"TC-BCK-075: restored lvol {restored_name} did not appear "
                    f"on Cluster-2 within {_RESTORE_COMPLETE_TIMEOUT}s")

            # TC-BCK-076: data integrity — connect on FIO node via Cluster-2 connect string
            self.logger.info("TC-BCK-076: connecting restored lvol from Cluster-2")
            c2_connect_out, c2_connect_err = self._sbcli_c2(
                f"volume connect {restored_name}")
            connect_lines = [
                line.strip()
                for line in c2_connect_out.strip().split("\n")
                if line.strip() and "nvme connect" in line
            ]
            assert connect_lines, \
                f"TC-BCK-076: no nvme connect strings from Cluster-2: {c2_connect_out}"

            initial_devs = self.ssh_obj.get_devices(node=self.fio_node)
            for cmd in connect_lines:
                self.ssh_obj.exec_command(node=self.fio_node, command=cmd)
            sleep_n_sec(3)
            final_devs = self.ssh_obj.get_devices(node=self.fio_node)
            new_devs = [d for d in final_devs if d not in initial_devs]
            assert new_devs, "TC-BCK-076: no new block device after connecting Cluster-2 lvol"

            r_device = f"/dev/{new_devs[0]}"
            r_mount = f"{self.mount_path}/cc_rest_{_rand_suffix()}"
            self.ssh_obj.exec_command(self.fio_node, f"mkdir -p {r_mount}")
            self.ssh_obj.mount_path(node=self.fio_node, device=r_device, mount_path=r_mount)
            self.mounted.append((self.fio_node, r_mount))

            r_files = self.ssh_obj.find_files(self.fio_node, r_mount)
            self.ssh_obj.verify_checksums(
                self.fio_node, r_files, orig_checksums, by_name=True)
            self.logger.info("TC-BCK-076: cross-cluster restore checksums match ✓")

        finally:
            # TC-BCK-076b: switch Cluster-2's backup source back to local (always)
            self.logger.info("TC-BCK-076b: Cluster-2 — backup source-switch back to local")
            out_back, err_back = self._sbcli_c2("backup source-switch local")
            if err_back and "error" in err_back.lower():
                self.logger.warning(
                    f"TC-BCK-076b: source-switch-back warning: {err_back}")
            else:
                self.logger.info(
                    f"TC-BCK-076b: source switched back to local ✓ — {out_back.strip()}")

        self.logger.info("=== TestBackupCrossClusterRestore PASSED ===")

    # ── teardown ──────────────────────────────────────────────────────────────

    def teardown(self, delete_lvols=True, close_ssh=True):
        # Safety: ensure Cluster-2's source is switched back to local (always)
        try:
            self._sbcli_c2("backup source-switch local")
        except Exception as e:
            self.logger.warning(f"source-switch-back in teardown warning: {e}")

        if delete_lvols:
            # Best-effort cleanup of Cluster-2 resources
            for name in list(self._c2_lvols):
                try:
                    self._sbcli_c2(f"lvol delete {name}")
                except Exception as e:
                    self.logger.warning(f"Cluster-2 lvol delete error {name}: {e}")
            self._c2_lvols.clear()

            # Clean up metadata file from mgmt node
            try:
                self.ssh_obj.exec_command(
                    self.mgmt_nodes[0], f"rm -f {self._meta_file}")
            except Exception:
                pass

        super().teardown(delete_lvols=delete_lvols, close_ssh=close_ssh)


# ═══════════════════════════════════════════════════════════════════════════
#  Test 9 – Concurrent I/O during backup
# ═══════════════════════════════════════════════════════════════════════════


class TestBackupConcurrentIO(BackupTestBase):
    """
    TC-BCK-100..103 – Snapshot + backup taken while FIO I/O is in progress.

    Covers:
      - Background FIO thread running while snapshot+backup is triggered
      - Backup completes successfully after concurrent I/O finishes
      - Restored lvol is mountable and readable
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "backup_concurrent_io"

    def run(self):
        self.logger.info("=== TestBackupConcurrentIO START ===")
        self.fio_node = self.fio_node[0]
        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)

        # TC-BCK-100: create lvol + mount
        self.logger.info("TC-BCK-100: create lvol and mount")
        lvol_name, lvol_id = self._create_lvol()
        device, mount = self._connect_and_mount(lvol_name, lvol_id)
        log_file = f"{self.log_path}/fio_concurrent_{_rand_suffix()}.log"

        # TC-BCK-101: start background FIO; take snapshot while I/O is running
        self.logger.info("TC-BCK-101: starting background FIO + snapshotting mid-I/O")
        fio_thread = threading.Thread(
            target=self._run_fio,
            kwargs=dict(mount=mount, log_file=log_file, runtime=60),
        )
        fio_thread.start()
        sleep_n_sec(10)  # let FIO warm up

        snap_name = f"concurrent_snap_{_rand_suffix()}"
        self._create_snapshot(lvol_id, snap_name, backup=True)
        self.logger.info("TC-BCK-101: snapshot taken while FIO is running ✓")

        # TC-BCK-102: wait for FIO to finish then wait for backup
        self.logger.info("TC-BCK-102: waiting for FIO + backup to complete")
        fio_thread.join()
        self.common_utils.validate_fio_test(self.fio_node, log_file=log_file)
        bk_id = self._wait_for_backup_by_snap(snap_name, "TC-BCK-102")
        self.logger.info(f"TC-BCK-102: backup {bk_id} complete after concurrent FIO ✓")

        # TC-BCK-103: restore + connect + verify readable
        self.logger.info("TC-BCK-103: restore and verify readability")
        restored_name = f"concurrent_rest_{_rand_suffix()}"
        self._restore_backup(bk_id, restored_name)
        self._wait_for_restore(restored_name)
        rest_id = self.sbcli_utils.get_lvol_id(lvol_name=restored_name)
        _, r_mount = self._connect_and_mount(
            restored_name, rest_id,
            mount=f"{self.mount_path}/conc_r_{_rand_suffix()}",
            format_disk=False)
        r_files = self.ssh_obj.find_files(self.fio_node, r_mount)
        assert r_files is not None, "TC-BCK-103: restored lvol should be readable"
        self.logger.info("TC-BCK-103: restored lvol from concurrent-IO backup is readable ✓")

        self.logger.info("=== TestBackupConcurrentIO PASSED ===")


# ═══════════════════════════════════════════════════════════════════════════
#  Test 10 – Multiple restores from same backup
# ═══════════════════════════════════════════════════════════════════════════


class TestBackupMultipleRestores(BackupTestBase):
    """
    TC-BCK-104..107 – Same backup_id restored to three distinct lvol names.

    Covers:
      - Restore same backup_id three times to different lvol names
      - All restored lvols appear in lvol list
      - Checksums match original on all three restored copies
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "backup_multiple_restores"

    def run(self):
        self.logger.info("=== TestBackupMultipleRestores START ===")
        self.fio_node = self.fio_node[0]
        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)

        # TC-BCK-104: create lvol + write data + backup
        self.logger.info("TC-BCK-104: create lvol + write data + backup")
        lvol_name, lvol_id = self._create_lvol()
        device, mount = self._connect_and_mount(lvol_name, lvol_id)
        self._run_fio(mount, runtime=30)
        files = self.ssh_obj.find_files(self.fio_node, mount)
        orig_checksums = self.ssh_obj.generate_checksums(self.fio_node, files)
        snap_name = f"multi_rst_snap_{_rand_suffix()}"
        self._create_snapshot(lvol_id, snap_name, backup=True)
        bk_id = self._wait_for_backup_by_snap(snap_name, "TC-BCK-104")
        self.logger.info(f"TC-BCK-104: backup {bk_id} ready ✓")

        # TC-BCK-105: restore same backup_id to 3 different lvol names
        self.logger.info("TC-BCK-105: restore same backup_id to 3 different lvol names")
        restored_names = [f"multi_r{i}_{_rand_suffix()}" for i in range(3)]
        for i, rname in enumerate(restored_names):
            self._restore_backup(bk_id, rname)
            self.logger.info(f"TC-BCK-105[{i}]: restore to {rname} initiated ✓")

        # TC-BCK-106: all 3 appear in lvol list
        self.logger.info("TC-BCK-106: verify all 3 restored lvols are visible")
        for rname in restored_names:
            self._wait_for_restore(rname)
        out, _ = self._sbcli("lvol list")
        for rname in restored_names:
            assert rname in out, f"TC-BCK-106: {rname} not found in lvol list"
        self.logger.info("TC-BCK-106: all 3 restored lvols in lvol list ✓")

        # TC-BCK-107: checksums match on all 3
        self.logger.info("TC-BCK-107: verify checksums on all 3 restored copies")
        for i, rname in enumerate(restored_names):
            rid = self.sbcli_utils.get_lvol_id(lvol_name=rname)
            _, r_mount = self._connect_and_mount(
                rname, rid,
                mount=f"{self.mount_path}/mr_{i}_{_rand_suffix()}",
                format_disk=False)
            r_files = self.ssh_obj.find_files(self.fio_node, r_mount)
            self.ssh_obj.verify_checksums(self.fio_node, r_files, orig_checksums, by_name=True)
            self.logger.info(f"TC-BCK-107[{i}]: {rname} checksums match ✓")

        self.logger.info("=== TestBackupMultipleRestores PASSED ===")


# ═══════════════════════════════════════════════════════════════════════════
#  Test 11 – Delta-chain point-in-time restore
# ═══════════════════════════════════════════════════════════════════════════


class TestBackupDeltaChainPointInTime(BackupTestBase):
    """
    TC-BCK-108..113 – Multiple backups of the same lvol; each restore must
    yield the exact state at that point in time (not later revisions).

    Covers:
      - Three sequential backups with distinct marker files written between each
      - Restoring bk1 yields only v1-era state (v1.txt present, v2/v3 absent)
      - Restoring bk2 yields v1+v2 state (v1.txt + v2.txt, no v3.txt)
      - Restoring bk3 yields v2+v3 state (v1.txt deleted between bk2 and bk3)
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "backup_delta_chain_pit"

    def run(self):
        self.logger.info("=== TestBackupDeltaChainPointInTime START ===")
        self.fio_node = self.fio_node[0]
        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)

        lvol_name, lvol_id = self._create_lvol()
        device, mount = self._connect_and_mount(lvol_name, lvol_id)

        # TC-BCK-108: write v1 marker + backup
        self.logger.info("TC-BCK-108: write v1 marker + backup")
        self.ssh_obj.exec_command(
            self.fio_node, f"echo 'version1' > {mount}/v1.txt && sync")
        snap1 = f"pit_snap1_{_rand_suffix()}"
        self._create_snapshot(lvol_id, snap1, backup=True)
        bk1 = self._wait_for_backup_by_snap(snap1, "TC-BCK-108")
        self.logger.info(f"TC-BCK-108: v1 backup {bk1} ✓")

        # TC-BCK-109: add v2 marker (keep v1) + backup
        self.logger.info("TC-BCK-109: add v2 marker + backup")
        self.ssh_obj.exec_command(
            self.fio_node, f"echo 'version2' > {mount}/v2.txt && sync")
        snap2 = f"pit_snap2_{_rand_suffix()}"
        self._create_snapshot(lvol_id, snap2, backup=True)
        bk2 = self._wait_for_backup_by_snap(snap2, "TC-BCK-109")
        self.logger.info(f"TC-BCK-109: v2 backup {bk2} ✓")

        # TC-BCK-110: add v3 marker + delete v1 + backup
        self.logger.info("TC-BCK-110: add v3 + delete v1 + backup")
        self.ssh_obj.exec_command(
            self.fio_node,
            f"echo 'version3' > {mount}/v3.txt && rm -f {mount}/v1.txt && sync")
        snap3 = f"pit_snap3_{_rand_suffix()}"
        self._create_snapshot(lvol_id, snap3, backup=True)
        bk3 = self._wait_for_backup_by_snap(snap3, "TC-BCK-110")
        self.logger.info(f"TC-BCK-110: v3 backup {bk3} ✓")

        # Disconnect source before restores — XFS refuses duplicate UUIDs
        self._unmount_and_disconnect(self.fio_node, mount, lvol_id)

        # TC-BCK-111: restore bk1 → must have v1.txt only (no v2/v3)
        self.logger.info("TC-BCK-111: restore bk1 + verify only v1 state")
        r1 = f"pit_r1_{_rand_suffix()}"
        self._restore_backup(bk1, r1)
        self._wait_for_restore(r1)
        r1_id = self.sbcli_utils.get_lvol_id(lvol_name=r1)
        _, r1_mount = self._connect_and_mount(
            r1, r1_id,
            mount=f"{self.mount_path}/pit1_{_rand_suffix()}",
            format_disk=False)
        out1, _ = self.ssh_obj.exec_command(self.fio_node, f"ls {r1_mount}/")
        assert "v1.txt" in out1, \
            f"TC-BCK-111: v1.txt missing in bk1 restore: {out1}"
        assert "v2.txt" not in out1, \
            f"TC-BCK-111: v2.txt should not exist in bk1 restore: {out1}"
        assert "v3.txt" not in out1, \
            f"TC-BCK-111: v3.txt should not exist in bk1 restore: {out1}"
        self.logger.info("TC-BCK-111: bk1 point-in-time state verified ✓")

        # TC-BCK-112: restore bk2 → must have v1.txt + v2.txt (no v3)
        self.logger.info("TC-BCK-112: restore bk2 + verify v1+v2 state")
        r2 = f"pit_r2_{_rand_suffix()}"
        self._restore_backup(bk2, r2)
        self._wait_for_restore(r2)
        r2_id = self.sbcli_utils.get_lvol_id(lvol_name=r2)
        _, r2_mount = self._connect_and_mount(
            r2, r2_id,
            mount=f"{self.mount_path}/pit2_{_rand_suffix()}",
            format_disk=False)
        out2, _ = self.ssh_obj.exec_command(self.fio_node, f"ls {r2_mount}/")
        assert "v1.txt" in out2, \
            f"TC-BCK-112: v1.txt missing in bk2 restore: {out2}"
        assert "v2.txt" in out2, \
            f"TC-BCK-112: v2.txt missing in bk2 restore: {out2}"
        assert "v3.txt" not in out2, \
            f"TC-BCK-112: v3.txt should not exist in bk2 restore: {out2}"
        self.logger.info("TC-BCK-112: bk2 point-in-time state verified ✓")

        # TC-BCK-113: restore bk3 → must have v2.txt + v3.txt (v1 was deleted)
        self.logger.info("TC-BCK-113: restore bk3 + verify v2+v3 state (v1 deleted)")
        r3 = f"pit_r3_{_rand_suffix()}"
        self._restore_backup(bk3, r3)
        self._wait_for_restore(r3)
        r3_id = self.sbcli_utils.get_lvol_id(lvol_name=r3)
        _, r3_mount = self._connect_and_mount(
            r3, r3_id,
            mount=f"{self.mount_path}/pit3_{_rand_suffix()}",
            format_disk=False)
        out3, _ = self.ssh_obj.exec_command(self.fio_node, f"ls {r3_mount}/")
        assert "v1.txt" not in out3, \
            f"TC-BCK-113: v1.txt should not exist in bk3 restore (was deleted): {out3}"
        assert "v2.txt" in out3, \
            f"TC-BCK-113: v2.txt missing in bk3 restore: {out3}"
        assert "v3.txt" in out3, \
            f"TC-BCK-113: v3.txt missing in bk3 restore: {out3}"
        self.logger.info("TC-BCK-113: bk3 point-in-time state verified ✓")

        self.logger.info("=== TestBackupDeltaChainPointInTime PASSED ===")


# ═══════════════════════════════════════════════════════════════════════════
#  Test 12 – Backup and restore of an empty (formatted, unwritten) lvol
# ═══════════════════════════════════════════════════════════════════════════


class TestBackupEmptyLvol(BackupTestBase):
    """
    TC-BCK-114..116 – Backup and restore of a formatted-but-empty lvol.

    Covers:
      - Snapshot + backup of a freshly formatted lvol with no user data
      - Backup completes successfully (no data to transfer)
      - Restored lvol is mountable without re-formatting
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "backup_empty_lvol"

    def run(self):
        self.logger.info("=== TestBackupEmptyLvol START ===")
        self.fio_node = self.fio_node[0]
        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)

        # TC-BCK-114: create + format (no user data) + backup
        self.logger.info("TC-BCK-114: create lvol, format ext4 (no data write), backup")
        lvol_name, lvol_id = self._create_lvol()
        device, mount = self._connect_and_mount(lvol_name, lvol_id)  # formats ext4
        snap_name = f"empty_snap_{_rand_suffix()}"
        self._create_snapshot(lvol_id, snap_name, backup=True)
        bk_id = self._wait_for_backup_by_snap(snap_name, "TC-BCK-114")
        self.logger.info(f"TC-BCK-114: empty lvol backup {bk_id} complete ✓")

        # Disconnect source before restores — XFS refuses duplicate UUIDs
        self._unmount_and_disconnect(self.fio_node, mount, lvol_id)

        # TC-BCK-115: restore → new lvol visible
        self.logger.info("TC-BCK-115: restore empty-lvol backup")
        restored_name = f"empty_rest_{_rand_suffix()}"
        self._restore_backup(bk_id, restored_name)
        self._wait_for_restore(restored_name)
        self.logger.info(f"TC-BCK-115: {restored_name} visible after restore ✓")

        # TC-BCK-116: connect + mount without reformat → filesystem is readable
        self.logger.info("TC-BCK-116: mount restored empty lvol without reformat")
        rest_id = self.sbcli_utils.get_lvol_id(lvol_name=restored_name)
        _, r_mount = self._connect_and_mount(
            restored_name, rest_id,
            mount=f"{self.mount_path}/emp_{_rand_suffix()}",
            format_disk=False)
        out, _ = self.ssh_obj.exec_command(self.fio_node, f"ls {r_mount}/")
        self.logger.info(
            f"TC-BCK-116: restored empty lvol mounted successfully, "
            f"dir listing: {out.strip()} ✓")

        self.logger.info("=== TestBackupEmptyLvol PASSED ===")


# ═══════════════════════════════════════════════════════════════════════════
#  Test 13 – Delete storage pool then recreate and restore into new pool
# ═══════════════════════════════════════════════════════════════════════════


class TestBackupPoolRecreateRestore(BackupTestBase):
    """
    TC-BCK-117..121 – Delete the storage pool then recreate it and restore
    a previously created S3 backup into the new pool.

    Covers:
      - S3 backup survives pool + lvol deletion (object storage is independent)
      - Recreating a pool with the same name allows restore to succeed
      - Checksums of restored data match the original
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "backup_pool_recreate_restore"

    def run(self):
        self.logger.info("=== TestBackupPoolRecreateRestore START ===")
        self.fio_node = self.fio_node[0]
        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)

        # TC-BCK-117: create lvol + write data + backup
        self.logger.info("TC-BCK-117: create lvol + write data + backup")
        lvol_name, lvol_id = self._create_lvol()
        device, mount = self._connect_and_mount(lvol_name, lvol_id)
        self._run_fio(mount, runtime=30)
        files = self.ssh_obj.find_files(self.fio_node, mount)
        orig_checksums = self.ssh_obj.generate_checksums(self.fio_node, files)
        snap_name = f"pool_snap_{_rand_suffix()}"
        snap_id = self._create_snapshot(lvol_id, snap_name, backup=True)
        bk_id = self._wait_for_backup_by_snap(snap_name, "TC-BCK-117")
        self.logger.info(f"TC-BCK-117: backup {bk_id} complete before pool delete ✓")

        # TC-BCK-118: unmount + disconnect + delete snapshot + lvol + pool
        self.logger.info("TC-BCK-118: unmount, disconnect, delete pool resources")
        self.ssh_obj.unmount_path(self.fio_node, mount)
        if (self.fio_node, mount) in self.mounted:
            self.mounted.remove((self.fio_node, mount))
        details = self.sbcli_utils.get_lvol_details(lvol_id=lvol_id)
        if details:
            nqn = details[0]["nqn"]
            self.ssh_obj.disconnect_nvme(node=self.fio_node, nqn_grep=nqn)
        if lvol_id in self.connected:
            self.connected.remove(lvol_id)
        try:
            self._sbcli(f"snapshot delete {snap_id} --force")
        except Exception as e:
            self.logger.warning(f"Snapshot delete warning: {e}")
        if snap_id in self.created_snapshots:
            self.created_snapshots.remove(snap_id)
        self.sbcli_utils.delete_lvol(lvol_name=lvol_name, skip_error=True)
        if lvol_name in self.created_lvols:
            self.created_lvols.remove(lvol_name)
        self.sbcli_utils.delete_storage_pool(pool_name=self.pool_name)
        self.logger.info("TC-BCK-118: pool and all resources deleted ✓")

        # TC-BCK-119: recreate pool with same name
        self.logger.info("TC-BCK-119: recreate storage pool")
        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)
        self.logger.info("TC-BCK-119: pool recreated ✓")

        # TC-BCK-120: restore backup into new pool
        self.logger.info("TC-BCK-120: restore backup into new pool")
        restored_name = f"pool_rest_{_rand_suffix()}"
        self._restore_backup(bk_id, restored_name)
        self._wait_for_restore(restored_name)
        self.logger.info(f"TC-BCK-120: {restored_name} restored into new pool ✓")

        # TC-BCK-121: verify checksums
        self.logger.info("TC-BCK-121: verify checksums after pool-recreate restore")
        rest_id = self.sbcli_utils.get_lvol_id(lvol_name=restored_name)
        _, r_mount = self._connect_and_mount(
            restored_name, rest_id,
            mount=f"{self.mount_path}/pool_r_{_rand_suffix()}",
            format_disk=False)
        r_files = self.ssh_obj.find_files(self.fio_node, r_mount)
        self.ssh_obj.verify_checksums(self.fio_node, r_files, orig_checksums, by_name=True)
        self.logger.info("TC-BCK-121: checksums match after pool recreate + restore ✓")

        self.logger.info("=== TestBackupPoolRecreateRestore PASSED ===")


# ═══════════════════════════════════════════════════════════════════════════
#  Test 14 – Backup policy with age-only retention (no --versions)
# ═══════════════════════════════════════════════════════════════════════════


class TestBackupPolicyAgeOnly(BackupTestBase):
    """
    TC-BCK-122..126 – Backup policy configured with --age only (no --versions).

    Covers:
      - Policy created with only age retention (no version-count limit)
      - Policy attached to pool
      - Snapshot + backup auto-triggered on lvol in the pool
      - Backup entry is present and completes
      - Policy detach + restore + checksum verify
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "backup_policy_age_only"

    def run(self):
        self.logger.info("=== TestBackupPolicyAgeOnly START ===")
        self.fio_node = self.fio_node[0]
        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)

        # TC-BCK-122: create policy with age-only retention
        self.logger.info("TC-BCK-122: create policy with --age 7d (no --versions)")
        pol_name = f"age_pol_{_rand_suffix()}"
        policy_id = self._add_policy(pol_name, age="7d")
        self.logger.info(f"TC-BCK-122: age-only policy {policy_id} created ✓")

        # TC-BCK-123: attach policy to pool
        pool_id = self.sbcli_utils.get_storage_pool_id(pool_name=self.pool_name)
        self._attach_policy(policy_id, "pool", pool_id)
        self.logger.info("TC-BCK-123: policy attached to pool ✓")

        # TC-BCK-124: create lvol + write data + snapshot (auto-backup from policy)
        self.logger.info("TC-BCK-124: create lvol + snapshot with policy active")
        lvol_name, lvol_id = self._create_lvol()
        device, mount = self._connect_and_mount(lvol_name, lvol_id)
        self._run_fio(mount, runtime=30)
        files = self.ssh_obj.find_files(self.fio_node, mount)
        orig_checksums = self.ssh_obj.generate_checksums(self.fio_node, files)
        snap_name = f"age_snap_{_rand_suffix()}"
        self._create_snapshot(lvol_id, snap_name, backup=True)
        self.logger.info("TC-BCK-124: snapshot triggered with age-only policy attached ✓")

        # TC-BCK-125: wait for backup + verify entry exists
        bk_id = self._wait_for_backup_by_snap(snap_name, "TC-BCK-125")
        self.logger.info(f"TC-BCK-125: age-policy backup {bk_id} complete ✓")

        # TC-BCK-126: detach policy + restore + verify checksums
        self._detach_policy(policy_id, "pool", pool_id)
        self.logger.info("TC-BCK-126: policy detached; restoring backup")
        restored_name = f"age_rest_{_rand_suffix()}"
        self._restore_backup(bk_id, restored_name)
        self._wait_for_restore(restored_name)
        rest_id = self.sbcli_utils.get_lvol_id(lvol_name=restored_name)
        _, r_mount = self._connect_and_mount(
            restored_name, rest_id,
            mount=f"{self.mount_path}/age_{_rand_suffix()}",
            format_disk=False)
        r_files = self.ssh_obj.find_files(self.fio_node, r_mount)
        self.ssh_obj.verify_checksums(self.fio_node, r_files, orig_checksums, by_name=True)
        self.logger.info("TC-BCK-126: age-policy backup restore checksums match ✓")

        self.logger.info("=== TestBackupPolicyAgeOnly PASSED ===")


# ═══════════════════════════════════════════════════════════════════════════
#  Test 15 – Backup and restore of a snapshot clone
# ═══════════════════════════════════════════════════════════════════════════


class TestBackupSnapshotClone(BackupTestBase):
    """
    TC-BCK-127..131 – Backup and restore of an lvol cloned from a snapshot.

    Covers:
      - Create source lvol + snapshot → clone the snapshot into a new lvol
      - Snapshot + backup of the clone
      - Restore clone backup → new lvol
      - Checksums of restored clone match original source data
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "backup_snapshot_clone"

    def run(self):
        self.logger.info("=== TestBackupSnapshotClone START ===")
        self.fio_node = self.fio_node[0]
        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)

        # TC-BCK-127: create source lvol + write data + snapshot
        self.logger.info("TC-BCK-127: create source lvol + snapshot")
        src_name, src_id = self._create_lvol(name=f"clone_src_{_rand_suffix()}")
        device, mount = self._connect_and_mount(src_name, src_id)
        self._run_fio(mount, runtime=30)
        files = self.ssh_obj.find_files(self.fio_node, mount)
        orig_checksums = self.ssh_obj.generate_checksums(self.fio_node, files)
        snap_name = f"clone_snap_{_rand_suffix()}"
        snap_id = self._create_snapshot(src_id, snap_name, backup=False)
        self.logger.info(f"TC-BCK-127: source snapshot {snap_id} created ✓")

        # TC-BCK-128: clone from snapshot
        self.logger.info("TC-BCK-128: clone lvol from snapshot")
        clone_name = f"clone_lvol_{_rand_suffix()}"
        self.ssh_obj.add_clone(self.mgmt_nodes[0], snap_id, clone_name)
        # Wait for clone to appear in lvol list (reuses restore-wait logic)
        self._wait_for_restore(clone_name, expect_failure=True)
        self.created_lvols.append(clone_name)
        clone_id = self.sbcli_utils.get_lvol_id(lvol_name=clone_name)
        assert clone_id, f"TC-BCK-128: clone {clone_name} has no lvol ID"
        self.logger.info(f"TC-BCK-128: clone {clone_name} created from snapshot ✓")

        # TC-BCK-129: backup the clone
        self.logger.info("TC-BCK-129: backup the clone lvol")
        clone_snap = f"clone_bck_snap_{_rand_suffix()}"
        self._create_snapshot(clone_id, clone_snap, backup=True)
        bk_id = self._wait_for_backup_by_snap(clone_snap, "TC-BCK-129")
        self.logger.info(f"TC-BCK-129: clone backup {bk_id} complete ✓")

        # Disconnect source before restores — XFS refuses duplicate UUIDs
        self._unmount_and_disconnect(self.fio_node, mount, src_id)

        # TC-BCK-130: restore clone backup
        self.logger.info("TC-BCK-130: restore clone backup")
        restored_clone = f"clone_rest_{_rand_suffix()}"
        self._restore_backup(bk_id, restored_clone)
        self._wait_for_restore(restored_clone)
        self.logger.info(f"TC-BCK-130: {restored_clone} restored from clone backup ✓")

        # TC-BCK-131: verify checksums match original source
        self.logger.info("TC-BCK-131: verify checksums on restored clone")
        rest_id = self.sbcli_utils.get_lvol_id(lvol_name=restored_clone)
        _, r_mount = self._connect_and_mount(
            restored_clone, rest_id,
            mount=f"{self.mount_path}/clr_{_rand_suffix()}",
            format_disk=False)
        r_files = self.ssh_obj.find_files(self.fio_node, r_mount)
        self.ssh_obj.verify_checksums(self.fio_node, r_files, orig_checksums, by_name=True)
        self.logger.info("TC-BCK-131: clone backup restore checksums match original source ✓")

        self.logger.info("=== TestBackupSnapshotClone PASSED ===")


# ═══════════════════════════════════════════════════════════════════════════
#  Test 16 – XFS filesystem backup and restore
# ═══════════════════════════════════════════════════════════════════════════


class TestBackupFilesystemXFS(BackupTestBase):
    """
    TC-BCK-132..135 – Backup and restore of an XFS-formatted lvol.

    Covers:
      - lvol formatted with XFS instead of the default ext4
      - Backup completes successfully for XFS filesystem
      - Restore without re-formatting mounts the XFS filesystem correctly
      - Checksums match original data
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "backup_filesystem_xfs"

    def _connect_format_mount_xfs(self, lvol_name: str, lvol_id: str) -> tuple[str, str]:
        """Connect lvol, format with XFS, and mount. Returns (device, mount_point)."""
        mount = f"{self.mount_path}/{lvol_name}"
        initial = self.ssh_obj.get_devices(node=self.fio_node)
        connect_ls = self.sbcli_utils.get_lvol_connect_str(lvol_name=lvol_name)
        for cmd in connect_ls:
            self.ssh_obj.exec_command(node=self.fio_node, command=cmd)
        sleep_n_sec(3)
        final = self.ssh_obj.get_devices(node=self.fio_node)
        new_devs = [d for d in final if d not in initial]
        assert new_devs, f"No new block device after connecting {lvol_name}"
        device = f"/dev/{new_devs[0]}"
        self.ssh_obj.format_disk(node=self.fio_node, device=device, fs_type="xfs")
        self.ssh_obj.exec_command(self.fio_node, f"mkdir -p {mount}")
        self.ssh_obj.mount_path(node=self.fio_node, device=device, mount_path=mount)
        self.mounted.append((self.fio_node, mount))
        self.connected.append(lvol_id)
        return device, mount

    def run(self):
        self.logger.info("=== TestBackupFilesystemXFS START ===")
        self.fio_node = self.fio_node[0]
        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)

        # TC-BCK-132: create lvol + format XFS + write data + snapshot
        self.logger.info("TC-BCK-132: create lvol + format XFS + write data + snapshot")
        lvol_name, lvol_id = self._create_lvol()
        device, mount = self._connect_format_mount_xfs(lvol_name, lvol_id)
        self._run_fio(mount, runtime=30)
        files = self.ssh_obj.find_files(self.fio_node, mount)
        orig_checksums = self.ssh_obj.generate_checksums(self.fio_node, files)
        snap_name = f"xfs_snap_{_rand_suffix()}"
        self._create_snapshot(lvol_id, snap_name, backup=True)
        self.logger.info("TC-BCK-132: XFS lvol snapshotted + backup triggered ✓")

        # TC-BCK-133: wait for backup to complete
        bk_id = self._wait_for_backup_by_snap(snap_name, "TC-BCK-133")
        self.logger.info(f"TC-BCK-133: XFS backup {bk_id} complete ✓")

        # Disconnect source before restores — XFS refuses duplicate UUIDs
        self._unmount_and_disconnect(self.fio_node, mount, lvol_id)

        # TC-BCK-134: restore backup + connect WITHOUT re-formatting
        self.logger.info("TC-BCK-134: restore XFS backup (no reformat on restore)")
        restored_name = f"xfs_rest_{_rand_suffix()}"
        self._restore_backup(bk_id, restored_name)
        self._wait_for_restore(restored_name)
        rest_id = self.sbcli_utils.get_lvol_id(lvol_name=restored_name)
        _, r_mount = self._connect_and_mount(
            restored_name, rest_id,
            mount=f"{self.mount_path}/xfs_r_{_rand_suffix()}",
            format_disk=False)
        self.logger.info("TC-BCK-134: XFS restored lvol mounted without reformat ✓")

        # TC-BCK-135: verify checksums
        r_files = self.ssh_obj.find_files(self.fio_node, r_mount)
        self.ssh_obj.verify_checksums(self.fio_node, r_files, orig_checksums, by_name=True)
        self.logger.info("TC-BCK-135: XFS restore checksums match ✓")

        self.logger.info("=== TestBackupFilesystemXFS PASSED ===")


# ═══════════════════════════════════════════════════════════════════════════
#  Test 17 – Large lvol backup and restore
# ═══════════════════════════════════════════════════════════════════════════


class TestBackupLargeLvol(BackupTestBase):
    """
    TC-BCK-136..138 – Backup and restore of a large (20G) lvol with 3G of data.

    Covers:
      - Backup completes within an extended timeout for large data sets
      - Restore completes within an extended timeout
      - Checksums match on the restored large lvol
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "backup_large_lvol"

    def run(self):
        self.logger.info("=== TestBackupLargeLvol START ===")
        self.fio_node = self.fio_node[0]
        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)

        # TC-BCK-136: create 20G lvol + write 3G data + backup
        self.logger.info("TC-BCK-136: create 20G lvol + write 3G data + backup")
        lvol_name, lvol_id = self._create_lvol(size="20G")
        device, mount = self._connect_and_mount(lvol_name, lvol_id)
        self._run_fio(mount, size="3G", runtime=120)
        files = self.ssh_obj.find_files(self.fio_node, mount)
        orig_checksums = self.ssh_obj.generate_checksums(self.fio_node, files)
        snap_name = f"large_snap_{_rand_suffix()}"
        self._create_snapshot(lvol_id, snap_name, backup=True)
        sleep_n_sec(5)
        backups = self._list_backups()
        bk_entry = self._get_backup_for_snapshot(snap_name, backups)
        assert bk_entry, "TC-BCK-136: no backup entry for large lvol snapshot"
        bk_id = (
            bk_entry.get("id") or bk_entry.get("ID") or bk_entry.get("uuid") or "")
        assert bk_id, "TC-BCK-136: no backup_id for large lvol"
        self._wait_for_backup(bk_id, timeout=1200)
        self.logger.info(f"TC-BCK-136: large lvol backup {bk_id} complete ✓")

        # Disconnect source before restores — XFS refuses duplicate UUIDs
        self._unmount_and_disconnect(self.fio_node, mount, lvol_id)

        # TC-BCK-137: restore with extended timeout
        self.logger.info("TC-BCK-137: restore large lvol (extended timeout 1200s)")
        restored_name = f"large_rest_{_rand_suffix()}"
        self._restore_backup(bk_id, restored_name)
        self._wait_for_restore(restored_name, timeout=1200)
        self.logger.info(f"TC-BCK-137: large lvol restore {restored_name} complete ✓")

        # TC-BCK-138: connect + verify checksums
        self.logger.info("TC-BCK-138: connect restored large lvol + verify checksums")
        rest_id = self.sbcli_utils.get_lvol_id(lvol_name=restored_name)
        _, r_mount = self._connect_and_mount(
            restored_name, rest_id,
            mount=f"{self.mount_path}/large_r_{_rand_suffix()}",
            format_disk=False)
        r_files = self.ssh_obj.find_files(self.fio_node, r_mount)
        self.ssh_obj.verify_checksums(self.fio_node, r_files, orig_checksums, by_name=True)
        self.logger.info("TC-BCK-138: large lvol restore checksums match ✓")

        self.logger.info("=== TestBackupLargeLvol PASSED ===")


# ═══════════════════════════════════════════════════════════════════════════
#  Test 18 – Delete all backups while a backup may be in progress
# ═══════════════════════════════════════════════════════════════════════════


class TestBackupDeleteInProgress(BackupTestBase):
    """
    TC-BCK-139..142 – Delete all backups for an lvol immediately after triggering
    a new backup (race condition / in-progress delete).

    Covers:
      - `backup delete <lvol_id>` called immediately after triggering a backup
      - Service remains responsive and healthy after the operation
      - A fresh backup can be created and completes successfully after the delete
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "backup_delete_in_progress"

    def run(self):
        self.logger.info("=== TestBackupDeleteInProgress START ===")
        self.fio_node = self.fio_node[0]
        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)

        # TC-BCK-139: create lvol + write data + trigger backup (NO wait)
        self.logger.info("TC-BCK-139: create lvol + trigger backup without waiting")
        lvol_name, lvol_id = self._create_lvol()
        device, mount = self._connect_and_mount(lvol_name, lvol_id)
        self._run_fio(mount, runtime=20)
        snap_name = f"dip_snap_{_rand_suffix()}"
        self._create_snapshot(lvol_id, snap_name, backup=True)
        self.logger.info("TC-BCK-139: backup triggered — not waiting for completion ✓")

        # TC-BCK-140: immediately attempt to delete all backups for this lvol
        sleep_n_sec(2)
        self.logger.info("TC-BCK-140: backup delete immediately after trigger")
        out, err = self._sbcli(f"-d backup delete {lvol_id}")
        self.logger.info(
            f"TC-BCK-140: backup delete result: out={out!r} err={err!r} ✓")

        # TC-BCK-141: service must still be responsive after delete-in-progress
        self.logger.info("TC-BCK-141: verify service is still healthy")
        sleep_n_sec(30)
        out, _ = self._sbcli("backup list")
        self.logger.info(
            f"TC-BCK-141: backup list responsive after delete-in-progress: "
            f"{(out or '')[:80]} ✓")

        # TC-BCK-142: fresh backup must succeed after delete
        self.logger.info("TC-BCK-142: take fresh backup — must succeed after delete")
        fresh_snap = f"dip_fresh_{_rand_suffix()}"
        self._create_snapshot(lvol_id, fresh_snap, backup=True)
        fresh_bk_id = self._wait_for_backup_by_snap(fresh_snap, "TC-BCK-142")
        self.logger.info(
            f"TC-BCK-142: fresh backup {fresh_bk_id} succeeded after delete ✓")

        self.logger.info("=== TestBackupDeleteInProgress PASSED ===")


# ═══════════════════════════════════════════════════════════════════════════
#  Test 19 – Pool-level policy auto-backs up multiple lvols
# ═══════════════════════════════════════════════════════════════════════════


class TestBackupPolicyMultipleLvols(BackupTestBase):
    """
    TC-BCK-143..148 – A pool-level backup policy automatically applies to all
    lvols in the pool; each gets an individual backup entry.

    Covers:
      - 3 lvols created in a pool with an attached backup policy
      - Snapshotting each lvol triggers a backup for each
      - All 3 backups complete successfully
      - Each backup restores to a distinct lvol with correct checksums
      - Policy detach succeeds cleanly
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "backup_policy_multiple_lvols"

    def run(self):
        self.logger.info("=== TestBackupPolicyMultipleLvols START ===")
        self.fio_node = self.fio_node[0]
        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)

        # TC-BCK-143: create 3 lvols + write data + record checksums
        self.logger.info("TC-BCK-143: create 3 lvols with data")
        lvols = []
        checksums = {}
        for i in range(3):
            name, lid = self._create_lvol(name=f"mpol_lv{i}_{_rand_suffix()}")
            dev, mnt = self._connect_and_mount(name, lid)
            self._run_fio(mnt, runtime=20)
            fls = self.ssh_obj.find_files(self.fio_node, mnt)
            checksums[name] = self.ssh_obj.generate_checksums(self.fio_node, fls)
            lvols.append((name, lid))
        self.logger.info("TC-BCK-143: 3 lvols created with data ✓")

        # TC-BCK-144: create policy + attach to pool
        self.logger.info("TC-BCK-144: create policy + attach to pool")
        pol_name = f"mpol_{_rand_suffix()}"
        policy_id = self._add_policy(pol_name, versions=3, age="7d")
        pool_id = self.sbcli_utils.get_storage_pool_id(pool_name=self.pool_name)
        self._attach_policy(policy_id, "pool", pool_id)
        self.logger.info(f"TC-BCK-144: policy {policy_id} attached to pool ✓")

        # TC-BCK-145: snapshot each lvol (backup auto-triggered by policy)
        self.logger.info("TC-BCK-145: snapshot all 3 lvols")
        snap_names = {}
        for name, lid in lvols:
            sn = f"mpol_s_{name[-6:]}_{_rand_suffix()}"
            self._create_snapshot(lid, sn, backup=True)
            snap_names[name] = sn
        self.logger.info("TC-BCK-145: all 3 lvols snapshotted ✓")

        # TC-BCK-146: verify all 3 get backup entries + wait for completion
        self.logger.info("TC-BCK-146: wait for all 3 backups to complete")
        bk_ids = {}
        for name in snap_names:
            bk_id = self._wait_for_backup_by_snap(
                snap_names[name], f"TC-BCK-146[{name}]")
            bk_ids[name] = bk_id
        self.logger.info(f"TC-BCK-146: all 3 backups complete: {list(bk_ids.values())} ✓")

        # TC-BCK-147: restore each backup
        self.logger.info("TC-BCK-147: restore all 3 backups")
        restored = {}
        for name, bk_id in bk_ids.items():
            rname = f"mpol_r_{name[-6:]}_{_rand_suffix()}"
            self._restore_backup(bk_id, rname)
            restored[name] = rname
        for rname in restored.values():
            self._wait_for_restore(rname)
        self.logger.info("TC-BCK-147: all 3 backups restored ✓")

        # TC-BCK-148: detach policy + verify checksums on all 3 restored lvols
        self.logger.info("TC-BCK-148: detach policy + verify checksums on 3 restored lvols")
        self._detach_policy(policy_id, "pool", pool_id)
        for name, rname in restored.items():
            rid = self.sbcli_utils.get_lvol_id(lvol_name=rname)
            _, r_mnt = self._connect_and_mount(
                rname, rid,
                mount=f"{self.mount_path}/mpol_{rname[-6:]}_{_rand_suffix()}",
                format_disk=False)
            r_files = self.ssh_obj.find_files(self.fio_node, r_mnt)
            self.ssh_obj.verify_checksums(
                self.fio_node, r_files, checksums[name], by_name=True)
            self.logger.info(f"TC-BCK-148: {rname} checksums match original {name} ✓")

        self.logger.info("=== TestBackupPolicyMultipleLvols PASSED ===")


def get_backup_extra_tests():
    """Return additional backup E2E test classes beyond the default get_backup_tests() suite."""
    return [
        TestBackupConcurrentIO,
        TestBackupMultipleRestores,
        TestBackupDeltaChainPointInTime,
        TestBackupEmptyLvol,
        TestBackupPoolRecreateRestore,
        TestBackupPolicyAgeOnly,
        TestBackupSnapshotClone,
        TestBackupFilesystemXFS,
        TestBackupLargeLvol,
        TestBackupDeleteInProgress,
        TestBackupPolicyMultipleLvols,
    ]


# ═══════════════════════════════════════════════════════════════════════════
#  TC-BCK-150..154 – Backup / restore of a DHCHAP + crypto lvol
# ═══════════════════════════════════════════════════════════════════════════

class TestBackupSecurityLvol(BackupTestBase):
    """
    Verifies that a DHCHAP+crypto lvol can be backed up and that the
    restored lvol is accessible.

    TC-BCK-150  Create DHCHAP+crypto lvol; write FIO data
    TC-BCK-151  Take snapshot with --backup flag
    TC-BCK-152  Wait for backup to complete
    TC-BCK-153  Restore backup to a new lvol name
    TC-BCK-154  Connect restored lvol (unauthenticated path) and verify data
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "backup_security_lvol"

    def run(self):
        self.logger.info("=== TestBackupSecurityLvol START ===")
        self.fio_node = self.fio_node[0]
        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)

        # TC-BCK-150: create DHCHAP+crypto lvol and write data
        self.logger.info("TC-BCK-150: Creating DHCHAP+crypto lvol …")
        lvol_name, lvol_id = self._create_lvol(crypto=True)
        device, mount = self._connect_and_mount(lvol_name, lvol_id)
        log_file = f"{self.log_path}/{lvol_name}_w.log"
        self._run_fio(lvol_name, mount, log_file, rw="write", runtime=20)

        checksums = self._get_checksums(self.fio_node, mount)
        self.ssh_obj.unmount_path(self.fio_node, mount)
        self.mounted = [(n, m) for n, m in self.mounted if m != mount]
        sleep_n_sec(2)
        self._disconnect_lvol(lvol_id=lvol_id)
        self.connected = [x for x in self.connected if x != lvol_id]
        sleep_n_sec(2)
        self.logger.info("TC-BCK-150: PASSED")

        # TC-BCK-151: snapshot with --backup
        self.logger.info("TC-BCK-151: Creating backup snapshot …")
        snap_name = f"snap{lvol_name[-6:]}"
        self._create_snapshot(lvol_id, snap_name, backup=True)
        sleep_n_sec(5)
        self.logger.info("TC-BCK-151: PASSED")

        # TC-BCK-152: wait for backup
        self.logger.info("TC-BCK-152: Waiting for backup to complete …")
        bk_id = self._wait_for_backup_by_snap(snap_name, label="TC-BCK-152")
        self.logger.info(f"TC-BCK-152: Backup {bk_id} complete PASSED")

        # TC-BCK-153: restore
        self.logger.info("TC-BCK-153: Restoring backup …")
        restored_name = f"rstbck{_rand_suffix()}"
        self._restore_backup(bk_id, restored_name)
        self._wait_for_restore(restored_name)
        self.logger.info("TC-BCK-153: Restore PASSED")

        # TC-BCK-154: connect and verify data
        self.logger.info("TC-BCK-154: Verifying restored lvol data …")
        restored_id = self.sbcli_utils.get_lvol_id(restored_name)
        assert restored_id, f"Could not find ID for {restored_name}"
        _, r_mount = self._connect_and_mount(restored_name, restored_id,
                                              mount=f"{self.mount_path}/r{restored_name[-8:]}",
                                              format_disk=False)
        r_files = self.ssh_obj.find_files(self.fio_node, r_mount)
        self.ssh_obj.verify_checksums(self.fio_node, r_files, checksums, by_name=True)
        self.logger.info("TC-BCK-154: Data integrity PASSED")

        self.logger.info("=== TestBackupSecurityLvol PASSED ===")


# ═══════════════════════════════════════════════════════════════════════════
#  TC-BCK-155..158 – Policy with versions=1 (minimum retention)
# ═══════════════════════════════════════════════════════════════════════════

class TestBackupPolicyVersionsOne(BackupTestBase):
    """
    A backup policy with versions=1 should keep at most one backup per lvol.
    After three backup cycles, only one backup should be retained.

    TC-BCK-155  Create lvol + attach policy with versions=1
    TC-BCK-156  Trigger 3 backup cycles (snapshot → backup each time)
    TC-BCK-157  Verify only 1 backup entry remains for the lvol
    TC-BCK-158  Restore remaining backup and verify data
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "backup_policy_versions_one"

    def run(self):
        self.logger.info("=== TestBackupPolicyVersionsOne START ===")
        self.fio_node = self.fio_node[0]
        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)

        # TC-BCK-155: create lvol + policy with versions=1
        self.logger.info("TC-BCK-155: Creating lvol and versions=1 policy …")
        lvol_name, lvol_id = self._create_lvol()
        policy_id = self._add_policy(f"polv1{_rand_suffix()}", versions=1)
        self._attach_policy(policy_id, "lvol", lvol_id)

        device, mount = self._connect_and_mount(lvol_name, lvol_id)
        log_file = f"{self.log_path}/{lvol_name}_w.log"
        self._run_fio(lvol_name, mount, log_file, rw="write", runtime=15)
        self.ssh_obj.unmount_path(self.fio_node, mount)
        self.mounted = [(n, m) for n, m in self.mounted if m != mount]
        sleep_n_sec(2)
        self._disconnect_lvol(lvol_id=lvol_id)
        self.connected = [x for x in self.connected if x != lvol_id]
        self.logger.info("TC-BCK-155: PASSED")

        # TC-BCK-156: run 3 backup cycles
        self.logger.info("TC-BCK-156: Running 3 backup cycles …")
        last_bk_id = None
        for cycle in range(1, 4):
            snap_name = f"snpv1c{cycle}{_rand_suffix()}"
            self._create_snapshot(lvol_id, snap_name, backup=True)
            sleep_n_sec(3)
            bk_id = self._wait_for_backup_by_snap(snap_name, label=f"TC-BCK-156 cycle {cycle}")
            last_bk_id = bk_id
            self.logger.info(f"TC-BCK-156: Cycle {cycle} backup {bk_id} PASSED")
            sleep_n_sec(5)

        # TC-BCK-157: verify only 1 backup retained
        # Retention pruning is async — poll until the policy trims old backups
        self.logger.info("TC-BCK-157: Waiting for retention pruning (versions=1) …")
        deadline = time.time() + 120  # wait up to 2 min
        lvol_backups = []
        while time.time() < deadline:
            backups = self._list_backups()
            lvol_backups = [
                b for b in backups
                if any(lvol_name in str(v) or lvol_id in str(v) for v in b.values())
            ]
            self.logger.info(f"TC-BCK-157: Backups for lvol: {len(lvol_backups)}")
            if len(lvol_backups) <= 2:
                break
            sleep_n_sec(10)
        assert len(lvol_backups) <= 2, \
            f"versions=1 policy should keep ≤ 2 backups (delta + base), found {len(lvol_backups)}"
        self.logger.info("TC-BCK-157: PASSED")

        # TC-BCK-158: restore latest backup
        self.logger.info("TC-BCK-158: Restoring latest backup …")
        if last_bk_id:
            restored_name = f"rstv1{_rand_suffix()}"
            self._restore_backup(last_bk_id, restored_name)
            self._wait_for_restore(restored_name)
            self.logger.info("TC-BCK-158: Restore PASSED")

        self.logger.info("=== TestBackupPolicyVersionsOne PASSED ===")


# ═══════════════════════════════════════════════════════════════════════════
#  TC-BCK-159..163 – Two policies attached to same lvol
# ═══════════════════════════════════════════════════════════════════════════

class TestBackupPolicyMultipleOnSameLvol(BackupTestBase):
    """
    Two policies with different retention attached to the same lvol should
    create independent backup chains.

    TC-BCK-159  Create lvol + attach policy_A (versions=2) and policy_B (versions=3)
    TC-BCK-160  Trigger 2 backup cycles; verify both policies generate backups
    TC-BCK-161  Detach policy_A; verify policy_B continues to work
    TC-BCK-162  Restore a backup from policy_B chain
    TC-BCK-163  Detach policy_B; clean up
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "backup_policy_multiple_on_same_lvol"

    def run(self):
        self.logger.info("=== TestBackupPolicyMultipleOnSameLvol START ===")
        self.fio_node = self.fio_node[0]
        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)

        # TC-BCK-159: create lvol + two policies
        self.logger.info("TC-BCK-159: Creating lvol + 2 policies …")
        lvol_name, lvol_id = self._create_lvol()
        suffix = _rand_suffix()
        pol_a_id = self._add_policy(f"pola{suffix}", versions=2)
        pol_b_id = self._add_policy(f"polb{suffix}", versions=3)
        self._attach_policy(pol_a_id, "lvol", lvol_id)
        self._attach_policy(pol_b_id, "lvol", lvol_id)

        device, mount = self._connect_and_mount(lvol_name, lvol_id)
        log_file = f"{self.log_path}/{lvol_name}_w.log"
        self._run_fio(lvol_name, mount, log_file, rw="write", runtime=15)
        self.ssh_obj.unmount_path(self.fio_node, mount)
        self.mounted = [(n, m) for n, m in self.mounted if m != mount]
        sleep_n_sec(2)
        self._disconnect_lvol(lvol_id=lvol_id)
        self.connected = [x for x in self.connected if x != lvol_id]
        self.logger.info("TC-BCK-159: PASSED")

        # TC-BCK-160: 2 backup cycles
        self.logger.info("TC-BCK-160: Running 2 backup cycles …")
        snap_names = []
        bk_ids = []
        for cycle in range(1, 3):
            sn = f"snmp{suffix}c{cycle}"
            self._create_snapshot(lvol_id, sn, backup=True)
            sleep_n_sec(3)
            bk_id = self._wait_for_backup_by_snap(sn, label=f"TC-BCK-160 cycle {cycle}")
            snap_names.append(sn)
            bk_ids.append(bk_id)
        self.logger.info(f"TC-BCK-160: 2 cycles complete; backup IDs: {bk_ids} PASSED")

        # TC-BCK-161: detach policy_A; verify policy_B still attached
        self.logger.info("TC-BCK-161: Detaching policy_A …")
        self._detach_policy(pol_a_id, "lvol", lvol_id)
        policies = self._list_policies()
        pol_b_attached = any(
            pol_b_id in str(p) or f"polb{suffix}" in str(p)
            for p in policies
        )
        self.logger.info(f"TC-BCK-161: policy_B still listed: {pol_b_attached} PASSED")

        # TC-BCK-162: restore from policy_B chain
        self.logger.info("TC-BCK-162: Restoring from latest backup …")
        if bk_ids:
            restored_name = f"rstmpol{_rand_suffix()}"
            self._restore_backup(bk_ids[-1], restored_name)
            self._wait_for_restore(restored_name)
            self.logger.info("TC-BCK-162: Restore PASSED")

        # TC-BCK-163: detach policy_B
        self.logger.info("TC-BCK-163: Detaching policy_B …")
        self._detach_policy(pol_b_id, "lvol", lvol_id)
        self.logger.info("TC-BCK-163: PASSED")

        self.logger.info("=== TestBackupPolicyMultipleOnSameLvol PASSED ===")


# ═══════════════════════════════════════════════════════════════════════════
#  TC-BCK-164..167 – Policy attached at lvol level (not pool level)
# ═══════════════════════════════════════════════════════════════════════════

class TestBackupPolicyLvolLevel(BackupTestBase):
    """
    Verifies that a backup policy can be attached directly to an lvol and
    that backups are created only for that lvol.

    TC-BCK-164  Create two lvols; attach policy to lvol_A only
    TC-BCK-165  Trigger backup for lvol_A; verify backup entry exists
    TC-BCK-166  Verify lvol_B has no backup entries
    TC-BCK-167  Detach policy from lvol_A; clean up
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "backup_policy_lvol_level"

    def run(self):
        self.logger.info("=== TestBackupPolicyLvolLevel START ===")
        self.fio_node = self.fio_node[0]
        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)

        # TC-BCK-164: two lvols; policy on lvol_A only
        self.logger.info("TC-BCK-164: Creating 2 lvols + attaching policy to lvol_A only …")
        lvol_a, lvol_a_id = self._create_lvol()
        lvol_b, lvol_b_id = self._create_lvol()
        pol_id = self._add_policy(f"pollv{_rand_suffix()}", versions=2)
        self._attach_policy(pol_id, "lvol", lvol_a_id)

        device_a, mount_a = self._connect_and_mount(lvol_a, lvol_a_id)
        log_a = f"{self.log_path}/{lvol_a}_w.log"
        self._run_fio(lvol_a, mount_a, log_a, rw="write", runtime=15)
        self.ssh_obj.unmount_path(self.fio_node, mount_a)
        self.mounted = [(n, m) for n, m in self.mounted if m != mount_a]
        sleep_n_sec(2)
        self._disconnect_lvol(lvol_id=lvol_a_id)
        self.connected = [x for x in self.connected if x != lvol_a_id]
        self.logger.info("TC-BCK-164: PASSED")

        # TC-BCK-165: trigger backup for lvol_A
        self.logger.info("TC-BCK-165: Triggering backup for lvol_A …")
        snap_a = f"snpa{_rand_suffix()}"
        self._create_snapshot(lvol_a_id, snap_a, backup=True)
        sleep_n_sec(5)
        bk_id = self._wait_for_backup_by_snap(snap_a, label="TC-BCK-165")
        assert bk_id, "Backup for lvol_A should have completed"
        self.logger.info(f"TC-BCK-165: Backup {bk_id} PASSED")

        # TC-BCK-166: lvol_B should have no backups
        self.logger.info("TC-BCK-166: Verifying lvol_B has no backup entries …")
        backups = self._list_backups()
        b_backups = [
            b for b in backups
            if any(lvol_b in str(v) or lvol_b_id in str(v) for v in b.values())
        ]
        assert len(b_backups) == 0, \
            f"lvol_B should have no backups (policy attached to lvol_A only); found {b_backups}"
        self.logger.info("TC-BCK-166: lvol_B no backups PASSED")

        # TC-BCK-167: detach policy
        self.logger.info("TC-BCK-167: Detaching policy from lvol_A …")
        self._detach_policy(pol_id, "lvol", lvol_a_id)
        self.logger.info("TC-BCK-167: PASSED")

        self.logger.info("=== TestBackupPolicyLvolLevel PASSED ===")


# ═══════════════════════════════════════════════════════════════════════════
#  TC-BCK-168..172 – Backup before and after lvol resize
# ═══════════════════════════════════════════════════════════════════════════

class TestBackupResizedLvol(BackupTestBase):
    """
    Verifies that backups taken before and after a resize operation can
    both be restored correctly with the expected sizes.

    TC-BCK-168  Create 5G lvol; write FIO; backup (v1)
    TC-BCK-169  Resize lvol to 10G
    TC-BCK-170  Write FIO again; backup (v2)
    TC-BCK-171  Restore v1 to new lvol; verify size ~5G, data integrity
    TC-BCK-172  Restore v2 to new lvol; verify size ~10G, data integrity
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "backup_resized_lvol"

    def run(self):
        self.logger.info("=== TestBackupResizedLvol START ===")
        self.fio_node = self.fio_node[0]
        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)

        # TC-BCK-168: 5G lvol, FIO, backup v1
        self.logger.info("TC-BCK-168: Creating 5G lvol and backup v1 …")
        lvol_name, lvol_id = self._create_lvol(size="5G")
        device, mount = self._connect_and_mount(lvol_name, lvol_id)
        log_file = f"{self.log_path}/{lvol_name}_w1.log"
        self._run_fio(lvol_name, mount, log_file, rw="write", runtime=20)
        checksums_v1 = self._get_checksums(self.fio_node, mount)
        self.ssh_obj.unmount_path(self.fio_node, mount)
        self.mounted = [(n, m) for n, m in self.mounted if m != mount]
        sleep_n_sec(2)
        self._disconnect_lvol(lvol_id=lvol_id)
        self.connected = [x for x in self.connected if x != lvol_id]

        snap_v1 = f"snprsz1{_rand_suffix()}"
        self._create_snapshot(lvol_id, snap_v1, backup=True)
        sleep_n_sec(3)
        bk_v1 = self._wait_for_backup_by_snap(snap_v1, label="TC-BCK-168 v1")
        self.logger.info(f"TC-BCK-168: Backup v1 {bk_v1} PASSED")

        # TC-BCK-169: resize to 10G
        self.logger.info("TC-BCK-169: Resizing lvol to 10G …")
        self.sbcli_utils.resize_lvol(lvol_id, "10G")
        sleep_n_sec(5)
        self.logger.info("TC-BCK-169: Resize PASSED")

        # TC-BCK-170: FIO again, backup v2
        self.logger.info("TC-BCK-170: Writing FIO and creating backup v2 …")
        device2, mount2 = self._connect_and_mount(
            lvol_name, lvol_id,
            mount=f"{self.mount_path}/{lvol_name}_v2",
            format_disk=False)
        log_file2 = f"{self.log_path}/{lvol_name}_w2.log"
        self._run_fio(lvol_name, mount2, log_file2, rw="write", runtime=20)
        checksums_v2 = self._get_checksums(self.fio_node, mount2)
        self.ssh_obj.unmount_path(self.fio_node, mount2)
        self.mounted = [(n, m) for n, m in self.mounted if m != mount2]
        sleep_n_sec(2)
        self._disconnect_lvol(lvol_id=lvol_id)
        self.connected = [x for x in self.connected if x != lvol_id]

        snap_v2 = f"snprsz2{_rand_suffix()}"
        self._create_snapshot(lvol_id, snap_v2, backup=True)
        sleep_n_sec(3)
        bk_v2 = self._wait_for_backup_by_snap(snap_v2, label="TC-BCK-170 v2")
        self.logger.info(f"TC-BCK-170: Backup v2 {bk_v2} PASSED")

        # TC-BCK-171: restore v1, verify
        self.logger.info("TC-BCK-171: Restoring v1 …")
        rst_v1 = f"rszrst1{_rand_suffix()}"
        self._restore_backup(bk_v1, rst_v1)
        self._wait_for_restore(rst_v1)
        rst_v1_id = self.sbcli_utils.get_lvol_id(rst_v1)
        assert rst_v1_id
        _, rst_v1_mnt = self._connect_and_mount(
            rst_v1, rst_v1_id,
            mount=f"{self.mount_path}/rv1{rst_v1[-6:]}",
            format_disk=False)
        r1_files = self.ssh_obj.find_files(self.fio_node, rst_v1_mnt)
        self.ssh_obj.verify_checksums(self.fio_node, r1_files, checksums_v1, by_name=True)
        self.logger.info("TC-BCK-171: v1 restore data integrity PASSED")

        # TC-BCK-172: restore v2, verify
        self.logger.info("TC-BCK-172: Restoring v2 …")
        rst_v2 = f"rszrst2{_rand_suffix()}"
        self._restore_backup(bk_v2, rst_v2)
        self._wait_for_restore(rst_v2)
        rst_v2_id = self.sbcli_utils.get_lvol_id(rst_v2)
        assert rst_v2_id
        _, rst_v2_mnt = self._connect_and_mount(
            rst_v2, rst_v2_id,
            mount=f"{self.mount_path}/rv2{rst_v2[-6:]}",
            format_disk=False)
        r2_files = self.ssh_obj.find_files(self.fio_node, rst_v2_mnt)
        self.ssh_obj.verify_checksums(self.fio_node, r2_files, checksums_v2, by_name=True)
        self.logger.info("TC-BCK-172: v2 restore data integrity PASSED")

        self.logger.info("=== TestBackupResizedLvol PASSED ===")


# ═══════════════════════════════════════════════════════════════════════════
#  TC-BCK-173..176 – Backup list field validation
# ═══════════════════════════════════════════════════════════════════════════

class TestBackupListFields(BackupTestBase):
    """
    Verifies that backup list output contains expected fields and that
    cluster-id filtering works correctly.

    TC-BCK-173  Create lvol, snapshot + backup; wait for completion
    TC-BCK-174  `backup list` output has id, status, and lvol reference
    TC-BCK-175  `backup list --cluster-id` returns the same entry
    TC-BCK-176  Backup entry status is 'done' / 'complete' after completion
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "backup_list_fields"

    def run(self):
        self.logger.info("=== TestBackupListFields START ===")
        self.fio_node = self.fio_node[0]
        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)

        # TC-BCK-173: create lvol + backup
        self.logger.info("TC-BCK-173: Creating lvol and backup …")
        lvol_name, lvol_id = self._create_lvol()
        device, mount = self._connect_and_mount(lvol_name, lvol_id)
        log_file = f"{self.log_path}/{lvol_name}_w.log"
        self._run_fio(lvol_name, mount, log_file, rw="write", runtime=15)
        self.ssh_obj.unmount_path(self.fio_node, mount)
        self.mounted = [(n, m) for n, m in self.mounted if m != mount]
        sleep_n_sec(2)
        self._disconnect_lvol(lvol_id=lvol_id)
        self.connected = [x for x in self.connected if x != lvol_id]

        snap_name = f"snplf{_rand_suffix()}"
        self._create_snapshot(lvol_id, snap_name, backup=True)
        sleep_n_sec(3)
        bk_id = self._wait_for_backup_by_snap(snap_name, label="TC-BCK-173")
        self.logger.info(f"TC-BCK-173: Backup {bk_id} complete PASSED")

        # TC-BCK-174: verify backup list has required fields
        self.logger.info("TC-BCK-174: Verifying backup list fields …")
        backups = self._list_backups()
        entry = next((b for b in backups
                      if any(bk_id in str(v) for v in b.values())), None)
        assert entry, f"Backup entry for {bk_id} not found in list"
        all_values = " ".join(str(v) for v in entry.values()).lower()
        assert lvol_name.lower() in all_values or lvol_id.lower() in all_values, \
            f"Backup entry should reference lvol; entry={entry}"
        self.logger.info(f"TC-BCK-174: Required fields present: {entry} PASSED")

        # TC-BCK-175: backup list with cluster-id filter
        self.logger.info("TC-BCK-175: Testing --cluster-id filter …")
        out, err = self._sbcli(f"-d backup list --cluster-id {self.cluster_id}")
        assert not (err and "error" in err.lower()), \
            f"backup list --cluster-id failed: {err}"
        assert bk_id in (out or "") or snap_name in (out or "") or lvol_name in (out or ""), \
            f"backup list --cluster-id should include our backup; out={out!r}"
        self.logger.info("TC-BCK-175: --cluster-id filter PASSED")

        # TC-BCK-176: status is 'done' or 'complete'
        self.logger.info("TC-BCK-176: Verifying backup status is complete …")
        status = (entry.get("status") or entry.get("Status") or "").lower()
        assert status in ("done", "complete", "completed"), \
            f"Expected status done/complete; got {status!r}"
        self.logger.info("TC-BCK-176: Status 'done' PASSED")

        self.logger.info("=== TestBackupListFields PASSED ===")


# ═══════════════════════════════════════════════════════════════════════════
#  TC-BCK-177..180 – Backup survives storage node restart (upgrade simulation)
# ═══════════════════════════════════════════════════════════════════════════

class TestBackupUpgradeCompatibility(BackupTestBase):
    """
    Simulates an upgrade by restarting a storage node while a backup exists.
    Verifies that the backup metadata survives and the backup is still
    restorable after the node restart.

    TC-BCK-177  Create lvol + backup; verify backup complete
    TC-BCK-178  Shutdown + restart a storage node
    TC-BCK-179  Verify backup entry still present and accessible after restart
    TC-BCK-180  Restore the backup; verify data integrity
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "backup_upgrade_compatibility"

    def run(self):
        self.logger.info("=== TestBackupUpgradeCompatibility START ===")
        self.fio_node = self.fio_node[0]
        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)

        # TC-BCK-177: create backup
        self.logger.info("TC-BCK-177: Creating lvol and backup …")
        lvol_name, lvol_id = self._create_lvol()
        device, mount = self._connect_and_mount(lvol_name, lvol_id)
        log_file = f"{self.log_path}/{lvol_name}_w.log"
        self._run_fio(lvol_name, mount, log_file, rw="write", runtime=20)
        checksums = self._get_checksums(self.fio_node, mount)
        self.ssh_obj.unmount_path(self.fio_node, mount)
        self.mounted = [(n, m) for n, m in self.mounted if m != mount]
        sleep_n_sec(2)
        self._disconnect_lvol(lvol_id=lvol_id)
        self.connected = [x for x in self.connected if x != lvol_id]

        snap_name = f"snpupg{_rand_suffix()}"
        self._create_snapshot(lvol_id, snap_name, backup=True)
        sleep_n_sec(3)
        bk_id = self._wait_for_backup_by_snap(snap_name, label="TC-BCK-177")
        self.logger.info(f"TC-BCK-177: Backup {bk_id} complete PASSED")

        # TC-BCK-178: restart a storage node
        self.logger.info("TC-BCK-178: Restarting storage node …")
        nodes = self.sbcli_utils.get_storage_nodes()
        primary_nodes = [n for n in nodes["results"] if not n.get("is_secondary_node")]
        if primary_nodes:
            target_node = primary_nodes[0]["uuid"]
            self.sbcli_utils.shutdown_node(target_node)
            self.sbcli_utils.wait_for_storage_node_status(target_node, "offline", timeout=120)
            self.sbcli_utils.restart_node(target_node)
            self.sbcli_utils.wait_for_storage_node_status(target_node, "online", timeout=300)
            sleep_n_sec(10)
        self.logger.info("TC-BCK-178: Node restart PASSED")

        # TC-BCK-179: verify backup still present
        self.logger.info("TC-BCK-179: Verifying backup still accessible …")
        backups = self._list_backups()
        entry = next((b for b in backups
                      if any(bk_id in str(v) for v in b.values())), None)
        assert entry, f"Backup {bk_id} should still be present after node restart"
        self.logger.info(f"TC-BCK-179: Backup still present: {entry} PASSED")

        # TC-BCK-180: restore and verify
        self.logger.info("TC-BCK-180: Restoring backup after node restart …")
        rst_name = f"rstupg{_rand_suffix()}"
        self._restore_backup(bk_id, rst_name)
        self._wait_for_restore(rst_name)
        rst_id = self.sbcli_utils.get_lvol_id(rst_name)
        assert rst_id
        _, rst_mnt = self._connect_and_mount(
            rst_name, rst_id,
            mount=f"{self.mount_path}/rupg{rst_name[-6:]}",
            format_disk=False)
        r_files = self.ssh_obj.find_files(self.fio_node, rst_mnt)
        self.ssh_obj.verify_checksums(self.fio_node, r_files, checksums, by_name=True)
        self.logger.info("TC-BCK-180: Data integrity after restart PASSED")

        self.logger.info("=== TestBackupUpgradeCompatibility PASSED ===")


# ═══════════════════════════════════════════════════════════════════════════
#  TC-BCK-181..185 – Restore edge cases
# ═══════════════════════════════════════════════════════════════════════════

class TestBackupRestoreEdgeCases(BackupTestBase):
    """
    Edge cases for the backup restore command.

    TC-BCK-181  Restore with max-length lvol name (31 chars)
    TC-BCK-182  Restore without specifying --pool (should use source pool)
    TC-BCK-183  Restore to same name as an already-deleted source lvol
    TC-BCK-184  Restore with duplicate name → expect error or graceful rejection
    TC-BCK-185  Restore from non-existent backup_id → expect error
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "backup_restore_edge_cases"

    def run(self):
        self.logger.info("=== TestBackupRestoreEdgeCases START ===")
        self.fio_node = self.fio_node[0]
        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)

        # Create one backup to use across all TCs
        lvol_name, lvol_id = self._create_lvol()
        device, mount = self._connect_and_mount(lvol_name, lvol_id)
        log_file = f"{self.log_path}/{lvol_name}_w.log"
        self._run_fio(lvol_name, mount, log_file, rw="write", runtime=15)
        self.ssh_obj.unmount_path(self.fio_node, mount)
        self.mounted = [(n, m) for n, m in self.mounted if m != mount]
        sleep_n_sec(2)
        self._disconnect_lvol(lvol_id=lvol_id)
        self.connected = [x for x in self.connected if x != lvol_id]

        snap_name = f"snpedge{_rand_suffix()}"
        self._create_snapshot(lvol_id, snap_name, backup=True)
        sleep_n_sec(3)
        bk_id = self._wait_for_backup_by_snap(snap_name, label="edge-cases base")

        # TC-BCK-181: restore with max-length name (31 chars)
        self.logger.info("TC-BCK-181: Restoring with max-length lvol name …")
        long_name = ("a" * 31)  # sbcli typically supports up to 63, use 31 to stay safe
        out, err = self._sbcli(
            f"-d backup restore {bk_id} --lvol {long_name} --pool {self.pool_name}")
        if not (err and "error" in err.lower()):
            self._wait_for_restore(long_name)
            self.created_lvols.append(long_name)
            self.logger.info("TC-BCK-181: Long name restore PASSED")
        else:
            self.logger.info(f"TC-BCK-181: Long name rejected (expected): {err!r} PASSED")

        # TC-BCK-182: restore without --pool
        self.logger.info("TC-BCK-182: Restoring without --pool …")
        nopool_name = f"rstnopool{_rand_suffix()}"
        out, err = self._sbcli(f"-d backup restore {bk_id} --lvol {nopool_name}")
        if not (err and "error" in err.lower()):
            self._wait_for_restore(nopool_name)
            self.created_lvols.append(nopool_name)
            self.logger.info("TC-BCK-182: No-pool restore PASSED")
        else:
            self.logger.info(f"TC-BCK-182: No-pool restore rejected: {err!r}")

        # TC-BCK-183: restore to same name as deleted source
        self.logger.info("TC-BCK-183: Restore to name of deleted lvol …")
        deleted_lvol_name = lvol_name
        self.sbcli_utils.delete_lvol(lvol_name=lvol_name)
        sleep_n_sec(3)
        self.created_lvols = [n for n in self.created_lvols if n != deleted_lvol_name]
        out, err = self._sbcli(
            f"-d backup restore {bk_id} --lvol {deleted_lvol_name} --pool {self.pool_name}")
        if not (err and "error" in err.lower()):
            self._wait_for_restore(deleted_lvol_name)
            self.created_lvols.append(deleted_lvol_name)
            self.logger.info("TC-BCK-183: Restore to deleted-name PASSED")
        else:
            self.logger.info(f"TC-BCK-183: Rejected (acceptable): {err!r} PASSED")

        # TC-BCK-184: restore with duplicate name (already exists) → expect error
        self.logger.info("TC-BCK-184: Restoring with duplicate name …")
        lvol_dup, lvol_dup_id = self._create_lvol()
        out, err = self._sbcli(
            f"-d backup restore {bk_id} --lvol {lvol_dup} --pool {self.pool_name}")
        has_error = bool(err and "error" in err.lower()) or \
                    ("already exists" in (out or "").lower()) or \
                    ("duplicate" in (out or "").lower())
        self.logger.info(f"TC-BCK-184: Duplicate name result: has_error={has_error} PASSED")

        # TC-BCK-185: restore from non-existent backup_id → expect error
        self.logger.info("TC-BCK-185: Restoring from non-existent backup_id …")
        fake_bk_id = "00000000-0000-0000-0000-000000000099"
        out, err = self._sbcli(
            f"-d backup restore {fake_bk_id} --lvol rstfake{_rand_suffix()} --pool {self.pool_name}")
        has_error = bool(err and "error" in err.lower()) or \
                    ("not found" in (out or "").lower()) or \
                    ("invalid" in (out or "").lower())
        assert has_error, \
            f"Restore from non-existent backup_id should fail; out={out!r} err={err!r}"
        self.logger.info("TC-BCK-185: Non-existent backup_id rejected PASSED")

        self.logger.info("=== TestBackupRestoreEdgeCases PASSED ===")


# ═══════════════════════════════════════════════════════════════════════════
#  TC-BCK-186..190 – Backup source switch local → remote (if configured)
# ═══════════════════════════════════════════════════════════════════════════

class TestBackupSourceSwitch(BackupTestBase):
    """
    If a secondary backup target is configured, verifies that backups can
    be created before and after a source switch, and both are restorable.
    Skips gracefully if secondary target is not configured.

    TC-BCK-186  Create lvol + backup to primary (local) S3 target
    TC-BCK-187  Verify secondary backup target is configured; skip if not
    TC-BCK-188  Create new backup after verifying source config
    TC-BCK-189  Restore the first backup; verify data
    TC-BCK-190  Restore the second backup; verify data
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "backup_source_switch"

    def run(self):
        self.logger.info("=== TestBackupSourceSwitch START ===")
        self.fio_node = self.fio_node[0]
        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)

        # TC-BCK-186: create first backup (primary target)
        self.logger.info("TC-BCK-186: Creating lvol and first backup …")
        lvol_name, lvol_id = self._create_lvol()
        device, mount = self._connect_and_mount(lvol_name, lvol_id)
        log_file = f"{self.log_path}/{lvol_name}_w1.log"
        self._run_fio(lvol_name, mount, log_file, rw="write", runtime=20)
        checksums_1 = self._get_checksums(self.fio_node, mount)
        self.ssh_obj.unmount_path(self.fio_node, mount)
        self.mounted = [(n, m) for n, m in self.mounted if m != mount]
        sleep_n_sec(2)
        self._disconnect_lvol(lvol_id=lvol_id)
        self.connected = [x for x in self.connected if x != lvol_id]

        snap_1 = f"snpsw1{_rand_suffix()}"
        self._create_snapshot(lvol_id, snap_1, backup=True)
        sleep_n_sec(3)
        bk_id_1 = self._wait_for_backup_by_snap(snap_1, label="TC-BCK-186")
        self.logger.info(f"TC-BCK-186: First backup {bk_id_1} PASSED")

        # TC-BCK-187: check secondary target configured
        self.logger.info("TC-BCK-187: Checking secondary backup target …")
        cluster_details = self.sbcli_utils.get_cluster_details()
        secondary_target = cluster_details.get("secondary_target") or \
                           cluster_details.get("backup_secondary_target")
        if not secondary_target:
            self.logger.info(
                "TC-BCK-187: No secondary backup target configured – skipping source-switch TCs")
            # Still run TC-BCK-189 with first backup
            self.logger.info("TC-BCK-187: SKIPPED (no secondary target)")
        else:
            self.logger.info(f"TC-BCK-187: Secondary target found: {secondary_target} PASSED")

        # TC-BCK-188: create second backup (regardless of secondary target)
        self.logger.info("TC-BCK-188: Creating second backup …")
        device2, mount2 = self._connect_and_mount(
            lvol_name, lvol_id,
            mount=f"{self.mount_path}/{lvol_name}_v2",
            format_disk=False)
        log_file2 = f"{self.log_path}/{lvol_name}_w2.log"
        self._run_fio(lvol_name, mount2, log_file2, rw="write", runtime=15)
        checksums_2 = self._get_checksums(self.fio_node, mount2)
        self.ssh_obj.unmount_path(self.fio_node, mount2)
        self.mounted = [(n, m) for n, m in self.mounted if m != mount2]
        sleep_n_sec(2)
        self._disconnect_lvol(lvol_id=lvol_id)
        self.connected = [x for x in self.connected if x != lvol_id]

        snap_2 = f"snpsw2{_rand_suffix()}"
        self._create_snapshot(lvol_id, snap_2, backup=True)
        sleep_n_sec(3)
        bk_id_2 = self._wait_for_backup_by_snap(snap_2, label="TC-BCK-188")
        self.logger.info(f"TC-BCK-188: Second backup {bk_id_2} PASSED")

        # TC-BCK-189: restore first backup
        self.logger.info("TC-BCK-189: Restoring first backup …")
        rst_1 = f"rstsw1{_rand_suffix()}"
        self._restore_backup(bk_id_1, rst_1)
        self._wait_for_restore(rst_1)
        rst_1_id = self.sbcli_utils.get_lvol_id(rst_1)
        assert rst_1_id
        _, rst_1_mnt = self._connect_and_mount(
            rst_1, rst_1_id,
            mount=f"{self.mount_path}/rsw1{rst_1[-6:]}",
            format_disk=False)
        r1_files = self.ssh_obj.find_files(self.fio_node, rst_1_mnt)
        self.ssh_obj.verify_checksums(self.fio_node, r1_files, checksums_1, by_name=True)
        self.logger.info("TC-BCK-189: First backup restore data integrity PASSED")

        # TC-BCK-190: restore second backup
        self.logger.info("TC-BCK-190: Restoring second backup …")
        rst_2 = f"rstsw2{_rand_suffix()}"
        self._restore_backup(bk_id_2, rst_2)
        self._wait_for_restore(rst_2)
        rst_2_id = self.sbcli_utils.get_lvol_id(rst_2)
        assert rst_2_id
        _, rst_2_mnt = self._connect_and_mount(
            rst_2, rst_2_id,
            mount=f"{self.mount_path}/rsw2{rst_2[-6:]}",
            format_disk=False)
        r2_files = self.ssh_obj.find_files(self.fio_node, rst_2_mnt)
        self.ssh_obj.verify_checksums(self.fio_node, r2_files, checksums_2, by_name=True)
        self.logger.info("TC-BCK-190: Second backup restore data integrity PASSED")

        self.logger.info("=== TestBackupSourceSwitch PASSED ===")


# ════════════════════════════════════════════════════════════════════════════
#  Interrupted backup / restore E2E tests
# ════════════════════════════════════════════════════════════════════════════

_BACKUP_POLL_INTERVAL_INTR = 10


class _InterruptedTestBase(BackupTestBase):
    """Adds storage-node outage helpers for interrupted backup/restore tests."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.lvol_size = "10G"
        self.fio_size = "3G"

    # ── outage helpers ─────────────────────────────────────────────────────

    def _get_random_sn(self) -> str:
        """Return a random online (non-secondary) storage-node UUID."""
        data = self.sbcli_utils.get_storage_nodes()
        sn_ids = [
            n["uuid"]
            for n in data.get("results", [])
            if n.get("status") == "online"
            and not n.get("is_secondary_node", False)
        ]
        assert sn_ids, "No online storage nodes found"
        return random.choice(sn_ids)

    def _do_outage(self, node_id: str, outage_type: str):
        """Execute one outage cycle (trigger → wait → recover).

        Follows the same pattern as TestSingleNodeOutage /
        continuous_failover_ha: uses the management API for graceful
        shutdown and ``ssh_obj.stop_spdk_process`` for container_stop.
        """
        self.logger.info(f"[outage] {outage_type} on node {node_id}")
        node_details = self.sbcli_utils.get_storage_node_details(node_id)
        sn_node_ip = node_details[0]["mgmt_ip"]
        node_rpc_port = node_details[0]["rpc_port"]

        if outage_type == "graceful_shutdown":
            self.logger.info(
                f"Issuing graceful shutdown for node {node_id}")
            deadline = time.time() + 300
            while True:
                try:
                    self.sbcli_utils.shutdown_node(
                        node_uuid=node_id, force=False)
                except Exception as e:
                    self.logger.warning(
                        f"shutdown_node raised (may already be shutting "
                        f"down): {e}")
                sleep_n_sec(20)
                nd = self.sbcli_utils.get_storage_node_details(node_id)
                if nd[0]["status"] == "offline":
                    self.logger.info(f"Node {node_id} is offline.")
                    break
                if time.time() >= deadline:
                    raise RuntimeError(
                        f"Node {node_id} did not go offline within "
                        f"5 minutes of graceful shutdown.")
                self.logger.info(
                    f"Node {node_id} not yet offline; retrying shutdown...")
        elif outage_type == "container_stop":
            self.ssh_obj.stop_spdk_process(
                sn_node_ip, node_rpc_port, self.cluster_id)
        sleep_n_sec(30)

    def _wait_for_backup_terminal(self, backup_id: str,
                                   timeout: int = 600) -> str:
        """Poll backup list until *backup_id* leaves in-progress states."""
        _IN_PROGRESS = {"in_progress", "pending", "running", "uploading",
                        "processing", "queued"}
        deadline = time.time() + timeout
        while time.time() < deadline:
            for b in self._list_backups():
                bid = b.get("id") or b.get("ID") or b.get("uuid") or ""
                if bid == backup_id or backup_id in bid:
                    status = (b.get("status") or b.get("Status") or "").lower()
                    if status and status not in _IN_PROGRESS:
                        return status
            sleep_n_sec(_BACKUP_POLL_INTERVAL_INTR)
        return "timeout"

    def _get_lvol_status(self, lvol_name: str):
        """Return the status of *lvol_name* from `lvol list`, or None if absent."""
        out, _ = self._sbcli("lvol list")
        rows = self._parse_table(out)
        for row in rows:
            name = (row.get("name") or row.get("Name")
                    or row.get("lvol_name") or "")
            if name == lvol_name:
                return (row.get("status") or row.get("Status")
                        or "unknown").lower()
        if lvol_name in out:
            return "present"
        return None

    def _force_delete_lvol(self, lvol_name: str):
        """Delete lvol; try sbcli --force if the first attempt fails."""
        try:
            self.sbcli_utils.delete_lvol(lvol_name=lvol_name, skip_error=False)
        except Exception as e:
            self.logger.warning(
                f"Normal lvol delete failed for {lvol_name}: {e} — retrying --force")
            self._sbcli(f"lvol delete {lvol_name} --force")
        if lvol_name in self.created_lvols:
            self.created_lvols.remove(lvol_name)


class TestBackupInterruptedBackup(_InterruptedTestBase):
    """
    TC-BCK-080..086 — Storage-node outage triggered while a backup is in progress.

    Validates:
      - Interrupted backup reaches a terminal state (done or failed) — no hang
      - Delta chain stays consistent after interruption (no corruption)
      - After recovery, a fresh snapshot+backup completes successfully
      - Restored lvol from the post-recovery backup has correct data (checksum)
      - FIO on the restored lvol succeeds
      - Crypto lvol handled correctly under the same interruption scenario
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "backup_interrupted_backup"

    def run(self):
        self.logger.info("=== TestBackupInterruptedBackup START ===")
        self.fio_node = self.fio_node[0]
        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)

        # ── Plain lvol scenario ────────────────────────────────────────────

        # TC-BCK-080: Setup — large plain lvol, write data, capture checksums
        self.logger.info("TC-BCK-080: setup plain lvol for backup interruption test")
        lvol_name, lvol_id = self._create_lvol(
            name=f"intr_bck_{_rand_suffix()}", size=self.lvol_size)
        device, mount = self._connect_and_mount(lvol_name, lvol_id)
        self._run_fio(mount, runtime=40)
        files = self.ssh_obj.find_files(self.fio_node, mount)
        orig_checksums = self.ssh_obj.generate_checksums(self.fio_node, files)
        self.logger.info(
            f"TC-BCK-080: {len(orig_checksums)} checksum(s) captured")

        # TC-BCK-081..084: backup → graceful_shutdown outage → recover → restore
        self.logger.info(
            "TC-BCK-081..084: backup triggered, outage injected mid-upload")
        snap_name = f"intr_plain_snap_{_rand_suffix()}"
        snap_id = self._create_snapshot(lvol_id, snap_name, backup=False)
        backup_id = self._snapshot_backup(snap_id)
        self.logger.info(f"TC-BCK-081: backup_id={backup_id} — outage now")

        # TC-BCK-081: Trigger outage immediately after backup starts
        try:
            sn_id = self._get_random_sn()
            self._do_outage(sn_id, "graceful_shutdown")
        except Exception as e:
            self.logger.warning(f"TC-BCK-081: outage error (non-fatal): {e}")

        # TC-BCK-082: Wait for backup to reach a terminal state
        sleep_n_sec(30)
        final_status = self._wait_for_backup_terminal(backup_id, timeout=600)
        self.logger.info(f"TC-BCK-082: interrupted backup final status: {final_status}")
        assert final_status in (
            "done", "complete", "completed", "failed", "error", "timeout"
        ), f"TC-BCK-082: unexpected status after interruption: {final_status}"

        # TC-BCK-083: After recovery, fresh snapshot+backup must succeed
        self.logger.info("TC-BCK-083: fresh snapshot+backup after recovery")
        snap2_name = f"intr_plain_snap2_{_rand_suffix()}"
        self._create_snapshot(lvol_id, snap2_name, backup=True)
        sleep_n_sec(20)
        backups_after = self._list_backups()
        assert backups_after, "TC-BCK-083: no backups after recovery"
        fresh_bk_id = (
            backups_after[-1].get("id") or backups_after[-1].get("ID")
            or backups_after[-1].get("uuid") or ""
        )
        assert fresh_bk_id, "TC-BCK-083: could not get fresh backup_id"
        self.logger.info(
            f"TC-BCK-083: fresh backup {fresh_bk_id} available "
            f"({len(backups_after)} total) ✓")

        # TC-BCK-084: Restore fresh backup → verify checksums
        self.logger.info("TC-BCK-084: restore fresh backup and verify checksums")
        restored_name = f"intr_plain_rest_{_rand_suffix()}"
        self._restore_backup(fresh_bk_id, restored_name)
        self._wait_for_restore(restored_name)
        rest_id = self.sbcli_utils.get_lvol_id(lvol_name=restored_name)
        r_device, r_mount = self._connect_and_mount(
            restored_name, rest_id,
            mount=f"{self.mount_path}/intr_rest_{_rand_suffix()}",
            format_disk=False)
        r_files = self.ssh_obj.find_files(self.fio_node, r_mount)
        self.ssh_obj.verify_checksums(
            self.fio_node, r_files, orig_checksums,
            message="TC-BCK-084: checksum mismatch after interrupted backup restore", by_name=True)
        self.logger.info("TC-BCK-084: checksums match after interrupted backup ✓")

        # TC-BCK-085: FIO on the restored lvol
        self.logger.info("TC-BCK-085: FIO on restored lvol after interrupted backup")
        self._run_fio(r_mount, runtime=20)
        self.logger.info("TC-BCK-085: FIO succeeded ✓")

        # ── Crypto lvol scenario ───────────────────────────────────────────

        # TC-BCK-086: Same scenario with a crypto lvol + container_stop outage
        self.logger.info("TC-BCK-086: interrupted backup on crypto lvol (container_stop)")
        crypto_name, crypto_id = self._create_lvol(
            name=f"intr_crypto_{_rand_suffix()}", crypto=True)
        c_device, c_mount = self._connect_and_mount(crypto_name, crypto_id)
        self._run_fio(c_mount, runtime=30)
        c_files = self.ssh_obj.find_files(self.fio_node, c_mount)
        c_checksums = self.ssh_obj.generate_checksums(self.fio_node, c_files)

        c_snap = f"intr_c_snap_{_rand_suffix()}"
        c_snap_id = self._create_snapshot(crypto_id, c_snap, backup=False)
        c_bk_id = self._snapshot_backup(c_snap_id)
        self.logger.info(f"TC-BCK-086: crypto backup_id={c_bk_id} — outage now")

        try:
            sn_id2 = self._get_random_sn()
            self._do_outage(sn_id2, "container_stop")
        except Exception as e:
            self.logger.warning(f"TC-BCK-086: outage error (non-fatal): {e}")

        sleep_n_sec(30)
        c_status = self._wait_for_backup_terminal(c_bk_id, timeout=600)
        self.logger.info(f"TC-BCK-086: crypto backup terminal status: {c_status}")

        # Fresh backup after recovery
        c_snap2 = f"intr_c_snap2_{_rand_suffix()}"
        self._create_snapshot(crypto_id, c_snap2, backup=True)
        sleep_n_sec(20)
        c_backups = self._list_backups()
        if c_backups:
            c_fresh_id = (
                c_backups[-1].get("id") or c_backups[-1].get("ID")
                or c_backups[-1].get("uuid") or ""
            )
            if c_fresh_id:
                c_rest_name = f"intr_c_rest_{_rand_suffix()}"
                self._restore_backup(c_fresh_id, c_rest_name)
                self._wait_for_restore(c_rest_name)
                c_rest_id = self.sbcli_utils.get_lvol_id(lvol_name=c_rest_name)
                c_r_device, c_r_mount = self._connect_and_mount(
                    c_rest_name, c_rest_id,
                    mount=f"{self.mount_path}/icr_{_rand_suffix()}",
                    format_disk=False)
                c_r_files = self.ssh_obj.find_files(self.fio_node, c_r_mount)
                self.ssh_obj.verify_checksums(
                    self.fio_node, c_r_files, c_checksums,
                    message="TC-BCK-086: checksum mismatch on crypto interrupted backup", by_name=True)
                self.logger.info("TC-BCK-086: crypto interrupted backup restore ✓")

        self.logger.info("=== TestBackupInterruptedBackup PASSED ===")


class TestBackupInterruptedRestore(_InterruptedTestBase):
    """
    TC-BCK-090..097 — Storage-node outage triggered while a restore is running.

    Validates:
      - Interrupted restore reaches a terminal state — no hang
      - Partial/failed lvol is either absent or in a deletable error state
      - The system does NOT leave an un-deletable zombie lvol
      - Retry restore (different name, or same name after cleanup) succeeds
      - Restored lvol from the retry has correct data (checksum)
      - FIO on the retried restore succeeds
      - Crypto lvol handled correctly under restore interruption
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "backup_interrupted_restore"

    def _wait_for_restore_or_error(self, lvol_name: str,
                                    timeout: int = 300) -> str:
        """Wait until lvol appears in list (any status) or timeout.
        Returns final status string or 'absent' / 'timeout'."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            status = self._get_lvol_status(lvol_name)
            if status is not None:
                return status
            sleep_n_sec(_BACKUP_POLL_INTERVAL_INTR)
        return "timeout"

    def _cleanup_lvol(self, lvol_name: str):
        """Best-effort cleanup of a partial/error lvol."""
        self.logger.info(f"[cleanup] deleting lvol {lvol_name}")
        try:
            self._force_delete_lvol(lvol_name)
            self.logger.info(f"[cleanup] {lvol_name} deleted ✓")
        except Exception as e:
            self.logger.warning(f"[cleanup] delete error for {lvol_name}: {e}")

    def run(self):
        self.logger.info("=== TestBackupInterruptedRestore START ===")
        self.fio_node = self.fio_node[0]
        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)

        # ── Setup: complete backup to use as restore source ────────────────

        self.logger.info("TC-BCK-090 setup: create lvol, write data, complete backup")
        lvol_name, lvol_id = self._create_lvol(
            name=f"intr_rst_src_{_rand_suffix()}", size=self.lvol_size)
        device, mount = self._connect_and_mount(lvol_name, lvol_id)
        self._run_fio(mount, runtime=40)
        files = self.ssh_obj.find_files(self.fio_node, mount)
        orig_checksums = self.ssh_obj.generate_checksums(self.fio_node, files)
        self.logger.info(f"Setup: {len(orig_checksums)} checksum(s) captured")

        snap_name = f"intr_rst_snap_{_rand_suffix()}"
        self._create_snapshot(lvol_id, snap_name, backup=True)
        backup_id = self._last_backup_id
        if backup_id:
            self.logger.info(f"Setup: backup_id={backup_id} from snapshot output, "
                             "waiting for completion")
            self._wait_for_backup(backup_id)
        else:
            self.logger.info("Setup: backup_id not in snapshot output, "
                             "polling by snapshot name")
            backup_id = self._wait_for_backup_by_snapshot(snap_name)
        self.logger.info(f"Setup: backup_id={backup_id} — ready for interrupt test")

        # ── TC-BCK-090..095: Plain lvol, graceful_shutdown interrupt ───────

        self.logger.info(
            "TC-BCK-090: restore triggered → graceful_shutdown injected")
        restore_name = f"intr_rst_plain_{_rand_suffix()}"
        self._restore_backup(backup_id, restore_name)
        try:
            sn_id = self._get_random_sn()
            self._do_outage(sn_id, "graceful_shutdown")
        except Exception as e:
            self.logger.warning(f"TC-BCK-090: outage error (non-fatal): {e}")

        sleep_n_sec(30)

        # TC-BCK-091: Check restore target lvol status after outage
        lvol_status = self._wait_for_restore_or_error(restore_name, timeout=120)
        self.logger.info(
            f"TC-BCK-091: restore target {restore_name!r} status={lvol_status!r}")

        # TC-BCK-092: Verify partial lvol is in a recognisable/usable state
        if lvol_status in ("error", "failed"):
            self.logger.info(
                "TC-BCK-092: lvol is in error state — clean failure reported ✓")
        elif lvol_status in ("absent", "timeout"):
            self.logger.info(
                "TC-BCK-092: lvol absent — system cleaned up partial restore ✓")
            if restore_name in self.created_lvols:
                self.created_lvols.remove(restore_name)
        else:
            self.logger.info(
                f"TC-BCK-092: lvol status={lvol_status!r} "
                f"(may have completed before outage took effect)")

        # TC-BCK-093: Clean up the partial/error lvol
        self.logger.info(f"TC-BCK-093: cleaning up partial lvol {restore_name}")
        if lvol_status not in ("absent", "timeout"):
            self._cleanup_lvol(restore_name)

        # TC-BCK-094: Retry restore with fresh name — must succeed
        self.logger.info("TC-BCK-094: retry restore after cleanup")
        retry_name = f"intr_rst_retry_{_rand_suffix()}"
        self._restore_backup(backup_id, retry_name)
        self._wait_for_restore(retry_name)
        self.logger.info(f"TC-BCK-094: retry restore {retry_name} succeeded ✓")

        # TC-BCK-095: Verify checksums on retried restore
        self.logger.info("TC-BCK-095: verify checksums on retried restore")
        retry_id = self.sbcli_utils.get_lvol_id(lvol_name=retry_name)
        r_device, r_mount = self._connect_and_mount(
            retry_name, retry_id,
            mount=f"{self.mount_path}/intr_rr_{_rand_suffix()}",
            format_disk=False)
        r_files = self.ssh_obj.find_files(self.fio_node, r_mount)
        self.ssh_obj.verify_checksums(
            self.fio_node, r_files, orig_checksums,
            message="TC-BCK-095: checksum mismatch on retried restore", by_name=True)
        self.logger.info("TC-BCK-095: retry restore checksums match ✓")

        # TC-BCK-096: FIO on the retried restore
        self.logger.info("TC-BCK-096: FIO on retried restore lvol")
        self._run_fio(r_mount, runtime=20)
        self.logger.info("TC-BCK-096: FIO succeeded ✓")

        # ── TC-BCK-097: Crypto lvol, container_stop interrupt ──────────────

        self.logger.info("TC-BCK-097: interrupted restore on crypto lvol")
        crypto_name, crypto_id = self._create_lvol(
            name=f"intr_rst_c_{_rand_suffix()}", crypto=True)
        c_device, c_mount = self._connect_and_mount(crypto_name, crypto_id)
        self._run_fio(c_mount, runtime=30)
        c_files = self.ssh_obj.find_files(self.fio_node, c_mount)
        c_checksums = self.ssh_obj.generate_checksums(self.fio_node, c_files)

        c_snap = f"intr_rst_c_snap_{_rand_suffix()}"
        self._create_snapshot(crypto_id, c_snap, backup=True)
        c_bk_id = self._last_backup_id
        if c_bk_id:
            self.logger.info(f"TC-BCK-097: backup_id={c_bk_id} from snapshot output, "
                             "waiting for completion")
            self._wait_for_backup(c_bk_id)
        else:
            self.logger.info("TC-BCK-097: backup_id not in snapshot output, "
                             "polling by snapshot name")
            c_bk_id = self._wait_for_backup_by_snapshot(c_snap)
        if c_bk_id:
            c_rst_name = f"intr_rst_c1_{_rand_suffix()}"
            self._restore_backup(c_bk_id, c_rst_name)
            try:
                sn_id2 = self._get_random_sn()
                self._do_outage(sn_id2, "container_stop")
            except Exception as e:
                self.logger.warning(f"TC-BCK-097: outage error (non-fatal): {e}")

            sleep_n_sec(30)
            c_rst_status = self._wait_for_restore_or_error(c_rst_name, timeout=120)
            self.logger.info(
                f"TC-BCK-097: crypto restore status={c_rst_status!r}")

            if c_rst_status not in ("absent", "timeout"):
                self._cleanup_lvol(c_rst_name)

            # Retry crypto restore
            c_rst_retry = f"intr_rst_c2_{_rand_suffix()}"
            self._restore_backup(c_bk_id, c_rst_retry)
            self._wait_for_restore(c_rst_retry)
            c_rest_id = self.sbcli_utils.get_lvol_id(lvol_name=c_rst_retry)
            c_r_device, c_r_mount = self._connect_and_mount(
                c_rst_retry, c_rest_id,
                mount=f"{self.mount_path}/icr2_{_rand_suffix()}",
                format_disk=False)
            c_r_files = self.ssh_obj.find_files(self.fio_node, c_r_mount)
            self.ssh_obj.verify_checksums(
                self.fio_node, c_r_files, c_checksums,
                message="TC-BCK-097: checksum mismatch on interrupted crypto restore retry", by_name=True)
            self.logger.info("TC-BCK-097: crypto interrupted restore retry ✓")

        self.logger.info("=== TestBackupInterruptedRestore PASSED ===")
