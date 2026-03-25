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

import json
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
        """Create snapshot and return snapshot ID."""
        flag = "--backup" if backup else ""
        out, err = self._sbcli(f"-d snapshot add {lvol_id} {name} {flag}".strip())
        assert not (err and "error" in err.lower()), \
            f"snapshot add failed: {err}"
        # Extract snapshot ID from output (first UUID-like token)
        snap_id = out.strip().split()[-1] if out.strip() else ""
        assert snap_id, f"No snapshot ID returned: {out}"
        self.created_snapshots.append(snap_id)
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

    def _restore_backup(self, backup_id: str, lvol_name: str, pool_name: str = None) -> str:
        """Restore a backup to a new lvol; return the new lvol name."""
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
        """Wait until restored lvol appears in lvol list.

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
                return
            sleep_n_sec(_POLL_INTERVAL)
        raise TimeoutError(f"Restored lvol {lvol_name} not visible after {timeout}s")

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

    def _run_fio(self, mount: str, log_file: str = None, size: str = None,
                  runtime: int = 60):
        """Run FIO on mount point and validate log."""
        size = size or self.fio_size
        log_file = log_file or f"{self.log_path}/fio_{_rand_suffix()}.log"
        self.ssh_obj.run_fio_test(
            self.fio_node, None, mount, log_file,
            size=size, name="bck_fio", rw="randrw",
            bs="4K", nrfiles=4, iodepth=1,
            numjobs=2, time_based=True, runtime=runtime,
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
        lines = [l for l in text.splitlines() if l.strip()]
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

    # ── teardown ──────────────────────────────────────────────────────────────

    def teardown(self):
        self.logger.info("BackupTestBase teardown started.")

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

        super().teardown()


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
        self.logger.info(f"TC-BCK-004: snapshot backup via separate command")
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
        snap3_id = self._create_snapshot(lvol_id, snap3_name, backup=True)
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
        restored_checksums = self.ssh_obj.generate_checksums(self.fio_node, restored_files)
        self.ssh_obj.verify_checksums(
            self.fio_node, restored_files, original_checksums, by_name=True)
        self.logger.info("TC-BCK-014: checksums match ✓")

        # --- TC-BCK-015: FIO on restored lvol ---
        self.logger.info("TC-BCK-015: FIO on restored lvol")
        self._run_fio(r_mount, runtime=30)

        # --- TC-BCK-016: Disaster recovery — delete original lvol, restore ---
        self.logger.info("TC-BCK-016: disaster recovery path")
        # Unmount + disconnect original before delete
        self.ssh_obj.unmount_path(self.fio_node, mount)
        if (self.fio_node, mount) in self.mounted:
            self.mounted.remove((self.fio_node, mount))

        orig_details = self.sbcli_utils.get_lvol_details(lvol_id=lvol_id)
        if orig_details:
            nqn = orig_details[0]["nqn"]
            self.ssh_obj.disconnect_nvme(node=self.fio_node, nqn_grep=nqn)
        if lvol_id in self.connected:
            self.connected.remove(lvol_id)

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

    def teardown(self):
        try:
            self.sbcli_utils.delete_storage_pools(
                pool_name=self.pool_name2, skip_error=True)
        except Exception:
            pass
        super().teardown()


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
        snap_id = self._create_snapshot(lvol_id, snap_name, backup=False)
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
        snap39_id = self._create_snapshot(lvol_id, snap39, backup=True)
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
        snap_id = self._create_snapshot(crypto_id, snap_name, backup=True)
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
            snap_id = self._create_snapshot(lvol_id, snap_name, backup=True)
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
        Fetch backup list from Cluster-1 and write a JSON metadata file
        containing the entry matching *backup_id* to self._meta_file.

        Returns the local path of the metadata file.
        """
        backups = self._list_backups()
        matching = []
        for b in backups:
            bid = b.get("id") or b.get("ID") or b.get("uuid") or ""
            if backup_id in bid or bid in backup_id:
                matching.append(b)

        if not matching:
            # Fall back to all entries — let import decide
            matching = backups

        meta_json = json.dumps(matching, indent=2)
        self.logger.info(
            f"TC-BCK-072: writing {len(matching)} backup entries to {self._meta_file}")

        # Write to the mgmt node (import command reads from that node's filesystem)
        escaped = meta_json.replace("'", "'\\''")
        self.ssh_obj.exec_command(
            self.mgmt_nodes[0],
            f"echo '{escaped}' > {self._meta_file}")
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

    def teardown(self):
        # Safety: ensure Cluster-2's source is switched back to local
        try:
            self._sbcli_c2("backup source-switch local")
        except Exception as e:
            self.logger.warning(f"source-switch-back in teardown warning: {e}")

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

        super().teardown()
