# S3 Backup / Restore — Test Plan

**Feature**: S3-based snapshot backup, delta chain management, backup policies, and restore
**Scope**: E2E tests + Stress tests (TCP, RDMA, custom ndcs/npcs, crypto lvols)

---

## Feature Summary

### Cluster bootstrap with S3

```
sbcli cluster create --use-backup s3.json
```

`s3.json` fields:
| Field | Description |
|---|---|
| `access_key_id` | S3 / MinIO access key |
| `secret_access_key` | S3 / MinIO secret key |
| `local_endpoint` | MinIO URL, e.g. `http://192.168.10.164:9000` |
| `snapshot_backups` | Enable snapshot-level S3 backup |
| `with_compression` | Enable compression for backup data |
| `secondary_target` | Secondary backup target index |
| `local_testing` | Use local-testing mode |

### Backup CLI

| Command | Description |
|---|---|
| `sbcli snapshot add <lvol_id> <name> --backup` | Create snapshot and immediately back it up to S3 |
| `sbcli snapshot backup <snapshot_id>` | Back up an existing snapshot to S3 |
| `sbcli backup list [--cluster-id]` | List all backups |
| `sbcli backup delete <lvol_id>` | Delete ALL backups for a logical volume |
| `sbcli backup restore <backup_id> [--lvol-name] [--pool]` | Restore a backup to a new lvol |
| `sbcli backup import <metadata.json>` | Import backup metadata from a JSON file |

### Policy CLI

| Command | Description |
|---|---|
| `sbcli backup policy-add <cluster_id> <name> [--versions N] [--age 1d] [--schedule ...]` | Create policy |
| `sbcli backup policy-remove <policy_id>` | Remove policy |
| `sbcli backup policy-list [--cluster-id]` | List all policies |
| `sbcli backup policy-attach <policy_id> <pool\|lvol> <target_id>` | Attach policy to pool or lvol |
| `sbcli backup policy-detach <policy_id> <pool\|lvol> <target_id>` | Detach policy |

### Delta chain behaviour

- Backups are **delta-chained**: only the first backup is a full base; subsequent ones are deltas.
- Individual mid-chain backups **cannot be deleted** independently — deleting merges to the next one.
- After ~3 generations or 2 hours, the service **auto-merges** the two oldest, keeping ≤ 3 entries total (1 base + 2 deltas).
- **Deleting from the top or middle** merges into the next backup.
- **Deleting the last backup** removes it outright.

---

## Terminology

| Term | Meaning |
|---|---|
| Base backup | First full S3 backup for an lvol |
| Delta backup | Incremental backup relative to the previous backup |
| Chain | Ordered set of backups for one lvol |
| Retention | Policy-driven pruning by `--versions` or `--age` |
| Crypto lvol | AES-256-XTS encrypted lvol (`--encrypt`) |
| ndcs / npcs | Data-copy count / parity-copy count for erasure coding |

---

## Test Cases — E2E

### TC-BCK-001..009 · Basic Positive

| ID | Title | Steps | Expected Result | Automated | Class |
|---|---|---|---|---|---|
| TC-BCK-001 | Snapshot + backup flag | `snapshot add <lvol_id> <name> --backup` | Snapshot created; backup task triggered | ✅ | `TestBackupBasicPositive` |
| TC-BCK-002 | Backup appears in list | `backup list` after TC-BCK-001 | ≥ 1 backup entry visible | ✅ | `TestBackupBasicPositive` |
| TC-BCK-003 | Backup list fields | Inspect each row in `backup list` | Each entry has an ID field | ✅ | `TestBackupBasicPositive` |
| TC-BCK-004 | `snapshot backup` on existing snapshot | `snapshot backup <snap_id>` | backup_id returned; no error | ✅ | `TestBackupBasicPositive` |
| TC-BCK-005 | Multiple backups for same lvol | Create 2 snapshots + backup each | `backup list` shows ≥ 2 entries | ✅ | `TestBackupBasicPositive` |
| TC-BCK-006 | Delete local snapshot, backup persists | `snapshot delete`; then `backup list` | Backup survives snapshot deletion | ✅ | `TestBackupBasicPositive` |
| TC-BCK-007 | 3rd backup triggers delta chain | 3 snapshot-backups on same lvol | Backup count ≤ 4 (merge window) | ✅ | `TestBackupBasicPositive` |
| TC-BCK-008 | `backup list --cluster-id` filter | `backup list --cluster-id <id>` | Output filtered; no error | ✅ | `TestBackupBasicPositive` |
| TC-BCK-009 | `policy-list` empty returns gracefully | `backup policy-list` with no policies | No error; empty result or message | ✅ | `TestBackupBasicPositive` |

---

### TC-BCK-011..017 · Restore & Data Integrity

| ID | Title | Steps | Expected Result | Automated | Class |
|---|---|---|---|---|---|
| TC-BCK-011 | Checksums before backup | Write FIO data; `md5sum` on all files before snapshot | Checksum map captured | ✅ | `TestBackupRestoreDataIntegrity` |
| TC-BCK-012 | Restore with custom lvol-name | `backup restore <id> --lvol-name <name>` | Lvol `<name>` appears in `lvol list` | ✅ | `TestBackupRestoreDataIntegrity` |
| TC-BCK-013 | Restored lvol is connectable | `volume connect` restored lvol; NVMe block device appears | Block device created | ✅ | `TestBackupRestoreDataIntegrity` |
| TC-BCK-014 | Checksum matches original | `md5sum` all files on restored lvol | All checksums match pre-backup values | ✅ | `TestBackupRestoreDataIntegrity` |
| TC-BCK-015 | FIO on restored lvol | Mount restored lvol; run FIO randrw | FIO completes with 0 errors | ✅ | `TestBackupRestoreDataIntegrity` |
| TC-BCK-016 | Disaster recovery — delete original, restore | Delete source lvol; `backup restore` | Restore succeeds; data matches | ✅ | `TestBackupRestoreDataIntegrity` |
| TC-BCK-017 | Restore to second pool | `backup restore --pool <pool2>` | Restored lvol appears in pool2 | ✅ | `TestBackupRestoreDataIntegrity` |

---

### TC-BCK-020..028 · Backup Policy

| ID | Title | Steps | Expected Result | Automated | Class |
|---|---|---|---|---|---|
| TC-BCK-020 | policy-add with --versions and --age | `backup policy-add ... --versions 3 --age 1d` | policy_id returned | ✅ | `TestBackupPolicy` |
| TC-BCK-021 | policy-list shows new policy | `backup policy-list` | Policy ID visible | ✅ | `TestBackupPolicy` |
| TC-BCK-022 | policy-attach to pool | `backup policy-attach <id> pool <pool_id>` | No error | ✅ | `TestBackupPolicy` |
| TC-BCK-023 | Snapshot in pool-attached policy triggers backup | Create lvol in pool; `snapshot add` | Backup appears automatically | ✅ | `TestBackupPolicy` |
| TC-BCK-024 | policy-attach to lvol directly | `backup policy-attach <id> lvol <lvol_id>` | No error | ✅ | `TestBackupPolicy` |
| TC-BCK-025 | Retention: 4 backups with versions=3 | Create 4 snapshot-backups | Count stays bounded | ✅ | `TestBackupPolicy` |
| TC-BCK-026 | policy-detach from lvol | `backup policy-detach`; then create snapshot | No new auto-backup generated | ✅ | `TestBackupPolicy` |
| TC-BCK-027 | policy-remove | `backup policy-remove <id>`; `policy-list` | Policy no longer listed | ✅ | `TestBackupPolicy` |
| TC-BCK-028 | policy-add with --schedule | `backup policy-add ... --schedule "15m,4 60m,11 24h,7"` | Policy created; no error | ✅ | `TestBackupPolicy` |

---

### TC-BCK-030..039 · Negative / Edge Cases

| ID | Title | Steps | Expected Result | Automated | Class |
|---|---|---|---|---|---|
| TC-BCK-030 | Restore invalid backup_id | `backup restore 00000000-... --lvol-name x` | Error returned | ✅ | `TestBackupNegative` |
| TC-BCK-031 | `snapshot backup` non-existent snap | `snapshot backup 00000000-...` | Error returned | ✅ | `TestBackupNegative` |
| TC-BCK-032 | policy-attach invalid target_id | `policy-attach <pid> lvol 00000000-...` | Error returned | ✅ | `TestBackupNegative` |
| TC-BCK-033 | policy-attach invalid target_type | `policy-attach <pid> invalid_type <id>` | CLI usage error | ✅ | `TestBackupNegative` |
| TC-BCK-034 | policy-remove non-existent policy | `policy-remove 00000000-...` | Error returned | ✅ | `TestBackupNegative` |
| TC-BCK-035 | backup import malformed JSON | `backup import /tmp/bad.json` | Error returned | ✅ | `TestBackupNegative` |
| TC-BCK-036 | backup import empty array | `backup import /tmp/empty.json` (`[]`) | 0 imported; no crash | ✅ | `TestBackupNegative` |
| TC-BCK-037 | Duplicate snapshot backup | `snapshot backup <same_id>` twice | Idempotent or clear error; no crash | ✅ | `TestBackupNegative` |
| TC-BCK-038 | `backup list` when S3 not configured | `backup list` on cluster without --use-backup | Empty result or informative message | ⚠️ Manual | — |
| TC-BCK-039 | Restore to existing lvol name | `backup restore ... --lvol-name <existing>` | Conflict error | ✅ | `TestBackupNegative` |

---

### TC-BCK-050..055 · Crypto Lvol Backup/Restore

| ID | Title | Steps | Expected Result | Automated | Class |
|---|---|---|---|---|---|
| TC-BCK-050 | Create crypto lvol | `lvol add --encrypt` | Encrypted lvol created | ✅ | `TestBackupCryptoLvol` |
| TC-BCK-051 | Snapshot + backup of crypto lvol | `snapshot add --backup` on crypto lvol | Backup entry appears | ✅ | `TestBackupCryptoLvol` |
| TC-BCK-052 | Restore crypto backup → new lvol | `backup restore <id>` | Restored lvol appears | ✅ | `TestBackupCryptoLvol` |
| TC-BCK-053 | Restored crypto lvol is connectable | `volume connect` on restored lvol | Block device appears | ✅ | `TestBackupCryptoLvol` |
| TC-BCK-054 | Checksum validation on restored crypto lvol | `md5sum` on restored files | Checksums match original | ✅ | `TestBackupCryptoLvol` |
| TC-BCK-055 | FIO on restored crypto lvol | Mount and run FIO | FIO completes with 0 errors | ✅ | `TestBackupCryptoLvol` |

---

### TC-BCK-070..076 · Cross-Cluster Restore ⚠️ Explicit-only

> **Not included in `get_backup_tests()` or the default E2E run.**
> Run with: `python e2e.py --testname TestBackupCrossClusterRestore`
>
> **Required extra env vars:**
> ```
> CLUSTER2_ID=<uuid>
> CLUSTER2_SECRET=<secret>
> CLUSTER2_API_BASE_URL=<url>
> CLUSTER2_POOL=<pool_name>   # optional; defaults to bck_test_pool
> ```
> Both clusters must share the same S3 / MinIO endpoint.

| ID | Title | Steps | Expected Result | Automated | Class |
|---|---|---|---|---|---|
| TC-BCK-070 | Prerequisites: env vars + clusters reachable | Check CLUSTER2_* vars; verify both APIs respond | No EnvironmentError raised | ✅ | `TestBackupCrossClusterRestore` |
| TC-BCK-071 | Cluster-1: write data + S3 backup | Create lvol → FIO → snapshot + `--backup` → capture checksums | Backup ID returned; checksums recorded | ✅ | `TestBackupCrossClusterRestore` |
| TC-BCK-072 | Export backup metadata from Cluster-1 | Build JSON from `backup list`; write to mgmt node temp file | Metadata file written | ✅ | `TestBackupCrossClusterRestore` |
| TC-BCK-073 | Cluster-2: `backup import` succeeds | `CLUSTER2_* sbcli backup import <file>` | No error; count ≥ 1 imported | ✅ | `TestBackupCrossClusterRestore` |
| TC-BCK-074 | Cluster-2: imported backup visible in `backup list` | `CLUSTER2_* sbcli backup list` | Original backup_id visible | ✅ | `TestBackupCrossClusterRestore` |
| TC-BCK-075 | Cluster-2: `backup restore` creates new lvol | `CLUSTER2_* sbcli backup restore <id> --lvol-name <n>` | Lvol appears in Cluster-2 `lvol list` | ✅ | `TestBackupCrossClusterRestore` |
| TC-BCK-076 | Data integrity: checksum matches Cluster-1 original | Connect restored lvol on FIO node via Cluster-2 connect string; `md5sum` | All checksums match | ✅ | `TestBackupCrossClusterRestore` |

---

### TC-BCK-060..063 · Custom Geometry (ndcs / npcs)

| ID | Title | ndcs/npcs | Automated | Class |
|---|---|---|---|---|
| TC-BCK-060a | Backup/restore ndcs=1 npcs=0 | 1+0 | ✅ | `TestBackupCustomGeometry` |
| TC-BCK-060b | Backup/restore ndcs=2 npcs=1 | 2+1 | ✅ | `TestBackupCustomGeometry` |
| TC-BCK-060c | Backup/restore ndcs=4 npcs=1 | 4+1 | ✅ | `TestBackupCustomGeometry` |

---

## Test Cases — Stress

### TC-BCK-STR-001..005 · Parallel Snapshot-Backups

| ID | Title | Automated | Class |
|---|---|---|---|
| TC-BCK-STR-001 | 6 concurrent lvols, all snapshot-backed up in parallel | ✅ | `BackupStressParallelSnapshots` |
| TC-BCK-STR-002 | All parallel backups appear in `backup list` | ✅ | `BackupStressParallelSnapshots` |
| TC-BCK-STR-003 | Service remains responsive under parallel backup load | ✅ | `BackupStressParallelSnapshots` |
| TC-BCK-STR-004 | Restore from any parallel backup; checksum matches | ✅ | `BackupStressParallelSnapshots` |
| TC-BCK-STR-005 | Rapid 4-snapshot chain: count stays bounded | ✅ | `BackupStressParallelSnapshots` |

---

### TC-BCK-STR-010..015 · Backup with TCP Failover

| ID | Title | Outage Type | Automated | Class |
|---|---|---|---|---|
| TC-BCK-STR-010 | Snapshot-backup triggered during FIO; outage injected | graceful_shutdown | ✅ | `BackupStressTcpFailover` |
| TC-BCK-STR-011 | Backup survives container_stop outage | container_stop | ✅ | `BackupStressTcpFailover` |
| TC-BCK-STR-012 | Backup survives full network interrupt | interface_full_network_interrupt | ✅ | `BackupStressTcpFailover` |
| TC-BCK-STR-013 | Restore after TCP failover cycle succeeds | all types | ✅ | `BackupStressTcpFailover` |
| TC-BCK-STR-014 | Crypto lvol backup survives failover | any | ✅ | `BackupStressTcpFailover` |
| TC-BCK-STR-015 | Custom geometry (ndcs=2 npcs=1) backup survives failover | any | ✅ | `BackupStressTcpFailover` |

---

### TC-BCK-STR-020..025 · Backup with RDMA Failover

| ID | Title | Outage Type | Automated | Class |
|---|---|---|---|---|
| TC-BCK-STR-020 | Same as TCP stress, RDMA fabric | graceful_shutdown | ✅ | `BackupStressRdmaFailover` |
| TC-BCK-STR-021 | RDMA: backup survives container crash | container_stop | ✅ | `BackupStressRdmaFailover` |
| TC-BCK-STR-022 | RDMA: restore after failover | all types | ✅ | `BackupStressRdmaFailover` |
| TC-BCK-STR-023 | RDMA: crypto lvol backup/restore | any | ✅ | `BackupStressRdmaFailover` |
| TC-BCK-STR-024 | RDMA skipped gracefully if not available | — | ✅ | `BackupStressRdmaFailover` |
| TC-BCK-STR-025 | RDMA: multi-outage type rotation | all 4 types | ✅ | `BackupStressRdmaFailover` |

---

### TC-BCK-STR-030..035 · Mixed Crypto + Geometry Concurrent Backup

| ID | Title | Combo | Automated | Class |
|---|---|---|---|---|
| TC-BCK-STR-030 | 5 combos (plain/crypto × ndcs/npcs) backed up concurrently | all 5 | ✅ | `BackupStressCryptoMix` |
| TC-BCK-STR-031 | All concurrent backup_ids returned | all 5 | ✅ | `BackupStressCryptoMix` |
| TC-BCK-STR-032 | Restore each; checksum matches original | all 5 | ✅ | `BackupStressCryptoMix` |
| TC-BCK-STR-033 | Service stable after 5-way concurrent backup | all 5 | ✅ | `BackupStressCryptoMix` |
| TC-BCK-STR-034 | FIO on each restored lvol | all 5 | ✅ | `BackupStressCryptoMix` |
| TC-BCK-STR-035 | Checksum: plain ndcs=4 npcs=1 | 4+1 | ✅ | `BackupStressCryptoMix` |

---

### TC-BCK-STR-040..045 · Policy Retention Under Load

| ID | Title | Automated | Class |
|---|---|---|---|
| TC-BCK-STR-040 | Policy versions=3 attached to lvol | ✅ | `BackupStressPolicyRetention` |
| TC-BCK-STR-041 | 10 rapid snapshot-backups trigger auto-merge | ✅ | `BackupStressPolicyRetention` |
| TC-BCK-STR-042 | Backup count stays bounded after rapid snapshots | ✅ | `BackupStressPolicyRetention` |
| TC-BCK-STR-043 | Restore latest backup after multiple merges | ✅ | `BackupStressPolicyRetention` |
| TC-BCK-STR-044 | After policy detach, snapshots do NOT auto-backup | ✅ | `BackupStressPolicyRetention` |
| TC-BCK-STR-045 | Service stable after 10 merge cycles | ✅ | `BackupStressPolicyRetention` |

---

### TC-BCK-STR-050..055 · Concurrent Restores

| ID | Title | Automated | Class |
|---|---|---|---|
| TC-BCK-STR-050 | 4 simultaneous restore operations | ✅ | `BackupStressRestoreConcurrent` |
| TC-BCK-STR-051 | Each restored lvol has correct data (checksum) | ✅ | `BackupStressRestoreConcurrent` |
| TC-BCK-STR-052 | All restored lvols connectable independently | ✅ | `BackupStressRestoreConcurrent` |
| TC-BCK-STR-053 | FIO on each concurrently restored lvol | ✅ | `BackupStressRestoreConcurrent` |
| TC-BCK-STR-054 | Service stable after 4 concurrent restores | ✅ | `BackupStressRestoreConcurrent` |
| TC-BCK-STR-055 | No cross-contamination between concurrent restores | ✅ | `BackupStressRestoreConcurrent` |

---

## Coverage Summary

| Category | Total TCs | Fully Automated | Manual |
|---|---|---|---|
| Basic positive | 9 | 9 | 0 |
| Restore & data integrity | 7 | 7 | 0 |
| Backup policy | 9 | 9 | 0 |
| Negative / edge cases | 10 | 9 | 1 |
| Crypto lvol | 6 | 6 | 0 |
| Custom geometry | 3 | 3 | 0 |
| Cross-cluster restore *(explicit-only)* | 7 | 7 | 0 |
| **E2E subtotal** | **51** | **50** | **1** |
| Stress: parallel snapshots | 5 | 5 | 0 |
| Stress: TCP failover | 6 | 6 | 0 |
| Stress: RDMA failover | 6 | 6 | 0 |
| Stress: crypto+geometry mix | 6 | 6 | 0 |
| Stress: policy retention | 6 | 6 | 0 |
| Stress: concurrent restores | 6 | 6 | 0 |
| **Stress subtotal** | **35** | **35** | **0** |
| **Grand total** | **86** | **85** | **1** |

---

## Automated Test Class Reference

| Class | File | TCs Covered |
|---|---|---|
| `TestBackupBasicPositive` | `e2e_tests/backup/test_backup_restore.py` | TC-BCK-001..009 |
| `TestBackupRestoreDataIntegrity` | `e2e_tests/backup/test_backup_restore.py` | TC-BCK-011..017 |
| `TestBackupPolicy` | `e2e_tests/backup/test_backup_restore.py` | TC-BCK-020..028 |
| `TestBackupNegative` | `e2e_tests/backup/test_backup_restore.py` | TC-BCK-030..039 |
| `TestBackupCryptoLvol` | `e2e_tests/backup/test_backup_restore.py` | TC-BCK-050..055 |
| `TestBackupCustomGeometry` | `e2e_tests/backup/test_backup_restore.py` | TC-BCK-060..063 |
| `TestBackupCrossClusterRestore` *(explicit-only)* | `e2e_tests/backup/test_backup_restore.py` | TC-BCK-070..076 |
| `BackupStressParallelSnapshots` | `stress_test/continuous_backup_stress.py` | TC-BCK-STR-001..005 |
| `BackupStressTcpFailover` | `stress_test/continuous_backup_stress.py` | TC-BCK-STR-010..015 |
| `BackupStressRdmaFailover` | `stress_test/continuous_backup_stress.py` | TC-BCK-STR-020..025 |
| `BackupStressCryptoMix` | `stress_test/continuous_backup_stress.py` | TC-BCK-STR-030..035 |
| `BackupStressPolicyRetention` | `stress_test/continuous_backup_stress.py` | TC-BCK-STR-040..045 |
| `BackupStressRestoreConcurrent` | `stress_test/continuous_backup_stress.py` | TC-BCK-STR-050..055 |

---

## Known Gaps / Not Yet Automated

| TC | Reason |
|---|---|
| TC-BCK-038 | Requires a second cluster without `--use-backup` configured |
| Middle-chain backup deletion merge | Needs `backup delete` to accept a specific backup_id (currently deletes all for an lvol) |
| `backup list` shows which policy each backup belongs to | CLI output format not yet standardised |
| Verify compressed backup size is smaller | Requires S3 object size inspection (outside CLI scope) |
| Cross-cluster restore delete cleanup | Cluster-2 `lvol delete` uses a different sbcli target; best-effort only in teardown |

---

## Manual Test Execution Notes

**TC-BCK-038** — No S3 configured
On a cluster started **without** `--use-backup`: run `sbcli-dev backup list` and confirm output is either empty or contains a clear "no backup store configured" message (not an unhandled exception).

**Middle-chain backup deletion / merge**
Currently `backup delete` operates on all backups for a given lvol.
When per-backup-ID deletion is added, add test cases verifying:
- Deleting backup[2] of a 4-chain merges into backup[3]
- Deleting backup[1] (last) removes it entirely
- The chain remains valid and restorable after every deletion

---

## Environment Prerequisites

| Requirement | Notes |
|---|---|
| Cluster started with `--use-backup s3.json` | MinIO endpoint reachable from storage nodes |
| `CLUSTER_ID`, `CLUSTER_SECRET`, `API_BASE_URL` env vars set | Standard E2E env |
| FIO node with NVMe-oF support | Same as other E2E tests |
| ≥ 2 storage nodes | For failover stress tests |
| RDMA-capable fabric | For `BackupStressRdmaFailover` only |

---

## Extended Backup Test Cases (TC-BCK-150..190)

### Security Lvol Backup

| ID | Title | Steps | Expected Result | Automated | Class |
|----|-------|-------|-----------------|-----------|-------|
| TC-BCK-150 | DHCHAP+Crypto Lvol Created and FIO Written | 1. Create lvol with crypto=True 2. Connect + mount + FIO | Write succeeds | Yes | TestBackupSecurityLvol |
| TC-BCK-151 | Snapshot With --backup Flag | 1. `snapshot add --backup` | Snapshot created, backup triggered | Yes | TestBackupSecurityLvol |
| TC-BCK-152 | Backup Completes | 1. Poll backup list until 'done' | Backup status 'done' within 300s | Yes | TestBackupSecurityLvol |
| TC-BCK-153 | Backup Restored to New Lvol | 1. `backup restore` to new name | Restored lvol appears in list | Yes | TestBackupSecurityLvol |
| TC-BCK-154 | Restored Lvol Data Integrity | 1. Connect restored lvol 2. Verify checksums | Checksums match original | Yes | TestBackupSecurityLvol |

### Policy Versions=1

| ID | Title | Steps | Expected Result | Automated | Class |
|----|-------|-------|-----------------|-----------|-------|
| TC-BCK-155 | Lvol + Policy versions=1 Created | 1. Create lvol 2. Add policy with versions=1 3. Attach to lvol | Policy attached | Yes | TestBackupPolicyVersionsOne |
| TC-BCK-156 | 3 Backup Cycles Triggered | 1. Snapshot+backup × 3 cycles | All 3 backups complete | Yes | TestBackupPolicyVersionsOne |
| TC-BCK-157 | Only 1 Backup Retained | 1. List backups for lvol | ≤ 2 entries (delta + base chain) | Yes | TestBackupPolicyVersionsOne |
| TC-BCK-158 | Latest Backup Restored | 1. Restore latest backup | Restore completes | Yes | TestBackupPolicyVersionsOne |

### Multiple Policies on Same Lvol

| ID | Title | Steps | Expected Result | Automated | Class |
|----|-------|-------|-----------------|-----------|-------|
| TC-BCK-159 | Two Policies Attached to Same Lvol | 1. Create lvol 2. Add policy_A (versions=2) + policy_B (versions=3) 3. Attach both | Both policies attached | Yes | TestBackupPolicyMultipleOnSameLvol |
| TC-BCK-160 | 2 Backup Cycles With Both Policies | 1. Snapshot+backup × 2 | Backup entries exist | Yes | TestBackupPolicyMultipleOnSameLvol |
| TC-BCK-161 | Detach Policy_A; Policy_B Continues | 1. policy-detach policy_A from lvol | Policy_B still listed | Yes | TestBackupPolicyMultipleOnSameLvol |
| TC-BCK-162 | Restore From Policy_B Chain | 1. Restore latest backup | Restore completes | Yes | TestBackupPolicyMultipleOnSameLvol |
| TC-BCK-163 | Detach Policy_B | 1. policy-detach policy_B | Policy removed | Yes | TestBackupPolicyMultipleOnSameLvol |

### Lvol-Level Policy

| ID | Title | Steps | Expected Result | Automated | Class |
|----|-------|-------|-----------------|-----------|-------|
| TC-BCK-164 | Policy Attached to Lvol_A Only (Not Pool) | 1. Create lvol_A + lvol_B 2. Attach policy only to lvol_A | Policy on lvol_A only | Yes | TestBackupPolicyLvolLevel |
| TC-BCK-165 | Backup Created for Lvol_A | 1. Snapshot+backup for lvol_A | Backup entry exists | Yes | TestBackupPolicyLvolLevel |
| TC-BCK-166 | Lvol_B Has No Backups | 1. List backups filtered by lvol_B | Zero entries | Yes | TestBackupPolicyLvolLevel |
| TC-BCK-167 | Policy Detached From Lvol_A | 1. policy-detach lvol_A | Policy removed | Yes | TestBackupPolicyLvolLevel |

### Resized Lvol Backup

| ID | Title | Steps | Expected Result | Automated | Class |
|----|-------|-------|-----------------|-----------|-------|
| TC-BCK-168 | 5G Lvol FIO + Backup v1 | 1. Create 5G lvol 2. FIO 3. Snapshot+backup | Backup v1 complete | Yes | TestBackupResizedLvol |
| TC-BCK-169 | Resize Lvol to 10G | 1. `resize_lvol(id, "10G")` | No error | Yes | TestBackupResizedLvol |
| TC-BCK-170 | FIO + Backup v2 After Resize | 1. FIO on 10G lvol 2. Snapshot+backup | Backup v2 complete | Yes | TestBackupResizedLvol |
| TC-BCK-171 | Restore v1 and Verify Data | 1. Restore v1 2. Connect + checksum verify | Data matches pre-resize content | Yes | TestBackupResizedLvol |
| TC-BCK-172 | Restore v2 and Verify Data | 1. Restore v2 2. Connect + checksum verify | Data matches post-resize content | Yes | TestBackupResizedLvol |

### Backup List Fields

| ID | Title | Steps | Expected Result | Automated | Class |
|----|-------|-------|-----------------|-----------|-------|
| TC-BCK-173 | Backup Created and Complete | 1. Create lvol + snapshot+backup | backup_id returned | Yes | TestBackupListFields |
| TC-BCK-174 | Backup List Has id + lvol Reference | 1. `backup list` output checked | Entry has id and lvol_name/id | Yes | TestBackupListFields |
| TC-BCK-175 | --cluster-id Filter Works | 1. `backup list --cluster-id <id>` | Entry present in filtered output | Yes | TestBackupListFields |
| TC-BCK-176 | Status Is 'done'/'complete' | 1. Check status field of entry | Status in (done, complete, completed) | Yes | TestBackupListFields |

### Node Restart Compatibility

| ID | Title | Steps | Expected Result | Automated | Class |
|----|-------|-------|-----------------|-----------|-------|
| TC-BCK-177 | Backup Created Before Node Restart | 1. Create lvol + complete backup | backup_id noted | Yes | TestBackupUpgradeCompatibility |
| TC-BCK-178 | Storage Node Shutdown + Restart | 1. Shutdown node 2. Wait offline 3. Restart 4. Wait online | Node back online within 300s | Yes | TestBackupUpgradeCompatibility |
| TC-BCK-179 | Backup Still Present After Restart | 1. List backups; find backup_id | Entry still present | Yes | TestBackupUpgradeCompatibility |
| TC-BCK-180 | Restore Backup After Restart + Data Integrity | 1. Restore backup 2. Verify checksums | Data integrity preserved | Yes | TestBackupUpgradeCompatibility |

### Restore Edge Cases

| ID | Title | Steps | Expected Result | Automated | Class |
|----|-------|-------|-----------------|-----------|-------|
| TC-BCK-181 | Restore With Max-Length Lvol Name | 1. Restore with 31-char name | Restore succeeds or graceful error | Yes | TestBackupRestoreEdgeCases |
| TC-BCK-182 | Restore Without --pool Flag | 1. `backup restore <id> --lvol <name>` (no pool) | Uses source pool; succeeds | Yes | TestBackupRestoreEdgeCases |
| TC-BCK-183 | Restore to Name of Deleted Source Lvol | 1. Delete source lvol 2. Restore to same name | Restore succeeds | Yes | TestBackupRestoreEdgeCases |
| TC-BCK-184 | Restore With Duplicate Lvol Name | 1. Create lvol with same target name 2. Restore with that name | Error or graceful rejection | Yes | TestBackupRestoreEdgeCases |
| TC-BCK-185 | Restore From Non-Existent Backup ID | 1. `backup restore` with fake UUID | Error returned | Yes | TestBackupRestoreEdgeCases |

### Backup Source Switch

| ID | Title | Steps | Expected Result | Automated | Class |
|----|-------|-------|-----------------|-----------|-------|
| TC-BCK-186 | First Backup to Primary Target | 1. Create lvol + backup | First backup_id obtained | Yes | TestBackupSourceSwitch |
| TC-BCK-187 | Secondary Target Availability Check | 1. Check cluster details for secondary_target | Test notes if secondary configured | Yes | TestBackupSourceSwitch |
| TC-BCK-188 | Second Backup Created | 1. Additional FIO + snapshot+backup | Second backup_id obtained | Yes | TestBackupSourceSwitch |
| TC-BCK-189 | First Backup Restorable | 1. Restore first backup 2. Checksum verify | Data integrity confirmed | Yes | TestBackupSourceSwitch |
| TC-BCK-190 | Second Backup Restorable | 1. Restore second backup 2. Checksum verify | Data integrity confirmed | Yes | TestBackupSourceSwitch |
