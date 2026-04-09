# simplyblock E2E & Functional Test Plan

> Generated: 2026-03-27
> Scope: All CLI commands, API operations, and integration scenarios
> Tracks **what to test**, **how to automate it**, and **which platform it runs on**

---

## Legend

| Tag | Meaning |
|-----|---------|
| `[Both]` | Runs on Docker **and** K8s (with k8s_test flag) |
| `[Docker]` | Docker/bare-metal only — uses SSH system ops not available in K8s |
| `[K8s]` | Kubernetes only — uses kubectl/pod operations |
| `[Auto]` | Fully automatable within existing framework |
| `[Partial]` | Automatable but requires special env setup or extension |
| `[Manual]` | Requires hardware/hypervisor access; hard to automate |

---

## Table of Contents
1. [Current Coverage Summary](#1-current-coverage-summary)
2. [Platform & Automation Summary](#2-platform--automation-summary)
3. [Functional Test Plan — by Resource](#3-functional-test-plan--by-resource)
4. [E2E Scenario Test Plan](#4-e2e-scenario-test-plan)
5. [Stress & Continuous Test Plan](#5-stress--continuous-test-plan)
6. [K8s-Specific Test Plan](#6-k8s-specific-test-plan)
7. [Automation Roadmap](#7-automation-roadmap)
8. [Test File Mapping](#8-test-file-mapping)

---

## 1. Current Coverage Summary

### Already Covered ✅
| Area | Test File(s) | Platform | Automatable |
|------|-------------|----------|-------------|
| LVOL create/delete/list | Most e2e tests | `[Both]` | `[Auto]` |
| LVOL connect/mount/FIO | Most e2e tests | `[Both]` | `[Auto]` |
| LVOL resize | `single_node_resize.py` | `[Both]` | `[Auto]` |
| LVOL snapshot + clone | `cloning_and_snapshot/`, `single_node_outage.py`, `single_node_failure.py` | `[Both]` | `[Auto]` |
| LVOL QoS (BW + IOPS limits) | `single_node_qos.py` | `[Both]` | `[Auto]` |
| LVOL security (allowed hosts, DHCHAP, crypto) | `security/test_lvol_security.py` | `[Both]` | `[Auto]` |
| LVOL migration | `data_migration/data_migration_ha_fio.py` | `[Both]` | `[Auto]` |
| Storage pool create/delete/list | Most e2e tests | `[Both]` | `[Auto]` |
| Snapshot backup/restore (S3) | `backup/test_backup_restore.py` | `[Both]` | `[Partial]` |
| Single node outage/failure | `single_node_outage.py`, `single_node_failure.py` | `[Both]` ✅ fixed | `[Auto]` |
| Single node reboot | `single_node_reboot.py` | `[Docker]` | `[Auto]` |
| HA single node outage/failure | Same files, HA classes | `[Both]` ✅ fixed | `[Auto]` |
| Add nodes during FIO | `add_node_fio_run.py` | `[Both]` | `[Auto]` |
| Management node reboot | `mgmt_restart_fio_run.py` | `[Docker]` | `[Auto]` |
| VM host reboot | `single_node_vm_reboot.py` | `[Docker]` | `[Manual]` |
| Restart node on another host | `reboot_on_another_node_fio_run.py` | `[Docker]` | `[Manual]` |
| Multi-node HA failover (stress) | `continuous_failover_ha*.py` | `[Both]` | `[Auto]` |
| K8s multi-outage stress | `continuous_failover_ha_k8s.py` | `[K8s]` | `[Auto]` |
| Namespace failover stress | `continuous_failover_ha_namespace.py` | `[K8s]` | `[Auto]` |
| Major upgrade | `upgrade_tests/major_upgrade.py` | `[Both]` | `[Partial]` |
| Journal device node restart | `ha_journal/lvol_journal_device_node_restart.py` | `[Both]` | `[Auto]` |
| Batch LVOL limits | `batch_lvol_limit.py` | `[Both]` | `[Auto]` |

### Not Covered / Gaps ❌
- Control plane add/remove/list (no dedicated test)
- Storage node: suspend/resume dedicated test
- Storage node: port-list, port-io-stats
- Storage node: check, check-device
- Storage node: add-device, remove-device, restart-device, set-failed-device
- Storage node: make-primary, new-device-from-failed
- Storage node: repair-lvstore
- LVOL inflate
- LVOL get-capacity, get-io-stats (validation of returned values)
- LVOL add-host / remove-host standalone test (covered only in security)
- LVOL get-secret standalone test
- Cluster: delete, change-name, complete-expand
- Cluster: cancel-task, get-subtasks
- Cluster: get-capacity, get-io-stats, get-logs validation
- Storage pool: enable / disable / set attributes
- Storage pool: get-capacity, get-io-stats
- Snapshot: backup (standalone, not via backup test suite)
- Backup: cross-cluster restore (requires dual cluster)
- Backup: policy retention with expiry
- Negative/error cases for most resources
- Out-of-capacity / limit enforcement
- Concurrent operation conflicts
- Multi-fabric (TCP vs RDMA switching)
- Large volume sizes (TB range)
- History / time-window queries for stats

---

## 2. Platform & Automation Summary

This section gives a one-stop view of every planned test, its platform support, and whether it can be automated.

### 2.1 Existing Tests — Platform Matrix

| Test Class | File | `[Docker]` | `[K8s]` | Notes |
|-----------|------|:---:|:---:|-------|
| `TestLvolFioNpcsCustom/0/1/2` | `single_node_multi_fio_perf.py` | ✅ | ✅ | |
| `TestSingleNodeOutage` | `single_node_outage.py` | ✅ | ✅ | Fixed |
| `TestHASingleNodeOutage` | `single_node_outage.py` | ✅ | ✅ | Fixed |
| `TestSingleNodeFailure` | `single_node_failure.py` | ✅ | ✅ | Fixed |
| `TestHASingleNodeFailure` | `single_node_failure.py` | ✅ | ✅ | Fixed |
| `TestSingleNodeResizeLvolCone` | `single_node_resize.py` | ✅ | ✅ | Fixed |
| `TestLvolFioQOSBW/IOPS` | `single_node_qos.py` | ✅ | ✅ | |
| `TestSingleNodeReboot/HA` | `single_node_reboot.py` | ✅ | ❌ | Uses SSH reboot |
| `TestRebootNodeHost` | `single_node_vm_reboot.py` | ✅ | ❌ | Hypervisor reboot |
| `TestRestartNodeOnAnotherHost` | `reboot_on_another_node_fio_run.py` | ✅ | ❌ | SSH deploy |
| `TestMgmtNodeReboot` | `mgmt_restart_fio_run.py` | ✅ | ❌ | SSH reboot |
| `TestAddNodesDuringFioRun` | `add_node_fio_run.py` | ✅ | ✅ | |
| `TestAddK8sNodesDuringFioRun` | `add_node_fio_run.py` | ❌ | ✅ | K8s-native |
| `TestManyLvolSameNode` | `multi_lvol_run_fio.py` | ✅ | ✅ | |
| `TestMultiFioSnapshotDowntime` | `multi_node_crash_fio_clone.py` | ✅ | ✅ | |
| `TestBatchLVOLsLimit` | `batch_lvol_limit.py` | ✅ | ✅ | |
| `TestMultiLvolFio` | `multi_lvol_snapshot_fio.py` | ✅ | ✅ | |
| `TestDeviceNodeRestart` | `ha_journal/lvol_journal_device_node_restart.py` | ✅ | ✅ | |
| `FioWorkloadTest` | `data_migration/data_migration_ha_fio.py` | ✅ | ✅ | |
| All backup tests | `backup/test_backup_restore.py` | ✅ | ❓ needs verify | S3 access required |
| All security tests | `security/test_lvol_security.py` | ✅ | ❓ needs verify | |
| `RandomFailoverTest` | `continuous_failover_ha.py` | ✅ | ✅ | |
| `RandomK8sMultiOutageFailoverTest` | `continuous_failover_ha_k8s.py` | ❌ | ✅ | K8s-native |
| `RandomMultiClientFailoverNamespaceTest` | `continuous_failover_ha_namespace.py` | ❌ | ✅ | K8s namespaces |
| `RandomMultiClientMultiFailoverTest` | `continuous_failover_ha_multi_outage.py` | ✅ | ✅ | |
| `RandomRDMAFailoverTest/Multi` | `continuous_failover_ha_rdma*.py` | ✅ | ❓ needs verify | RDMA HW required |
| `TestMajorUpgrade` | `upgrade_tests/major_upgrade.py` | ✅ | ✅ | |

### 2.2 Planned New Tests — Platform & Automation Matrix

| Planned Test File | `[Docker]` | `[K8s]` | `[Auto]` | Blocker for Automation |
|------------------|:---:|:---:|:---:|----------------------|
| `test_lvol_basic.py` | ✅ | ✅ | ✅ | — |
| `test_lvol_stats.py` | ✅ | ✅ | ✅ | — |
| `test_lvol_negative.py` | ✅ | ✅ | ✅ | — |
| `test_lvol_inflate.py` | ✅ | ✅ | ✅ | — |
| `test_lvol_migration_load.py` | ✅ | ✅ | ✅ | — |
| `test_snapshot_negative.py` | ✅ | ✅ | ✅ | — |
| `test_pool_attributes.py` | ✅ | ✅ | ✅ | — |
| `test_pool_enable_disable.py` | ✅ | ✅ | ✅ | — |
| `test_pool_stats.py` | ✅ | ✅ | ✅ | — |
| `test_pool_negative.py` | ✅ | ✅ | ✅ | — |
| `test_node_suspend_resume.py` | ✅ | ✅ | ✅ | — |
| `test_storage_node_devices.py` | ✅ | ✅ | ⚠️ | `add-device` needs test-device mode or real disk |
| `test_storage_node_stats.py` | ✅ | ✅ | ✅ | — |
| `test_storage_node_ports.py` | ✅ | ✅ | ✅ | — |
| `test_storage_node_primary.py` | ✅ | ✅ | ✅ | — |
| `test_storage_node_repair.py` | ✅ | ✅ | ⚠️ | Needs controlled lvstore corruption |
| `test_cluster_stats.py` | ✅ | ✅ | ✅ | — |
| `test_cluster_tasks.py` | ✅ | ✅ | ✅ | — |
| `test_cluster_secret.py` | ✅ | ✅ | ✅ | — |
| `test_cluster_expand.py` | ✅ | ✅ | ⚠️ | Needs spare node in cluster env |
| `test_cluster_lifecycle.py` | ✅ | ✅ | ⚠️ | `cluster delete` is destructive; isolated env needed |
| `test_cluster_full_lifecycle.py` | ✅ | ✅ | ⚠️ | Full teardown/recreate; isolated env |
| `test_control_plane.py` | ✅ | ✅ | ⚠️ | `cp remove` risky in shared env |
| `test_qos_class.py` | ✅ | ✅ | ✅ | — |
| `test_qos_enforcement.py` | ✅ | ✅ | ✅ | — |
| `test_device_failure_recovery.py` | ✅ | ✅ | ⚠️ | `add-device` needs test-device or real disk |
| `test_pool_disable_io.py` | ✅ | ✅ | ✅ | — |
| `test_security_full.py` | ✅ | ✅ | ✅ | — |
| `test_batch_limits.py` | ✅ | ✅ | ✅ | — |
| `test_negative_cases.py` | ✅ | ✅ | ✅ | — |
| `k8s/test_k8s_pod_restart.py` | ❌ | ✅ | ✅ | K8s-only |
| `k8s/test_k8s_node_drain.py` | ❌ | ✅ | ✅ | K8s-only |
| `continuous_migration_stress.py` | ✅ | ✅ | ✅ | — |
| `continuous_device_failure.py` | ✅ | ✅ | ⚠️ | Same as device add/remove |
| `continuous_pool_disable_failover.py` | ✅ | ✅ | ✅ | — |

> **⚠️ = Automatable with env setup** — not blocked by framework, only by infrastructure requirements.

---

## 3. Functional Test Plan — by Resource

### 3.1 LVOL (Logical Volumes)

#### TC-LVOL-001 — Basic CRUD `[Both]` `[Auto]`
- **Create** lvol with default params → assert present in list
- **List** → verify name, ID
- **Get** → verify size, pool, node assignment
- **Delete** → assert absent from list
- **Automate in**: new `test_lvol_basic.py`

#### TC-LVOL-002 — Create with all parameters `[Both]` `[Auto]`
- distr_ndcs, distr_npcs, distr_bs, distr_chunk_bs combinations
- Explicit host_id placement
- Size units (M, G, T)
- **Automate in**: `test_lvol_basic.py`

#### TC-LVOL-003 — Connect / Disconnect `[Both]` `[Auto]`
- Connect string returned → NVMe connect on client
- Device appears in lsblk
- Disconnect → device gone
- Multiple simultaneous connects (primary + secondary)
- **Automate in**: `test_lvol_basic.py`

#### TC-LVOL-004 — Resize `[Both]` `[Auto]`
- Resize up while FIO running
- Resize of clone
- Verify capacity reporting after resize
- **Covered by**: `single_node_resize.py` — extend capacity check

#### TC-LVOL-005 — QoS `[Both]` `[Auto]`
- `qos-set` mid-run bandwidth change
- `qos-set` mid-run IOPS change
- Remove QoS limits
- **Covered by**: `single_node_qos.py` — add mid-run change test

#### TC-LVOL-006 — Inflate ❌ NEW `[Both]` `[Auto]`
- Create thin-provisioned lvol
- Write data to fill it
- Run `lvol inflate`
- Verify capacity changes
- **Automate in**: new `test_lvol_inflate.py`

#### TC-LVOL-007 — Migration ❌ PARTIAL `[Both]` `[Auto]`
- Migrate lvol to different storage node while FIO runs
- Validate no I/O errors during migration
- Verify final placement via `lvol get`
- Cancel migration mid-flight
- List in-progress migrations
- **Covered by**: `data_migration_ha_fio.py` — add cancel + list validation

#### TC-LVOL-008 — Allowed Hosts `[Both]` `[Auto]`
- add-host / remove-host
- get-secret
- Covered in security tests — ensure standalone test exists too

#### TC-LVOL-009 — Capacity & IO Stats ❌ NEW `[Both]` `[Auto]`
- `lvol get-capacity` → verify used/total/provisioned values
- `lvol get-io-stats` → verify IOPS/BW values while FIO running
- History window query (`--history`)
- **Automate in**: new `test_lvol_stats.py`

#### TC-LVOL-010 — Negative Cases ❌ NEW `[Both]` `[Auto]`
- Create lvol with name collision → expect error
- Create on offline node → expect error
- Delete non-existent lvol → expect error
- Resize to smaller than current data → verify behavior
- Connect non-existent lvol → expect error
- **Automate in**: `test_lvol_negative.py`

---

### 3.2 Snapshots & Clones

#### TC-SNAP-001 — Basic snapshot lifecycle `[Both]` `[Auto]`
- Create snapshot from online lvol
- Verify in snapshot list
- Delete snapshot
- Snapshot name uniqueness enforcement

#### TC-SNAP-002 — Clone lifecycle `[Both]` `[Auto]`
- Create clone from snapshot
- Connect / mount clone
- Verify data matches snapshot point-in-time
- Delete clone, then delete snapshot

#### TC-SNAP-003 — Multiple clones from one snapshot `[Both]` `[Auto]` PARTIAL
- **Covered by**: `single_lvol_multi_clone.py` — verify 25+ clones

#### TC-SNAP-004 — Snapshot during FIO `[Both]` `[Auto]` PARTIAL
- Create snapshot while FIO writing
- Checksum clone vs original
- **Covered by**: `multi_lvol_snapshot_fio.py`

#### TC-SNAP-005 — Snapshot backup to S3 ❌ NEW `[Both]` `[Partial]`
- Create snapshot
- Run `snapshot backup` (manual S3 backup)
- Verify backup listed
- Restore from backup
- Requires S3 bucket configured
- **Automate in**: extend `backup/test_backup_restore.py`

#### TC-SNAP-006 — Negative cases ❌ NEW `[Both]` `[Auto]`
- Snapshot of deleted lvol
- Clone from deleted snapshot
- Snapshot name collision
- Delete snapshot with existing clone → expect error or handle
- **Automate in**: `test_snapshot_negative.py`

---

### 3.3 Storage Pool

#### TC-POOL-001 — Basic CRUD `[Both]` `[Auto]` (covered)
- Create / list / delete pool
- Verify in list after create, absent after delete

#### TC-POOL-002 — Pool attributes & limits ❌ NEW `[Both]` `[Auto]`
- Create with `max_rw_iops`, `max_rw_mbytes`, `max_r_mbytes`, `max_w_mbytes`
- Verify limits enforced on lvols in pool
- `pool set` to change attributes mid-run
- **Automate in**: `test_pool_attributes.py`

#### TC-POOL-003 — Enable / Disable ❌ NEW `[Both]` `[Auto]`
- Create pool, create lvol
- `pool disable` → verify lvols reject new connections or I/O
- `pool enable` → verify lvols resume
- **Automate in**: `test_pool_enable_disable.py`

#### TC-POOL-004 — Capacity & IO Stats ❌ NEW `[Both]` `[Auto]`
- `pool get-capacity` → verify values
- `pool get-io-stats` → verify while FIO running
- **Automate in**: `test_pool_stats.py`

#### TC-POOL-005 — Negative Cases ❌ NEW `[Both]` `[Auto]`
- Delete pool with active lvols → expect error
- Create pool with duplicate name → expect error
- **Automate in**: `test_pool_negative.py`

---

### 3.4 Storage Nodes

#### TC-SN-001 — Node lifecycle `[Both]` `[Auto]` (partially covered)
- List nodes → verify count
- Get node → verify fields
- Node check → verify health status

#### TC-SN-002 — Suspend / Resume ❌ PARTIAL `[Both]` `[Auto]`
- Suspend node while FIO runs
- Verify node status = suspended, lvols still online
- Resume node → verify node back online, no I/O errors
- **Automate in**: `test_storage_node_suspend_resume.py`

#### TC-SN-003 — Port operations ❌ NEW `[Both]` `[Auto]`
- `sn port-list` → verify ports returned
- `sn port-io-stats` → verify while I/O active
- **Automate in**: `test_storage_node_ports.py`

#### TC-SN-004 — Device operations ❌ NEW `[Both]` `[Partial]`
- `sn list-devices` → verify all devices listed — `[Auto]`
- `sn get-device` → verify device details — `[Auto]`
- `sn restart-device` → verify device comes back online — `[Auto]`
- `sn check-device` → verify health response — `[Auto]`
- `sn add-device` → add new device, verify in list — `[Partial]` (test-device mode or real disk)
- `sn remove-device` → logically remove device — `[Partial]`
- `sn set-failed-device` → set device failed, verify failover — `[Auto]`
- **Automate in**: `test_storage_node_devices.py`

#### TC-SN-005 — make-primary ❌ NEW `[Both]` `[Auto]`
- Identify secondary node
- `sn make-primary` → verify node becomes primary
- Verify secondary relationship updates
- **Automate in**: `test_storage_node_primary.py`

#### TC-SN-006 — repair-lvstore ❌ NEW `[Both]` `[Partial]`
- Trigger lvstore inconsistency (controlled)
- Run `sn repair-lvstore`
- Verify cluster returns to healthy state
- Requires controlled way to corrupt lvstore state
- **Automate in**: `test_storage_node_repair.py`

#### TC-SN-007 — IO / Capacity Stats ❌ NEW `[Both]` `[Auto]`
- `sn get-io-stats` → verify while FIO running
- `sn get-capacity` → verify values
- `sn get-capacity-device` → per-device capacity
- `sn get-io-stats-device` → per-device IO stats
- **Automate in**: `test_storage_node_stats.py`

---

### 3.5 Cluster

#### TC-CLUSTER-001 — Status / Info `[Both]` `[Auto]` (partially covered)
- `cluster status` → verify all nodes online
- `cluster get` → verify cluster fields
- `cluster check` → verify health

#### TC-CLUSTER-002 — Expand cluster ❌ NEW `[Both]` `[Partial]`
- Add new storage node while cluster running + FIO
- `cluster complete-expand` → create lvstore on new node
- Verify new node appears in storage nodes list
- Requires spare node in test environment
- **Automate in**: `test_cluster_expand.py`

#### TC-CLUSTER-003 — Delete cluster ❌ NEW `[Both]` `[Partial]`
- Clean up all lvols, pools
- `cluster delete` → verify cluster removed
- Destructive — needs isolated env (not shared cluster)
- **Automate in**: `test_cluster_lifecycle.py`

#### TC-CLUSTER-004 — Change name ❌ NEW `[Both]` `[Auto]`
- `cluster change-name` → verify new name in list
- **Automate in**: `test_cluster_lifecycle.py`

#### TC-CLUSTER-005 — Tasks ❌ NEW `[Both]` `[Auto]`
- List tasks during/after operations
- Cancel in-progress task
- Get subtasks for a task
- Verify task completion
- **Automate in**: `test_cluster_tasks.py`

#### TC-CLUSTER-006 — Capacity & IO Stats ❌ NEW `[Both]` `[Auto]`
- `cluster get-capacity` → verify values
- `cluster get-io-stats` → verify while FIO running
- History window queries
- **Automate in**: `test_cluster_stats.py`

#### TC-CLUSTER-007 — Secret management ❌ NEW `[Both]` `[Auto]`
- `cluster get-secret` → verify secret returned
- `cluster update-secret` → update and verify
- Connect with new secret
- **Automate in**: `test_cluster_secret.py`

---

### 3.6 Control Plane

#### TC-CP-001 — List control plane nodes ❌ NEW `[Both]` `[Auto]`
- `cp list` → verify management nodes listed
- **Automate in**: `test_control_plane.py`

#### TC-CP-002 — Remove control plane node ❌ NEW `[Both]` `[Partial]`
- In multi-mgmt setup: remove one mgmt node
- Verify cluster still operational
- Requires multi-mgmt-node environment
- **Automate in**: `test_control_plane.py`

---

### 3.7 Backup & Restore

#### TC-BCK-001 to 010 — Basic Backup ✅ `[Both]` `[Partial]` (S3 required)
#### TC-BCK-011 to 018 — Data Integrity ✅ `[Both]` `[Partial]`
#### TC-BCK-020 to 028 — Backup Policy ✅ `[Both]` `[Partial]`
#### TC-BCK-030 to 040 — Negative Cases ✅ `[Both]` `[Auto]`
#### TC-BCK-050 to 055 — Crypto Lvol Backup ✅ `[Both]` `[Partial]`
#### TC-BCK-060 to 063 — Custom Geometry Backup ✅ `[Both]` `[Partial]`
#### TC-BCK-070 to 076 — Cross-cluster Restore ✅ `[Both]` `[Partial]` (dual cluster required)

#### TC-BCK-080 — Policy with retention/expiry ❌ NEW `[Both]` `[Partial]`
- Create policy with short retention (e.g. 1 hour)
- Verify old backups are removed after expiry
- **Automate in**: extend backup stress tests

#### TC-BCK-081 — Backup during node outage ❌ NEW `[Both]` `[Auto]`
- Start continuous backup
- Trigger node outage
- Verify backup completes or resumes after recovery
- **Automate in**: extend `BackupStressTcpFailover`

---

### 3.8 QoS Classes

#### TC-QOS-001 — QoS class lifecycle ❌ NEW `[Both]` `[Auto]`
- `qos add` → create class
- `qos list` → verify in list
- `qos delete` → verify removed
- Attach QoS class to lvol
- Verify I/O limits enforced
- **Automate in**: `test_qos_class.py`

---

## 4. E2E Scenario Test Plan

### 4.1 Full Cluster Lifecycle ❌ NEW `[Both]` `[Partial]`
**File**: `test_cluster_full_lifecycle.py`
```
1. Create cluster
2. Add 3 storage nodes
3. Create pool
4. Create 5 lvols, connect, mount
5. Run FIO on all
6. Take snapshots, create clones
7. Run FIO on clones
8. Validate checksums
9. Delete clones → delete snapshots → delete lvols
10. Delete pool
11. Remove storage nodes
12. Delete cluster
```
> Requires isolated/single-use cluster env. K8s needs `cluster delete` support.

---

### 4.2 Storage Node Device Failure & Recovery ❌ NEW `[Both]` `[Partial]`
**File**: `test_device_failure_recovery.py`
```
1. Create lvols across all nodes
2. Run FIO on all lvols
3. Set one device to failed state (sn set-failed-device)
4. Verify lvols remain online, FIO continues
5. Check node/device status changes in event log
6. Add new device (sn add-device) or restore device
7. Verify data integrity via checksums
```
> `sn add-device` requires test-device mode or real hardware device.

---

### 4.3 Pool Disable During I/O ❌ NEW `[Both]` `[Auto]`
**File**: `test_pool_disable_io.py`
```
1. Create pool, lvols, run FIO
2. Disable pool (pool disable)
3. Verify FIO behavior (error or pause)
4. Re-enable pool (pool enable)
5. Verify FIO resumes, checksums match
```

---

### 4.4 LVOL Migration Under Load ❌ PARTIAL `[Both]` `[Auto]`
**File**: `test_lvol_migration_load.py`
```
1. Create 10 lvols across 3 nodes, run FIO
2. Migrate 3 lvols to different nodes (lvol migrate)
3. During migration: verify migration list shows in-progress
4. Verify no I/O errors during migration
5. Cancel one migration mid-flight (migrate-cancel)
6. Verify cancelled lvol returns to original node
7. Validate final checksums
```

---

### 4.5 Cluster Expand During FIO ❌ NEW `[Both]` `[Partial]`
**File**: `test_cluster_expand.py`
```
1. Start with 3 storage nodes + FIO running on lvols
2. Add new storage node (sn add-node)
3. cluster complete-expand
4. Verify new node in storage node list
5. Create new lvols on new node
6. Verify FIO on existing lvols uninterrupted
7. Validate placement distribution
```
> Requires spare node in environment.

---

### 4.6 QoS Enforcement End-to-End ❌ NEW `[Both]` `[Auto]`
**File**: `test_qos_enforcement.py`
```
1. Create pool with pool-level QoS limits
2. Create lvols, run FIO at high load
3. Verify actual IOPS/BW capped at pool limits
4. Create QoS class, attach to lvol
5. Verify per-lvol limits enforced
6. Mid-test: adjust QoS limits via qos-set
7. Verify new limits take effect within 30s
```

---

### 4.7 Security: Full Crypto + Allowed Hosts ❌ PARTIAL `[Both]` `[Auto]`
**File**: `test_security_full.py` (extend existing security tests)
```
1. Create encrypted lvol (crypto + key1 + key2)
2. Add specific allowed host NQN
3. Connect from allowed host → verify success
4. Connect from non-allowed host → verify failure
5. get-secret → verify credentials
6. Snapshot of encrypted lvol
7. Clone from encrypted snapshot → verify encryption inherited
8. Remove allowed host
9. Verify connection revoked
```

---

### 4.8 Node Suspend / Resume with Active I/O ❌ NEW `[Both]` `[Auto]`
**File**: `test_node_suspend_resume.py`
```
1. Create lvols across 3 nodes, run FIO
2. Suspend one node (no lvols on it)
3. Verify: node=suspended, lvols=online, FIO continues
4. Attempt to create new lvol on suspended node → expect error
5. Resume node
6. Verify: node=online
7. Create lvol on resumed node → success
8. Validate FIO ran uninterrupted
```

---

### 4.9 Batch Operations & Limits ❌ PARTIAL `[Both]` `[Auto]`
**File**: extend `batch_lvol_limit.py`
```
1. Create max allowed lvols per pool (hit limit)
2. Attempt to create one more → expect clear error
3. Delete batch of lvols
4. Re-create to verify limit reset
5. Test max snapshots per node
6. Test max concurrent connections per lvol
```

---

### 4.10 Negative / Error Handling Suite ❌ NEW `[Both]` `[Auto]`
**File**: `test_negative_cases.py`
```
Resource: LVOL
- Create with invalid size (0, negative, non-numeric)
- Create on non-existent pool
- Create on offline node
- Delete while FIO running
- Connect already-connected lvol
- Resize to same size (idempotent?)

Resource: Pool
- Delete with active lvols
- Create duplicate name
- Disable non-existent pool

Resource: Snapshot
- Snapshot non-existent lvol
- Clone from non-existent snapshot
- Delete snapshot with clones

Resource: Node
- Restart already-online node
- Suspend already-offline node
- Resume online node (idempotent?)

Resource: Cluster
- Delete cluster with active lvols
```

---

## 5. Stress & Continuous Test Plan

### 5.1 Continuous Failover with Pool Disable ❌ NEW `[Both]` `[Auto]`
**File**: `stress_test/continuous_pool_disable_failover.py`
- Run continuous FIO on 20 lvols
- Randomly: failover nodes, disable/re-enable pool, resize lvols, take snapshots
- Validate no data loss, FIO error rate < 1%

### 5.2 Mixed Geometry Stress ❌ PARTIAL `[Both]` `[Auto]`
**File**: `continuous_failover_ha_geometry.py` (exists, extend)
- Mix ndcs=1/2/4 with npcs=0/1/2 in same cluster
- Run continuous failover
- Verify different geometry lvols behave independently

### 5.3 Continuous Migration Stress ❌ NEW `[Both]` `[Auto]`
**File**: `stress_test/continuous_migration_stress.py`
- Create 30 lvols, run FIO
- Continuously migrate lvols between nodes in background
- Randomly fail one node
- Validate: no I/O errors, all migrations complete or gracefully cancelled

### 5.4 Backup + Failover Stress ❌ PARTIAL `[Both]` `[Partial]`
**File**: `stress_test/continuous_backup_failover.py` (exists, extend)
- Parallel: continuous backups to S3 + node failovers
- Verify: no backup corruption, all restores validate checksums
- Duration: 6+ hours; S3 required

### 5.5 Rapid Device Failure Stress ❌ NEW `[Both]` `[Partial]`
**File**: `stress_test/continuous_device_failure.py`
- Set devices to failed state rapidly (every 5 min)
- Verify lvols remain online via secondary
- Add new device to recover
- Repeat 20+ times
- `add-device` needs test-device mode or real disk

---

## 6. K8s-Specific Test Plan

### 6.1 K8s Compatibility Matrix

| Test | Status | Notes |
|------|--------|-------|
| `TestLvolFioNpcsCustom` | ✅ Compatible | |
| `TestSingleNodeOutage` | ✅ Fixed | snap/clone guarded |
| `TestHASingleNodeOutage` | ✅ Fixed | restart_node guarded |
| `TestSingleNodeFailure` | ✅ Fixed | stop_spdk → restart_spdk_pod |
| `TestHASingleNodeFailure` | ✅ Fixed | stop_spdk → restart_spdk_pod |
| `TestSingleNodeResizeLvolCone` | ✅ Fixed | list_files guarded |
| `TestSingleNodeReboot` | ❌ Docker-only | SSH `reboot_node` — no K8s equiv |
| `TestRebootNodeHost` | ❌ Docker-only | Hypervisor reboot |
| `TestRestartNodeOnAnotherHost` | ❌ Docker-only | SSH deploy |
| `TestMgmtNodeReboot` | ❌ Docker-only | SSH reboot |
| `TestAddK8sNodesDuringFioRun` | ✅ K8s-native | |
| All backup tests | ❓ Verify | S3 + K8s env needed |
| All security tests | ❓ Verify | |
| `RandomK8sMultiOutageFailoverTest` | ✅ K8s-native | |
| `RandomMultiClientFailoverNamespaceTest` | ✅ K8s-native | |

### 6.2 K8s Pod Restart Scenarios ❌ NEW `[K8s]` `[Auto]`
**File**: `k8s/test_k8s_pod_restart.py`
```
1. Delete SPDK pod (restart_spdk_pod) → verify node goes offline/back online
2. Delete management pod → verify cluster recovers
3. Delete multiple pods simultaneously
4. Verify FIO continues through pod restarts
```

### 6.3 K8s Namespace Isolation ❌ PARTIAL `[K8s]` `[Auto]`
**File**: extend `continuous_failover_ha_namespace.py`
```
1. Create lvols in different namespaces
2. Verify namespace isolation (cross-namespace access should fail)
3. Failover nodes while namespaced lvols active
4. Verify per-namespace QoS enforcement
```

### 6.4 K8s Node Drain ❌ NEW `[K8s]` `[Auto]`
**File**: `k8s/test_k8s_node_drain.py`
```
1. Run FIO on lvols across all nodes
2. kubectl drain storage node (evict pods gracefully)
3. Verify SPDK pod migrates / lvols failover
4. kubectl uncordon node
5. Verify node re-joins cluster
```

### 6.5 Items Needing K8s Verification
All tests marked `❓` above need a verification pass with `--run_k8s=True`:
- backup tests (S3 access from within cluster)
- security tests (NQN and DHCHAP flow in K8s)
- RDMA tests (hardware-dependent)

---

## 7. Automation Roadmap

### Phase 1 — High Priority `[Both]` (immediately automatable)
> Target: 2 weeks | All `[Auto]`, all `[Both]`

| Test File | TCs Covered | Priority | Platform | Automatable |
|-----------|-------------|----------|----------|-------------|
| `test_lvol_basic.py` | TC-LVOL-001, 002, 003 | P0 | `[Both]` | ✅ `[Auto]` |
| `test_lvol_stats.py` | TC-LVOL-009 | P0 | `[Both]` | ✅ `[Auto]` |
| `test_lvol_negative.py` | TC-LVOL-010 | P0 | `[Both]` | ✅ `[Auto]` |
| `test_snapshot_negative.py` | TC-SNAP-006 | P0 | `[Both]` | ✅ `[Auto]` |
| `test_pool_attributes.py` | TC-POOL-002 | P1 | `[Both]` | ✅ `[Auto]` |
| `test_pool_enable_disable.py` | TC-POOL-003 | P1 | `[Both]` | ✅ `[Auto]` |
| `test_pool_negative.py` | TC-POOL-005 | P0 | `[Both]` | ✅ `[Auto]` |
| `test_node_suspend_resume.py` | TC-SN-002, Scenario 4.8 | P1 | `[Both]` | ✅ `[Auto]` |
| `test_negative_cases.py` | Scenario 4.10 | P0 | `[Both]` | ✅ `[Auto]` |
| `test_pool_disable_io.py` | Scenario 4.3 | P1 | `[Both]` | ✅ `[Auto]` |

### Phase 2 — Medium Priority `[Both]`
> Target: 1 month | Mostly `[Auto]`

| Test File | TCs Covered | Priority | Platform | Automatable |
|-----------|-------------|----------|----------|-------------|
| `test_storage_node_stats.py` | TC-SN-007 | P2 | `[Both]` | ✅ `[Auto]` |
| `test_storage_node_ports.py` | TC-SN-003 | P2 | `[Both]` | ✅ `[Auto]` |
| `test_storage_node_devices.py` | TC-SN-004 | P1 | `[Both]` | ⚠️ `[Partial]` — device add/remove needs hw or test mode |
| `test_cluster_stats.py` | TC-CLUSTER-006 | P2 | `[Both]` | ✅ `[Auto]` |
| `test_cluster_tasks.py` | TC-CLUSTER-005 | P1 | `[Both]` | ✅ `[Auto]` |
| `test_cluster_secret.py` | TC-CLUSTER-007 | P1 | `[Both]` | ✅ `[Auto]` |
| `test_qos_class.py` | TC-QOS-001 | P1 | `[Both]` | ✅ `[Auto]` |
| `test_qos_enforcement.py` | Scenario 4.6 | P1 | `[Both]` | ✅ `[Auto]` |
| `test_lvol_inflate.py` | TC-LVOL-006 | P2 | `[Both]` | ✅ `[Auto]` |
| `test_lvol_migration_load.py` | TC-LVOL-007, Scenario 4.4 | P1 | `[Both]` | ✅ `[Auto]` |
| `test_pool_stats.py` | TC-POOL-004 | P2 | `[Both]` | ✅ `[Auto]` |

### Phase 3 — Complex / Env-dependent
> Target: 2 months

| Test File | TCs Covered | Priority | Platform | Automatable |
|-----------|-------------|----------|----------|-------------|
| `test_cluster_full_lifecycle.py` | Scenario 4.1 | P1 | `[Both]` | ⚠️ `[Partial]` — needs isolated env |
| `test_cluster_expand.py` | TC-CLUSTER-002, Scenario 4.5 | P1 | `[Both]` | ⚠️ `[Partial]` — needs spare node |
| `test_cluster_lifecycle.py` | TC-CLUSTER-003, 004 | P2 | `[Both]` | ⚠️ `[Partial]` — `cluster delete` is destructive |
| `test_device_failure_recovery.py` | Scenario 4.2 | P1 | `[Both]` | ⚠️ `[Partial]` — device add needs hw/test mode |
| `test_security_full.py` | Scenario 4.7 | P1 | `[Both]` | ✅ `[Auto]` |
| `test_batch_limits.py` | Scenario 4.9 | P2 | `[Both]` | ✅ `[Auto]` |
| `test_control_plane.py` | TC-CP-001, 002 | P3 | `[Both]` | ⚠️ `[Partial]` — `cp remove` risky in shared env |
| `test_storage_node_primary.py` | TC-SN-005 | P2 | `[Both]` | ✅ `[Auto]` |
| `test_storage_node_repair.py` | TC-SN-006 | P2 | `[Both]` | ⚠️ `[Partial]` — needs controlled lvstore corruption |

### Phase 4 — Stress Tests
> Target: 3 months

| Stress Test | Priority | Platform | Automatable |
|-------------|----------|----------|-------------|
| `continuous_migration_stress.py` | P1 | `[Both]` | ✅ `[Auto]` |
| `continuous_device_failure.py` | P1 | `[Both]` | ⚠️ `[Partial]` — device add/remove |
| `continuous_pool_disable_failover.py` | P2 | `[Both]` | ✅ `[Auto]` |
| Extend `continuous_failover_ha_geometry.py` | P2 | `[Both]` | ✅ `[Auto]` |

### Phase 5 — K8s Parity
> Target: Ongoing alongside Phases 1-4

| Task | Priority | Platform | Automatable |
|------|----------|----------|-------------|
| Verify all Phase 1 tests pass with `--run_k8s` | P0 | `[K8s]` | ✅ `[Auto]` |
| `k8s/test_k8s_pod_restart.py` | P1 | `[K8s]` | ✅ `[Auto]` |
| `k8s/test_k8s_node_drain.py` | P1 | `[K8s]` | ✅ `[Auto]` |
| Verify backup tests with `--run_k8s` | P1 | `[K8s]` | ⚠️ `[Partial]` — S3 from cluster |
| Verify security tests with `--run_k8s` | P1 | `[K8s]` | ✅ `[Auto]` |
| Verify RDMA tests on K8s | P2 | `[K8s]` | ⚠️ `[Partial]` — RDMA hardware required |

---

## 8. Test File Mapping

### Existing Test Files
```
e2e/e2e_tests/
├── backup/
│   └── test_backup_restore.py        [Both][Partial] TC-BCK-001..081
├── cloning_and_snapshot/
│   ├── lvol_batch_clone.py           [Both][Auto]   TC-SNAP-003
│   ├── multi_lvol_snapshot_fio.py    [Both][Auto]   TC-SNAP-004
│   └── single_lvol_multi_clone.py    [Both][Auto]   TC-SNAP-003
├── data_migration/
│   └── data_migration_ha_fio.py      [Both][Auto]   TC-LVOL-007
├── ha_journal/
│   └── lvol_journal_device_node_restart.py [Both][Auto]
├── security/
│   └── test_lvol_security.py         [Both][Auto]   TC-LVOL-008
├── upgrade_tests/
│   └── major_upgrade.py              [Both][Partial]
├── add_node_fio_run.py               [Both][Auto]   TC-CLUSTER-002 (partial)
├── batch_lvol_limit.py               [Both][Auto]   Scenario 4.9
├── mgmt_restart_fio_run.py           [Docker][Auto]
├── multi_lvol_run_fio.py             [Both][Auto]
├── multi_node_crash_fio_clone.py     [Both][Auto]
├── reboot_on_another_node_fio_run.py [Docker][Manual]
├── single_node_failure.py            [Both][Auto]   ✅ K8s fixed
├── single_node_multi_fio_perf.py     [Both][Auto]   TC-LVOL-002 (perf)
├── single_node_outage.py             [Both][Auto]   ✅ K8s fixed
├── single_node_qos.py                [Both][Auto]   TC-LVOL-005
├── single_node_reboot.py             [Docker][Auto]
├── single_node_resize.py             [Both][Auto]   ✅ K8s fixed
└── single_node_vm_reboot.py          [Docker][Manual]
```

### New Test Files to Create (by phase/priority)
```
e2e/e2e_tests/
│
│ ── Phase 1 (P0/P1) ── [Both][Auto] ──────────────────────────────
├── test_lvol_basic.py                P0 [Both][Auto]
├── test_lvol_stats.py                P0 [Both][Auto]
├── test_lvol_negative.py             P0 [Both][Auto]
├── test_snapshot_negative.py         P0 [Both][Auto]
├── test_pool_attributes.py           P1 [Both][Auto]
├── test_pool_enable_disable.py       P1 [Both][Auto]
├── test_pool_negative.py             P0 [Both][Auto]
├── test_node_suspend_resume.py       P1 [Both][Auto]
├── test_pool_disable_io.py           P1 [Both][Auto]
├── test_negative_cases.py            P0 [Both][Auto]
│
│ ── Phase 2 (P1/P2) ── [Both][Auto/Partial] ───────────────────────
├── test_lvol_inflate.py              P2 [Both][Auto]
├── test_lvol_migration_load.py       P1 [Both][Auto]
├── test_storage_node_devices.py      P1 [Both][Partial]
├── test_storage_node_stats.py        P2 [Both][Auto]
├── test_storage_node_ports.py        P2 [Both][Auto]
├── test_cluster_stats.py             P2 [Both][Auto]
├── test_cluster_tasks.py             P1 [Both][Auto]
├── test_cluster_secret.py            P1 [Both][Auto]
├── test_qos_class.py                 P1 [Both][Auto]
├── test_qos_enforcement.py           P1 [Both][Auto]
├── test_pool_stats.py                P2 [Both][Auto]
│
│ ── Phase 3 (P1/P2) ── [Both][Partial] ────────────────────────────
├── test_cluster_full_lifecycle.py    P1 [Both][Partial]
├── test_cluster_expand.py            P1 [Both][Partial]
├── test_cluster_lifecycle.py         P2 [Both][Partial]
├── test_device_failure_recovery.py   P1 [Both][Partial]
├── test_security_full.py             P1 [Both][Auto]
├── test_batch_limits.py              P2 [Both][Auto]
├── test_control_plane.py             P3 [Both][Partial]
├── test_storage_node_primary.py      P2 [Both][Auto]
├── test_storage_node_repair.py       P2 [Both][Partial]
│
│ ── Phase 5 (K8s-only) ─────────────────────────────────────────────
└── k8s/
    ├── test_k8s_pod_restart.py       P1 [K8s][Auto]
    └── test_k8s_node_drain.py        P1 [K8s][Auto]

e2e/stress_test/
│ ── Phase 4 ────────────────────────────────────────────────────────
├── continuous_migration_stress.py    P1 [Both][Auto]
├── continuous_device_failure.py      P1 [Both][Partial]
└── continuous_pool_disable_failover.py P2 [Both][Auto]
```

---

## Notes

- All new test classes must inherit from `TestClusterBase` and set `self.test_name`
- All new tests targeting `[Both]` must use `if self.k8s_test:` guards for any SSH/system ops
- Register new tests in `e2e/__init__.py` in the appropriate `get_*_tests()` function
- Negative tests should use `requests.exceptions.HTTPError` assertions for API errors
- Stats tests should run FIO in background thread while validating stats in main thread
- Each test class should be runnable standalone: `python stress.py --testname <TestName>`
- `[Partial]` tests need infrastructure prerequisites documented in the test's class docstring
- Docker-only tests (`[Docker]`) should NOT be registered in `get_stress_tests()` if run in K8s CI
