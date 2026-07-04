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
9. [Deprecated / Uncertain Tests](#9-deprecated--uncertain-tests)

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
- Async replication / DR: replication-start, stop, trigger, status, commit, failback (zero coverage)
- Cluster graceful-shutdown / graceful-startup lifecycle
- Cluster change-name
- Cluster update-fabric (TCP ↔ RDMA switching)
- Failure domain placement (`--enable-failure-domain` + anti-affinity enforcement)
- Node anti-affinity (`--strict-node-anti-affinity` placement validation)
- Namespace / subsystem limits (`max_namespace_per_subsys` enforcement)
- Namespaced lvol placement (child lvol lands on parent's node, shares parent subsystem)
- Multi-client concurrent connect (multiple clients connecting same lvol simultaneously)
- Volume suspend / resume subsystems
- Volume clone-lvol (combined snapshot + clone operation)
- LVOL delete while connected (verify graceful disconnect or error)
- Pool DHCHAP (pool-level `add-host` / `remove-host` standalone test)
- Capacity threshold alerts (warning/critical level triggers at 89%/99%)
- Provisioned capacity overcommit (warning at 250%, critical at 500%)
- QPair / queue-size tuning (cluster-level and per-volume overrides)
- Snapshot replication (`snapshot replication-status`, `delete-replication-only`)
- Node remove with data migration (sn remove — verify data migrated to other nodes)
- Cluster add-replication (assign snapshot replication target cluster)
- KMS / HashiCorp Vault integration test (volume encryption with external KMS)
- RDMA-only cluster (fabric_type=rdma end-to-end)
- Mixed HA types (ha_type per lvol within same cluster)
- Volume priority class assignment and enforcement
- Cluster shared placement (`set-shared-placement` for per-chunk data placement binding)

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

#### TC-SN-002 — Suspend / Resume ❌ PARTIAL `[Both]` `[Auto]` ⚠️ DEPRECATED
- ~~Suspend node while FIO runs~~
- ~~Verify node status = suspended, lvols still online~~
- ~~Resume node → verify node back online, no I/O errors~~
- `sn suspend` / `sn resume` are **DEPRECATED no-ops** in CLI (`cli-reference.yaml`)
- Test verifies deprecation behavior (command succeeds as no-op, node status unchanged)
- **Implemented in**: `test_node_suspend_resume.py` — **commented out in `get_all_tests()`**

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

#### TC-QOS-001 — QoS class lifecycle ❌ NEW `[Both]` `[Auto]` ⚠️ DEPRECATED
- `qos add` → create class
- `qos list` → verify in list
- `qos delete` → verify removed
- Attach QoS class to lvol
- Verify I/O limits enforced
- **Status**: DEPRECATED — QoS class API (`qos add`/`list`/`delete`) not verified as working CLI commands
- **Implemented in**: `test_qos_class.py` — **commented out in `get_all_tests()`**

---

### 3.9 Async Replication / DR

#### TC-REPL-001 — Replication start / stop `[Both]` `[Partial]` ❌ NEW
- Configure replication target cluster (`cluster add-replication`)
- Create lvol, write data
- `volume replication-start` (failover mode) → verify replication begins
- `volume replication-status` → verify source/target, state=replicating
- `volume replication-info` → verify time lag decreasing, outstanding data
- `volume replication-stop` → verify replication stops cleanly
- **Requires**: dual cluster setup (source + target)
- **Automate in**: `test_replication_basic.py`

#### TC-REPL-002 — Replication trigger and commit `[Both]` `[Partial]` ❌ NEW
- Start replication for lvol
- `volume replication-trigger` → trigger snapshot replication
- Wait for replication to complete
- `volume replication-commit` → commit migration/fail-back cutover
- Verify lvol accessible on target cluster
- **Automate in**: `test_replication_basic.py`

#### TC-REPL-003 — Failback `[Both]` `[Partial]` ❌ NEW
- After TC-REPL-002 commit to target cluster
- `volume replication-failback` → configure fail-back to source
- Start replication in reverse
- Commit failback → verify lvol back on source cluster
- Verify data integrity across both failover and failback
- **Automate in**: `test_replication_failback.py`

#### TC-REPL-004 — Replication during node outage `[Both]` `[Partial]` ❌ NEW
- Start replication
- Trigger source node outage
- Verify replication pauses / resumes after recovery
- Verify no data loss on target
- **Automate in**: `test_replication_failover.py`

#### TC-REPL-005 — Snapshot replication `[Both]` `[Partial]` ❌ NEW
- `snapshot replication-status` → list replicated snapshots
- `snapshot delete-replication-only` → delete replicated version, keep source
- Verify source snapshot still intact
- **Automate in**: `test_replication_basic.py`

---

### 3.10 Cluster Lifecycle Operations

#### TC-CL-LIFE-001 — Graceful shutdown / startup `[Both]` `[Auto]` ❌ NEW
- Create lvols, run FIO
- `cluster graceful-shutdown` → verify all storage nodes go offline in order
- Verify FIO gets I/O errors (expected)
- `cluster graceful-startup` → verify all nodes come back online
- Reconnect lvols, verify data integrity
- **Automate in**: `test_cluster_graceful_shutdown.py`

#### TC-CL-LIFE-002 — Cluster change-name `[Both]` `[Auto]` ❌ NEW
- `cluster change-name <new-name>` → verify new name in `cluster list`
- Verify existing lvols still accessible after name change
- Verify NQN prefix unchanged (name is metadata only)
- **Automate in**: `test_cluster_lifecycle.py`

#### TC-CL-LIFE-003 — Cluster update-fabric `[Both]` `[Partial]` ❌ NEW
- Create cluster with TCP fabric
- `cluster update-fabric rdma` → verify fabric type changes
- Create new lvols → verify they use RDMA transport
- Verify existing lvol connections still work (or require reconnect)
- **Requires**: RDMA-capable hardware
- **Automate in**: `test_cluster_fabric.py`

---

### 3.11 Failure Domain & Node Affinity

#### TC-FD-001 — Failure domain placement `[Both]` `[Partial]` ❌ NEW
- Create cluster with `--enable-failure-domain`
- Add nodes in different racks/failure domains
- Create HA lvols (npcs=1 or 2)
- Verify primary and secondary replicas are placed in different failure domains
- **Requires**: multi-rack or simulated failure domain labels
- **Automate in**: `test_failure_domain.py`

#### TC-FD-002 — Strict node anti-affinity `[Both]` `[Auto]` ❌ NEW ⚠️ UNCERTAIN
- Create cluster with `--strict-node-anti-affinity`
- Create lvol with npcs=1 → verify primary and secondary on different nodes
- Create lvol with npcs=2 → verify all 3 replicas on distinct nodes
- Attempt to create lvol when not enough distinct nodes available → expect error
- **Status**: UNCERTAIN — requires `--strict-node-anti-affinity` cluster flag at creation time
- **Implemented in**: `test_node_anti_affinity.py` — **commented out in `get_all_tests()`**

#### TC-FD-003 — Shared placement binding `[Both]` `[Auto]` ❌ NEW ⚠️ UNCERTAIN
- `cluster set-shared-placement` → enable per-chunk data placement binding
- Create lvols with distributed RAID
- Verify chunks are co-located per the binding policy
- **Status**: UNCERTAIN — requires specific cluster configuration for shared placement
- **Implemented in**: `test_shared_placement.py` — **commented out in `get_all_tests()`**

---

### 3.12 Namespace & Subsystem Management

#### TC-NS-001 — Namespace per subsystem limit enforcement `[Both]` `[Auto]` ❌ NEW
- Create parent lvol with `max_namespace_per_subsys=5`
- Create 4 children with `namespace=True, host_id=<parent_node>` → all should join parent subsystem
- Create 5th child → should create new subsystem (limit exceeded)
- Verify NS IDs: first 4 children have NS ID > 1 on same subsystem, 5th has NS ID 1
- **Automate in**: `test_namespace_limits.py`

#### TC-NS-002 — Namespaced lvol placement `[Both]` `[Auto]` ❌ NEW
- Create parent lvol on node A with `host_id=<nodeA>`
- Create child lvol with `namespace=True, host_id=<nodeA>`
- Verify child lands on node A (same as parent)
- Verify child has NS ID > 1 (sharing parent subsystem)
- Verify child inherits parent's `max_namespace_per_subsys`
- **Automate in**: `test_namespace_placement.py`

#### TC-NS-003 — Namespaced lvol with FIO `[Both]` `[Auto]` ❌ NEW
- Create parent + 9 children in same subsystem
- Connect, mount, run FIO on all 10 namespaces
- Verify all FIO instances complete without error
- Delete children → verify parent still functional
- **Automate in**: `test_namespace_fio.py`

#### TC-NS-004 — Namespace negative cases `[Both]` `[Auto]` ❌ NEW
- Create child with `namespace=True` but no available subsystem on target node → verify new subsystem created
- Create child with `namespace=True` on node with no parent → verify behavior (new subsystem)
- Delete parent while children exist → expect error or cascading behavior
- **Automate in**: `test_namespace_negative.py`

---

### 3.13 Volume Advanced Operations

#### TC-VOL-ADV-001 — Volume suspend / resume `[Both]` `[Auto]` ❌ NEW ⚠️ UNCERTAIN
- Create lvol, connect, run FIO
- `volume suspend` → verify subsystems suspended
- Verify FIO gets I/O errors (expected)
- `volume resume` → verify subsystems resumed
- Verify FIO resumes, data intact
- **Status**: UNCERTAIN — `volume suspend`/`resume` may not be wired to working API endpoint
- **Implemented in**: `test_volume_suspend_resume.py` — **commented out in `get_all_tests()`**

#### TC-VOL-ADV-002 — Volume clone-lvol (combined snapshot + clone) `[Both]` `[Auto]` ❌ NEW
- Create lvol, write data
- `volume clone-lvol` → creates snapshot then clone in one command
- Verify clone exists with correct data
- Verify intermediate snapshot exists
- **Automate in**: `test_volume_clone_lvol.py`

#### TC-VOL-ADV-003 — LVOL delete while connected `[Both]` `[Auto]` ❌ NEW
- Create lvol, connect from client, run FIO
- `volume delete` while connected → verify behavior (error or forced disconnect)
- Verify lvol removed from list after disconnect
- **Automate in**: `test_lvol_negative.py`

#### TC-VOL-ADV-004 — Multi-client concurrent connect `[Both]` `[Auto]` ❌ NEW
- Create HA lvol (npcs >= 1)
- Connect from client A via primary path
- Connect from client B via secondary path (multi-path HA)
- Run FIO from both clients simultaneously
- Verify no data corruption (checksum validation)
- **Automate in**: `test_multi_client_connect.py`

#### TC-VOL-ADV-005 — Volume priority class `[Both]` `[Auto]` ❌ NEW ⚠️ UNCERTAIN
- Create lvols with different priority classes (0, 1, 2)
- Run concurrent FIO on all
- Verify higher-priority lvols get I/O preference under contention
- **Status**: UNCERTAIN — priority enforcement mechanism unclear; may not have observable effects without contention
- **Implemented in**: `test_volume_priority.py` — **commented out in `get_all_tests()`**

---

### 3.14 Pool Advanced Operations

#### TC-POOL-ADV-001 — Pool DHCHAP host management `[Both]` `[Auto]` ❌ NEW
- Create pool with DHCHAP enabled
- `pool add-host <nqn>` → verify host in allowed list
- Create lvol → connect from allowed host → success
- Connect from non-allowed host → failure
- `pool remove-host <nqn>` → verify host removed
- **Automate in**: `test_pool_dhchap.py`

#### TC-POOL-ADV-002 — Pool capacity overcommit `[Both]` `[Auto]` ❌ NEW
- Create pool with limited capacity
- Create lvols until provisioned capacity exceeds 250% → verify warning event
- Continue until 500% → verify critical event
- Verify I/O still works (overcommit is thin provisioning)
- **Automate in**: `test_pool_capacity_limits.py`

---

### 3.15 KMS / Encryption

#### TC-KMS-001 — External KMS encryption `[Both]` `[Partial]` ❌ NEW
- Deploy cluster with HashiCorp Vault / OpenBao KMS
- Create lvol with `--encrypt --hashicorp-vault-url <url>`
- Write data, take snapshot, restore from backup
- Verify data encrypted at rest (raw device has no plaintext)
- Verify data accessible after KMS key rotation
- **Requires**: cert-manager + KMS deployed (see `docs/kms/README.md`)
- **Automate in**: `test_kms_encryption.py`

#### TC-KMS-002 — KMS unavailability `[Both]` `[Partial]` ❌ NEW
- Create encrypted lvol with external KMS
- Take down KMS (delete openbao pod)
- Attempt new lvol creation with encryption → expect failure
- Verify existing encrypted lvol I/O still works (keys cached)
- Restore KMS → verify new encrypted lvols can be created again
- **Automate in**: `test_kms_encryption.py`

---

### 3.16 Capacity & Threshold Alerts

#### TC-CAP-001 — Cluster capacity warning threshold `[Both]` `[Auto]` ❌ NEW
- Create lvols until cluster capacity exceeds 89% (warning level)
- Verify warning event in `cluster get-logs`
- Continue to 99% (critical level)
- Verify critical event logged
- Verify I/O still works at critical level
- **Automate in**: `test_capacity_thresholds.py`

#### TC-CAP-002 — Capacity history queries `[Both]` `[Auto]` ❌ NEW
- Run FIO for 30+ minutes
- `cluster get-capacity --history` → verify 15-min interval data points
- `sn get-capacity --history` → verify per-node history
- `volume get-capacity --history` → verify per-volume history
- Verify history goes back up to 10 days (or as far as test duration allows)
- **Automate in**: `test_capacity_thresholds.py`

---

### 3.17 RDMA & Fabric

#### TC-RDMA-001 — RDMA-only cluster `[Both]` `[Partial]` ❌ NEW
- Create cluster with `fabric_type=rdma`
- Add storage nodes
- Create lvols, connect via RDMA transport
- Run FIO → verify performance and no errors
- **Requires**: RDMA-capable NICs (ConnectX-5+)
- **Automate in**: `test_rdma_cluster.py`

#### TC-RDMA-002 — Mixed TCP/RDMA `[Both]` `[Partial]` ❌ NEW
- Create cluster with TCP
- Create some lvols on TCP
- `cluster update-fabric rdma`
- Create new lvols → verify RDMA transport
- Verify TCP lvols still accessible
- **Automate in**: `test_rdma_cluster.py`

#### TC-RDMA-003 — QPair tuning `[Both]` `[Auto]` ❌ NEW ⚠️ UNCERTAIN
- Create cluster with custom `qpair_count_per_lvol` and `client_qpair_count_per_lvol`
- Create lvols, connect
- Verify actual QPair count matches configuration (via NVMe subsystem info)
- Run FIO → verify I/O works with custom QPair settings
- **Status**: UNCERTAIN — requires RDMA-enabled cluster; test skips gracefully if not configured
- **Implemented in**: `test_qpair_tuning.py` — **commented out in `get_all_tests()`**

---

### 3.18 Node Remove & Device Lifecycle

#### TC-SN-RM-001 — Storage node remove with migration `[Both]` `[Partial]` ❌ NEW
- Create lvols distributed across all nodes
- Run FIO on all
- `sn remove <node>` → verify data migrated to remaining nodes
- Verify FIO continues without errors during migration
- Verify removed node no longer in `sn list`
- Verify lvols that were on removed node are now on other nodes
- **Requires**: at least 4 storage nodes (3 remaining after remove)
- **Automate in**: `test_storage_node_remove.py`

#### TC-SN-DEV-001 — Device add / remove lifecycle `[Both]` `[Partial]` ❌ NEW
- `sn list-devices` → record initial devices
- `sn add-device <node> <device>` → verify device appears in list
- Verify auto-rebalancing task created
- Wait for rebalancing to complete
- Create lvol on new device → verify placement
- `sn remove-device <device>` → verify data migrated off
- **Requires**: spare NVMe device or test-device mode
- **Automate in**: `test_device_lifecycle.py`

#### TC-SN-DEV-002 — Device restart `[Both]` `[Auto]` ❌ NEW
- Create lvols, run FIO
- `sn restart-device <device>` → verify device goes offline then back online
- Verify FIO continues (HA failover to secondary during restart)
- Verify device healthy after restart (`sn check-device`)
- **Automate in**: `test_device_lifecycle.py`

#### TC-SN-DEV-003 — Journal device operations `[Both]` `[Partial]` ❌ NEW
- `sn restart-jm-device` → restart journal device
- Verify journal device comes back online
- Verify no data loss (journal replay)
- **Automate in**: `test_journal_device.py`

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

### 4.6 QoS Enforcement End-to-End ❌ NEW `[Both]` `[Auto]` ⚠️ DEPRECATED
**File**: `test_qos_enforcement.py` — **commented out in `get_all_tests()`**
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

### 4.11 Cluster Graceful Shutdown / Startup ❌ NEW `[Both]` `[Auto]`
**File**: `test_cluster_graceful_shutdown.py`
```
1. Create lvols across all nodes, run FIO
2. cluster graceful-shutdown → verify ordered node shutdown
3. Verify all nodes offline in cluster status
4. cluster graceful-startup → verify ordered startup
5. Reconnect lvols, verify data integrity via checksums
6. Run FIO again → verify no errors
```

### 4.12 Namespaced LVOL End-to-End ❌ NEW `[Both]` `[Auto]`
**File**: `test_namespace_e2e.py`
```
1. Create parent lvol with max_namespace_per_subsys=10 on node A
2. Create 9 children with namespace=True, host_id=<nodeA>
3. Verify all children have NS ID > 1 (sharing parent subsystem)
4. Connect all 10 namespaces, mount, run FIO
5. Take snapshot of parent → verify children unaffected
6. Delete 5 children → verify parent and remaining children functional
7. Create 5 new children → verify they join existing subsystem
8. Delete all children → delete parent
```

### 4.13 Async Replication End-to-End ❌ NEW `[Both]` `[Partial]`
**File**: `test_replication_e2e.py`
```
1. Setup: source cluster + target cluster with add-replication
2. Create lvol on source, write test data, take snapshot
3. Start replication → verify status shows replicating
4. Wait for sync → verify replication-info shows zero lag
5. Trigger node outage on source
6. Commit failover on target
7. Verify lvol accessible on target with correct data
8. Failback: start reverse replication
9. Commit failback → verify lvol back on source
10. Validate checksums at every step
```
> Requires dual cluster infrastructure.

### 4.14 Multi-Client HA Connect ❌ NEW `[Both]` `[Auto]`
**File**: `test_multi_client_connect.py`
```
1. Create HA lvol (npcs >= 1) on cluster with 3+ nodes
2. Get connect strings (primary + secondary paths)
3. Connect from client A (primary path), client B (secondary path)
4. Run FIO from both clients simultaneously (read workload)
5. Verify no I/O errors on either client
6. Disconnect client A → verify client B still functional
7. Reconnect client A → verify both functional
```

### 4.15 Volume Suspend / Resume ❌ NEW `[Both]` `[Auto]`
**File**: `test_volume_suspend_resume.py`
```
1. Create lvol, connect, run FIO
2. volume suspend → verify subsystem paused
3. Verify I/O stalls or errors (expected)
4. volume resume → verify subsystem resumed
5. Verify FIO resumes, data intact
6. Repeat suspend/resume 5 times rapidly → verify stability
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
| `test_node_suspend_resume.py` | TC-SN-002, Scenario 4.8 | P1 | `[Both]` | ✅ `[Auto]` ⚠️ DEPRECATED |
| `test_negative_cases.py` | Scenario 4.10 | P0 | `[Both]` | ✅ `[Auto]` |
| `test_pool_disable_io.py` | Scenario 4.3 | P1 | `[Both]` | ✅ `[Auto]` |
| `test_namespace_placement.py` | TC-NS-002 | P1 | `[Both]` | ✅ `[Auto]` |
| `test_namespace_fio.py` | TC-NS-003 | P1 | `[Both]` | ✅ `[Auto]` |
| `test_namespace_limits.py` | TC-NS-001 | P1 | `[Both]` | ✅ `[Auto]` |
| `test_namespace_negative.py` | TC-NS-004 | P1 | `[Both]` | ✅ `[Auto]` |
| `test_volume_suspend_resume.py` | TC-VOL-ADV-001, Scenario 4.15 | P1 | `[Both]` | ✅ `[Auto]` ⚠️ UNCERTAIN |
| `test_volume_clone_lvol.py` | TC-VOL-ADV-002 | P1 | `[Both]` | ✅ `[Auto]` |
| `test_node_anti_affinity.py` | TC-FD-002 | P1 | `[Both]` | ✅ `[Auto]` ⚠️ UNCERTAIN |

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
| `test_qos_class.py` | TC-QOS-001 | P1 | `[Both]` | ✅ `[Auto]` ⚠️ DEPRECATED |
| `test_qos_enforcement.py` | Scenario 4.6 | P1 | `[Both]` | ✅ `[Auto]` ⚠️ DEPRECATED |
| `test_lvol_inflate.py` | TC-LVOL-006 | P2 | `[Both]` | ✅ `[Auto]` |
| `test_lvol_migration_load.py` | TC-LVOL-007, Scenario 4.4 | P1 | `[Both]` | ✅ `[Auto]` |
| `test_pool_stats.py` | TC-POOL-004 | P2 | `[Both]` | ✅ `[Auto]` |
| `test_cluster_graceful_shutdown.py` | TC-CL-LIFE-001, Scenario 4.11 | P1 | `[Both]` | ✅ `[Auto]` |
| `test_multi_client_connect.py` | TC-VOL-ADV-004, Scenario 4.14 | P1 | `[Both]` | ✅ `[Auto]` |
| `test_pool_dhchap.py` | TC-POOL-ADV-001 | P1 | `[Both]` | ✅ `[Auto]` |
| `test_pool_capacity_limits.py` | TC-POOL-ADV-002, TC-CAP-001 | P2 | `[Both]` | ✅ `[Auto]` |
| `test_capacity_thresholds.py` | TC-CAP-002 | P2 | `[Both]` | ✅ `[Auto]` |
| `test_qpair_tuning.py` | TC-RDMA-003 | P2 | `[Both]` | ✅ `[Auto]` ⚠️ UNCERTAIN |
| `test_shared_placement.py` | TC-FD-003 | P2 | `[Both]` | ✅ `[Auto]` ⚠️ UNCERTAIN |
| `test_volume_priority.py` | TC-VOL-ADV-005 | P2 | `[Both]` | ✅ `[Auto]` ⚠️ UNCERTAIN |
| `test_namespace_e2e.py` | Scenario 4.12 | P1 | `[Both]` | ✅ `[Auto]` |
| `test_device_restart.py` | TC-SN-DEV-002 | P2 | `[Both]` | ✅ `[Auto]` |

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
| `test_storage_node_remove.py` | TC-SN-RM-001 | P1 | `[Both]` | ⚠️ `[Partial]` — needs 4+ nodes |
| `test_device_lifecycle.py` | TC-SN-DEV-001 | P1 | `[Both]` | ⚠️ `[Partial]` — needs spare device or test mode |
| `test_journal_device.py` | TC-SN-DEV-003 | P2 | `[Both]` | ⚠️ `[Partial]` |
| `test_failure_domain.py` | TC-FD-001 | P1 | `[Both]` | ⚠️ `[Partial]` — needs multi-rack labels |
| `test_cluster_fabric.py` | TC-CL-LIFE-003 | P2 | `[Both]` | ⚠️ `[Partial]` — needs RDMA hardware |
| `test_rdma_cluster.py` | TC-RDMA-001, 002 | P2 | `[Both]` | ⚠️ `[Partial]` — needs RDMA hardware |
| `test_kms_encryption.py` | TC-KMS-001, 002 | P1 | `[Both]` | ⚠️ `[Partial]` — needs cert-manager + KMS |
| `test_replication_basic.py` | TC-REPL-001, 002, 005 | P1 | `[Both]` | ⚠️ `[Partial]` — needs dual cluster |
| `test_replication_failback.py` | TC-REPL-003 | P1 | `[Both]` | ⚠️ `[Partial]` — needs dual cluster |
| `test_replication_failover.py` | TC-REPL-004 | P1 | `[Both]` | ⚠️ `[Partial]` — needs dual cluster |
| `test_replication_e2e.py` | Scenario 4.13 | P1 | `[Both]` | ⚠️ `[Partial]` — needs dual cluster |

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
├── test_namespace_placement.py       P1 [Both][Auto]
├── test_namespace_fio.py             P1 [Both][Auto]
├── test_namespace_limits.py          P1 [Both][Auto]
├── test_namespace_negative.py        P1 [Both][Auto]
├── test_volume_suspend_resume.py     P1 [Both][Auto]
├── test_volume_clone_lvol.py         P1 [Both][Auto]
├── test_node_anti_affinity.py        P1 [Both][Auto]
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
├── test_cluster_graceful_shutdown.py P1 [Both][Auto]
├── test_multi_client_connect.py      P1 [Both][Auto]
├── test_pool_dhchap.py               P1 [Both][Auto]
├── test_pool_capacity_limits.py      P2 [Both][Auto]
├── test_capacity_thresholds.py       P2 [Both][Auto]
├── test_qpair_tuning.py              P2 [Both][Auto]
├── test_shared_placement.py          P2 [Both][Auto]
├── test_volume_priority.py           P2 [Both][Auto]
├── test_namespace_e2e.py             P1 [Both][Auto]
├── test_device_restart.py            P2 [Both][Auto]
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
├── test_storage_node_remove.py       P1 [Both][Partial]
├── test_device_lifecycle.py          P1 [Both][Partial]
├── test_journal_device.py            P2 [Both][Partial]
├── test_failure_domain.py            P1 [Both][Partial]
├── test_cluster_fabric.py            P2 [Both][Partial]
├── test_rdma_cluster.py              P2 [Both][Partial]
├── test_kms_encryption.py            P1 [Both][Partial]
├── test_replication_basic.py         P1 [Both][Partial]
├── test_replication_failback.py      P1 [Both][Partial]
├── test_replication_failover.py      P1 [Both][Partial]
├── test_replication_e2e.py           P1 [Both][Partial]
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

## 9. Deprecated / Uncertain Tests

The following tests have been implemented but are **commented out** in `get_all_tests()` in `e2e/__init__.py`.
They are still importable and present in `ALL_TESTS` (for `--testname` fuzzy matching) but will NOT run in
the default E2E suite until verified.

| Test Class | File | Status | Reason |
|-----------|------|--------|--------|
| `TestNodeSuspendResume` | `test_node_suspend_resume.py` | **DEPRECATED** | `sn suspend` / `sn resume` are marked as DEPRECATED no-ops in `cli-reference.yaml`. The CLI prints a deprecation warning and returns immediately. Test verifies the no-op behavior. |
| `TestVolumeSuspendResume` | `test_volume_suspend_resume.py` | **UNCERTAIN** | `volume suspend` / `volume resume` — unclear if wired to a working API endpoint. May return 404 or no-op. Needs manual verification before enabling. |
| `TestNodeAntiAffinity` | `test_node_anti_affinity.py` | **UNCERTAIN** | Requires cluster created with `--strict-node-anti-affinity` flag. Standard CI clusters may not have this enabled. Test skips gracefully if flag not set. |
| `TestQosClass` | `test_qos_class.py` | **DEPRECATED** | QoS class API (`qos add` / `qos list` / `qos delete`) — not verified to exist as working CLI commands. May need API-side implementation first. |
| `TestQosEnforcement` | `test_qos_enforcement.py` | **DEPRECATED** | QoS enforcement (pool-level + per-lvol class) — depends on `TestQosClass` working. Pool-level QoS limits are covered separately in `test_pool_attributes.py`. |
| `TestVolumePriority` | `test_volume_priority.py` | **UNCERTAIN** | Volume priority class assignment — unclear enforcement mechanism. The `--priority` flag on lvol create may not have observable effects in CI without contention. |
| `TestSharedPlacement` | `test_shared_placement.py` | **UNCERTAIN** | `cluster set-shared-placement` — advanced feature for per-chunk data placement binding. Requires specific cluster configuration that may not be available in CI. |
| `TestQpairTuning` | `test_qpair_tuning.py` | **UNCERTAIN** | QPair tuning requires RDMA-enabled cluster with specific NIC hardware. Test skips gracefully if RDMA not configured but should not be in default suite. |

### How to enable

To run any of these tests explicitly:
```bash
# Run a single deprecated/uncertain test by name
python e2e.py --testname TestNodeSuspendResume

# Or import and add to a custom test list in __init__.py
```

To re-enable in the default suite, uncomment the corresponding line in `get_all_tests()` in `e2e/__init__.py`.

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
