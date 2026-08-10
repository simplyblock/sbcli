# Edge-clusters e2e suite

Deployment infrastructure + staged tests for `simplyblock_edge`
(docs/edge_clusters_spec.md). AWS-based: one central k3s cluster (CP + 3-node
hyperscale storage on three workers) and eight edge k3s clusters covering the
drive matrix — 4x 1-node and 4x 2-node with 1 drive / 2 drives / 2 partitions
/ 4 drives per node (the original ask said "3x 2-node" but enumerated four
configs and eight clusters total; drop one in `topology.py` if intended).

Edge instances are 4-vCPU `c5a.xlarge` with **1 vCPU for SPDK**
(`SIMPLYBLOCK_EDGE_POD_CPU=1`, the default).

## Flow

```
pip install boto3 requests pytest
export AWS_PROFILE=...            # credentials with EC2 rights

python edge_e2e/provision.py --region eu-west-1 --key-name <ec2-keypair>
#  -> creates VPC + instances + EBS volumes, installs k3s via cloud-init,
#     writes edge_e2e/state.json. Wait ~5 min for cloud-init.

python edge_e2e/deploy.py         # == TEST 1: deploy simplyblock everywhere
#  -> bootstraps the central CP (override with EDGE_E2E_BOOTSTRAP_CMD; the
#     default clones simplyblock-deploy and runs bootstrap-cluster.sh — after
#     a manual bootstrap, set central.api_url/cluster_id/cluster_secret in
#     state.json and rerun with --skip-central),
#  -> per edge cluster: sgdisk partitioning (-2p variants), ServiceAccount
#     token + CA minting, POST /api/v2/clusters/edge, node adds (ONLINE
#     gates), the standard 30G test volume.

pytest edge_e2e/test_edge_e2e.py -v -x     # tests 2-6, ordered

python edge_e2e/provision.py --region eu-west-1 --destroy
```

## Test map

| # | test | asserts |
|---|------|---------|
| 1 | `deploy.py` succeeding | every cluster deployed + ACTIVE + volume created |
| 2 | `test_02_parallel_fio_all_clusters` | the standard fio job (2 jobs, iodepth 2, 10G, rwmix 30/70 read/write, `max_latency=20s`) completes on the central + all edge clusters in parallel |
| 3a | `test_03a_reboot_single_node` | instance reboot: IO interruption IS detected (fio max-latency trip), cluster SUSPENDED while out, node walks unreachable → offline → online, cluster ACTIVE again |
| 3b | `test_03b_reboot_two_node_both_nodes` | reboot each node in turn (second only after rebuild): IO NEVER interrupted (dual active/passive paths + lvstore fail-over/fail-back verified via `hosts_lvstore`), cluster DEGRADED only, node cycles unreachable → offline → online |
| 4 | `test_04_device_remove_and_restart` | graceful device removal (API) → partition `offline`, raid keeps serving; device restart → `online`, raid member again; IO unaffected on every cluster with >1 device/partition |
| 5a | `test_05a_device_error_detach_reattach` | EBS force-detach → monitor marks partition `unavailable`, IO unaffected; reattach + device restart → `online` |
| 5b | `test_05b_permanent_replacement_with_new_volume` | force-detach + replace with a brand-new EBS volume via the replace API → new device `online`, raid rebuilt |
| 6 | `test_06_cp_edge_connection_faults` | flaky (tc netem) and broken (iptables drop) CP↔edge links on 3 random clusters: nodes/cluster go `unreachable`/degraded-suspended, local IO NEVER interrupted, full recovery (online/active) after healing |

## Notes & knobs

- The suite drives everything through the v2 API (`helpers.EdgeApi`) with each
  edge cluster's own secret; instance faults via boto3 (reboot, force-detach,
  attach, create-volume); network faults via tc/iptables over SSH.
- fio runs in a privileged hostNetwork pod per cluster and nvme-connects
  every path from `GET .../connect` (active + passive), so 2-node takeovers
  activate the second path without a reconnect.
- Device remove/restart currently goes through the API (the `sbctl` edge CLI
  group is still a deferred item — swap the calls once it lands).
- `EDGE_E2E_DRIVE_GB`, `EDGE_E2E_EDGE_INSTANCE_TYPE`,
  `EDGE_E2E_CENTRAL_INSTANCE_TYPE`, `EDGE_E2E_BOOTSTRAP_CMD` override the
  defaults. `state.json` is the single source of truth between stages.
- Everything is tagged `simplyblock-edge-e2e`; `--destroy` sweeps by tag, so
  teardown works even with a lost state file.

## One-shot orchestration (`run_all.py`)

`run_all.py` is the entry point for a full campaign or an unattended soak. It
chains provision → deploy (test 1) → tests 2-6, writes a self-contained run
directory (`edge_e2e/runs/run-<ts>/`: per-stage logs, junit xml, pre/post
cluster-status snapshots, and on failure a `cluster-logs/` capture of nodes,
pods, events and k3s journals from every cluster), and exits non-zero if any
stage failed.

```bash
python edge_e2e/run_all.py --region eu-west-1 --key-name mykey   # full campaign + teardown
python edge_e2e/run_all.py --skip-provision --only 04,05a        # re-run a subset
python edge_e2e/run_all.py --soak-cycles 12 --keep               # overnight fault soak
python edge_e2e/run_all.py --teardown-only                       # clean up by tag
```

`--soak-cycles N` repeats the fault stages N times against the same
environment (stopping early on the first failing cycle) — that is the soak
mode for the reboot / device-failure / connection-fault scenarios.

## Tier isolation

The suite lives at the repo top level (`edge_e2e/`, not under `e2e/`) because
`e2e/__init__.py` imports the legacy `e2e_tests` framework at package-import
time, which makes anything beneath it uncollectable outside that environment.
`norecursedirs` keeps it out of the unit/integration tiers, `conftest.py`
tags every case `edge_e2e` and raises the per-test timeout from the repo-wide
30s budget to 3h.
