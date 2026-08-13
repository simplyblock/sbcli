# coding=utf-8
"""Edge-cluster tuning knobs and defaults (docs/edge_clusters_spec.md)."""

import os

from simplyblock_core import constants as core_constants

# Per-node service ports (hostNetwork pod).
EDGE_RPC_PORT = 8080          # spdk proxy (JSON-RPC over HTTP, basic auth)
EDGE_NVMF_PORT = 4420         # client-facing nvmf-tcp listener
EDGE_REPL_PORT = 4430         # internal node-to-node replication listener

# Stack geometry.
EDGE_AIO_BLOCK_SIZE = 4096
EDGE_RAID5_STRIP_SIZE_KB = 64
EDGE_LVS_CLUSTER_SZ = 4 * 1024 * 1024
MAX_EDGE_NODES = 2

# Monitor cadence: WAN-tolerant, bounded probes (spec §7).
EDGE_MONITOR_INTERVAL_SEC = 10
EDGE_MONITOR_FAST_INTERVAL_SEC = 3
EDGE_MONITOR_FAILURE_THRESHOLD = 60
EDGE_K8S_PROBE_TIMEOUT_SEC = 5
EDGE_RPC_PROBE_TIMEOUT_SEC = 3

# Fail-back: how long to wait for the returning primary's mirror leg to
# resync before moving the lvstore home.
EDGE_RESYNC_TIMEOUT_SEC = int(os.getenv("SIMPLYBLOCK_EDGE_RESYNC_TIMEOUT", "7200"))
EDGE_RESYNC_POLL_SEC = 5

# Task runner.
EDGE_TASK_INTERVAL_SEC = 5
EDGE_TASK_BACKOFF_BASE_SEC = 3
EDGE_TASK_BACKOFF_MAX_SEC = 300
EDGE_NODE_RESTART_MAX_RETRY = 11

# SPDK pod.
EDGE_POD_PREFIX = "edge-spdk-"
# vCPUs for the SPDK pod. E2e/edge sites run 4-vCPU instances with a single
# vCPU dedicated to SPDK; larger boxes can raise this.
EDGE_POD_CPU = int(os.getenv("SIMPLYBLOCK_EDGE_POD_CPU", "1"))
EDGE_POD_HUGEPAGES_MIB = int(os.getenv("SIMPLYBLOCK_EDGE_POD_HUGEPAGES_MIB", "1024"))
# Same images the central k8s storage-node pods run: the ultra image IS the
# spdk fork with the product processing edge depends on (primary/secondary
# lvstore, bdev_lvol_register/update_lvstore/set_leader), and the proxy
# container just runs the RPC http proxy from the simplyblock image. The
# previous defaults were nonexistent placeholders — first live pod create
# sat in ImagePullBackOff for 3h (2026-08-13).
EDGE_SPDK_IMAGE = os.getenv("SIMPLYBLOCK_EDGE_SPDK_IMAGE",
                            core_constants.SIMPLY_BLOCK_SPDK_ULTRA_IMAGE)
EDGE_PROXY_IMAGE = os.getenv("SIMPLYBLOCK_EDGE_PROXY_IMAGE",
                             core_constants.SIMPLY_BLOCK_DOCKER_IMAGE)
# The same node-preparation CPU-topology Job central clusters run (kubelet
# static cpu-manager policy + reserved system cpus).
# Default OFF (2026-08-13): the central cpu-topology job mutates kubelet
# config and restarts the kubelet — on a 1-node k3s edge cluster that
# restarts the embedded API server mid-node-add (the very channel the CP is
# using), and the kubeadm-style script crash-loops on k3s anyway. Exclusive
# reactor cores on edge come from the cluster's own kubelet policy
# (cpu-manager-policy=static via k3s config), set when the edge cluster is
# provisioned; a plain high/RT priority is no substitute (RT throttling
# stalls pollers 50ms/s by default, and CFS nice still time-shares the core).
EDGE_CPU_TOPOLOGY_ENABLED = os.getenv("SIMPLYBLOCK_EDGE_CPU_TOPOLOGY", "false").lower() == "true"
EDGE_RESERVED_SYSTEM_CPUS = os.getenv("SIMPLYBLOCK_EDGE_RESERVED_SYSTEM_CPUS", "0")

# Node add: how long to wait for the SPDK proxy to answer after pod deploy.
# 600 not 120: the wait covers the SPDK pod's FIRST image pull on the edge
# node (multi-GB over the edge site's uplink), not just process start.
EDGE_RPC_WAIT_TIMEOUT_SEC = int(os.getenv("SIMPLYBLOCK_EDGE_RPC_WAIT_SEC", "600"))
EDGE_RPC_WAIT_INTERVAL_SEC = 2
