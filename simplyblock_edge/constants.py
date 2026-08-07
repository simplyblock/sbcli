# coding=utf-8
"""Edge-cluster tuning knobs and defaults (docs/edge_clusters_spec.md)."""

import os

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

# Task runner.
EDGE_TASK_INTERVAL_SEC = 5
EDGE_TASK_BACKOFF_BASE_SEC = 3
EDGE_TASK_BACKOFF_MAX_SEC = 300
EDGE_NODE_RESTART_MAX_RETRY = 11

# SPDK pod.
EDGE_POD_PREFIX = "edge-spdk-"
EDGE_POD_CPU = 2
EDGE_POD_HUGEPAGES_MIB = 1024
EDGE_SPDK_IMAGE = os.getenv("SIMPLYBLOCK_EDGE_SPDK_IMAGE", "simplyblock/spdk:edge-latest")
EDGE_PROXY_IMAGE = os.getenv("SIMPLYBLOCK_EDGE_PROXY_IMAGE", "simplyblock/spdk-proxy:latest")

# Node add: how long to wait for the SPDK proxy to answer after pod deploy.
EDGE_RPC_WAIT_TIMEOUT_SEC = 120
EDGE_RPC_WAIT_INTERVAL_SEC = 2
