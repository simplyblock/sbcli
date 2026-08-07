# coding=utf-8
"""simplyblock_edge — spdk-only edge clusters (1-2 nodes, kubernetes-only).

Managed by the same centralized control plane as hyperscale clusters (same FDB,
same API/security), but talking to the edge site over exactly two channels: the
edge cluster's kubernetes API and SPDK JSON-RPC. See docs/edge_clusters_spec.md.

Dependency rules: this package imports simplyblock_core (models, rpc_client,
db) and simplyblock_lib (runner/monitor bases). Nothing in core/web imports
this package except the explicit mount points (the v2 router registration and
the JobSchedule FN_EDGE_* task-type constants).
"""
