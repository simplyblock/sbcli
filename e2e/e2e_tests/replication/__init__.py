"""Async replication coverage.

Two simplyblock clusters under ONE control plane, which is what
"cross-cluster" means for 26.3/26.4 as built: the control plane cannot live in
two Kubernetes clusters, only span availability zones within one. A true
site-loss test is therefore not possible until the per-cluster operator
refactor lands, and nothing in here pretends otherwise.

Test IDs follow the QA plan (documentation/async_replication_QA_test_plan.xlsx):

    AR-S  harness and setup
    AR-F  functional
    AR-I  integrity
    AR-C  consistency groups
    AR-R  failover / failback / migration
    AR-O  outages
    AR-N  negative and limits
"""
