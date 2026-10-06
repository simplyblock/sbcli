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
    AR-K  the Kubernetes DR surface (csi-addons, as Ramen drives it)

TWO SURFACES, AND WHICH ONE RETIRES
-----------------------------------
AR-S through AR-N drive the ENGINE: `sbctl` and the v2 REST API. AR-K drives
the same engine through `VolumeReplication`, which is how a Kubernetes
operator and Ramen reach it. Both are kept on purpose, and the reason is in
the operator's own csi-addons design (§8, §13):

  * They are mutually exclusive PER VOLUME, not per cluster. A volume is
    owned by a ReplicationSlot or by a VolumeReplication, never both, and
    `EnableVolumeReplication` returns FAILED_PRECONDITION if a slot already
    owns it -- even when both name the same policy, because ownership is the
    conflict. AR-K-004 is that assertion.

  * `ReplicationOps` "stays imperative and internal" for bulk fail-over of a
    whole policy or target until `VolumeGroupReplication` lands, which is
    Phase 4 and Planned. So AR-R-002 and AR-R-003 (policy and target scope)
    have no csi-addons equivalent yet and cannot be retired.

  * The docker lane has no Kubernetes at all. The engine is the ONLY surface
    there, so every AR-S..AR-N case stays load-bearing regardless of what
    happens on k8s.

RETIREMENT TRIGGER. The design's stated direction is that the annotation
path becomes a compatibility layer and the four legacy kinds retire "once
Ramen-driven replication is proven". Concretely, a case in AR-R may be
retired from the K8S lane (never from docker) when all three hold:

    1. its csi-addons equivalent exists in AR-K and has passed on k8s
    2. `VolumeGroupReplication` (Phase 4) has shipped, so policy and target
       scope have a csi-addons route
    3. the annotation path is actually removed from the operator

None of the three holds today. Nothing is deleted or commented out until
they do -- removing engine coverage while the engine is still the only thing
doing the work would leave us testing a translation layer over an untested
translation.
"""
