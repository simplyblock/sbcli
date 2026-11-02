"""The replication vocabulary as Kubernetes resources.

On Kubernetes the lane must not reach for ``sbctl``. That goes straight to
the control plane and skips the operator, which is the half of the product
a Kubernetes run exists to exercise -- the reconcilers, the webhooks, the
annotation that attaches a volume, the ``status`` a controller writes. A
test that bypasses them passes on a build where every one of them is
broken.

Everything here is ``storage.simplyblock.io/v1alpha1``. The four
replication kinds have not moved to ``v1alpha2`` and carry
``conversion: None``, so they are served directly and coexist with the
v1alpha2 core kinds -- confirmed by dev, who are migrating them later.
When they move, the only thing that changes here is API_VERSION.

The resource model, from
``docs/kubernetes/operations/data-protection/asynchronous-replication.md``:

    ReplicationPair     one per cluster pair, created by us
    ReplicationPolicy   one per schedule, created by us
    ReplicationSlot     one per replicated PVC, created by THE OPERATOR
    ReplicationOps      one per operation, created by us to trigger it

Two things about that model trip up a direct translation from the CLI.

**A volume opts in by annotation, not by a verb.** There is no "attach this
volume to this policy" resource. The operator watches for
``storage.simplyblock.io/replication-policy`` on a PVC (or on its
StorageClass) and creates the slot itself, named ``<policy>-<pvc>``. So
``policy_set`` is ``kubectl annotate`` and the slot is something we wait
for rather than create.

**Which means the thing being replicated has to be a PVC.** The lane
currently creates volumes with ``sbcli_utils.add_lvol``, which produces an
lvol the CSI driver knows nothing about and no PVC to annotate. Volumes
have to come from ``_create_lvol_dual``, which already provisions a PVC on
Kubernetes and records the name in ``_volume_registry``.

**The vocabulary is not quite the CLI's.** ``ReplicationSlot.status``
spells direction ``source``/``target`` where the control plane says
``to_target``/``to_source``, so the slot is translated into the shape
``relationship_for`` and ``await_state`` already expect rather than
teaching every assertion two dialects.

What has no Kubernetes expression at all is listed in NO_NATIVE_PATH
below. Those raise rather than silently falling back to ``sbctl``: a
k8s run that quietly used the CLI for one verb is exactly the hole this
module exists to close, and the gap is a finding for dev, not something
for the harness to paper over.
"""

from __future__ import annotations

import json
import time

API_VERSION = "storage.simplyblock.io/v1alpha1"

#: Verbs the CLI has and the CRDs do not. Named here so the message a test
#: fails with says which operation is missing, rather than leaving someone
#: to work it out from a kubectl error.
NO_NATIVE_PATH = {
    "replication_trigger": (
        "The control plane has POST .../replication/trigger, but no "
        "reconciler calls it -- it appears only in the generated client and "
        "the simulator -- so no CR forces an off-schedule cycle. Everything "
        "runs on ReplicationPolicy.spec.interval."),
    "policy_snapshot": (
        "There is no on-demand replication snapshot endpoint at all. The "
        "nearest things the operator calls are reads: "
        ".../policies/{id}/latest-generation and "
        ".../relationships/{lvol}/latest-snapshot."),
}

#: ReplicationSlot.status.direction -> the control plane's spelling, which
#: is what every assertion in this lane already reads.
_DIRECTION = {"source": "to_target", "target": "to_source"}


class K8sReplication:
    """The native half of the replication vocabulary.

    Mixed into ReplicationTestBase, which calls these only when
    ``self.k8s_test``. Every method is the k8s counterpart of the
    identically named sbctl wrapper beside it.
    """

    # ── plumbing ──────────────────────────────────────────────────────────
    @property
    def _ns(self):
        """The namespace the replication resources live in.

        Cluster A's, always: a ReplicationPair resolves its source by name
        in its own namespace, and cross-namespace references are not
        supported. Cluster B is named by UUID instead, which is why a
        two-namespace topology works at all.
        """
        return self._ensure_k8s_utils().namespace

    def _k8s_cluster_cr_name(self):
        """The local StorageCluster CR name, discovered rather than assumed.

        v1alpha2 admits one StorageCluster per namespace, so listing is
        unambiguous. The fallback matters only for a namespace where the
        list read fails, and naming the default is better than an empty
        sourceCluster the webhook will reject with less context.
        """
        out, _ = self._kube("get storageclusters --no-headers "
                            "-o custom-columns=NAME:.metadata.name")
        names = [n.strip() for n in (out or "").splitlines() if n.strip()]
        return names[0] if names else "simplyblock-cluster"

    def _kube(self, args, check=False):
        """One kubectl call in the replication namespace."""
        k8s = self._ensure_k8s_utils()
        out, err = k8s._exec_kubectl(f"kubectl -n {k8s.namespace} {args}",
                                     supress_logs=True)
        if check and not (out or "").strip():
            raise AssertionError(f"[AR/k8s] `kubectl {args}` returned nothing: "
                                 f"{(err or '')[:300]}")
        return (out or ""), (err or "")

    def _apply(self, body):
        """Apply one object, as JSON on stdin so nothing has to be escaped."""
        k8s = self._ensure_k8s_utils()
        doc = json.dumps(body)
        out, err = k8s._exec_kubectl(
            f"echo {json.dumps(doc)} | kubectl -n {k8s.namespace} apply -f -",
            supress_logs=True)
        combined = (out or "") + (err or "")
        if " created" not in combined and " configured" not in combined \
                and " unchanged" not in combined:
            raise AssertionError(
                f"[AR/k8s] applying {body['kind']}/{body['metadata']['name']} "
                f"did not take: {combined[:400]}")
        return combined

    def _get(self, kind, name=None, jsonpath=None):
        """Read one object or a list, as parsed JSON."""
        target = f"{kind}/{name}" if name else kind
        args = f"get {target} -o json"
        out, err = self._kube(args)
        if not (out or "").strip():
            return None
        try:
            return json.loads(out)
        except Exception:                                 # noqa: BLE001
            return None

    def _await_ready(self, kind, name, timeout=300):
        """Block until ``status.ready``, or say what it was stuck on.

        A controller that cannot create the backend object leaves the
        reason in ``status.message`` and the detail in the conditions, and
        reporting "timed out" without either is how a replication bug gets
        filed as a flake.
        """
        deadline = time.time() + timeout
        last = None
        while time.time() < deadline:
            obj = self._get(kind, name) or {}
            st = obj.get("status") or {}
            if st.get("ready") is True:
                return st
            reading = f"{st.get('ready')}/{st.get('message', '')[:80]}"
            if reading != last:
                self.logger.info("[AR/k8s] %s/%s ready=%s %s", kind, name,
                                 st.get("ready"), st.get("message", "") or "")
                last = reading
            time.sleep(5)
        obj = self._get(kind, name) or {}
        st = obj.get("status") or {}
        raise AssertionError(
            f"[AR/k8s] {kind}/{name} never became ready within {timeout}s. "
            f"message={st.get('message') or '-'} "
            f"conditions={json.dumps(st.get('conditions') or [])[:400]}")

    def _pvc_for(self, volume_id):
        """The PVC behind an lvol id, from the registry the dual helper fills.

        A volume created with ``sbcli_utils.add_lvol`` has no PVC, and the
        native path has nothing to annotate -- so this says that plainly
        rather than failing later inside kubectl.
        """
        for rec in (getattr(self, "_volume_registry", {}) or {}).values():
            if rec.get("lvol_id") == volume_id and rec.get("pvc_name"):
                return rec["pvc_name"]
        raise AssertionError(
            f"[AR/k8s] no PVC is registered for volume {volume_id}. On "
            f"Kubernetes a volume is opted into replication by annotating "
            f"its PVC, so it has to be created with _create_lvol_dual (which "
            f"provisions one) rather than sbcli_utils.add_lvol (which does "
            f"not).")

    # ── ReplicationPair: the target ───────────────────────────────────────
    def k8s_target_add(self, name, target_cluster, target_pool=None,
                       timeout=None):
        """A ReplicationPair. ``cluster replication-target-add``.

        sourceCluster is the LOCAL StorageCluster by name; targetCluster
        takes a name or a UUID, and the UUID form is what makes this work
        across namespaces -- ResolveClusterUUID short-circuits on a UUID
        before it ever lists local objects, which is deliberate, for a
        target that has no object on this API server at all.
        """
        if target_pool or timeout is not None:
            self.logger.warning(
                "[AR/k8s] ReplicationPair has no field for target_pool or "
                "timeout; ignoring target_pool=%r timeout=%r",
                target_pool, timeout)
        self._apply({
            "apiVersion": API_VERSION,
            "kind": "ReplicationPair",
            "metadata": {"name": name, "namespace": self._ns},
            "spec": {
                "sourceCluster": self._k8s_cluster_cr_name(),
                "targetCluster": target_cluster,
            },
        })
        self._await_ready("replicationpair", name)
        self._repl_targets.append(name)
        return name

    def k8s_target_list(self, cluster_id=None):
        out, _ = self._kube("get replicationpair -o wide")
        return out

    def k8s_target_remove(self, name):
        return self._kube(f"delete replicationpair {name} --ignore-not-found")

    # ── ReplicationPolicy: the schedule ───────────────────────────────────
    def k8s_policy_add(self, name, target, interval_min=None, mode=None,
                       consistency_group=False, retention=None,
                       rpo_target=None):
        """A ReplicationPolicy. ``cluster replication-policy-add``."""
        if consistency_group or rpo_target is not None:
            self.logger.warning(
                "[AR/k8s] ReplicationPolicy has no field for "
                "consistency_group or rpo_target; ignoring "
                "consistency_group=%r rpo_target=%r",
                consistency_group, rpo_target)
        spec = {"pairRef": target}
        if interval_min is None:
            interval_min = self.REPL_INTERVAL_MIN
        # A duration string, not a count. Rounded to whole minutes by the
        # operator, with one minute the floor.
        spec["interval"] = f"{interval_min}m"
        if mode:
            spec["mode"] = mode
        if retention is not None:
            spec["snapshotRetention"] = int(retention)
        self._apply({
            "apiVersion": API_VERSION,
            "kind": "ReplicationPolicy",
            "metadata": {"name": name, "namespace": self._ns},
            "spec": spec,
        })
        self._await_ready("replicationpolicy", name)
        self._repl_policies.append(name)
        return name

    def k8s_policy_list(self):
        out, _ = self._kube("get replicationpolicy -o wide")
        return out

    def k8s_policy_remove(self, name):
        # Deletion order is enforced: a policy with slots still on it is
        # refused, which is a correct refusal and worth surfacing as one.
        return self._kube(f"delete replicationpolicy {name} --ignore-not-found")

    # ── the annotation: attaching a volume ────────────────────────────────
    def k8s_policy_set(self, volume_id, policy):
        """``volume replication-policy-set`` -- an annotation on the PVC.

        The operator creates the ReplicationSlot itself once the PVC is
        Bound and the policy is ready, so this returns as soon as the
        annotation lands; await_state does the waiting.
        """
        pvc = self._pvc_for(volume_id)
        return self._kube(
            f"annotate pvc {pvc} --overwrite "
            f"storage.simplyblock.io/replication-policy={policy}")

    def k8s_policy_clear(self, volume_id):
        """Removing the annotation detaches and deletes the snapshots."""
        pvc = self._pvc_for(volume_id)
        return self._kube(
            f"annotate pvc {pvc} storage.simplyblock.io/replication-policy-")

    # ── ReplicationSlot: the per-volume state ─────────────────────────────
    def k8s_slot_for(self, volume_id):
        """The slot whose source or target lvol is *volume_id*, raw."""
        listing = self._get("replicationslot") or {}
        for item in listing.get("items", []):
            st = item.get("status") or {}
            spec = item.get("spec") or {}
            # spec.volumeID is "<cluster>:<pool>:<volume>", so the bare id is
            # its last segment -- the same shape _create_lvol_dual extracts
            # from the CSI volumeHandle.
            vid = (spec.get("volumeID") or "").rsplit(":", 1)[-1]
            if volume_id in (st.get("sourceLvolID"), st.get("targetLvolID"),
                             vid):
                return item
        return None

    def k8s_relationship(self, volume_id):
        """The slot, in the shape the control plane's JSON has.

        Translated rather than passed through: every assertion in this lane
        reads state/direction/source_lvol_id, and teaching them a second
        dialect per platform is how the two paths drift apart.
        """
        slot = self.k8s_slot_for(volume_id)
        if not slot:
            return "[]"
        st = slot.get("status") or {}
        spec = slot.get("spec") or {}
        return json.dumps([{
            "state": st.get("state"),
            "direction": _DIRECTION.get(st.get("direction"),
                                        st.get("direction")),
            "source_lvol_id": st.get("sourceLvolID"),
            "target_lvol_id": st.get("targetLvolID"),
            "lvol_id": (spec.get("volumeID") or "").rsplit(":", 1)[-1],
            "last_replicated_at": st.get("lastReplicatedAt"),
            "target_nqn": st.get("targetNQN"),
            "message": st.get("message"),
            # Named so a reader of a failure dump knows which side produced
            # it without having to diff two formats.
            "_source": "ReplicationSlot/" + slot["metadata"]["name"],
        }])

    # ── ReplicationOps: the operations ────────────────────────────────────
    def _ops(self, action, scope, ref, delete_source=False,
             source_cluster_id=None, timeout=1800):
        """Create a ReplicationOps and wait for it to settle.

        One operation per resource: an Ops that has reached Succeeded or
        Failed is never run again, so a repeat needs a new object and the
        name carries a stamp.
        """
        name = f"e2e-{action}-{scope}-{int(time.time()) % 100000}"
        spec = {"action": action, "scope": scope, "ref": ref}
        if delete_source:
            spec["deleteSource"] = True
        if source_cluster_id:
            spec["sourceClusterID"] = source_cluster_id
        self._apply({
            "apiVersion": API_VERSION,
            "kind": "ReplicationOps",
            "metadata": {"name": name, "namespace": self._ns},
            "spec": spec,
        })

        deadline = time.time() + timeout
        last = None
        while time.time() < deadline:
            st = ((self._get("replicationops", name) or {}).get("status")) or {}
            phase = st.get("phase")
            reading = f"{phase}/{st.get('subphase', '')}"
            if reading != last:
                self.logger.info("[AR/k8s] %s %s", name, reading)
                last = reading
            if phase in ("Succeeded", "Failed"):
                # Both are returned, not raised. The CLI wrappers return
                # (out, err) and several cases assert on a REFUSAL, so a
                # failed operation is an answer rather than an error.
                return (json.dumps(st), "" if phase == "Succeeded"
                        else st.get("message") or "operation Failed")
            time.sleep(5)
        raise AssertionError(
            f"[AR/k8s] {name} did not settle within {timeout}s "
            f"(last: {last}).")

    def k8s_failover_target(self, target_id):
        return self._ops("failover", "target", target_id)

    def k8s_failover_policy(self, policy_id):
        return self._ops("failover", "policy", policy_id)

    def k8s_failback(self, volume_id, source_cluster_id=None):
        """Scope volume: ref is the SLOT, not the lvol id.

        A target-scoped failback is rejected outright, so a pair-wide
        failback is one policy at a time -- which is why there is no
        k8s_failback_target.
        """
        slot = self.k8s_slot_for(volume_id)
        if not slot:
            raise AssertionError(
                f"[AR/k8s] no ReplicationSlot for volume {volume_id}, so "
                f"there is nothing to fail back.")
        return self._ops("failback", "volume", slot["metadata"]["name"],
                         source_cluster_id=source_cluster_id)

    # ── the migration entry point ─────────────────────────────────────────
    def k8s_replication_start(self, volume_id, cluster_id, mode=None,
                              interval_min=None):
        """``volume replication-start`` -- a pair, a policy and an annotation.

        The CLI starts replicating one volume to one cluster in a single
        verb. There is no CR that does that, because the model separates
        the relationship from the schedule from the volume: the native
        sequence is a ReplicationPair for the two clusters, a
        ReplicationPolicy on that pair, and the annotation that opts the
        volume in.

        So this builds the three rather than refusing. Both the pair and
        the policy are reused when they already exist, which is what makes
        a second call for a second volume cheap, and matches the CLI --
        calling replication-start twice against the same cluster does not
        create two relationships either.
        """
        pair = f"e2e-pair-{str(cluster_id)[:8]}"
        if not self._get("replicationpair", pair):
            self.k8s_target_add(pair, cluster_id)

        policy = f"e2e-pol-{str(cluster_id)[:8]}-{mode or 'failover'}"
        if not self._get("replicationpolicy", policy):
            self.k8s_policy_add(policy, pair, interval_min=interval_min,
                                mode=mode)

        return self.k8s_policy_set(volume_id, policy)

    def k8s_commit(self, volume_id, delete_source=False):
        """``volume replication-commit`` -- ReplicationOps action: migration.

        Not a gap, though I first read it as one: the doc describes
        cutover_pending as "awaiting its commit" and never says what
        commits it, and the CRD's third action is undocumented. The
        controller settles it -- reconcileMigration POSTs the very same
        .../replication/commit endpoint the CLI verb does, honours
        deleteSource, and marks the slot cutover_pending for the backend
        to drive to cutover_done.

        Volume scope, so one slot: the CLI verb is per volume. Policy
        scope commits every slot on the policy, which is a different
        operation and belongs to whichever case wants it.
        """
        slot = self.k8s_slot_for(volume_id)
        if not slot:
            raise AssertionError(
                f"[AR/k8s] no ReplicationSlot for volume {volume_id}, so "
                f"there is nothing to commit.")
        return self._ops("migration", "volume", slot["metadata"]["name"],
                         delete_source=delete_source)

    # ── the gaps ──────────────────────────────────────────────────────────
    def k8s_no_native(self, verb):
        """Raise for a verb the CRDs do not express.

        Deliberately not a fallback to sbctl. A Kubernetes run that quietly
        used the CLI for one operation is the hole this module closes, and
        a missing verb is a finding for dev rather than something to route
        around.
        """
        raise AssertionError(
            f"[AR/k8s] `{verb}` has no Kubernetes-native equivalent. "
            f"{NO_NATIVE_PATH.get(verb, '')} "
            f"This case cannot run natively until the CRDs express it.")
