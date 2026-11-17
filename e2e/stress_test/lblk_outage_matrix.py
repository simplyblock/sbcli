"""Does an lblk cluster keep serving, and keep your bytes, through outages.

The other lblk cases each prove one narrow thing. This one asks the question a
prospect actually asks: take a node out, any way you like, one at a time, and
does IO carry on and does the data still match afterwards.

Two independent lanes, because they catch different failures:

* **Static volumes.** Written once before any fault and never touched again,
  then md5'd. No overlapping IO, no verify_backlog, no torn-write ambiguity --
  which makes md5 completely reliable here, unlike the live FIO lane where
  fio's own concurrency can manufacture a mismatch. This is the durability
  gate, and it is re-checked after *every* outage rather than only at the end,
  so a divergence names the outage that caused it.

* **Live FIO.** Runs continuously across the whole outage sequence. This is the
  availability gate: with ndcs/npcs 2/2 losing one node is meant to be
  survivable, so any io_u error is a defect, not a blip. Sudden loss is the
  normal case in the field -- most outages are not graceful -- so a crash-type
  outage gets exactly the same bar as a planned one.

The volume set is deliberately mixed: plain, encrypted and DHCHAP-authenticated
volumes, formatted and raw, plus a clone taken from a snapshot. Encryption and
DHCHAP on lblk are an untested combination; a failure confined to those lanes
is a real finding about lblk rather than a bug in this test.
"""

import json
import os
import random
import re
import threading
import time
from typing import ClassVar

from e2e_tests.lblk.test_lblk import (
    LblkPreconditionError,
    _LblkBase,
    _LblkDockerMixin,
    _LblkK8sMixin,
)
from utils.common_utils import sleep_n_sec
from utils.fio_defaults import FIO_MAX_LATENCY


class _LblkOutageMatrix(_LblkBase):
    """Every outage type once, on a different node each time."""

    #: Let the rebalance finish before the next outage, unlike the base.
    #:
    #: This is the lane the default is wrong for. _LblkBase defaults it off so
    #: that the rapid and no-gap families, whose whole subject is a migration
    #: that never completes, are never quietly converted into paced runs. This
    #: test is the opposite: OUTAGES below says "the cluster is fully healthy
    #: again before the next", and it asks whether each outage type is survived
    #: from a settled starting point, one type at a time.
    #:
    #: Without it the two questions blur. A failure in cycle 5 could be
    #: node_network_isolation mishandled, or it could be that cycle 4's
    #: migration was still running -- and at 450s a cycle against migrations
    #: that have run for tens of minutes on this lab, that is not a remote
    #: possibility. Waiting is what makes a cycle's result attributable to the
    #: outage it names.
    #:
    #: Both platforms, by different readings of the same thing: k8s reads
    #: StorageCluster.status.phase, which the operator began publishing on
    #: 2026-09-28, and docker reads the control plane's migration task list,
    #: which it has always had.
    WAIT_FOR_REBALANCE = True

    #: One outage per node, in the order a cluster is most likely to meet them:
    #: planned first, then progressively less polite. Each is applied to a
    #: different node so no node is asked to survive two in a row, and the
    #: cluster is fully healthy again before the next.
    OUTAGES = (
        "graceful_shutdown",
        "container_stop",
        "short_network_interrupt",
        "short_network_interrupt_fio_worker",
        "interface_full_network_interrupt",
        "interface_full_network_interrupt_fio_worker",
        "node_network_isolation",
        # Straight after its own generic form, and ahead of anything
        # reboot-based: this one uses a mechanism that works, so it
        # should not sit behind one that currently does not.
        "node_network_isolation_fio_worker",
        # Last, because it is the one that currently cannot recover. Cordoning
        # a worker raises a HostMaintenance operation whose Releasing step
        # waits on a DaemonSet pod that never goes, so it expires after 15
        # minutes and leaves the node Offline with a stale PDB and label --
        # and Restart, the documented recovery, then expires too. Both runs of
        # 2026-09-28 and 2026-09-29 ended here, at cycle 9 of 17, with the four
        # network types never reached.
        #
        # Ordering it last does not make it pass. It means the types that do
        # work are exercised first, so a run that dies on the reboot still
        # reports on everything before it instead of reporting on nothing.
        # Move it back up once the operator can complete a maintenance window:
        # see k8s_hostmaintenance_releasing_waits_on_daemonset_pod_rca_20260929.
        "storage_node_reboot",
        # Last of all: it inherits that same recovery problem on top of
        # the question it is actually asking.
        "storage_node_reboot_fio_worker",
    )

    #: Outage types this platform leaves out.
    #:
    #: Docker skips node_network_isolation and nothing else: that outage tests
    #: pod eviction and rescheduling, and docker has no scheduler to react --
    #: dropping a docker node's NICs is already what
    #: interface_full_network_interrupt does. Everything else applies, because
    #: a docker node does nothing but storage, so isolating it isolates
    #: exactly what the test means to.
    #:
    #: LBLK_MATRIX_SKIP_OUTAGES overrides either, "" meaning skip nothing.
    SKIP_OUTAGES = ("node_network_isolation",)

    #: Where create_utility_pod and the FIO job mount a PVC inside the pod.
    #: On k8s nothing is mounted on the runner, so every read of a volume's
    #: contents goes through a pod and sees this path -- whatever the dual
    #: helper happened to return as "mount".
    K8S_MOUNT = "/spdkvol"

    #: Outages that ask Kubernetes to move the workload, rather than taking
    #: it away. storage_node_reboot cordons and drains first -- the documented
    #: maintenance procedure -- so every pod on that node is evicted by
    #: design, live FIO included. For these, a FIO job that was rescheduled
    #: has behaved correctly and the continuity claim does not apply; for
    #: every other outage it still does.
    #: Outages that remove the pod on purpose, so "live FIO stopped" is the
    #: expected result and only "IO never came back" is a finding.
    #:
    #: node_network_isolation belongs here and was missing. NODE_ISOLATION_SEC
    #: is 420s against an EVICTION_TOLERATION_SEC of 300s -- the constant
    #: exists to outlast the toleration -- so a live FIO pod on the isolated
    #: node is always evicted. Run 20261004-125130 failed cycle 14 asserting
    #: continuity for a pod the outage had deliberately thrown off the node.
    DRAINING_OUTAGES = ("storage_node_reboot", "node_network_isolation")

    def _base_outage(self, outage_type):
        """The generic outage a *_fio_worker variant is a variant OF.

        FIO_WORKER_OUTAGES is the authority -- it is the mapping the recovery
        path already uses -- with a suffix strip as the fallback so a variant
        added to OUTAGES but forgotten in the dict still classifies correctly
        rather than silently behaving like an unrelated outage.
        """
        return self.FIO_WORKER_OUTAGES.get(
            outage_type, (outage_type or "").replace("_fio_worker", ""))

    def _cycles_for(self, outage, nodes, no_client_evict):
        """The (node, outage) pairs this outage should produce.

        One implementation for the full and the trimmed node lists. They were
        two separate expressions, and the trimmed one never learned about
        FIO_WORKER_OUTAGES: with LBLK_MATRIX_NODES set, the *_fio_worker
        variants would have run on EVERY node instead of once on the FIO
        worker. On any other node there is no client to move, so
        assert_clean_reschedule would wait out its full 900s hunting for a
        reschedule that was never going to happen and then fail the run.

        Nobody has set LBLK_MATRIX_NODES yet, which is the only reason that
        has not fired.
        """
        if outage in self.FIO_WORKER_OUTAGES:
            # Exactly one cycle, on the one node that matters for it. Running
            # it per-node would be 3-4 repeats of the same question at up to
            # eleven minutes each, and on any node but the FIO worker it is
            # just the generic outage again.
            if not self._fio_home_node:
                self.logger.warning(
                    "[matrix] skipping %s: no reserved FIO worker on this "
                    "platform, so there is no node whose loss would move the "
                    "client. Nothing to assert.", outage)
                return []
            return [(self._fio_home_node, outage)]
        targets = (no_client_evict
                   if self._base_outage(outage) in self.CLIENT_EVICTING_OUTAGES
                   else nodes)
        return [(node, outage) for node in targets]

    def _drains(self, outage_type):
        """Does this outage remove the pod on purpose?

        Matched on the BASE name, so the *_fio_worker variants count as the
        draining outages they are. Listing the exact strings was wrong twice:
        node_network_isolation went in and node_network_isolation_fio_worker
        did not, so run 20261005-114437 gave the latter the 180s quick-outage
        budget and failed with the node still offline -- a node that a 420s
        isolation had just taken down, and that the previous runs needed seven
        to fourteen minutes to get back.

        A *_fio_worker cycle differs only in WHICH node it targets. It uses
        the same mechanism for the same duration, so it drains exactly as much
        and deserves the same patience.
        """
        return self._base_outage(outage_type) in self.DRAINING_OUTAGES

    #: How long a client may take to come back on another node.
    #:
    #: Generous on purpose. A drained node hands its pods over in seconds,
    #: but a node that merely went NotReady keeps its RWO volumes until
    #: taint-based eviction AND the force-detach timer have both elapsed --
    #: minutes, by kubernetes' own design. A tight bound here would report a
    #: product failure every time kubernetes was being patient, which is the
    #: most expensive kind of false positive we can write.
    RESCHEDULE_SEC = 900

    #: Outages aimed at the node running live FIO, rather than at storage.
    #:
    #: Named types rather than "whichever node the rotation happens to pick",
    #: because when one of these fails the name has to say what broke. A
    #: failure reading `node_network_isolation on 192.168.10.246` leaves the
    #: reader to work out that .246 was the FIO worker that day; one reading
    #: `node_network_isolation_fio_worker` does not. They are also then
    #: deterministic, skippable on their own, and visible in the plan dump.
    #:
    #: Each maps to the mechanism it borrows. The mechanism is not new -- what
    #: is new is the target, and what is asserted afterwards: the client must
    #: be rescheduled onto a surviving node and get its RWO volume back, which
    #: no k8s outage suite has ever checked.
    #:
    #: k8s only. Docker has no scheduler to move anything.
    FIO_WORKER_OUTAGES: ClassVar = {
        "storage_node_reboot_fio_worker": "storage_node_reboot",
        "node_network_isolation_fio_worker": "node_network_isolation",
        "short_network_interrupt_fio_worker": "short_network_interrupt",
        "interface_full_network_interrupt_fio_worker":
            "interface_full_network_interrupt",
    }

    #: Of those, the ones where the client must end up on a DIFFERENT node.
    #:
    #: Only two of the four outages actually evict. A node stops being Ready
    #: after ~40s of missed heartbeats, but the default
    #: `node.kubernetes.io/unreachable` toleration then holds its pods for a
    #: further 300s -- so 420s of isolation moves them and 120s does not, and
    #: a drain moves them by definition. Requiring a move after the short
    #: cuts would fail a cluster that behaved perfectly.
    FIO_WORKER_MOVE_OUTAGES = (
        "storage_node_reboot_fio_worker",
        "node_network_isolation_fio_worker",
    )

    #: How long the client may take to be doing IO again after a cut that did
    #: not move it. Short: nothing has to be scheduled or reattached here, the
    #: Job only has to restart a container, so minutes would be hiding a
    #: problem rather than allowing for one.
    FIO_RESUME_SEC = 300

    #: Outages the live-FIO client cannot survive on the node being broken, so
    #: the reserved node sits these out.
    #:
    #: It is not every destructive outage, and the distinction is what the
    #: client actually runs on. graceful_shutdown, restart and pod delete take
    #: down the SPDK pod; the FIO pod is a different pod on the same worker and
    #: keeps running throughout, which is the continuity this test is for. A
    #: network cut isolates the worker, and storage_node_reboot cordons and
    #: drains it -- both take the client away with the storage, and a FIO
    #: failure then measures where the client was scheduled rather than
    #: anything about the product.
    #:
    #: storage_node_reboot was missing from this until 2026-09-28, which is a
    #: coverage bug rather than a correctness one: the reboot cycle would have
    #: evicted the FIO pod and read as a loss of availability.
    CLIENT_EVICTING_OUTAGES = (
        "interface_full_network_interrupt",
        "short_network_interrupt",
        "node_network_isolation",
        "storage_node_reboot",
    )

    #: How far back _assert_attached looks for volume-attach events. One
    #: cycle's worth: the outage, its recovery, and the checks since. Long
    #: enough to catch this cycle's failure, short enough that the previous
    #: cycle's is not re-reported against this one.
    ATTACH_WINDOW_SEC = 900

    #: Static payload per volume. Large enough to span many stripes -- and so
    #: to involve every node's parity -- small enough that re-md5ing five
    #: volumes after each of four outages is not the bulk of the runtime.
    STATIC_MB = 256

    #: Budget per outage cycle when sizing the live FIO job, from the twelve
    #: cycles measured on the k8s run of 2026-09-21: min 164s, max 401s, mean
    #: 227s. 450 sits above the worst one with room to spare.
    #:
    #: It was 900, a guess, which made a 16-cycle run ask FIO for 15000s --
    #: four times the work. An over-estimate is not free: the job then far
    #: outlives the outages, so it can never be allowed to finish, and both
    #: platforms ended up killing it and grepping the wreckage instead of
    #: reading a completed run. Sized properly, FIO finishes on its own
    #: shortly after the last cycle and is judged on its real summary.
    SEC_PER_CYCLE = 450

    #: Extra time beyond the planned cycles, so the tail of the last outage is
    #: still under IO.
    FIO_SLACK_SEC = 600

    #: What each outage type actually costs, end to end, including recovery
    #: and the post-cycle checks. A flat average stopped working once the
    #: types diverged by 8x: a node reboot is now cordon + drain (which
    #: retries against pod disruption budgets) + a real RHCOS reboot measured
    #: at 7 minutes + uncordon, while a short network blip is under two.
    #:
    #: Sizing FIO off the flat 450s average would have asked for 6000s against
    #: 7470s of cycles on k8s -- the last 24 minutes of outages would have run
    #: with no live IO at all, and the availability lane would have been blind
    #: for them without saying so.
    SEC_PER_OUTAGE: ClassVar = {
        "graceful_shutdown": 170,
        "container_stop": 190,
        "storage_node_reboot": 900,
        "short_network_interrupt": 110,
        "interface_full_network_interrupt": 400,
        "node_network_isolation": 700,
    }

    #: Ceiling on the live FIO runtime. A safety stop, not a working value --
    #: the runtime comes from SEC_PER_OUTAGE summed over the planned cycles,
    #: and this only catches a plan that has grown beyond what any single run
    #: should be. Every second FIO runs past the last outage is a second spent
    #: waiting for it, so the estimate wants to be close, not merely large.
    FIO_MAX_RUNTIME = 10800


    def run(self):
        self._init_lblk()
        self.assert_cluster_is_lblk()
        self.assert_journals_live()

        pool = self._make_pool()
        self._baselines = {}      # lvol -> {path: md5}, taken before any fault
        self._static = []         # (lvol, mount) pairs in the durability lane

        nodes = self.sbcli_utils.get_storage_nodes()["results"]
        if len(nodes) < 2:
            raise LblkPreconditionError(
                f"need at least 2 storage nodes to take one down, have "
                f"{len(nodes)}")

        # Every outage type against every node. Ordered by outage rather than
        # by node so consecutive cycles land on different machines: taking the
        # same node down four times in a row tests recovery from a warm cache
        # more than it tests the cluster, and it leaves the other nodes
        # untouched for a quarter of the run.
        # Where live FIO runs, and the one outage it cannot sit through.
        #
        # This cluster has no client-role nodes, so an unpinned FIO pod lands
        # on a storage worker. Reserve one node for the client -- real
        # deployments separate the two -- and pin every live FIO job there.
        #
        # That node still takes its turn at most outages. Losing SPDK on the
        # machine the client happens to sit on is if anything the sharper
        # test: the pod keeps its network, so it must carry on being served by
        # the surviving peers, and the erasure coding has to do exactly what
        # it exists for.
        #
        # The network cut is the exception. It drops that node's traffic to
        # its peers in BOTH directions, so a client there cannot reach any
        # other node either. FIO would fail for lack of a path rather than for
        # lack of data, and the run would record a loss of availability that
        # only means the client was inside the blast radius. So the reserved
        # node sits that one out.
        self._fio_home_node = self._pick_fio_home(nodes)
        no_client_evict = [n for n in nodes
                           if n is not self._fio_home_node] or nodes
        skip_env = os.environ.get("LBLK_MATRIX_SKIP_OUTAGES")
        skip = ({o.strip() for o in skip_env.split(",") if o.strip()}
                if skip_env is not None else set(self.SKIP_OUTAGES))
        outages = [o for o in self.OUTAGES if o not in skip]
        if not outages:
            raise LblkPreconditionError(
                f"[matrix] every outage type is skipped ({sorted(skip)}), so "
                f"this run would prove nothing")
        for o in sorted(skip):
            self.logger.warning(
                "[matrix] NOT exercising outage type %r on this platform", o)

        cycles = []
        for outage in outages:
            # Named targets, not "pool": this loop used to bind its node list
            # to `pool`, clobbering the storage pool created above. Every
            # volume was then requested in a pool whose name was a list of
            # node dicts, and the API answered "Pool not found:" followed by a
            # dump of every storage node -- which reads like a cluster fault
            # rather than a variable collision.
            cycles += self._cycles_for(outage, nodes, no_client_evict)

        # Full coverage is every type on every node, and on a six-node cluster
        # that is 24 cycles at roughly ten minutes each -- about four hours.
        # That is the right default for the question this test answers, but it
        # does not fit a short pipeline slot, so LBLK_MATRIX_NODES trims the
        # node count without touching the code or dropping an outage type:
        # every type still runs, just against fewer machines.
        cap = int(os.environ.get("LBLK_MATRIX_NODES", "0") or 0)
        if cap and cap < len(nodes):
            trimmed = nodes[:cap]
            trimmed_no_cut = [n for n in trimmed
                              if n is not self._fio_home_node] or trimmed
            cycles = []
            for outage in outages:
                # Same helper as the full path, so trimming cannot quietly
                # change WHICH outages land on the FIO worker. This used to be
                # its own expression and had drifted: it knew about
                # CLIENT_EVICTING_OUTAGES but not about FIO_WORKER_OUTAGES.
                cycles += self._cycles_for(outage, trimmed, trimmed_no_cut)
            self.logger.warning(
                "[matrix] LBLK_MATRIX_NODES=%d: running %d of %d nodes, so "
                "%d cycles instead of %d. Coverage of outage TYPES is "
                "unchanged; coverage of nodes is not.",
                cap, len(trimmed), len(nodes), len(cycles),
                len(outages) * len(nodes))
        by_type = {}
        for node, outage in cycles:
            by_type.setdefault(outage, []).append(node.get("mgmt_ip"))
        for outage, ips in by_type.items():
            self.logger.info("[matrix]   %-34s %d node(s): %s",
                             outage, len(ips), ", ".join(ips))
        self.logger.info(
            "[matrix] %d cycles planned across %d outage type(s), "
            "~%.1fh at %ds per cycle",
            len(cycles), len(outages),
            len(cycles) * self.SEC_PER_CYCLE / 3600.0, self.SEC_PER_CYCLE)

        self._build_static_set(pool)
        live = self._start_live_fio(pool, cycles)

        for i, (node, outage) in enumerate(cycles, start=1):
            self.logger.info("[matrix] cycle %d/%d: %s on %s",
                             i, len(cycles), outage, node.get("mgmt_ip"))
            # Where each FIO job is standing BEFORE the outage. For the
            # *_fio_worker cycles this is the thing being tested -- it has to
            # be read now, because afterwards the old pod is gone and there
            # is nothing left to say where it used to be.
            self._fio_nodes_before = self._fio_job_nodes(live)
            self._outage_and_recover(node, outage)
            # Durability, checked here rather than only at the end so a
            # mismatch names the outage and the node that produced it.
            where = f"after {outage} on {node.get('mgmt_ip')}"
            # Cluster first. A cycle that leaves the cluster degraded has
            # failed, whatever the data says -- and if it is not caught here
            # the next cycle cuts a node on an already-degraded cluster and
            # stops being the single-node outage it reports itself as.
            self._assert_cluster_healthy(where, outage_type=outage)
            self._assert_static_unchanged(where)
            self._verify_raw(where)
            self._scan_spdk_logs(where)
            self._assert_attached(where)
            self._assert_fio_alive(live, where, outage_type=outage,
                                   outage_ip=node.get("mgmt_ip"))

        self._finish_live_fio(live)
        self._assert_static_unchanged("after all outages")
        self.logger.info(
            "[matrix] %d cycles survived (%d outage types x %d nodes): %d "
            "static volume(s) byte identical throughout, live FIO "
            "uninterrupted on %d volume(s)",
            len(cycles), len(outages), len(nodes), len(self._static),
            len(live))

    def _pick_fio_home(self, nodes):
        """Reserve one storage node to host live FIO. Returns the node dict.

        None on docker, where FIO runs on real client machines that were never
        storage nodes, and None on a cluster with client-role nodes, where the
        FIO job already keeps away from storage on its own.
        """
        self._fio_home_worker = None
        if not self.k8s_test or not nodes:
            return None
        k8s = self._ensure_k8s_utils()
        if k8s.has_client_nodes():
            self.logger.info("[matrix] cluster has client-role nodes; FIO "
                             "already avoids storage nodes")
            return None

        home = nodes[0]
        ip = home.get("mgmt_ip")
        try:
            worker = k8s.get_pod_node_name(k8s.get_spdk_pod_name(ip))
        except Exception as exc:                      # noqa: BLE001
            self.logger.warning(
                "[matrix] could not resolve the worker behind %s (%s); live "
                "FIO will be scheduled freely and a network outage on its "
                "node may fail it for reasons unrelated to the product",
                ip, str(exc)[:120])
            return None
        if not worker:
            return None
        self._fio_home_worker = worker
        self.logger.warning(
            "[matrix] no client-role nodes: reserving %s (%s) for live FIO "
            "and excluding it from the outages that would take the client with "
            "it (%s), so a FIO failure never just means the client was inside "
            "the blast radius. Shutdown, restart and pod delete still run on "
            "it: those stop the SPDK pod, not the worker, and FIO keeps going",
            worker, ip, ", ".join(self.CLIENT_EVICTING_OUTAGES))
        return home

    # ── the durability lane ───────────────────────────────────────────────

    def _build_static_set(self, pool):
        """Write the payload that must survive, and record what it hashes to.

        Each flavour is provisioned the way its platform does it, then filled
        with the same payload, so a divergence between flavours points at the
        flavour rather than at the workload.
        """
        if not isinstance(pool, str) or not pool:
            raise LblkPreconditionError(
                f"[matrix] pool must be a name, got {type(pool).__name__} "
                f"{str(pool)[:120]!r}. The API accepts whatever it is handed "
                f"and reports 'Pool not found' with the value echoed back, so "
                f"a wrong type here surfaces as a cluster fault.")
        flavours = [
            ("plain", dict()),
            ("crypto", dict(crypto=True)),
            ("dhchap", dict(dhchap=True)),
            ("nsvol", dict(namespaced=True)),
        ]
        for label, opts in flavours:
            name = f"mx{label}{random.randint(100, 999)}"
            mount = self._provision_typed(name, pool, **opts)
            self._write_static(name, mount)
            self._baselines[name] = self._static_md5(name, mount)
            self._static.append((name, mount))
            self.logger.info("[matrix] %s volume %s seeded, md5=%s",
                             label, name, self._baselines[name])

        # A clone carries its parent's bytes, so it is the cheapest check that
        # snapshot/clone on lblk preserves data across the same outages.
        # Clone a volume WITHOUT namespace slots, so the clone opens its own
        # subsystem and connects normally. self._static[0] is the namespaced
        # child's host and is the one volume that would swallow it.
        host = (getattr(self, "_ns_parent_info", None) or {}).get("name")
        parent = next((n for n, _m in self._static if n != host),
                      self._static[0][0])
        snap = f"mxsnap{random.randint(100, 999)}"
        snap_id = self._create_snapshot_dual(parent, snap)
        clone = f"mxclone{random.randint(100, 999)}"
        if self.k8s_test:
            _dev, cmount = self._create_clone_dual(
                snap_id, clone, size=self.LVOL_SIZE,
                mount_path=f"/mnt/{clone}", format_disk=False)
        else:
            cmount = self._clone_and_attach(snap_id, clone)
        if self.k8s_test:
            # _create_clone_dual returns (pvc_name, pvc_name) on k8s -- its
            # "mount" is the claim name, not a path, because nothing is
            # mounted on the runner. The bytes live at create_utility_pod's
            # mount_path inside the pod that reads them, which is where every
            # other k8s volume in this test is read from too. Using the
            # returned value produced md5sum "can't open
            # 'mxclone363/static.dat'".
            cmount = self.K8S_MOUNT
        self._baselines[clone] = self._static_md5(clone, cmount)
        self._static.append((clone, cmount))
        self.logger.info("[matrix] clone %s of %s seeded, md5=%s",
                         clone, parent, self._baselines[clone])

        # Raw block device, stamped with crc32c rather than md5: no filesystem
        # in the way, which is the strongest integrity check available here and
        # the one the rest of the lblk suite gates on.
        raw = f"mxraw{random.randint(100, 999)}"
        # _provision_raw, not _create_and_connect. On k8s the latter makes an
        # ordinary filesystem PVC and returns (None, name) without touching
        # _lblk_devices or building a verifier -- so _stamp_all and
        # _verify_raw would both iterate an empty dict and do nothing, while
        # the log claimed a device had been stamped. _provision_raw asks CSI
        # for a volumeMode: Block PVC and attaches it through volumeDevices,
        # which is the only way to get a device with no filesystem on it here.
        self._provision_raw(raw, pool)
        if not self._lblk_devices:
            raise LblkPreconditionError(
                f"[matrix] {raw} produced no raw device, so the crc32c lane "
                f"would verify nothing. A raw block device is the one check "
                f"here with no filesystem in the way; silently skipping it "
                f"would leave the strongest gate untested.")
        self._stamp_all()
        self.logger.info("[matrix] raw device(s) %s stamped with crc32c",
                         ", ".join(f"{n}->{d}"
                                   for n, (_h, d) in self._lblk_devices.items()))

    def _provision_typed(self, name, pool, crypto=False, dhchap=False,
                         namespaced=False):
        """A formatted volume of the requested flavour, on either platform.

        Docker varies the lvol: crypto and namespace packing are per-lvol
        flags, DHCHAP a property of the pool it lives in.

        K8s goes through CSI, so the flavour lives in the StorageClass -- but
        not uniformly. Crypto and namespaced use a class we build ourselves,
        which is fine because nothing about them depends on node identity.
        DHCHAP must use the class the OPERATOR generates for its pool: that
        one carries dhchap_node_label, and the nodeAffinity CSI writes from it
        is the thing that actually enforces allowedNodes at mount time. See
        _dhchap_k8s for why building our own there does not work.
        """
        label = ("dhchap" if dhchap else "crypto" if crypto
                 else "nsvol" if namespaced else "plain")
        try:
            return self._provision_typed_inner(name, pool, crypto, dhchap,
                                               namespaced)
        except Exception as exc:                      # noqa: BLE001
            # Encryption and DHCHAP on lblk have never been exercised. When one
            # cannot even be provisioned that IS the finding, so name the
            # flavour rather than leaving a bare "PVC not Bound within 300s"
            # that says nothing about which combination is unsupported.
            raise LblkPreconditionError(
                f"[matrix] could not provision a {label} volume on an lblk "
                f"cluster: {type(exc).__name__}: {exc}") from exc

    def _provision_typed_inner(self, name, pool, crypto, dhchap,
                               namespaced=False):
        if not self.k8s_test:
            # Docker: every flavour is an lvol flag or a property of its pool.
            kwargs = dict(lvol_name=name, size=self.LVOL_SIZE,
                          pool_name=(self._dhchap_pool() if dhchap else pool))
            if crypto:
                kwargs["crypto"] = True

            if namespaced:
                # A namespaced volume is a CHILD: it joins a subsystem that
                # already exists rather than opening one. Two things make that
                # happen, and the previous attempt had neither right.
                #
                # `namespace=True` alone on a standalone volume still opens a
                # fresh subsystem -- it came back as ns_id 1 of its own NQN,
                # which then could not be resolved because nothing had
                # connected it. And the slots have to exist on the PARENT:
                # max_namespace_per_subsys belongs on the volume being joined,
                # not on the one joining.
                #
                # So pin it to the parent's node, because a subsystem is
                # per-node and an unpinned child lands elsewhere and opens its
                # own. Same shape as _create_namespaced_children in
                # continuous_failover_ha_multi_outage.py.
                parent = self._ns_parent()
                kwargs["namespace"] = True
                if parent.get("node_id"):
                    kwargs["host_id"] = parent["node_id"]
                self.sbcli_utils.add_lvol(**kwargs)
                return self._mount_namespaced(name, parent=parent)

            # Slots go ONLY on the volume that will host the namespaced
            # child -- the first one created. Handing them to every volume
            # had a second effect I did not intend: a clone of a parent with
            # free slots stays in the parent's subsystem instead of opening
            # its own, so it too arrived with no new controller and
            # _connect_and_mount_dual failed with
            #
            #   No new block device after connecting mxclone761
            #
            # which is the same discovery problem as the namespaced child,
            # reached from a different direction. Restricting the slots keeps
            # every clone in a subsystem of its own.
            if not getattr(self, "_ns_parent_info", None):
                kwargs["max_namespace_per_subsys"] = 30
            self.sbcli_utils.add_lvol(**kwargs)
            _dev, mount = self._connect_and_mount_dual(
                name, mount_path=f"/mnt/{name}", format_disk=True)
            self._remember_ns_parent(name)
            return mount

        k8s = self._ensure_k8s_utils()
        if crypto:
            self._assert_kms_usable()
        if dhchap:
            sc, pin = self._dhchap_k8s()
        else:
            sc, pin = self._typed_storage_class(crypto, namespaced), None

        pvc = self._k8s_normalize_name(name)
        k8s.create_pvc(name=pvc, size=self.LVOL_SIZE.replace("G", "Gi"),
                       storage_class=sc)
        if pin:
            # The operator's DHCHAP class is WaitForFirstConsumer, so the PVC
            # stays Pending until something is scheduled against it -- and it
            # must be scheduled on an allowed node, because CSI writes a
            # matching nodeAffinity onto the PV. Bind it with a throwaway pod
            # pinned there.
            binder = f"bind-{pvc}"[:63]
            k8s.create_utility_pod(binder, pvc, node_selector=pin)
            try:
                # Wait on the CLAIM, not on the pod. Binding happens as soon
                # as the scheduler places the pod, which is the whole point of
                # WaitForFirstConsumer; the pod itself never has to reach
                # Running. Waiting for Running instead is what turned a bound
                # PVC into a 300s timeout.
                k8s.wait_pvc_bound(pvc)
            finally:
                k8s.delete_pod(binder, wait=True)
        else:
            k8s.wait_pvc_bound(pvc)
        self._volume_registry[name] = {"pvc_name": pvc, "mount": self.K8S_MOUNT,
                                       "node_selector": pin}
        # Deliberately NOT registered in _fs_volumes. _verify_all runs a write
        # workload over everything in there, and a static volume that gets
        # written to is no longer a static volume -- the md5 baseline would be
        # measuring the test's own IO.
        return self.K8S_MOUNT

    def _assert_kms_usable(self):
        """An encrypted volume needs KMS, so check it before asking for one.

        Without this the failure is a 300s PVC timeout whose only detail is
        "not Bound", and the cause sits three layers down: the CSI driver's
        CreateVolume gets POST 500 from the control plane, whose log shows
        KMSException("Authentication failed") from _hcp.py -- because openbao
        is sealed.

        It re-seals on its own. The seal type is shamir with no auto-unseal,
        so any fresh openbao process comes up sealed, and the workflow unseals
        only once at setup. On 2026-09-20 the pod was recreated after setup
        (restartCount 0 but newer than its namespace), came back sealed, and
        every encrypted volume failed from then on.

        Checking pod readiness rather than the seal status directly: openbao's
        readiness probe already tracks sealed state, and `bao status` needs
        BAO_SKIP_VERIFY here because the server certificate carries no IP SAN.
        """
        k8s = self._ensure_k8s_utils()
        out, _err = k8s._exec_kubectl(
            "kubectl get pods -n vault -l app.kubernetes.io/name=openbao "
            "--no-headers -o custom-columns=NAME:.metadata.name,"
            "READY:.status.containerStatuses[0].ready 2>/dev/null || true")
        rows = [ln.split() for ln in (out or "").splitlines() if ln.strip()]
        if not rows:
            raise LblkPreconditionError(
                "[matrix] no openbao pod in namespace vault, so KMS cannot "
                "serve an encryption key and every crypto volume will fail to "
                "provision. Run the workflow's 'Setup KMS (vault)' step, or "
                "drop the crypto flavour for this run.")
        unready = [r[0] for r in rows if len(r) < 2 or r[1] != "true"]
        if unready:
            raise LblkPreconditionError(
                f"[matrix] openbao {unready} is not ready, which on a shamir "
                f"seal means sealed. The control plane will answer CreateVolume "
                f"with POST 500 (KMSException: Authentication failed) for every "
                f"encrypted volume. Unseal it -- the keys from setup are in "
                f"openbao-init.txt on the runner -- then re-run.")
        self.logger.info("[matrix] KMS ready: %s",
                         " ".join(r[0] for r in rows))

    def _remember_ns_parent(self, name):
        """Record the first connected volume as the namespaced one's parent."""
        if getattr(self, "_ns_parent_info", None):
            return
        try:
            lvol_id = self.sbcli_utils.get_lvol_id(name)
            det = self.sbcli_utils.get_lvol_details(lvol_id=lvol_id)[0]
            self._ns_parent_info = {"name": name, "nqn": det.get("nqn"),
                                    "node_id": det.get("node_id")}
            self.logger.info("[matrix] %s will host the namespaced child "
                             "(node=%s)", name, det.get("node_id"))
        except Exception as exc:                      # noqa: BLE001
            self.logger.warning("[matrix] could not record %s as a namespace "
                                "parent: %s", name, str(exc)[:120])
            self._ns_parent_info = {}

    def _ns_parent(self):
        parent = getattr(self, "_ns_parent_info", None)
        if not parent:
            raise LblkPreconditionError(
                "[matrix] no connected volume to host a namespaced child. A "
                "namespaced volume joins an existing subsystem; without a "
                "parent it opens its own and is not testing namespace "
                "sharing at all.")
        return parent

    def _clone_and_attach(self, snap_id, clone):
        """Create a clone on docker and attach it, whichever way it landed.

        Where a clone ends up is not decided by what it was cloned FROM. The
        backend puts it in any subsystem on that node with free namespace
        slots, so once one volume on the node has slots -- and one must, to
        host the namespaced child -- a clone can be packed in beside it. Then
        there is no new controller to find and the connect path fails with

          No new block device after connecting mxclone348

        Choosing a differently-sourced parent did not avoid this, because the
        source was never what governed it. So: try the ordinary connect, and
        if the clone turns out to have joined an existing subsystem, resolve
        it by (NQN, ns_id) exactly as a namespaced volume is resolved.

        Never formatted either way. A clone carries its parent's bytes and
        this test compares its md5 against them; mkfs would destroy the very
        thing being checked.
        """
        self.sbcli_utils.add_clone(snapshot_id=snap_id, clone_name=clone)
        try:
            _dev, mount = self._connect_and_mount_dual(
                clone, mount_path=f"/mnt/{clone}", format_disk=False)
            if mount:
                return mount
            raise AssertionError("no mount returned")
        except AssertionError as exc:
            self.logger.info(
                "[matrix] %s did not bring up a controller of its own (%s); "
                "it joined an existing subsystem -- resolving by ns_id",
                clone, str(exc)[:80])
        return self._mount_namespaced(clone, format_fs=False)

    def _mount_namespaced(self, name, parent=None, retries=5, delay=4,
                          format_fs=True):
        """Find a namespaced volume's device without connecting to it.

        A namespaced volume joins an existing subsystem rather than opening
        its own, so it has no controller of its own and must NOT be
        nvme connect-ed. The client is already attached: connect answers
        "already connected", no new controller appears, and a before/after
        device diff finds nothing --

          AssertionError: No new block device after connecting mxlivensvol191

        which is how this failed while the namespace was present and working
        the whole time. It is surfaced by rescanning the controllers and
        locating it by (NQN, ns_id), the same way _create_namespaced_children
        does in continuous_failover_ha_multi_outage.py.
        """
        lvol_id = self.sbcli_utils.get_lvol_id(name)
        details = self.sbcli_utils.get_lvol_details(lvol_id=lvol_id)[0]
        nqn, ns_id = details.get("nqn"), details.get("ns_id")
        if parent and parent.get("nqn") and nqn != parent["nqn"]:
            # Not fatal: the backend puts a child in any subsystem on that
            # node with room, which may be a different volume's. Worth saying
            # so, because the resolve below then looks somewhere unexpected.
            self.logger.warning(
                "[matrix] %s joined %s rather than %s's subsystem",
                name, (nqn or "?")[-24:], parent.get("name"))
        if not isinstance(ns_id, int) or ns_id < 1:
            raise LblkPreconditionError(
                f"[matrix] {name} reports ns_id={ns_id!r}. Without a usable "
                f"NSID the only way to pick a device is 'any head on this "
                f"NQN', which on a shared subsystem is a sibling's device -- "
                f"and the next step formats whatever it is handed.")

        client = (self.fio_node or self.client_machines)[0]
        for _ in range(retries):
            self.ssh_obj.rescan_live_nvme_controllers(client)
            device = self.ssh_obj.get_nvme_device_for_nqn(client, nqn,
                                                          ns_id=ns_id)
            if device:
                mount = f"/mnt/{name}"
                self.logger.info(
                    "[matrix] %s is ns_id %s on %s (no connect needed) -> %s",
                    name, ns_id, nqn[-24:], device)
                if format_fs:
                    self.ssh_obj.format_disk(node=client, device=device,
                                             fs_type="ext4")
                self.ssh_obj.mount_path(node=client, device=device,
                                        mount_path=mount)
                if not self.ssh_obj.is_mountpoint(client, mount):
                    raise LblkPreconditionError(
                        f"[matrix] {name} resolved to {device} but would not "
                        f"mount at {mount}")
                self._volume_registry[name] = {
                    "device": device, "mount": mount, "lvol_id": lvol_id}
                return mount
            sleep_n_sec(delay)
        raise LblkPreconditionError(
            f"[matrix] {name} never surfaced on {client} as ns_id {ns_id} of "
            f"{nqn}. The namespace exists on the target; the client did not "
            f"see it after {retries} controller rescans.")

    def _pin_for(self, lvol_name):
        """nodeSelector a pod touching this volume must carry, or None."""
        return (self._volume_registry.get(lvol_name) or {}).get("node_selector")

    def _dhchap_pool(self):
        """A DHCHAP-enabled pool, created once per run (docker)."""
        if getattr(self, "_dhchap_pool_name", None):
            return self._dhchap_pool_name
        self._dhchap_pool_name = f"mxdhchap{random.randint(100, 999)}"
        self._add_pool_dual(pool_name=self._dhchap_pool_name, dhchap=True)
        return self._dhchap_pool_name

    def _dhchap_k8s(self):
        """DHCHAP on k8s, the operator's own path. Returns (sc, nodeSelector).

        The first attempt created a second pool with dhchap=True and no
        allowedNodes, then built its own StorageClass. The operator never
        reconciled it -- "Pool not visible in sbcli after 300s" -- because a
        DHCHAP pool with no allowedNodes has no hosts to authorise. Four
        things are required, and the security suite established all of them:

        1. allowedNodes must be a real subset, so the operator can derive each
           allowed node's NQN and label those nodes.
        2. The StorageClass has to be the operator's, not ours: it carries
           dhchap_node_label, which is what makes CSI write a nodeAffinity
           onto every PV, and that is what enforces the restriction at mount.
        3. The CSI node plugin snapshots node labels as topology keys only at
           REGISTRATION, so a pool created while it is already running is
           invisible to it and every PVC fails on topology. The daemonset has
           to be restarted once per pool.
        4. That class is WaitForFirstConsumer, so a PVC does not bind until a
           pod is scheduled against it on an allowed node.
        """
        cached = getattr(self, "_dhchap_k8s_cache", None)
        if cached:
            return cached
        k8s = self._ensure_k8s_utils()

        workers = self._k8s_worker_names()
        if len(workers) < 2:
            raise LblkPreconditionError(
                f"[matrix] DHCHAP needs at least two workers so allowedNodes "
                f"can be a strict subset; saw {workers}")
        allowed = workers[:-1]

        pool = f"mxdhchap{random.randint(100, 999)}"
        # sbcli_utils, not k8s_utils: add_storage_pool lives on K8sSbcliUtils
        # (k8s_utils.py:3837) and only that one takes allowed_nodes and
        # storage_class_parameters. K8sUtils has no add_storage_pool at all,
        # and calling it there raised AttributeError after the pool had
        # already been named. In k8s mode self.sbcli_utils IS a K8sSbcliUtils
        # (cluster_test_base.py:210), which is how the security suite reaches
        # the same method.
        actual = self.sbcli_utils.add_storage_pool(
            pool_name=pool, cluster_id=self.cluster_id, dhchap=True,
            allowed_nodes=allowed,
            storage_class_parameters={"filesystem": "ext4"})
        pool = actual or pool

        crd = self._k8s_pool_crd_name(pool)
        sc = k8s.operator_storage_class_name(crd)
        if not k8s.wait_storage_class_exists(sc):
            raise LblkPreconditionError(
                f"[matrix] the operator did not generate StorageClass {sc!r} "
                f"for DHCHAP pool {crd!r}; nothing to provision from")

        label = self._discover_pool_label(allowed, workers[-1:])
        if not k8s.restart_csi_node_driver(expect_topology_key=label,
                                           expect_on_nodes=allowed):
            self.logger.warning(
                "[matrix] CSI re-register did not surface %r as a topology "
                "key; DHCHAP provisioning may fail on allowedTopologies",
                label)

        # A NODE NAME, not a label expression. create_utility_pod and
        # create_fio_job both render node_selector as
        #   nodeSelector: { kubernetes.io/hostname: <value> }
        # so passing "<label>=allowed" asked the scheduler for a node whose
        # hostname was literally that string. Nothing matched, the binder pod
        # stayed Pending, and it timed out after 300s. The label still matters
        # -- it is what CSI turns into the PV's nodeAffinity, and what the
        # topology key check above looks for -- but it is not how a pod is
        # pinned. Same as _k8s_bind_pvc in the security suite, which passes
        # self._dhchap_allowed_nodes[0].
        pin = allowed[0]
        self.logger.info(
            "[matrix] DHCHAP pool %s: allowed=%s, sc=%s, label=%s, pinning to %s",
            crd, allowed, sc, label, pin)
        self._dhchap_k8s_cache = (sc, pin)
        self._dhchap_allowed = allowed
        return self._dhchap_k8s_cache

    def _discover_pool_label(self, allowed, disallowed, timeout=180):
        """Read the operator's pool label off the nodes it labelled.

        Rebuilding the key is what broke this. The real one is

            storage.simplyblock.io/storage-pool.<pool uuid> = allowed

        and the version constructed from namespace, cluster id and CRD name --
        simplyblock.io/pool.<ns>.<cluster>.<crd> -- matches nothing. The binder
        pod's nodeSelector then selected no node at all and sat Pending until
        the 300s timeout, with the CSI re-register warning as the only hint.

        Identified by behaviour rather than by name: the key carries the value
        "allowed" on every allowed node and is absent from the excluded one.
        That pins down this pool's label even when other pools have labelled
        the same nodes, and it keeps working if the operator changes the
        naming scheme.
        """
        k8s = self._ensure_k8s_utils()
        deadline = time.time() + timeout
        seen = set()
        while True:
            out, _err = k8s._exec_kubectl(
                "kubectl get nodes -o json 2>/dev/null || true")
            try:
                nodes = {n["metadata"]["name"]: (n["metadata"].get("labels") or {})
                         for n in json.loads(out).get("items", [])}
            except (ValueError, AttributeError, KeyError):
                nodes = {}

            if nodes:
                on_allowed = [
                    {k for k, v in nodes.get(n, {}).items() if v == "allowed"}
                    for n in allowed]
                common = set.intersection(*on_allowed) if on_allowed else set()
                for n in disallowed:
                    common -= set(nodes.get(n, {}))
                seen = common
                if len(common) == 1:
                    label = common.pop()
                    self.logger.info("[matrix] pool node label: %s", label)
                    return label
                if len(common) > 1:
                    # More than one pool has labelled exactly this set. Prefer
                    # the storage-pool key; anything else is not ours to pin on.
                    pref = sorted(k for k in common if "storage-pool" in k)
                    if pref:
                        self.logger.info(
                            "[matrix] %d candidate labels %s; using %s",
                            len(common), sorted(common), pref[-1])
                        return pref[-1]

            if time.time() >= deadline:
                raise LblkPreconditionError(
                    f"[matrix] the operator never labelled {allowed} for this "
                    f"DHCHAP pool within {timeout}s (candidates seen: "
                    f"{sorted(seen) or 'none'}). Without that label there is no "
                    f"nodeSelector that selects an allowed node, so the PVC "
                    f"cannot bind and DHCHAP is not enforced either.")
            sleep_n_sec(5)

    def _k8s_worker_names(self):
        k8s = self._ensure_k8s_utils()
        out, _ = k8s._exec_kubectl(
            "kubectl get nodes -l node-role.kubernetes.io/control-plane!= "
            "--no-headers -o custom-columns=NAME:.metadata.name")
        return [ln.strip() for ln in (out or "").splitlines() if ln.strip()]

    def _k8s_pool_crd_name(self, pool_name):
        """The StoragePool CRD name the operator built its label from.

        Not necessarily the backend pool name: the CRD name can pick up a
        suffix or be truncated to fit the 63-char label budget, and the label
        is derived from the CRD name, not from the backend name.
        """
        try:
            details = self.sbcli_utils.get_pool_by_id(
                self.sbcli_utils.get_storage_pool_id(pool_name))
            if isinstance(details, list):
                details = details[0] if details else {}
            cr_name = (details or {}).get("cr_name")
            if cr_name:
                return cr_name
        except Exception as exc:                      # noqa: BLE001
            self.logger.info("[matrix] cr_name unavailable (%s); assuming the "
                             "CRD is named after the pool", str(exc)[:100])
        return pool_name

    def _typed_storage_class(self, crypto, namespaced):
        """Our own StorageClass for the non-DHCHAP flavours.

        Safe to build here, unlike DHCHAP: no node label is involved, so
        nothing depends on the operator's generated class, and ours is
        volumeBindingMode: Immediate, which keeps provisioning simple.
        """
        key = f"crypto={crypto},ns={namespaced}"
        cache = getattr(self, "_sc_cache", None)
        if cache is None:
            cache = self._sc_cache = {}
        if key in cache:
            return cache[key]
        if not crypto and not namespaced:
            cache[key] = self._k8s_storage_class_name
            return cache[key]

        k8s = self._ensure_k8s_utils()
        name = f"{self._k8s_storage_class_name}-{'enc' if crypto else 'ns'}"
        k8s.create_storage_class(
            name=name, cluster_id=self.cluster_id, pool_name=self.pool_name,
            ndcs=self.ndcs, npcs=self.npcs, encryption=crypto,
            max_namespace_per_subsys=(4 if namespaced else 1))
        cache[key] = name
        self.logger.info("[matrix] StorageClass %s (%s)", name, key)
        return name

    def _write_static(self, lvol_name, mount):
        """Fill the volume once, with data nothing will touch again."""
        cmd = (f"dd if=/dev/urandom of={mount}/static.dat bs=1M "
               f"count={self.STATIC_MB} conv=fsync 2>&1 | tail -1")
        if self.k8s_test:
            k8s = self._ensure_k8s_utils()
            pvc = self._volume_registry[lvol_name]["pvc_name"]
            pod = f"seed-{pvc}"[:63]
            # A DHCHAP volume's PV carries a nodeAffinity for the pool's
            # allowed nodes. An unpinned pod can land elsewhere and fail to
            # mount, which would look like a storage fault rather than a
            # scheduling one.
            k8s.create_utility_pod(pod, pvc,
                                   node_selector=self._pin_for(lvol_name))
            try:
                k8s.wait_pod_running(pod)
                k8s.exec_in_pod(pod, cmd)
                k8s.exec_in_pod(pod, "sync")
            finally:
                k8s.delete_pod(pod, wait=True)
        else:
            node = self.client_machines[0]
            self.ssh_obj.exec_command(node=node, command=f"sudo {cmd}")
            self.ssh_obj.exec_command(node=node, command="sudo sync")

    def _assert_attached(self, context):
        """After any outage, nothing may be stuck waiting for a volume.

        k8s only -- docker has no attach/detach controller to get this wrong.

        Deliberately not gated on whether the outage should have moved pods.
        Multi-Attach does not need a full eviction: any detach/attach cycle
        can strand a VolumeAttachment, and the pod that wants that volume then
        waits in ContainerCreating indefinitely with nothing failing. A pod
        restarted in place re-attaches its volume too, so the assertion is
        worth making after every cycle, not only the long ones.

        STUCK pods, not raw events. A Multi-Attach event by itself is routine:
        delete a pod holding an RWO volume and create another immediately, and
        the second is told the volume is still exclusively attached until the
        first attachment is released; the controller retries and it clears.
        Our own seed and md5 utility pods do exactly that, back to back. On
        2026-09-23 an event-only check failed cycle 1 on a Multi-Attach raised
        at 09:59:13 during volume SEEDING -- before any outage -- because the
        seed pod took 31s to delete and the md5 pod was created the same
        second it finished. It resolved; the run had already used the volume
        successfully by the time the check looked.

        A pod that is Running got its volume, whatever was logged getting
        there. Only a pod still not Running, with a volume complaint against
        it, is the failure this is for.
        """
        if not self.k8s_test:
            return
        k8s = self._ensure_k8s_utils()
        # Scoped to this cycle so an earlier one's churn is not re-reported
        # against it.
        stuck = k8s.pods_stuck_on_volumes(within_sec=self.ATTACH_WINDOW_SEC)
        if stuck:
            raise LblkPreconditionError(
                f"[matrix] volume attach error {context} -- pod(s) still not "
                f"running and still waiting for a volume:\n    "
                + "\n    ".join(stuck[:6]))
        # Transient ones are worth seeing without failing on: a rise in them
        # is a signal even when everything eventually attached.
        seen = k8s.multi_attach_errors(within_sec=self.ATTACH_WINDOW_SEC)
        if seen:
            self.logger.info(
                "[matrix] %d volume-attach event(s) %s, all resolved -- every "
                "pod is running. First: %s",
                len(seen), context, seen[0][:160])
        else:
            self.logger.info("[matrix] volumes all attached %s", context)

    #: How long the cluster may stay degraded after an outage before the
    #: cycle is failed. Several outage types pass through degraded on the way
    #: back -- the node restarts, devices re-register, migration drains -- so
    #: a window is required. Being degraded when the NEXT outage is about to
    #: land is the thing this rules out. Same order as the other post-outage
    #: waits, and inside the 300s already allowed for node health.
    CLUSTER_SETTLE_SEC = 180

    #: The same, for outages that evict and restart a node. Sized from what
    #: recovery actually takes rather than guessed -- 180s was guessed twice
    #: and was marginally short both times, which is worse than being wildly
    #: wrong because it looks like a real failure:
    #:
    #:   run 20261004-164452  .244  offline 18:06:32 -> online 18:13:42   7m10s
    #:   run 20261004-215107  .245  unreach 23:08:29 -> online 23:22:10  13m41s
    #:
    #: and the node bounces on the way (in_restart -> in_shutdown -> offline
    #: -> in_restart -> online), so this is not one clean transition to wait
    #: on. Matches RESCHEDULE_SEC, whose docstring already explains why
    #: kubernetes takes minutes here. Draining outages only: a quick outage
    #: that has not settled in 180s really is stuck, and waiting 15 minutes to
    #: say so would cost every run that genuinely breaks.
    DRAIN_SETTLE_SEC = 900

    def _assert_cluster_healthy(self, where, outage_type=None):
        """Fail the cycle unless the cluster is active and every node online.

        Added after dev pointed out that run 20261003-080237 went degraded
        during cycle 12 -- a 30s cut on worker-3 took worker-4's devices
        unavailable -- and the suite passed the cycle and cut another node.
        The gate only looked at data, and the data was fine.
        """
        # One budget for both conditions, polled together. They were
        # sequential at first -- wait for the cluster, then snapshot the
        # nodes -- and run 20261004-164452 failed cycle 15 on
        # ".244=in_restart" when that node reached online FOURTEEN SECONDS
        # later. The cluster reports active while a node is still finishing
        # its restart, so a node check with no patience of its own fails a
        # cycle that was about to be fine.
        budget = (self.DRAIN_SETTLE_SEC
                  if self._drains(outage_type)
                  else self.CLUSTER_SETTLE_SEC)
        deadline = time.time() + budget
        status, not_online = None, []
        while True:
            try:
                details = self.sbcli_utils.get_cluster_details(
                    cluster_id=self.cluster_id) or {}
                status = details.get("status")
                not_online = [
                    f"{n.get('mgmt_ip')}={n.get('status')}"
                    for n in self.sbcli_utils.get_storage_nodes()["results"]
                    if n.get("status") != "online"]
            except Exception as exc:                  # noqa: BLE001
                status, not_online = f"<unreadable: {str(exc)[:80]}>", []
            if status == "active" and not not_online:
                break
            if time.time() >= deadline:
                raise LblkPreconditionError(
                    f"[matrix] cluster did not settle {where} within "
                    f"{budget}s: status={status!r}"
                    + (f", not online: {', '.join(not_online)}"
                       if not_online else "")
                    + ". Refusing to start the next outage -- cutting another "
                      "node now would exceed the 1/1 fault tolerance this run "
                      "is configured for.")
            # Say something while waiting. A draining outage can legitimately
            # take 14 minutes to settle, and a silent poll loop that long is
            # indistinguishable from a hang in the log.
            waited = int(budget - (deadline - time.time()))
            if waited % 60 < 10:
                self.logger.info(
                    "[matrix] waiting for the cluster to settle %s "
                    "(%ds/%ds): status=%s%s", where, waited, budget, status,
                    f", not online: {', '.join(not_online)}"
                    if not_online else "")
            sleep_n_sec(10)
        self.logger.info("[matrix] cluster active, all nodes online %s", where)

    def _assert_static_unchanged(self, context):
        """Every static volume must hash exactly as it did before any fault."""
        drifted = []
        for name, mount in self._static:
            expected = self._baselines[name]
            actual = self._static_md5(name, mount)
            if actual != expected:
                drifted.append(f"{name} {expected} -> {actual}")
        if drifted:
            raise LblkPreconditionError(
                f"[matrix] static data changed {context} -- nothing wrote to "
                f"these volumes after the baseline, so this is corruption, not "
                f"a workload artefact: {drifted}")
        self.logger.info("[matrix] %d static volume(s) unchanged %s",
                         len(self._static), context)

    def _verify_raw(self, context):
        """crc32c on the raw device only.

        _verify_all would also run a filesystem FIO over every registered
        formatted volume, which on this test means writing to the static set.
        The static md5 check above already covers those, and covers them more
        strictly than a fresh FIO would.
        """
        if not self.RAW_VERIFY:
            return
        for name, (client, dev) in self._lblk_devices.items():
            # _stamp_all and _verify_all both do this and this one did not,
            # which is how the gate that runs after every outage was the one
            # path with no pod check on it at all.
            client = self._ensure_raw_pod(name, client)
            self._verifier.verify(client, dev,
                                  region_size=self.VERIFY_REGION,
                                  context=f"{context} [{name}]")

    def _static_pod(self, lvol_name):
        """A utility pod kept alive for the whole run, one per static volume.

        _generate_checksums_dual creates and deletes a pod per call. Across
        every outage type on every node that is cycles x volumes pod
        lifecycles -- on a six-node cluster, well over a hundred -- which would
        cost more wall-clock than the outages themselves. Registered in
        _k8s_utility_pods so the standard teardown removes it.

        Kept alive, but not assumed to still be there. These are bare Pods
        with no controller, so nothing recreates them, and an outage can take
        one away: the drain that now precedes a node reboot reported

          evicting pod simplyblock/md5-mxdhchap637
          pod/md5-mxdhchap637 evicted

        and six minutes later the next md5 read failed with "pods
        md5-mxdhchap637 not found". The name stayed in this cache forever
        because nothing checked. A drain is only the loudest way to lose one --
        eviction under pressure or a node that never comes back would do the
        same -- so the cache is verified rather than trusted.
        """
        pods = getattr(self, "_static_pods", None)
        if pods is None:
            pods = self._static_pods = {}

        k8s = self._ensure_k8s_utils()
        pod = pods.get(lvol_name)
        if pod:
            phase = (k8s.get_pod_status_detail(pod) or {}).get("phase")
            if phase == "Running":
                return pod
            self.logger.warning(
                "[matrix] utility pod %s for %s is %s, not Running -- "
                "recreating it. Losing one does not invalidate the volume; "
                "failing to notice would, because the read that follows "
                "cannot tell a missing pod from an unchanged file.",
                pod, lvol_name, phase or "gone")
            try:
                k8s.delete_pod(pod)
            except Exception:                         # noqa: BLE001
                pass

        pvc = self._volume_registry[lvol_name]["pvc_name"]
        pod = f"md5-{pvc}"[:63]
        k8s.create_utility_pod(pod, pvc,
                               node_selector=self._pin_for(lvol_name))
        k8s.wait_pod_running(pod)
        if pod not in self._k8s_utility_pods:
            self._k8s_utility_pods.append(pod)
        pods[lvol_name] = pod
        return pod

    def _static_md5(self, lvol_name, mount):
        """md5 of the one file on a static volume.

        Refuses to return anything that is not a digest. An md5sum that prints
        nothing -- pod gone, file missing, mount vanished -- would otherwise
        compare equal to the next empty result and report the data as
        unchanged, which is the failure mode this whole lane exists to catch.
        """
        if not str(mount).startswith("/"):
            raise LblkPreconditionError(
                f"[matrix] {lvol_name} was handed {mount!r} as a mount point. "
                f"That is a claim name, not a path -- the dual helpers return "
                f"one of each depending on the platform, and only an absolute "
                f"path can be read.")
        path = f"{mount}/static.dat"
        cmd = f"md5sum {path}"
        if self.k8s_test:
            out, err = self._ensure_k8s_utils().exec_in_pod(
                self._static_pod(lvol_name), cmd)
        else:
            out, err = self.ssh_obj.exec_command(
                node=self.client_machines[0], command=f"sudo {cmd}")
        digest = (out or "").strip().split()[0] if (out or "").strip() else ""
        if len(digest) != 32:
            raise LblkPreconditionError(
                f"[matrix] could not hash {path} on {lvol_name}: md5sum gave "
                f"{out!r} / {err!r}. Refusing to treat an unreadable volume as "
                f"an unchanged one.")
        return digest

    # ── the availability lane ─────────────────────────────────────────────

    def _planned_seconds(self, cycles):
        """How long the planned cycles should take, by outage type.

        *cycles* is the (node, outage) plan, so this costs each cycle for what
        it actually is rather than multiplying a count by an average. An
        unknown type falls back to SEC_PER_CYCLE and says so -- a new outage
        added without a cost entry should undersize loudly, not silently.
        """
        total = 0
        for _node, outage in cycles:
            cost = self.SEC_PER_OUTAGE.get(outage)
            if cost is None:
                cost = self.SEC_PER_CYCLE
                self.logger.warning(
                    "[matrix] no SEC_PER_OUTAGE entry for %r; sizing it at "
                    "the %ds average, which may be wrong in either direction",
                    outage, cost)
            total += cost
        self.logger.info("[matrix] %d cycles should take about %d min",
                         len(cycles), total // 60)
        return total

    def _start_live_fio(self, pool, cycles):
        """FIO that must run, uninterrupted, across every outage.

        One volume per flavour, matching the static set. Encryption in
        particular belongs here rather than only in the static lane: the crypto
        layer sits in the IO path, so an encrypted volume carrying load while a
        node disappears is the most likely place for this to come apart, and a
        plain-only live lane would never touch it.
        """
        runtime = self._fio_runtime = min(
            self._planned_seconds(cycles) + self.FIO_SLACK_SEC,
            self.FIO_MAX_RUNTIME)
        self._fio_started_at = time.time()
        self.logger.info("[matrix] live FIO sized for %d cycles: %ds",
                         len(cycles), runtime)
        flavours = [("plain", dict()),
                    ("crypto", dict(crypto=True)),
                    ("dhchap", dict(dhchap=True)),
                    ("nsvol", dict(namespaced=True))]

        handles = []
        #: job name -> zero-arg callable that starts that job again.
        #: Needed because create_fio_job sets backoffLimit: 0, so an evicted
        #: pod is never replaced by kubernetes and the only way to resume the
        #: live lane after a draining outage is to launch it ourselves.
        self._fio_relaunch = {}
        for label, opts in flavours:
            name = f"mxlive{label}{random.randint(100, 999)}"
            mount = self._provision_typed(name, pool, **opts)
            log = (None if self.k8s_test
                   else f"{self.log_path}/fio_mx_{label}.log")
            job = f"mxlive{label}"
            # Bound as defaults, not closed over: the loop rebinds every
            # one of these and a late-binding closure would relaunch four
            # copies of the last flavour.
            def _again(_n=name, _m=mount, _l=log, _j=job, _rt=runtime):
                # Same placement rules as the first launch. A DHCHAP volume
                # has allowed nodes, so dropping node_selector here would
                # relaunch it somewhere it cannot attach.
                return self._run_fio_dual(
                    _n, mount_path=_m, log_path=_l, runtime=_rt, name=_j,
                    rw="randrw", bs="4K", numjobs=2, nrfiles=4, size="512M",
                    time_based=True,
                    node_selector=self._pin_for(_n),
                    prefer_node=(None if self._pin_for(_n)
                                 else getattr(self, "_fio_home_worker", None)))
            started = self._run_fio_dual(
                name, mount_path=mount, log_path=log,
                runtime=runtime, name=job,
                rw="randrw", bs="4K", numjobs=2, nrfiles=4, size="512M",
                time_based=True,
                # The latency ceiling is not set here: it is
                # utils.fio_defaults.FIO_MAX_LATENCY, applied to both
                # platforms by _run_fio_dual. Left to the old defaults
                # they disagreed -- run_fio_test applied 20s of its own
                # while create_fio_job applied none -- so a single IO
                # slower than 20s killed the job with err=110 on docker
                # and k8s could not trip it whatever the cluster did.
                # That is what ended mxliveplain 21s into cycle 8 on
                # 2026-09-21.
                # A pin only where the volume demands one (DHCHAP allowed
                # nodes); otherwise a preference. The FIO home used to be a
                # hard nodeSelector, which meant the Job could never be
                # rescheduled: lose that node and the replacement sits
                # Pending for ever. That is why *_fio_worker exists and why
                # it could not have been written against the old placement.
                node_selector=self._pin_for(name),
                prefer_node=(None if self._pin_for(name)
                             else getattr(self, "_fio_home_worker", None)))
            # Keyed by what _run_fio_dual RETURNS, which is the k8s Job name
            # ("fio-mxlivedhchap"), not `job` ("mxlivedhchap"). _assert_fio_alive
            # carries that same value as `handle`, so keying it any other way
            # makes every relaunch lookup miss silently.
            self._fio_relaunch[started] = _again
            handles.append((name, log, job, started))
            self.logger.info("[matrix] live FIO started on %s volume %s",
                             label, name)
        return handles

    #: job name -> node it was on before the current cycle's outage. Set per
    #: cycle in run(); the default keeps _assert_fio_alive safe for callers
    #: that never went through the loop, such as the smoke paths.
    _fio_nodes_before = None

    @staticmethod
    def _latency_ceiling_seconds():
        """FIO_MAX_LATENCY as a number, or None if it cannot be read.

        Parsed rather than duplicated. The value is deliberately a single
        constant in utils.fio_defaults and has already been 20s and 40s; 5s
        is the target with dev. Anything here that compared against a written
        down number would silently start lying the day it changes.
        """
        m = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*(us|ms|s|m)?\s*",
                         str(FIO_MAX_LATENCY))
        if not m:
            return None
        scale = {"us": 1e-6, "ms": 1e-3, "s": 1, "m": 60, None: 1}
        return float(m.group(1)) * scale[m.group(2)]

    def _cut_seconds(self, outage_type):
        """How long *outage_type* takes the client's network away, or None.

        Only meaningful for the cut-based outages. A drain or a reboot has no
        single duration worth comparing against a per-IO ceiling.
        """
        base = self.FIO_WORKER_OUTAGES.get(outage_type, outage_type)
        return {
            "short_network_interrupt": self.SHORT_NETWORK_OUTAGE_SEC,
            "interface_full_network_interrupt": self.NETWORK_OUTAGE_SEC,
            "node_network_isolation": self.NODE_ISOLATION_SEC,
        }.get(base)

    def _fio_job_nodes(self, handles):
        """Which node each live FIO job is on right now. K8s only.

        Missing entries are normal and not an error: a job between pods has
        no node, and on docker there are no jobs at all. The caller treats an
        absent entry as "cannot judge the move", which is honest, rather than
        inventing a node to compare against.
        """
        if not self.k8s_test:
            return {}
        k8s = self._ensure_k8s_utils()
        placed = {}
        for _name, _log, job, handle in handles:
            if not isinstance(handle, str):
                continue
            try:
                node = k8s.job_pod_node(handle)
            except Exception as exc:                  # noqa: BLE001
                self.logger.warning("[matrix] could not read the node for "
                                    "%s: %s", handle, str(exc)[:120])
                continue
            if node:
                placed[handle] = node
        return placed

    def _assert_fio_alive(self, handles, outage, outage_type=None,
                          outage_ip=None):
        """FIO must still be running. A job that died is an interruption.

        On k8s the handle is the Job NAME -- a non-empty string -- so the old
        `bool(handle)` was true forever and this check reported "still
        running" after every outage without looking at anything. The whole
        availability lane was a no-op on the platform it mattered most on.
        Ask the cluster instead: a Job with no pod, or whose pod has gone
        Failed/Succeeded, is a Job that stopped doing IO.
        """
        moves_asserted = 0
        for name, _log, job, handle in handles:
            if isinstance(handle, str):
                alive = self._k8s_fio_running(handle)
            else:
                # NOT handle.is_alive(). run_fio_test starts FIO in a DETACHED
                # tmux session and returns once it has confirmed the session
                # exists, so the launching thread finishes seconds later while
                # FIO runs for its full runtime. Checking the thread reported
                # "live FIO stopped" one cycle into a run where every job was
                # healthy -- a false failure on the availability gate, which
                # is worse than having no gate. Ask the client instead.
                alive = self._docker_fio_running(job)
            if outage_type in self.FIO_WORKER_MOVE_OUTAGES:
                # The whole point of this cycle. Being moved is the pass
                # condition, not a tolerated side effect, so it is asserted
                # rather than waited out: a replacement must exist, it must
                # be on a different node, and nothing may be stuck on a
                # volume. assert_clean_reschedule says which of those failed.
                was_on = (self._fio_nodes_before or {}).get(handle)
                k8s = self._ensure_k8s_utils()
                # Which k8s node did the outage actually take? prefer_node is
                # a SOFT preference, so the live FIO jobs do not all sit on
                # the reserved worker -- run 20261005-144243 had
                # fio-mxlivecrypto on worker-2 while the cut took worker-4.
                # Demanding a move from a job that was never on the cut node
                # fails a cycle for doing exactly the right thing.
                cut_node = ""
                if outage_ip:
                    try:
                        cut_node = k8s._get_k8s_node_name(outage_ip)
                    except Exception as exc:          # noqa: BLE001
                        self.logger.warning(
                            "[matrix] cannot resolve the k8s node for %s "
                            "(%s); judging the move on the recorded node "
                            "alone", outage_ip, str(exc)[:120])
                if not was_on:
                    self.logger.warning(
                        "[matrix] %s: no recorded node for %s before the "
                        "outage, so the move cannot be judged. Treating as "
                        "the plain liveness check.", outage_type, handle)
                elif cut_node and was_on != cut_node:
                    # Nothing to move. Still has to be alive, which the
                    # liveness check below covers.
                    self.logger.info(
                        "[matrix] %s: live FIO %s was on %s, not on the cut "
                        "node %s, so no move is expected of it. Checking only "
                        "that it is still running.",
                        outage_type, name, was_on, cut_node)
                    if not alive:
                        raise LblkPreconditionError(
                            f"[matrix] live FIO on {name} stopped during "
                            f"{outage_type}, and it was on {was_on} -- NOT on "
                            f"{cut_node}, the node the outage took. Nothing "
                            f"touched its node, so this is a loss of "
                            f"availability rather than an expected move.")
                    continue
                else:
                    moves_asserted += 1
                    landed = k8s.assert_clean_reschedule(
                        handle, was_on, timeout=self.RESCHEDULE_SEC)
                    self.logger.info(
                        "[matrix] %s: live FIO %s moved %s -> %s and its "
                        "volume followed. Continuity is NOT claimed for this "
                        "cycle -- the outage took its node on purpose.",
                        outage_type, name, was_on, landed)
                    continue
            elif outage_type in self.FIO_WORKER_OUTAGES:
                # A cut too short to evict anything, aimed at the client's own
                # node. Whether FIO can survive it is not a judgement call: it
                # is the cut length against FIO_MAX_LATENCY, which fails the
                # job outright when one IO exceeds it. Derived rather than
                # written down, because the ceiling is a moving number -- 40s
                # today, 20s not long ago, and 5s is what we are aiming at
                # with dev. At 5s every cut here outlasts it and this branch
                # has to keep meaning the right thing without being edited.
                #
                # No ceiling saves a client whose OWN network is cut: its IO
                # cannot complete while it has no path, so beyond the ceiling
                # stopping is physics, not a defect. The 5s target is a claim
                # about clients OUTSIDE the blast radius, and the generic
                # lanes are where that gets tested.
                cut = self._cut_seconds(outage_type)
                ceiling = self._latency_ceiling_seconds()
                # Unknown either way means 'do not claim it should have
                # survived'. Asserting survival on a guess would fail a
                # healthy cluster; requiring recovery never does.
                expect_stop = (cut is None or not ceiling
                               or cut > ceiling)
                if alive:
                    if expect_stop:
                        self.logger.warning(
                            "[matrix] %s: live FIO %s survived a %ss cut "
                            "even though the %s ceiling should have failed "
                            "it. Worth knowing -- either the IO in flight was "
                            "luckier than expected or the ceiling is not "
                            "being applied.", outage_type, name, cut,
                            FIO_MAX_LATENCY)
                    else:
                        self.logger.info(
                            "[matrix] %s: live FIO %s rode out a %ss cut, "
                            "inside the %s ceiling.",
                            outage_type, name, cut, FIO_MAX_LATENCY)
                    continue
                if not expect_stop:
                    raise LblkPreconditionError(
                        f"[matrix] {outage_type}: live FIO {name} stopped "
                        f"during a {cut}s cut of its own node, which is "
                        f"INSIDE the {FIO_MAX_LATENCY} ceiling. It should "
                        f"have ridden this out, so this is a real loss of "
                        f"availability rather than our own timeout firing.")
                self.logger.info(
                    "[matrix] %s: live FIO %s stopped during a %ss cut, "
                    "which the %s ceiling makes expected. Waiting up to %ds "
                    "for IO to resume.", outage_type, name, cut,
                    FIO_MAX_LATENCY, self.FIO_RESUME_SEC)
                deadline = time.time() + self.FIO_RESUME_SEC
                while time.time() < deadline:
                    sleep_n_sec(15)
                    if self._k8s_fio_running(handle):
                        self.logger.info("[matrix] %s: live FIO %s is "
                                         "running again.", outage_type, name)
                        break
                else:
                    raise LblkPreconditionError(
                        f"[matrix] {outage_type}: live FIO {name} stopped "
                        f"during the cut and was still not running "
                        f"{self.FIO_RESUME_SEC}s after it ended. Stopping is "
                        f"expected -- the cut outlasts the "
                        f"{FIO_MAX_LATENCY} ceiling -- but the Job should "
                        f"have restarted it once the network came back.")
                continue
            if not alive and self._drains(outage_type):
                # The drain asked for this. Give the Job controller a moment
                # to place the replacement, then judge it on whether IO
                # resumed -- not on whether it never stopped.
                self.logger.info(
                    "[matrix] live FIO on %s was moved by the drain; waiting "
                    "for the Job to place it again", name)
                sleep_n_sec(60)
                alive = (self._k8s_fio_running(handle)
                         if isinstance(handle, str)
                         else self._docker_fio_running(job))
                if alive:
                    self.logger.info(
                        "[matrix] live FIO on %s resumed after the drain. "
                        "Continuity is NOT claimed for this cycle -- the "
                        "outage evicted it on purpose -- but IO is flowing "
                        "again and the data checks still gate the run.", name)
                    continue
                # Nothing is coming: create_fio_job sets backoffLimit: 0, so
                # the evicted pod counts as a failed one and the Job is done.
                # That is our Job spec, not a product failure, so relaunch
                # rather than fail -- but relaunch explicitly and loudly
                # rather than raising backoffLimit, which would also retry a
                # genuinely failing FIO and bury the first pod's IO errors.
                again = getattr(self, "_fio_relaunch", {}).get(handle)
                if again is not None:
                    self.logger.warning(
                        "[matrix] live FIO on %s was evicted by %s and the "
                        "Job cannot replace it (backoffLimit: 0). Relaunching "
                        "it. Continuity is NOT claimed for this cycle, and "
                        "the IO it would have done during the outage is not "
                        "covered -- the static md5 and raw crc32c checks are "
                        "what gate integrity here.", name, outage)
                    try:
                        again()
                        continue
                    except Exception as exc:          # noqa: BLE001
                        raise LblkPreconditionError(
                            f"[matrix] live FIO on {name} was evicted by "
                            f"{outage} and could not be relaunched: "
                            f"{str(exc)[:200]}") from exc
                raise LblkPreconditionError(
                    f"[matrix] live FIO on {name} was evicted by the drain "
                    f"during {outage} and did not come back, and no relaunch "
                    f"was recorded for job {handle!r}.")
            if not alive:
                ran = time.time() - getattr(self, "_fio_started_at", 0)
                if ran >= self._fio_runtime:
                    # It finished its run rather than dying. The outages
                    # outlasted the job, which is a sizing problem on our
                    # side, not a loss of availability -- say which.
                    self.logger.warning(
                        "[matrix] live FIO on %s completed its %ds runtime "
                        "before the outages finished (%.0fs elapsed); later "
                        "cycles ran without it. Raise SEC_PER_CYCLE.",
                        name, self._fio_runtime, ran)
                    continue
                raise LblkPreconditionError(
                    f"[matrix] live FIO on {name} stopped during {outage}. "
                    f"With ndcs/npcs {self.ndcs}/{self.npcs} one node down is "
                    f"meant to be survivable, so IO ending here is a loss of "
                    f"availability, not an expected blip.")
        if outage_type in self.FIO_WORKER_MOVE_OUTAGES and not moves_asserted:
            # The cycle ran, nothing broke, and it tested nothing. prefer_node
            # is a soft preference, so every live FIO job can drift off the
            # reserved worker and the cut then lands on a node with no client
            # on it. Passing silently here is how a lane keeps reporting green
            # while covering less and less.
            self.logger.warning(
                "[matrix] %s took its node but NO live FIO job was on it, so "
                "nothing was asked to move and this cycle proves nothing "
                "about moving a client off a dead node. prefer_node is a soft "
                "preference; the jobs had drifted elsewhere.", outage_type)
        self._collect_fio_findings(handles, outage)
        self.logger.info("[matrix] live FIO still running after %s", outage)

    def _collect_fio_findings(self, handles, outage):
        """Bank what the FIO logs say now, before a pod can take them away.

        _finish_live_fio reads the logs of the pods a Job has at the END. A
        pod evicted by a drain takes its log with it, so an io_u error it
        recorded is gone by then and the final verdict silently covers only
        the FIO that ran since the last eviction. Scanning every cycle and
        keeping the findings closes that hole; the final read stays, and the
        two are reported together.

        k8s only -- on docker FIO writes to a file on the client that no
        outage removes.
        """
        if not self.k8s_test:
            return
        kept = getattr(self, "_fio_findings", None)
        if kept is None:
            kept = self._fio_findings = []
        k8s = self._ensure_k8s_utils()
        for name, _log, _job, handle in handles:
            if not isinstance(handle, str):
                continue
            try:
                for pod in (k8s.get_job_pod_names(handle) or []):
                    logs = k8s.get_pod_logs(pod, tail=2000) or ""
                    for line in logs.splitlines():
                        low = line.strip().lower()
                        if "max latency exceeded" in low or re.search(
                                r"\berr=\s*110\b", low):
                            continue          # latency, judged separately
                        if (any(m in low for m in self.FIO_ERROR_MARKERS)
                                or re.search(r"\berr=\s*[1-9]", low)):
                            entry = f"[{outage}] {pod}: {line.strip()[:160]}"
                            if entry not in kept:
                                kept.append(entry)
            except Exception as exc:          # noqa: BLE001
                self.logger.warning(
                    "[matrix] could not scan FIO logs for %s after %s: %s",
                    name, outage, str(exc)[:120])

    def _await_fio_done(self, handles):
        """Wait for every live FIO job to finish, then leave it to be judged.

        Bounded by the runtime it was given plus slack: if a job is still
        going well past that, something is wrong with it rather than with the
        cluster, and hanging here forever would hide that.
        """
        started = getattr(self, "_fio_started_at", time.time())
        deadline = started + self._fio_runtime + self.FIO_SLACK_SEC + 300
        for name, _log, job, handle in handles:
            while time.time() < deadline:
                running = (self._k8s_fio_running(job) if self.k8s_test
                           else self._docker_fio_running(job))
                if not running:
                    self.logger.info("[matrix] live FIO on %s finished after "
                                     "%.0fs", name, time.time() - started)
                    break
                sleep_n_sec(20)
            else:
                self.logger.warning(
                    "[matrix] live FIO on %s was still running %.0fs after it "
                    "started, past its %ds runtime; judging it where it is",
                    name, time.time() - started, self._fio_runtime)
                if self.k8s_test:
                    continue
                client = (self.fio_node or self.client_machines)[0]
                self.ssh_obj.exec_command(
                    node=client,
                    command=(f"sudo tmux kill-session -t fio_{job} "
                             f"2>/dev/null || true"))

    def _docker_fio_running(self, job):
        """Is FIO still running on the client, as a tmux session or process?

        Both halves of this check used to answer "yes" unconditionally.

        `pgrep -f` matches the FULL command line of every process, and the
        shell running this very command has `fio_<job>` in its own cmdline --
        which `fio.*<job>` matches inside that single token. So the fallback
        found itself, every time. On the run of 2026-09-21 mxliveplain died at
        22:39:23, 21s into cycle 8, and this reported it "still running" at
        22:39:45 and after all eight remaining cycles; the failure only
        surfaced at final validation two hours later. _await_fio_done then sat
        from 22:58 to 00:27 waiting for four processes that had already gone.
        `[f]io` cannot match the literal string that produced it, which is the
        oldest fix there is for exactly this. BOTH patterns need it, not just
        the pgrep one: they share a command line, so a bare `fio_<job>` in the
        tmux half is still a `[f]io.*<job>` match for the pgrep half. As a grep
        pattern `[f]io_<job>` matches the session name exactly as before.

        `tmux has-session -t` resolves a target by exact name, then PREFIX,
        then pattern, so `fio_mxlive` would happily match `fio_mxlivecrypto`.
        Comparing against `list-sessions` output is exact, and greps tmux's
        output rather than the process table.
        """
        client = (self.fio_node or self.client_machines)[0]
        out, _err = self.ssh_obj.exec_command(
            node=client,
            command=(f"sudo tmux list-sessions -F '#S' 2>/dev/null "
                     f"| grep -qx '[f]io_{job}' && echo ALIVE "
                     f"|| (pgrep -f '[f]io.*{job}' >/dev/null "
                     f"&& echo ALIVE || echo GONE)"))
        return "ALIVE" in (out or "")

    #: Lines that mean the workload actually hit an error, as opposed to the
    #: word "error" appearing in a summary. A clean FIO log says "err= 0";
    #: only a non-zero err, or one of these phrases, is a failure.
    FIO_ERROR_MARKERS = ("io_u error", "verify failed", "bad magic header",
                         "hdr_fail", "data mismatch", "checksum error")

    def _k8s_finish_fio(self, job, volume):
        """Stop a live FIO job and fail on anything its log recorded.

        The job runs for the length of the matrix, so there is nothing to wait
        for: every io_u error raised during an outage is already in the log by
        the time the last cycle finishes.
        """
        k8s = self._ensure_k8s_utils()
        pods = k8s.get_job_pod_names(job) or []
        if not pods:
            raise LblkPreconditionError(
                f"[matrix] live FIO job {job} ({volume}) has no pod to read, "
                f"so nothing can be said about whether IO continued.")
        io_errors, lat_breaches = [], []
        worst_ns = 0
        for pod in pods:
            logs = k8s.get_pod_logs(pod, tail=4000) or ""
            for line in logs.splitlines():
                low = line.strip().lower()
                m = re.search(r"latency of (\d+) nsec", low)
                if m:
                    worst_ns = max(worst_ns, int(m.group(1)))
                # err=110 is ETIMEDOUT from our own --max_latency, never the
                # storage saying no. Keep it out of the IO-error bucket: FIO's
                # summary repeats it per job block, so counting it as an error
                # both overstates the damage and hides a real EIO among it.
                is_lat = ("max latency exceeded" in low
                          or re.search(r"\berr=\s*110\b", low))
                if is_lat:
                    lat_breaches.append(f"{pod}: {line.strip()[:160]}")
                elif any(mk in low for mk in self.FIO_ERROR_MARKERS):
                    io_errors.append(f"{pod}: {line.strip()[:160]}")
                elif re.search(r"\berr=\s*[1-9]", low):
                    io_errors.append(f"{pod}: {line.strip()[:160]}")
        try:
            k8s.delete_job(job)
        except Exception as exc:                      # noqa: BLE001
            self.logger.warning("[matrix] could not delete FIO job %s: %s",
                                job, str(exc)[:120])
        worst_s = worst_ns / 1e9
        if io_errors:
            raise LblkPreconditionError(
                f"[matrix] live FIO on {volume}: IO ERRORS. The storage failed "
                f"IO outright across {len(self.OUTAGES)} outage types. "
                f"With ndcs/npcs {self.ndcs}/{self.npcs} one node down is meant "
                f"to be survivable, so this is a loss of availability."
                + (f" Worst single IO also took {worst_s:.1f}s."
                   if worst_s else "")
                + "\n    " + "\n    ".join(io_errors[:6]))
        if lat_breaches:
            raise LblkPreconditionError(
                f"[matrix] live FIO on {volume}: LATENCY BREACH, no IO errors. "
                f"Every IO was eventually served and no data was lost -- FIO "
                f"ended the job because a single IO exceeded "
                f"--max_latency={FIO_MAX_LATENCY}"
                + (f"; the worst took {worst_s:.1f}s" if worst_s else "")
                + ". At --iodepth=1 that is one operation blocked that long, "
                "not queueing. Not the same finding as an EIO, and not a "
                "threshold artefact either: the ceiling detected the stall, "
                "it did not cause it.\n    "
                + "\n    ".join(lat_breaches[:4]))
        self.logger.info("[matrix] live FIO on %s: %d pod log(s), no IO errors "
                         "and no latency breach (worst IO %.3fs)",
                         volume, len(pods), worst_s)

    def _k8s_fio_running(self, job_name):
        """Is this FIO Job still moving IO?"""
        k8s = self._ensure_k8s_utils()
        pods = k8s.get_job_pod_names(job_name) or []
        if not pods:
            self.logger.warning("[matrix] FIO job %s has no pod", job_name)
            return False
        for pod in pods:
            detail = k8s.get_pod_status_detail(pod) or {}
            phase = (detail.get("phase") or detail.get("reason") or "").lower()
            if phase in ("running", "podinitializing", "containercreating"):
                return True
            self.logger.warning("[matrix] FIO pod %s is %r (%s)", pod,
                                phase or "unknown",
                                str(detail.get("message", ""))[:120])
        return False

    def _finish_live_fio(self, handles):
        """Join, then hold every job to zero IO errors.

        Deliberately strict. Sudden node loss is the normal case in the field,
        and the cluster's erasure coding exists precisely so a client never
        sees it, so an io_u error during any outage -- graceful or not -- is a
        defect rather than something to triage away.
        """
        # Let the jobs finish rather than killing them. Sized from measured
        # cycle time, FIO ends on its own shortly after the last outage, so
        # what gets validated is a completed run with a real summary instead
        # of whatever a killed process happened to have flushed.
        self._await_fio_done(handles)
        sleep_n_sec(10)
        # Judge all four, then report together. This used to raise on the
        # first one, so when the plain volume failed on 2026-09-21 the crypto,
        # dhchap and namespaced jobs were never looked at at all -- and the
        # single most useful thing about a four-flavour lane is whether a
        # failure hit one of them or all of them.
        failures = []
        for name, log, job, handle in handles:
            if isinstance(handle, threading.Thread):
                # Reaping the launcher, which has long since returned. NOT
                # hasattr(handle, "join"): on k8s the handle is the Job NAME
                # and str.join exists, so that called "jobname".join(
                # timeout=...) and died with "str.join() takes no keyword
                # arguments" after all 12 cycles had passed.
                handle.join(timeout=60)
            try:
                self._judge_one_fio(name, log, handle)
            except Exception as exc:                  # noqa: BLE001
                failures.append(f"{name}: {str(exc)[:400]}")
                self.logger.error("[matrix] live FIO on %s FAILED: %s",
                                  name, str(exc)[:200])
            else:
                self.logger.info("[matrix] live FIO on %s completed clean",
                                 name)
        banked = getattr(self, "_fio_findings", [])
        if banked:
            failures.append(
                f"IO errors recorded DURING the run, on pods that a later "
                f"drain evicted and whose logs are gone from the final read "
                f"({len(banked)}):\n        " + "\n        ".join(banked[:8]))
        if failures:
            raise LblkPreconditionError(
                f"[matrix] {len(failures)} of {len(handles)} live FIO "
                f"volume(s) did not survive the outages:\n    "
                + "\n    ".join(failures))

    def _judge_one_fio(self, name, log, handle):
        """Hold one live FIO job to zero IO errors."""
        if self.k8s_test:
            # NOT validate_fio_job: it calls wait_job_complete(timeout=600)
            # and this job is deliberately sized to outlast the whole matrix,
            # so it was still Running and the run failed with
            #
            #   FIO Job 'fio-mxliveplain' did not succeed (status=timeout)
            #   (pod phase=Running)
            #
            # after all 12 cycles had passed. Same shape as the docker side:
            # stop the job, then judge what it wrote.
            self._k8s_finish_fio(handle, name)
        else:
            self.common_utils.validate_fio_test(
                node=self.client_machines[0], log_file=log,
                md5_severity=self._md5_severity)


class LblkOutageMatrixDocker(_LblkDockerMixin, _LblkOutageMatrix):
    """Availability and durability across every outage type, docker."""


class LblkOutageMatrixK8s(_LblkK8sMixin, _LblkOutageMatrix):
    """Availability and durability across every outage type, k8s-native.

    The network cut is left out here, and only here. A k8s node is not just a
    storage node: OVN's geneve overlay runs between the same node IPs, so a
    blanket cut took every pod-to-pod link across nodes with it, FoundationDB
    lost quorum and the control plane could no longer read its own database
    -- FDBError 1031, on cycle 13 of 15, after twelve clean cycles.

    _network_outage has since been narrowed to the node's own service ports
    so the overlay and the database survive, but that has not been proven on
    a real run, and an outage that breaks the cluster rather than the node
    invalidates every cycle after it. Docker has no such entanglement and
    keeps all four types.

    Put it back with LBLK_MATRIX_SKIP_OUTAGES="".
    """

    #: interface_full_network_interrupt stays out (see above). The two new
    #: ones are k8s's to run, and they replace what it loses:
    #:
    #: * short_network_interrupt -- the same storage-port cut, 30s, so the node
    #:   never leaves Ready. Tests the data path riding out a blip, which is
    #:   what the skipped outage was for, at a duration that cannot take OVN
    #:   and FoundationDB down with it.
    #: * node_network_isolation -- total isolation, held past the 300s
    #:   unreachable toleration so the scheduler evicts and moves the pods.
    #:   k8s-only by definition. This one WILL disturb the overlay, which is
    #:   the point: it is the only way to reach eviction, and the cut restores
    #:   itself from a timer on the host, so the node rejoins without help.
    SKIP_OUTAGES = ("interface_full_network_interrupt",)
