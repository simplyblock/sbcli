"""Dual-outage case matrix: every outage-type pair, at every chain separation.

Why this exists
---------------
Every existing multi-outage test picks its victims through `_pick_outage_nodes`
(continuous_failover_ha_multi_outage.py:125), which deliberately blocks a node
and its own secondary from being chosen together. So the one topology that
matters most -- losing both nodes that can serve the same lvstore -- is the one
case no current test can produce.

The 2026-09-13 k8s RCA landed exactly there: LVS_7 lost its primary while its
secondary and tertiary were mid role-change, and a read failed with EREMOTEIO
inside the documented fault budget. This module tests that relationship
deliberately, across every combination of outage type, instead of hoping a
random sample stumbles into it.

Shape
-----
    outage types (5)
    pairs with repetition                          = 15
    x separation {0, 1, 2}                         = 45
    x migration {drain, inflight}                  = 90 case definitions
    run on docker and on k8s-native                = 180 executions

One driver plus a case table, not 90 leaf classes: two registered classes,
`DualOutageMatrixDocker` and `DualOutageMatrixK8s`, with the case chosen by
`stress.py --case`.

Separation
----------
Distance along the secondary chain, which forms a ring
(`sn_primary_secondary_map[primary] = secondary_node_id`; confirmed on a live
6-node cluster as worker-0 -> 1 -> 2 -> 3 -> 4 -> 5 -> 0):

    sep 0   B = secondary(A)      A and its own secondary. The case no current
                                  test can produce.
    sep 1   B = secondary^2(A)    one node between them (A and its tertiary)
    sep 2   B = secondary^3(A)    two nodes between them

Needs >= 6 storage nodes and npcs >= 2; below that the case is skipped with a
message saying which precondition failed, never silently downgraded.
"""

import itertools
import random
import threading
import time

from stress_test.continuous_failover_ha_multi_outage_all_nodes import (
    RandomMultiClientMultiFailoverAllNodesTest,
)
from stress_test.continuous_k8s_native_failover import K8sNativeFailoverTest
from utils.common_utils import sleep_n_sec
from utils.run_state import RunState, adopt_by_prefix, reconcile


# ── the axes ──────────────────────────────────────────────────────────────
OUTAGE_TYPES = (
    "graceful_shutdown",
    "forced_shutdown",
    "container_stop",
    "storage_node_reboot",
    "interface_full_network_interrupt",
)

SEPARATIONS = (0, 1, 2)

#: drain  = wait for the migration to finish and assert it did
#: inflight = do not wait, do not assert (what the rapid no-gap tests do)
MIGRATION_MODES = ("drain", "inflight")


def _build_cases():
    """The full table, generated once from the three axes.

    Pairs use combinations_with_replacement, so (A, A) is included: two nodes
    failing the same way is as real as two failing differently, and it is the
    cheapest case to reason about when something breaks.
    """
    cases = []
    for type_a, type_b in itertools.combinations_with_replacement(OUTAGE_TYPES, 2):
        for sep in SEPARATIONS:
            for migration in MIGRATION_MODES:
                cases.append({
                    "id": "dual_%s_%s_sep%d_%s" % (type_a, type_b, sep, migration),
                    "type_a": type_a,
                    "type_b": type_b,
                    "separation": sep,
                    "migration": migration,
                })
    return cases


CASES = _build_cases()
CASES_BY_ID = {c["id"]: c for c in CASES}


class DualOutageSkip(Exception):
    """Raised when the cluster topology cannot express the requested case.

    A distinct type because a skip is not a failure: a 4-node cluster cannot
    produce separation 2, and reporting that as a test failure would bury the
    real ones.
    """


class _DualOutageMixin:
    """Case selection, pair picking and outage dispatch for the matrix."""

    # ── case under test (set by stress.py --case) ──────────────────────────
    CASE_ID = CASES[0]["id"]

    # ── topology preconditions ────────────────────────────────────────────
    MIN_NODES = 6           # sep 2 needs 4 distinct nodes plus headroom
    MIN_NPCS = 2

    # ── timing knobs, previously hard-coded sleeps ────────────────────────
    INTRA_PAIR_DELAY_SEC = 0     # spacing between the two outage launches
    PRE_RESTART_DELAY_SEC = 280  # was sleep_n_sec(280), multi_outage.py:1424
    INTER_RESTART_DELAY_SEC = 100  # was sleep_n_sec(100), :1437
    INTER_SET_GAP_SEC = 50       # was MIN/MAX_OUTAGE_GAP_SEC, 50-90

    # ── multipath axis ────────────────────────────────────────────────────
    MULTIPATH_MODE = "off"       # off | single_nic_down | random_flap
    MULTIPATH_FLAP_INTERVAL_SEC = 120

    def _init_mixin_state(self):
        """Mutable state. Never class attributes: two leaf classes share this
        mixin, and a mutable class attribute would leak between them."""
        self._case = self._resolve_case()
        self._pair = []
        self._pair_separation = None
        self._multipath_flap_thread = None
        self._multipath_flap_stop = threading.Event()
        self._multipath_flapped_nics = []
        self._dual_outage_events = []
        self._run_state = None
        self._resumed_from = None
        self._checkpoint_iter = 0

    # ── resume ────────────────────────────────────────────────────────────
    def _state(self):
        """The RunState for this (test, cluster). Built lazily: nfs_log_base
        and cluster_id are set by the base setup(), not at mixin init."""
        if self._run_state is None:
            self._run_state = RunState(
                nfs_log_base=self.nfs_log_base,
                test_name=type(self).__name__,
                cluster_id=self.cluster_id,
                logger=self.logger,
            )
        return self._run_state

    def resume_point(self):
        """The iteration to re-enter at, or None to start fresh.

        Returns None unless --resume was passed AND a checkpoint exists for
        this exact cluster. Never guesses: a checkpoint from another cluster is
        refused by RunState.load() rather than adopted, because adopting
        objects by name across clusters binds the run to whatever happened to
        share a prefix.
        """
        if not getattr(self, "resume_requested", False):
            return None
        doc = self._state().load()
        if not doc:
            return None
        self._resumed_from = doc
        # The prefixes are what make adoption possible at all: they are random
        # per process, so without them a resumed run cannot tell its own
        # objects from anything else on the cluster.
        for attr in ("lvol_base", "clone_base", "snap_base"):
            if doc.get(attr):
                setattr(self, attr, doc[attr])
        if doc.get("pool_name"):
            self.pool_name = doc["pool_name"]
        if doc.get("case_id") and doc["case_id"] != self._case["id"]:
            self.logger.warning(
                "[dual-outage] checkpoint is for case %s but this run is %s -- "
                "continuing with the requested case", doc["case_id"],
                self._case["id"])
        self.logger.info(
            "[dual-outage] resuming %s at iteration %s with prefixes "
            "lvol=%s clone=%s snap=%s", self._case["id"], doc.get("iter"),
            doc.get("lvol_base"), doc.get("clone_base"), doc.get("snap_base"))
        return doc.get("iter")

    def adopt_existing_objects(self):
        """Reconcile the checkpoint's inventory against the live cluster.

        Reports what is missing rather than failing: a node that died mid-delete
        can legitimately leave the cluster short, and refusing to resume there
        throws away the whole point.
        """
        doc = self._resumed_from
        if not doc:
            return {}
        try:
            live = [lv["lvol_name"] for lv in
                    (self.sbcli_utils.list_lvols() or [])]
        except Exception as exc:                      # noqa: BLE001
            self.logger.warning("[dual-outage] could not list lvols for "
                                "adoption: %s", exc)
            return {}
        adopted = adopt_by_prefix(live, doc.get("lvol_base"))
        reconcile(doc.get("lvols"), adopted, self.logger, kind="lvol")
        self.logger.info("[dual-outage] adopted %d lvol(s) by prefix %s",
                         len(adopted), doc.get("lvol_base"))
        return {"lvols": adopted}

    def checkpoint(self, iteration=None):
        """Persist enough to adopt later. Called once per outage set."""
        self._checkpoint_iter = iteration or (self._checkpoint_iter + 1)
        self._state().save(
            run_dir=getattr(self, "docker_logs_path", None),
            iter=self._checkpoint_iter,
            iteration=self._checkpoint_iter,
            case_id=self._case["id"],
            lvol_base=getattr(self, "lvol_base", None),
            clone_base=getattr(self, "clone_base", None),
            snap_base=getattr(self, "snap_base", None),
            pool_name=getattr(self, "pool_name", None),
            lvols=sorted(getattr(self, "lvol_devices", {}) or {}),
            clones=sorted(getattr(self, "clone_devices", {}) or {}),
            snapshots=list(getattr(self, "snapshot_names", []) or []),
            last_outage={
                "nodes": list(self._pair),
                "types": [self._case["type_a"], self._case["type_b"]],
                "separation": self._case["separation"],
            },
        )

    def finish_clean(self):
        """Drop the checkpoint so the next run starts fresh rather than
        adopting a completed run."""
        if self._run_state is not None:
            self._run_state.clear()

    # ── case resolution ───────────────────────────────────────────────────
    def _resolve_case(self):
        case = CASES_BY_ID.get(self.CASE_ID)
        if case is None:
            raise ValueError(
                "unknown case id %r. %d cases available, e.g. %s"
                % (self.CASE_ID, len(CASES), CASES[0]["id"]))
        return case

    # ── pair selection ────────────────────────────────────────────────────
    def _pick_pair_by_separation(self, sep):
        """Pick (A, B) where B is A's secondary walked `sep + 1` hops.

        Deliberately does NOT call `_pick_outage_nodes`: that helper exists to
        forbid precisely what separation 0 requires. Walking the map directly
        is the point of this module.
        """
        chain = self.sn_primary_secondary_map
        if not chain:
            raise DualOutageSkip(
                "sn_primary_secondary_map is empty -- the node map is built at "
                "the top of run(); pair selection ran too early")

        total_nodes = len(set(self.sn_nodes_with_sec) | set(chain))
        if total_nodes < self.MIN_NODES:
            raise DualOutageSkip(
                "case %s needs >= %d storage nodes for separation %d, cluster "
                "has %d" % (self._case["id"], self.MIN_NODES, sep, total_nodes))
        if int(getattr(self, "npcs", 0) or 0) < self.MIN_NPCS:
            raise DualOutageSkip(
                "case %s needs npcs >= %d, run has npcs=%s"
                % (self._case["id"], self.MIN_NPCS, getattr(self, "npcs", None)))

        starts = list(chain.keys())
        random.shuffle(starts)
        for start in starts:
            node, walked = start, []
            ok = True
            for _ in range(sep + 1):
                node = chain.get(node)
                if not node:
                    ok = False
                    break
                walked.append(node)
            # Walking a ring can come back to the start; A and B must differ,
            # and every hop in between must be a distinct node, or the case is
            # not the topology it claims to be.
            if ok and node != start and len(set(walked)) == len(walked):
                self._pair_separation = sep
                self.logger.info(
                    "[dual-outage] case=%s sep=%d pair A=%s B=%s (chain: %s)",
                    self._case["id"], sep, start, node,
                    " -> ".join([start] + walked))
                return [start, node]

        raise DualOutageSkip(
            "no node pair at separation %d in a %d-node chain; the secondary "
            "ring is too short or has gaps" % (sep, len(chain)))

    # ── multipath axis ────────────────────────────────────────────────────
    def _apply_multipath_mode(self):
        """Deterministic multipath, replacing the 50/50 coin flip.

        The old log line covered both "fewer than 2 data NICs" and "lost the
        coin flip" with one message, so a skip was not diagnosable. Each
        precondition now says which one it was.
        """
        if self.MULTIPATH_MODE == "off":
            self.multipath_nic_disabled = False
            self.log_outage_event("ALL_NODES", "multipath",
                                  "SKIPPED (MULTIPATH_MODE=off)")
            return False

        if not self._is_multipath_enabled():
            self.multipath_nic_disabled = False
            self.log_outage_event(
                "ALL_NODES", "multipath",
                "SKIPPED (MULTIPATH_MODE=%s but not every node has 2+ data "
                "NICs)" % self.MULTIPATH_MODE)
            return False

        if self.MULTIPATH_MODE == "single_nic_down":
            self.multipath_nic_disabled = True
            nic_plans = self._disconnect_single_data_nic_all_nodes()
            self.log_outage_event(
                "ALL_NODES", "multipath_single_nic_down",
                "Disabled 1 data NIC on %d nodes (until recovery)"
                % len(nic_plans))
            self.logger.info("[dual-outage] waiting 30s for multipath failover")
            sleep_n_sec(30)
            return True

        if self.MULTIPATH_MODE == "random_flap":
            self._start_multipath_flap()
            self.multipath_nic_disabled = False
            self.log_outage_event(
                "ALL_NODES", "multipath_random_flap",
                "Flapping a random data NIC every %ds"
                % self.MULTIPATH_FLAP_INTERVAL_SEC)
            return False

        raise ValueError("unknown MULTIPATH_MODE %r" % self.MULTIPATH_MODE)

    def _start_multipath_flap(self):
        """Background NIC flapper, following the existing start_*/stop_* daemon
        pattern. Always restores the NIC it downed, including on stop, so a
        stuck test cannot strand a node with a dead interface."""
        if self._multipath_flap_thread:
            return
        self._multipath_flap_stop.clear()

        def _worker():
            while not self._multipath_flap_stop.is_set():
                try:
                    self._flap_one_random_nic()
                except Exception as exc:              # noqa: BLE001
                    self.logger.warning("[dual-outage] multipath flap failed: "
                                        "%s: %s", type(exc).__name__, exc)
                self._multipath_flap_stop.wait(self.MULTIPATH_FLAP_INTERVAL_SEC)

        self._multipath_flap_thread = threading.Thread(
            target=_worker, name="multipath-flap", daemon=True)
        self._multipath_flap_thread.start()
        self.logger.info("[dual-outage] multipath flapper started (every %ds)",
                         self.MULTIPATH_FLAP_INTERVAL_SEC)

    def _stop_multipath_flap(self):
        if not self._multipath_flap_thread:
            return
        self._multipath_flap_stop.set()
        self._multipath_flap_thread.join(timeout=60)
        self._multipath_flap_thread = None
        try:
            self._reconnect_multipath_nics()
        except Exception as exc:                      # noqa: BLE001
            self.logger.warning("[dual-outage] could not restore NICs: %s", exc)
        self.logger.info("[dual-outage] multipath flapper stopped")

    def _flap_one_random_nic(self):
        """Down one data NIC on one node, then bring it back. Platform-specific
        mechanics live in the two bases below."""
        raise NotImplementedError

    # ── the seam: replaces random victim selection ────────────────────────
    def perform_n_plus_k_outages(self):
        """Trigger the case's two outages on a pair at the case's separation.

        Same contract as the method it overrides: returns a list of
        (node, effective_type, duration) and populates
        `self.current_outage_nodes`, so every inherited recovery and validation
        step keeps working unchanged.
        """
        case = self._case

        # First time through, honour --resume before anything is touched.
        if self._checkpoint_iter == 0:
            self.resume_point()
            if self._resumed_from:
                self.adopt_existing_objects()

        self.checkpoint()
        self._apply_multipath_mode()

        pair = self._pick_pair_by_separation(case["separation"])
        self._pair = pair
        self.current_outage_nodes = []

        if getattr(self, "COLLECT_PRE_OUTAGE_DIAGNOSTICS", True):
            self.collect_outage_diagnostics(
                "pre_outage_dual_%s" % "_".join(n[:8] for n in pair))

        outage_combinations = []
        for idx, (node, outage_type) in enumerate(
                zip(pair, (case["type_a"], case["type_b"]))):
            if idx and self.INTRA_PAIR_DELAY_SEC:
                sleep_n_sec(self.INTRA_PAIR_DELAY_SEC)

            details = self.sbcli_utils.get_storage_node_details(node)
            node_ip = details[0]["mgmt_ip"]
            node_rpc_port = details[0]["rpc_port"]

            # About to make this node unreachable on purpose: reset its SSH
            # unreachable clock so a planned outage cannot trip the 2h rule.
            self.ssh_obj.notify_outage_started([node_ip])

            self.logger.info("[dual-outage] %s on %s (position %d of pair)",
                             outage_type, node, idx)
            duration = self._apply_platform_outage(
                node, outage_type, node_ip, node_rpc_port)

            effective = outage_type
            if outage_type == "interface_full_network_interrupt" and duration:
                effective = "%s_%dsec" % (outage_type, duration)

            self.log_outage_event(node, effective, "Outage started")
            outage_combinations.append((node, effective, duration or 0))
            self.current_outage_nodes.append(node)
            self._dual_outage_events.append(
                {"node": node, "type": effective, "position": idx})

        self.outage_start_time = int(time.time())
        return outage_combinations

    def _apply_platform_outage(self, node, outage_type, node_ip, node_rpc_port):
        raise NotImplementedError

    # ── migration axis ────────────────────────────────────────────────────
    @property
    def wait_for_balancing(self):
        """drain waits for the migration to finish; inflight does not."""
        return self._case["migration"] == "drain"

    def _should_assert_migration(self):
        return self._case["migration"] == "drain"


class _DualOutageDocker(_DualOutageMixin,
                        RandomMultiClientMultiFailoverAllNodesTest):
    """Docker platform binding.

    Base is the all-nodes multi-outage test, not TestLvolHACluster as first
    sketched: `_graceful_shutdown_node`, `_disconnect_full_interface` and
    `log_outage_event` live on RandomMultiClientMultiFailoverTest and
    `_forced_shutdown_node` on its all-nodes subclass, so this is the one class
    that already has all five outage mechanics.
    """

    def _apply_platform_outage(self, node, outage_type, node_ip, node_rpc_port):
        if outage_type == "container_stop":
            self.ssh_obj.stop_spdk_process(node_ip, node_rpc_port,
                                           self.cluster_id)
            return 0
        if outage_type == "graceful_shutdown":
            self._graceful_shutdown_node(node)
            return 0
        if outage_type == "forced_shutdown":
            self._forced_shutdown_node(node)
            return 0
        if outage_type == "storage_node_reboot":
            threading.Thread(target=self.ssh_obj.reboot_node,
                             args=(node_ip,), daemon=True).start()
            return 0
        if outage_type == "interface_full_network_interrupt":
            return self._disconnect_full_interface(node, node_ip)
        raise ValueError("unhandled outage type %r" % outage_type)

    def _flap_one_random_nic(self):
        plans = self._disconnect_single_data_nic_all_nodes()
        sleep_n_sec(30)
        self._reconnect_multipath_nics()
        self.logger.info("[dual-outage] flapped %d NICs", len(plans or ()))


class _DualOutageK8s(_DualOutageMixin, K8sNativeFailoverTest):
    """K8s-native platform binding."""

    def _apply_platform_outage(self, node, outage_type, node_ip, node_rpc_port):
        if outage_type == "container_stop":
            self._k8s_stop_spdk_pod(node_ip, node)
            return 0
        if outage_type == "graceful_shutdown":
            self._graceful_shutdown_node(node)
            return 0
        if outage_type == "forced_shutdown":
            self._operator_shutdown_node(node)
            return 0
        if outage_type == "storage_node_reboot":
            threading.Thread(target=self.ssh_obj.reboot_node,
                             args=(node_ip,), daemon=True).start()
            return 0
        if outage_type == "interface_full_network_interrupt":
            duration = random.choice([30, 300, 600])
            self._k8s_network_outage(node_ip, duration)
            return duration
        raise ValueError("unhandled outage type %r" % outage_type)

    def _flap_one_random_nic(self):
        """Down one data NIC on one node via the k8s nsenter+iptables path.

        Modelled on `_k8s_network_outage`, which already schedules an auto-flush
        of its rules; scoping to one NIC keeps the auto-restore, so a stuck test
        cannot strand a node.
        """
        node = random.choice(list(self.sn_primary_secondary_map.keys()))
        details = self.sbcli_utils.get_storage_node_details(node)[0]
        nics = details.get("data_nics") or []
        if len(nics) < 2:
            self.logger.info("[dual-outage] %s has %d data NIC(s), nothing to "
                             "flap without cutting the node off", node, len(nics))
            return
        iface = random.choice(nics)["if_name"]
        self._k8s_disconnect_data_nic(details["mgmt_ip"], iface, duration=30)
        self.logger.info("[dual-outage] flapped %s on %s for 30s", iface, node)


# ── registered leaf classes ───────────────────────────────────────────────
class DualOutageMatrixDocker(_DualOutageDocker):
    """Dual-outage matrix on docker. Case chosen with `stress.py --case`."""


class DualOutageMatrixK8s(_DualOutageK8s):
    """Dual-outage matrix on k8s-native. Case chosen with `stress.py --case`."""
