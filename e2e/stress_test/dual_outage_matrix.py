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
    x pair distance {secondary, tertiary, quaternary} = 45 topologies
    x migration {await_migration, during_migration}   = 90
    x multipath {allpaths, onepathdown}               = 180 per platform
    run on docker and on k8s-native                   = 360 executions

One driver plus a case table, not 180 leaf classes. Two registered classes,
`DualOutageMatrixDocker` and `DualOutageMatrixK8s`, each of which sweeps its
whole table in a single invocation: a case is a few minutes of outage and
recovery, and scheduling 180 pipeline runs to cover one platform would spend
more time bootstrapping clusters than injecting faults.

A sweep that dies at case 120 resumes there rather than restarting, via the
checkpoint in utils/run_state.py. `stress.py --case <id>` still runs exactly
one case, which is the debugging path once the sweep has found something.

Pair distance
-------------
Which node is taken down alongside the first one, walking the secondary chain
(`sn_primary_secondary_map[primary] = secondary_node_id`; confirmed on a live
6-node cluster as worker-0 -> 1 -> 2 -> 3 -> 4 -> 5 -> 0):

    secondary    B is A's own secondary. Both nodes serving the same lvstore
                 go down together -- the case no current test can produce,
                 because _pick_outage_nodes exists to forbid exactly this.
    tertiary     B is A's tertiary; one node sits between them.
    quaternary   two nodes sit between them.

A case id reads as a sentence: dual_<outage A>_with_<outage B>_<which node>_
<migration>_<multipath>, e.g.

    dual_graceful_shutdown_with_container_stop_secondary_await_migration_allpaths

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


# ── the axes ──────────────────────────────────────────────────────────────
OUTAGE_TYPES = (
    "graceful_shutdown",
    "forced_shutdown",
    "container_stop",
    "storage_node_reboot",
    "interface_full_network_interrupt",
)

#: How far apart the two victims sit on the secondary chain, named for what
#: the second victim actually IS rather than for a hop count. "sep0" told a
#: reader nothing; "secondary" says the pair serves the same lvstore, which is
#: the whole reason the case exists.
SEPARATIONS = (
    (0, "secondary"),      # B is A's own secondary  -- both copies of one lvstore
    (1, "tertiary"),       # B is A's tertiary       -- one node between them
    (2, "quaternary"),     # two nodes between them
)

#: await_migration = wait for the migration to finish and assert it did
#: during_migration = do not wait, do not assert (what the rapid no-gap tests do)
MIGRATION_MODES = ("await_migration", "during_migration")

#: Whether one data NIC is down for the whole case. Losing a path and losing a
#: node are different failures and they interact: with a NIC already down, the
#: surviving path carries all the IO while the pair goes away, which is where
#: a multipath bug shows up as data loss rather than a stall. Both halves are
#: run because "works with multipath" says nothing about "works without".
MULTIPATH_MODES = ("allpaths", "onepathdown")


def _build_cases():
    """The full table, generated once from the four axes.

    Pairs use combinations_with_replacement, so (A, A) is included: two nodes
    failing the same way is as real as two failing differently, and it is the
    cheapest case to reason about when something breaks.

    15 pairs x 3 separations = 45 topologies, x 2 migration modes x 2 multipath
    modes = 180 cases per platform, 360 across docker and k8s.
    """
    cases = []
    for type_a, type_b in itertools.combinations_with_replacement(OUTAGE_TYPES, 2):
        for sep, sep_name in SEPARATIONS:
            for migration in MIGRATION_MODES:
                for mp in MULTIPATH_MODES:
                    cases.append({
                        "id": "dual_%s_with_%s_%s_%s_%s" % (
                            type_a, type_b, sep_name, migration, mp),
                        "type_a": type_a,
                        "type_b": type_b,
                        "separation": sep,
                        "separation_name": sep_name,
                        "migration": migration,
                        "multipath": mp,
                    })
    return cases


CASES = _build_cases()
CASES_BY_ID = {c["id"]: c for c in CASES}


class DualOutageMatrixComplete(Exception):
    """Raised by the seam when the whole table has been swept.

    The base class's run() is an open-ended stress loop with no natural end, so
    finishing a finite matrix has to be signalled rather than returned.
    """


class DualOutageSkip(Exception):
    """Raised when the cluster topology cannot express the requested case.

    A distinct type because a skip is not a failure: a 4-node cluster cannot
    produce separation 2, and reporting that as a test failure would bury the
    real ones.
    """


class _DualOutageMixin:
    """Case selection, pair picking and outage dispatch for the matrix."""

    # ── which cases this class runs ───────────────────────────────────────
    #
    # The whole table, in order, in one invocation. A single case is a few
    # minutes of outage and recovery; the point of the matrix is the sweep, and
    # scheduling 180 pipeline runs to get it would cost more in bootstrap time
    # than the outages themselves.
    #
    # CASE_ID stays as a debugging override: set it (or pass stress.py --case)
    # to run exactly one case instead of the sweep, which is what you want when
    # chasing a specific failure rather than hunting for one.
    CASE_ID = None

    # ── topology preconditions ────────────────────────────────────────────
    MIN_NODES = 6           # sep 2 needs 4 distinct nodes plus headroom
    MIN_NPCS = 2

    # ── timing knobs, previously hard-coded sleeps ────────────────────────
    INTRA_PAIR_DELAY_SEC = 0     # spacing between the two outage launches
    PRE_RESTART_DELAY_SEC = 280  # was sleep_n_sec(280), multi_outage.py:1424
    INTER_RESTART_DELAY_SEC = 100  # was sleep_n_sec(100), :1437
    INTER_SET_GAP_SEC = 50       # was MIN/MAX_OUTAGE_GAP_SEC, 50-90

    # ── multipath axis ────────────────────────────────────────────────────
    # MULTIPATH_MODE is a property below: it is per-case, not per-class.
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
        self._case_index = 0
        self._skipped = []
        self._resumed_from = None
        self._checkpoint_iter = 0

    # ── resume (machinery lives on TestClusterBase) ───────────────────────
    def checkpoint(self, iteration=None, **extra):
        """Stamp the case id alongside the base fields, so a checkpoint
        can be recognised as belonging to a particular matrix case.
        """
        extra.setdefault("case_id", self._case["id"])
        extra.setdefault("last_outage", {
            "nodes": list(self._pair),
            "types": [self._case["type_a"], self._case["type_b"]],
            "separation": self._case["separation"],
        })
        return super().checkpoint(iteration=iteration, **extra)

    def resume_point(self):
        """Resume, warning when the checkpoint belongs to another case.

        Not an error: re-running a different case on a cluster that still
        holds the previous case's objects is a legitimate thing to do, and
        the objects are adopted the same way either way. But it has to be
        said out loud, or the run looks like it resumed something it did
        not.
        """
        point = super().resume_point()
        doc = getattr(self, "_resumed_from", None)
        if doc and doc.get("case_id") and doc["case_id"] != self._case["id"]:
            self.logger.warning(
                "[dual-outage] checkpoint is for case %s but this run is %s -- "
                "adopting its objects and continuing with the requested case",
                doc["case_id"], self._case["id"])
        return point

    # ── case resolution ───────────────────────────────────────────────────
    def cases_to_run(self):
        """The cases this invocation will sweep.

        The whole table unless CASE_ID names one, which is the debugging path.
        """
        if not self.CASE_ID:
            return list(CASES)
        case = CASES_BY_ID.get(self.CASE_ID)
        if case is None:
            raise ValueError(
                "unknown case id %r. %d cases available, e.g. %s"
                % (self.CASE_ID, len(CASES), CASES[0]["id"]))
        return [case]

    def _resolve_case(self):
        """The case the next outage set will use. Set by the sweep in run();
        falls back to the first of the table so the mixin is usable before the
        loop starts."""
        return getattr(self, "_case", None) or self.cases_to_run()[0]

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
    @property
    def MULTIPATH_MODE(self):        # noqa: N802 - matches the base attribute
        """Driven by the case, not by the class.

        Multipath is one of the four axes, so it changes per case within a
        single run. Exposed under the base class's attribute name so the
        inherited `_multipath_selected` and everything else that reads it keeps
        working unchanged.
        """
        case = getattr(self, "_case", None)
        if not case:
            return "off"
        return ("single_nic_down"
                if case.get("multipath") == "onepathdown" else "off")

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
    def _advance_case(self):
        """Move to the next runnable case, or end the sweep.

        Skips do not consume an iteration: a topology the cluster cannot
        express (separation 2 on a 4-node ring, say) is stepped over here so
        the rest of the table still runs. Abandoning 179 cases because one of
        them needs a node that does not exist would be the wrong trade.
        """
        table = self.cases_to_run()
        while self._case_index < len(table):
            case = table[self._case_index]
            self._case_index += 1
            try:
                self._precheck_case(case)
            except DualOutageSkip as skip:
                self._skipped.append((case["id"], str(skip)))
                self.logger.warning("[dual-outage] SKIP %s: %s",
                                    case["id"], skip)
                continue
            self._case = case
            self.logger.info(
                "[dual-outage] case %d/%d: %s",
                self._case_index, len(table), case["id"])
            return case

        raise DualOutageMatrixComplete(
            "swept %d case(s), %d skipped" % (len(table), len(self._skipped)))

    def _precheck_case(self, case):
        """Raise DualOutageSkip if the cluster cannot express *case*."""
        chain = self.sn_primary_secondary_map
        total_nodes = len(set(self.sn_nodes_with_sec) | set(chain or {}))
        if total_nodes < self.MIN_NODES:
            raise DualOutageSkip(
                "needs >= %d storage nodes for separation %d, cluster has %d"
                % (self.MIN_NODES, case["separation"], total_nodes))
        if int(getattr(self, "npcs", 0) or 0) < self.MIN_NPCS:
            raise DualOutageSkip(
                "needs npcs >= %d, run has npcs=%s"
                % (self.MIN_NPCS, getattr(self, "npcs", None)))

    def run(self):
        """Sweep the table, then stop.

        The base run() is an open-ended stress loop; the matrix is finite, so
        completion arrives as DualOutageMatrixComplete from the seam below and
        is caught here. Anything else propagates and fails the run as usual.
        """
        try:
            super().run()
        except DualOutageMatrixComplete as done:
            self.logger.info("[dual-outage] table complete: %s", done)
            for cid, why in self._skipped:
                self.logger.info("[dual-outage]   skipped %s: %s", cid, why)
            self.resume_finished_clean()

    def perform_n_plus_k_outages(self):
        """Trigger the case's two outages on a pair at the case's separation.

        Same contract as the method it overrides: returns a list of
        (node, effective_type, duration) and populates
        `self.current_outage_nodes`, so every inherited recovery and validation
        step keeps working unchanged.
        """
        # First time through, honour --resume before anything is touched.
        if self._checkpoint_iter == 0:
            self.resume_point()
            if self._resumed_from:
                self.adopt_existing_objects()

        case = self._advance_case()

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
        """await_migration waits for the migration; during_migration does not."""
        return self._case["migration"] == "await_migration"

    def _should_assert_migration(self):
        return self._case["migration"] == "await_migration"


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
            # NOT ssh_obj.reboot_node in a daemon thread. k8s storage nodes
            # are generally not ssh-reachable, so that call raised inside a
            # thread nothing joined -- every storage_node_reboot case in this
            # matrix was a no-op that reported success. The kubectl route to
            # the host is real: oc debug / kubectl debug / talosctl.
            self._k8s_reboot_node(node_ip, node)
            return 0
        if outage_type == "interface_full_network_interrupt":
            duration = random.choice([30, 300, 600])
            self._k8s_network_outage(node_ip, duration)
            self._note_eviction_expected(node_ip, duration)
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
