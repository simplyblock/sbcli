# coding=utf-8
"""Offline protocol generator for cluster-expansion scenarios.

Given a scenario (host topology, FTT, lvols-per-primary), this module:

* Calls :func:`compute_role_diff_topology` to produce the move list.
* Expands each move into the per-move RPC sequence that
  :class:`SpdkMoveExecutor` would invoke (see ``step6_design.md``), per
  the four primitive shapes:

    * **create-primary**: alceml + distrib + raid + lvstore + hublvol on
      newcomer; one nvmf subsystem + listener per lvol.
    * **create-sec / -tert**: attach hublvol controller from primary on
      holder; build alceml + distrib + raid stack; per-lvol nvmf
      subsystem + listener with ANA non_optimized + ``bdev_examine``.
    * **re-home sec / -tert**: same as create-sec on the recipient,
      followed by per-lvol subsystem_delete and bdev stack teardown on
      the donor; sec_1 case adds a sibling failover reattach.

* Wraps each move in the port-block / blocked / port-unblock phase
  machine on both donor and recipient (matches
  ``recreate_lvstore_on_non_leader``'s production behaviour at
  ``storage_node_ops.py:4346-4375``).

* Pretends each RPC takes its p50 (from ``rpc_timing.json``) and emits a
  human-readable protocol: a chronological RPC trace plus per-move and
  global summaries. Output goes to a directory the operator can review.

Important: this generator does **not** drive the executor. It synthesizes
what the executor *would* do based on the design. Useful for review,
documentation, and as a regression spec — once the runtime end-to-end is
unblocked (FDB + executor fixes), the same protocol can be the assertion
target for the live integration tests.
"""

from __future__ import annotations

import dataclasses
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from simplyblock_core.cluster_expand_planner import (
    ROLE_PRIMARY,
    ROLE_SECONDARY,
    ROLE_TERTIARY,
    RoleMove,
    compute_role_diff_topology,
)


# ---------------------------------------------------------------------------
# Timing table
# ---------------------------------------------------------------------------


# Fallback p50 (ms) when an RPC isn't present in the extracted log table.
# Sourced from the same archive: deletes are typically <= their create
# counterpart on the same SPDK process, so we use the create cost as the
# upper bound when the log window didn't capture a delete.
_FALLBACK_MS = {
    "bdev_distrib_delete": 80.0,
    "bdev_raid_delete": 6.0,
    "bdev_PT_NoExcl_create": 6.0,
    "bdev_PT_NoExcl_delete": 6.0,
    "nvmf_delete_subsystem": 5.0,
    "bdev_nvme_remove_trid": 11.0,   # sibling of attach; usually similar
    "bdev_lvol_set_leader": 6.0,
    "bdev_lvol_create_lvstore": 8.0,
    "bdev_lvol_delete_lvstore": 8.0,
    "create_lvstore": 8.0,
    "subsystem_create": 5.0,         # alias for nvmf_create_subsystem
    "subsystem_delete": 5.0,
    "listeners_create": 6.0,
    "firewall_set_port": 1.0,        # local HTTP, not SPDK; cheap
}


def load_timing(path: Path) -> Dict[str, float]:
    """Load the extracted timing JSON and return ``{rpc_name -> p50_ms}``."""
    data = json.loads(path.read_text())
    return {name: entry["p50_ms"] for name, entry in data["rpcs"].items()}


def rpc_cost_ms(timing: Dict[str, float], rpc_name: str) -> float:
    """Return p50_ms for ``rpc_name``, falling back to the static map and
    finally to a generic 5ms when neither has the call."""
    if rpc_name in timing:
        return timing[rpc_name]
    if rpc_name in _FALLBACK_MS:
        return _FALLBACK_MS[rpc_name]
    return 5.0


# ---------------------------------------------------------------------------
# Scenario specification
# ---------------------------------------------------------------------------


@dataclass
class ScenarioSpec:
    name: str                                     # e.g. "p1_ftt2_4_to_5"
    description: str
    ftt: int
    current_topology: List[List[str]]             # hosts × slots before
    new_topology: List[List[str]]                 # hosts × slots after
    lvols_per_primary: int = 8                    # FDB load
    nodes_per_host_label: int = 1                 # for protocol header

    @property
    def newcomer_node_ids(self) -> List[str]:
        existing = {n for h in self.current_topology for n in h}
        return [n for h in self.new_topology for n in h if n not in existing]


def all_scenarios(lvols_per_primary: int = 8) -> List[ScenarioSpec]:
    """The four scenarios from the design matrix."""
    return [
        ScenarioSpec(
            name="p1_ftt1_4_to_5",
            description="1 node/host, FTT1 — add a single host (= node) to "
                        "a 4-host cluster.",
            ftt=1,
            current_topology=[["n1"], ["n2"], ["n3"], ["n4"]],
            new_topology=[["n1"], ["n2"], ["n3"], ["n4"], ["n5"]],
            lvols_per_primary=lvols_per_primary,
            nodes_per_host_label=1,
        ),
        ScenarioSpec(
            name="p1_ftt2_4_to_5",
            description="1 node/host, FTT2 — add a single host to a 4-host "
                        "FTT2 cluster.",
            ftt=2,
            current_topology=[["n1"], ["n2"], ["n3"], ["n4"]],
            new_topology=[["n1"], ["n2"], ["n3"], ["n4"], ["n5"]],
            lvols_per_primary=lvols_per_primary,
            nodes_per_host_label=1,
        ),
        ScenarioSpec(
            name="p2_ftt1_4_to_5",
            description="2 nodes/host, FTT1 — add a host (2 nodes) to a "
                        "4-host × 2-slot FTT1 cluster.",
            ftt=1,
            current_topology=[["n1", "n2"], ["n3", "n4"],
                              ["n5", "n6"], ["n7", "n8"]],
            new_topology=[["n1", "n2"], ["n3", "n4"], ["n5", "n6"],
                          ["n7", "n8"], ["n9", "n10"]],
            lvols_per_primary=lvols_per_primary,
            nodes_per_host_label=2,
        ),
        ScenarioSpec(
            name="p2_ftt2_4_to_5",
            description="2 nodes/host, FTT2 — add a host (2 nodes) to a "
                        "4-host × 2-slot FTT2 cluster.",
            ftt=2,
            current_topology=[["n1", "n2"], ["n3", "n4"],
                              ["n5", "n6"], ["n7", "n8"]],
            new_topology=[["n1", "n2"], ["n3", "n4"], ["n5", "n6"],
                          ["n7", "n8"], ["n9", "n10"]],
            lvols_per_primary=lvols_per_primary,
            nodes_per_host_label=2,
        ),
    ]


# ---------------------------------------------------------------------------
# RPC trace entry
# ---------------------------------------------------------------------------


@dataclass
class RpcEvent:
    t_ms: float                # cumulative timestamp from t0 (= 0)
    target_node: str           # which node's SPDK process is hit
    rpc: str                   # function name (matches log entry)
    detail: str = ""           # short context (lvol id, bdev name, etc.)
    duration_ms: float = 0.0   # how long this RPC took (p50)
    move_index: Optional[int] = None  # which move emitted this RPC
    role: str = ""             # role being created / re-homed / created-primary
    phase: str = ""            # "pre_block" | "blocked" | "post_unblock" | "" (Phase B)


# ---------------------------------------------------------------------------
# Per-move RPC sequence builders
# ---------------------------------------------------------------------------


def _seq_create_primary(move: RoleMove, lvols_per_primary: int) -> List[Tuple[str, str, str]]:
    """Sequence of (target_node, rpc, detail) for a Phase B create-primary
    move. Mirrors what ``storage_node_ops.create_lvstore`` does.

    No port-block/unblock for create-primary — there's no donor and no
    in-flight IO on the newcomer.
    """
    target = move.to_node_id
    lvs = f"LVS_{target}"
    seq: List[Tuple[str, str, str]] = []
    # 1. Bring up the bdev stack: alceml -> distrib -> raid -> lvstore.
    seq.append((target, "bdev_alceml_create", f"alceml_{target}_0"))
    seq.append((target, "bdev_distrib_create", f"distr_{target}_0"))
    seq.append((target, "bdev_raid_create", f"raid_{target}"))
    seq.append((target, "bdev_PT_NoExcl_create", f"pt_{target}"))
    seq.append((target, "bdev_jm_create", f"jm_{target}"))
    seq.append((target, "bdev_lvol_create_lvstore", lvs))
    seq.append((target, "bdev_lvol_set_lvs_opts", f"{lvs} primary cntlid=0"))
    seq.append((target, "bdev_lvol_create_hublvol", f"hub_{target}"))
    seq.append((target, "nvmf_create_subsystem", f"hub_subsys {target}"))
    seq.append((target, "nvmf_subsystem_add_listener", f"hub_listener {target}:4421"))
    # 2. Per-lvol subsystem + listener (ANA optimized).
    for j in range(lvols_per_primary):
        nqn = f"nqn.lvol.{target}.{j}"
        seq.append((target, "nvmf_create_subsystem", nqn))
        seq.append((target, "nvmf_subsystem_add_ns", f"{nqn} ns1"))
        seq.append((target, "nvmf_subsystem_add_listener",
                    f"{nqn} {target}:4420 ANA=optimized"))
    return seq


def _seq_create_sec(move: RoleMove, primary_id: str,
                    lvols_per_primary: int) -> List[Tuple[str, str, str]]:
    """Build a sec/tert stack on ``move.to_node_id`` for ``primary_id``.

    The recipient connects to the primary's hublvol and rebuilds the
    distrib/raid stack identical to the primary's lvstore_stack template;
    then creates per-lvol subsystems with ANA non_optimized.
    """
    holder = move.to_node_id
    lvs = f"LVS_{primary_id}"
    seq: List[Tuple[str, str, str]] = []
    # Connect to the primary's hublvol via NVMe-oF and discover the lvstore.
    seq.append((holder, "bdev_nvme_attach_controller",
                f"hub_{primary_id} -> {primary_id}:4421"))
    seq.append((holder, "bdev_lvol_connect_hublvol", f"hub_{primary_id}"))
    # Replicate the primary's bdev stack on the holder.
    seq.append((holder, "bdev_alceml_create", f"alceml_{primary_id}_on_{holder}"))
    seq.append((holder, "bdev_distrib_create", f"distr_{primary_id}_on_{holder}"))
    seq.append((holder, "bdev_raid_create", f"raid_{primary_id}_on_{holder}"))
    seq.append((holder, "bdev_PT_NoExcl_create", f"pt_{primary_id}_on_{holder}"))
    seq.append((holder, "bdev_lvol_get_lvstores", lvs))
    seq.append((holder, "bdev_lvol_set_lvs_opts",
                f"{lvs} non_leader cntlid={1000 if move.role == ROLE_SECONDARY else 2000}"))
    seq.append((holder, "bdev_examine", lvs))
    # Per-lvol subsystem + listener with ANA non_optimized.
    for j in range(lvols_per_primary):
        nqn = f"nqn.lvol.{primary_id}.{j}"
        seq.append((holder, "nvmf_create_subsystem", f"{nqn} on {holder}"))
        seq.append((holder, "nvmf_subsystem_add_ns", f"{nqn} ns1"))
        seq.append((holder, "nvmf_subsystem_add_listener",
                    f"{nqn} {holder}:4420"))
        seq.append((holder, "nvmf_subsystem_listener_set_ana_state",
                    f"{nqn} ANA=non_optimized"))
    return seq


def _seq_teardown_donor(donor_id: str, primary_id: str,
                        lvols_per_primary: int) -> List[Tuple[str, str, str]]:
    """Inverse of :func:`_seq_create_sec` for teardown on the donor."""
    seq: List[Tuple[str, str, str]] = []
    # Per-lvol subsystem deletion.
    for j in range(lvols_per_primary):
        nqn = f"nqn.lvol.{primary_id}.{j}"
        seq.append((donor_id, "nvmf_delete_subsystem", f"{nqn} on {donor_id}"))
    # Tear down the bdev stack in reverse order.
    seq.append((donor_id, "bdev_PT_NoExcl_delete", f"pt_{primary_id}_on_{donor_id}"))
    seq.append((donor_id, "bdev_raid_delete", f"raid_{primary_id}_on_{donor_id}"))
    seq.append((donor_id, "bdev_distrib_delete", f"distr_{primary_id}_on_{donor_id}"))
    seq.append((donor_id, "bdev_nvme_detach_controller", f"hub_{primary_id}"))
    return seq


def _seq_sibling_reattach(sibling_id: str, primary_id: str,
                          old_failover_id: str,
                          new_failover_id: str) -> List[Tuple[str, str, str]]:
    """Additive-then-subtractive multipath repoint on a sibling sec_2.
    See ``storage_node_ops.reattach_sibling_failover``."""
    return [
        (sibling_id, "bdev_nvme_attach_controller",
         f"hub_{primary_id} multipath += {new_failover_id}"),
        (sibling_id, "bdev_nvme_remove_trid",
         f"hub_{primary_id} multipath -= {old_failover_id}"),
    ]


# ---------------------------------------------------------------------------
# Protocol generation
# ---------------------------------------------------------------------------


def _emit(events: List[RpcEvent], t: float, target: str, rpc: str,
          detail: str, timing: Dict[str, float],
          move_index: Optional[int] = None,
          role: str = "", phase: str = "") -> float:
    """Append an event with its p50 cost and return the new ``t``."""
    cost = rpc_cost_ms(timing, rpc)
    events.append(RpcEvent(
        t_ms=round(t, 3),
        target_node=target,
        rpc=rpc,
        detail=detail,
        duration_ms=cost,
        move_index=move_index,
        role=role,
        phase=phase,
    ))
    return t + cost


def generate_protocol(spec: ScenarioSpec,
                      timing: Dict[str, float]) -> Tuple[List[RoleMove],
                                                          List[RpcEvent]]:
    """Compute the planner's move list and the synthesized RPC trace."""
    moves = compute_role_diff_topology(
        spec.current_topology, spec.new_topology, ftt=spec.ftt)

    # Build a quick lookup so re-home moves can decide whether sibling
    # reattach is needed.
    primary_to_secs: Dict[str, Dict[str, str]] = {}
    # For the *post-expand* layout we know sec_1 / sec_2 from desired
    # rotation; reuse the planner formula here for sibling lookup.
    from simplyblock_core.cluster_expand_planner import _host_rotation_layout
    for pid, sec, tert in _host_rotation_layout(spec.current_topology, spec.ftt):
        primary_to_secs[pid] = {ROLE_SECONDARY: sec, ROLE_TERTIARY: tert}

    events: List[RpcEvent] = []
    t = 0.0

    for move_index, move in enumerate(moves):
        is_phase_b = move.from_node_id == ""
        primary = move.lvs_primary_node_id
        recipient = move.to_node_id
        donor = move.from_node_id

        if move.role == ROLE_PRIMARY:
            assert is_phase_b, f"primary move must be create-only: {move}"
            for tgt, rpc, det in _seq_create_primary(move, spec.lvols_per_primary):
                t = _emit(events, t, tgt, rpc, det, timing,
                          move_index=move_index, role="primary", phase="")
            continue

        # Sec/tert moves: wrapped in port-block / blocked / post-unblock on
        # both donor and recipient (Phase A) or just on recipient (Phase B
        # newcomer — no donor to coordinate with).
        # Pre-block phase: block per-lvstore IO on the relevant nodes.
        if not is_phase_b:
            t = _emit(events, t, donor, "firewall_set_port",
                      f"block {donor}:4420 (sec subsys)",
                      timing, move_index=move_index, role=move.role,
                      phase="pre_block")
            t = _emit(events, t, recipient, "firewall_set_port",
                      f"block {recipient}:4420 (sec subsys)",
                      timing, move_index=move_index, role=move.role,
                      phase="pre_block")
        else:
            t = _emit(events, t, recipient, "firewall_set_port",
                      f"block {recipient}:4420 (sec subsys)",
                      timing, move_index=move_index, role=move.role,
                      phase="pre_block")

        # Blocked phase: heavy work on recipient; donor teardown on Phase A.
        for tgt, rpc, det in _seq_create_sec(move, primary, spec.lvols_per_primary):
            t = _emit(events, t, tgt, rpc, det, timing,
                      move_index=move_index, role=move.role, phase="blocked")

        if not is_phase_b:
            for tgt, rpc, det in _seq_teardown_donor(
                    donor, primary, spec.lvols_per_primary):
                t = _emit(events, t, tgt, rpc, det, timing,
                          move_index=move_index, role=move.role, phase="blocked")
            # Sibling failover reattach (only for sec_1 moves with an
            # existing sec_2 in the BEFORE layout — sec_2 stays where it
            # is; the donor of sec_1 is being replaced).
            if move.role == ROLE_SECONDARY and spec.ftt >= 2:
                pre = primary_to_secs.get(primary, {})
                sibling = pre.get(ROLE_TERTIARY, "")
                if sibling:
                    for tgt, rpc, det in _seq_sibling_reattach(
                            sibling, primary, donor, recipient):
                        t = _emit(events, t, tgt, rpc, det, timing,
                                  move_index=move_index,
                                  role=move.role, phase="blocked")

        # Post-unblock phase.
        if not is_phase_b:
            t = _emit(events, t, recipient, "firewall_set_port",
                      f"unblock {recipient}:4420",
                      timing, move_index=move_index, role=move.role,
                      phase="post_unblock")
            t = _emit(events, t, donor, "firewall_set_port",
                      f"unblock {donor}:4420",
                      timing, move_index=move_index, role=move.role,
                      phase="post_unblock")
        else:
            t = _emit(events, t, recipient, "firewall_set_port",
                      f"unblock {recipient}:4420",
                      timing, move_index=move_index, role=move.role,
                      phase="post_unblock")

        # Cluster-map push so clients learn about the new listener set.
        t = _emit(events, t, recipient, "distr_send_cluster_map",
                  f"propagate listener change for LVS@{primary}",
                  timing, move_index=move_index, role=move.role, phase="")

    return moves, events


# ---------------------------------------------------------------------------
# Protocol formatting
# ---------------------------------------------------------------------------


def format_protocol(spec: ScenarioSpec,
                    moves: List[RoleMove],
                    events: List[RpcEvent]) -> str:
    """Return a multi-section human-readable protocol string."""
    lines: List[str] = []
    lines.append("=" * 78)
    lines.append(f"EXPANSION PROTOCOL — {spec.name}")
    lines.append("=" * 78)
    lines.append(f"description    : {spec.description}")
    lines.append(f"FTT            : {spec.ftt}")
    lines.append(f"nodes per host : {spec.nodes_per_host_label}")
    lines.append(f"lvols/primary  : {spec.lvols_per_primary}")
    lines.append(f"current topology : {spec.current_topology}")
    lines.append(f"new topology     : {spec.new_topology}")
    lines.append(f"newcomers        : {spec.newcomer_node_ids}")
    lines.append("")
    lines.append("-" * 78)
    lines.append(f"PLAN — {len(moves)} moves")
    lines.append("-" * 78)
    for i, m in enumerate(moves):
        kind = "create " if m.from_node_id == "" else "rehome "
        donor = m.from_node_id or "—"
        lines.append(
            f"  [{i:>2}] {kind}{m.role:<9} LVS@{m.lvs_primary_node_id:<3} "
            f"  {donor:>4} -> {m.to_node_id:<4}")
    lines.append("")
    lines.append("-" * 78)
    lines.append(f"RPC TIMELINE — {len(events)} events, "
                 f"total {events[-1].t_ms + events[-1].duration_ms if events else 0:.1f} ms")
    lines.append("-" * 78)
    lines.append(f"{'t_ms':>8s}  {'dur':>5s}  {'mv':>3s} {'phase':<13s}  "
                 f"{'role':<9s}  {'target':<5s}  {'rpc':<37s}  detail")
    last_move = None
    for ev in events:
        if ev.move_index != last_move:
            mv = moves[ev.move_index] if ev.move_index is not None else None
            if mv is not None:
                kind = "create" if mv.from_node_id == "" else "rehome"
                lines.append(
                    f"  --- move {ev.move_index}: {kind} {mv.role} "
                    f"LVS@{mv.lvs_primary_node_id} "
                    f"{mv.from_node_id or '-'} -> {mv.to_node_id} ---")
            last_move = ev.move_index
        lines.append(
            f"  {ev.t_ms:>7.1f}  {ev.duration_ms:>5.1f}  "
            f"{ev.move_index if ev.move_index is not None else '-':>3}"
            f" {ev.phase:<13s}  {ev.role:<9s}  {ev.target_node:<5s}  "
            f"{ev.rpc:<37s}  {ev.detail}")

    # Per-RPC summary
    lines.append("")
    lines.append("-" * 78)
    lines.append("RPC FREQUENCY")
    lines.append("-" * 78)
    counts: Dict[str, int] = {}
    cumulative_ms: Dict[str, float] = {}
    for ev in events:
        counts[ev.rpc] = counts.get(ev.rpc, 0) + 1
        cumulative_ms[ev.rpc] = cumulative_ms.get(ev.rpc, 0.0) + ev.duration_ms
    lines.append(f"  {'RPC':<37s}  {'count':>6s}  {'cumulative_ms':>14s}")
    for name in sorted(counts, key=lambda n: -cumulative_ms[n]):
        lines.append(
            f"  {name:<37s}  {counts[name]:>6d}  "
            f"{cumulative_ms[name]:>14.1f}")

    # Port-block summary (firewall events specifically)
    fw_events = [ev for ev in events if ev.rpc == "firewall_set_port"]
    lines.append("")
    lines.append("-" * 78)
    lines.append(f"PORT BLOCK / UNBLOCK EVENTS — {len(fw_events)}")
    lines.append("-" * 78)
    for ev in fw_events:
        lines.append(f"  {ev.t_ms:>7.1f}  {ev.target_node:<5s}  "
                     f"{ev.phase:<13s}  {ev.detail}")

    lines.append("")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# CLI driver
# ---------------------------------------------------------------------------


def main(argv=None):
    import argparse
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--timing", type=Path,
                   default=Path(__file__).parent / "rpc_timing.json")
    p.add_argument("--out-dir", type=Path,
                   default=Path(__file__).parent / "protocols")
    p.add_argument("--lvols-per-primary", type=int, default=8)
    p.add_argument("--scenario", type=str, default=None,
                   help="Run only the named scenario (default: all four)")
    args = p.parse_args(argv)

    timing = load_timing(args.timing)
    args.out_dir.mkdir(parents=True, exist_ok=True)

    scenarios = all_scenarios(lvols_per_primary=args.lvols_per_primary)
    if args.scenario:
        scenarios = [s for s in scenarios if s.name == args.scenario]
        if not scenarios:
            raise SystemExit(f"unknown scenario: {args.scenario}")

    for spec in scenarios:
        moves, events = generate_protocol(spec, timing)
        text = format_protocol(spec, moves, events)
        out_path = args.out_dir / f"{spec.name}.txt"
        out_path.write_text(text)
        total_ms = (events[-1].t_ms + events[-1].duration_ms) if events else 0
        print(f"  {spec.name:20s}  moves={len(moves):>3d}  "
              f"rpcs={len(events):>4d}  total={total_ms:>7.1f}ms  "
              f"-> {out_path}")


if __name__ == "__main__":
    main()
