# coding=utf-8
"""Pytest harness that generates the four expansion protocols and asserts
structural invariants on each.

Running this test produces four reviewable protocol files under
``tests/expansion_sim/protocols/`` (one per scenario) — readable
RPC-by-RPC traces with port-block events and timing. The test then
checks invariants that should hold regardless of timing data:

* Move counts match the planner's expected output.
* Every Phase A re-home is wrapped in a 4-event port-block / -unblock
  pair (donor block, recipient block, recipient unblock, donor unblock).
* Every Phase B newcomer-create produces 1 + lvols-per-primary nvmf
  subsystems on the recipient (hublvol + per-lvol).
* Every re-home of sec_1 with an existing sec_2 sibling emits a
  ``bdev_nvme_attach_controller`` *followed by* ``bdev_nvme_remove_trid``
  on the sibling (additive-then-subtractive ordering).
* RPC timestamps are monotonically non-decreasing.

Unlike the live ``test_expand_4_to_5_ftt1.py`` E2E test, this harness
does NOT require FoundationDB or a working executor — it derives the
expected RPC sequence from the design (planner output + per-move
templates in :mod:`protocol_generator`). Useful for review and as a
regression spec.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import List

import pytest

from simplyblock_core.cluster_expand_planner import (
    ROLE_PRIMARY,
    ROLE_SECONDARY,
    ROLE_TERTIARY,
)
from tests.expansion_sim.protocol_generator import (
    ScenarioSpec,
    all_scenarios,
    format_protocol,
    generate_protocol,
    load_timing,
)


_HERE = Path(__file__).parent
_TIMING_PATH = _HERE / "rpc_timing.json"
_OUT_DIR = _HERE / "protocols"


@pytest.fixture(scope="module")
def timing():
    if not _TIMING_PATH.exists():
        pytest.skip(
            f"timing table {_TIMING_PATH} missing — run "
            f"'python3 -m tests.expansion_sim.extract_rpc_timing' first")
    return load_timing(_TIMING_PATH)


@pytest.fixture(scope="module", autouse=True)
def _ensure_out_dir():
    _OUT_DIR.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# Per-scenario expected counts.
#
# Derived by hand from the worked examples in single_node_expansion_plan.md
# and the p2 worked example we walked through. Anchoring these counts
# guards the planner output from silent regressions and the per-move RPC
# templates in protocol_generator.py from unintended drift.
# ---------------------------------------------------------------------------
_EXPECTED_MOVES = {
    "p1_ftt1_4_to_5": 3,    # 1 sec re-home + 1 prim + 1 sec create
    "p1_ftt2_4_to_5": 6,    # 3 re-homes + 3 newcomer creates (prim/sec/tert)
    "p2_ftt1_4_to_5": 6,    # 2 sec re-homes + 2 newcomer prims + 2 sec creates
    "p2_ftt2_4_to_5": 12,   # 6 re-homes + 2*(prim+sec+tert) creates
}


@pytest.mark.parametrize(
    "spec", all_scenarios(lvols_per_primary=8),
    ids=lambda s: s.name)
def test_protocol_invariants(spec: ScenarioSpec, timing):
    moves, events = generate_protocol(spec, timing)

    # Sanity: protocol writes to disk for review.
    text = format_protocol(spec, moves, events)
    out = _OUT_DIR / f"{spec.name}.txt"
    out.write_text(text)

    # ---- Move counts ----
    assert len(moves) == _EXPECTED_MOVES[spec.name], (
        f"{spec.name}: planner produced {len(moves)} moves, "
        f"expected {_EXPECTED_MOVES[spec.name]}")

    # Phase A precedes Phase B in the move list.
    seen_create = False
    for m in moves:
        if m.from_node_id == "":
            seen_create = True
        else:
            assert not seen_create, (
                f"Phase A move {m} appeared after a Phase B move — "
                f"breaks create-before-destroy ordering")

    # ---- Port-block events ----
    fw_events = [ev for ev in events if ev.rpc == "firewall_set_port"]

    rehomes = [m for m in moves if m.from_node_id != ""]
    creates_with_block = [m for m in moves
                          if m.from_node_id == "" and m.role != ROLE_PRIMARY]
    expected_fw = 4 * len(rehomes) + 2 * len(creates_with_block)
    assert len(fw_events) == expected_fw, (
        f"{spec.name}: {len(fw_events)} port-block events, "
        f"expected {expected_fw} (4 per re-home + 2 per "
        f"newcomer-sec/tert)")

    # Every Phase A re-home must produce its 4 firewall events in order:
    # donor pre_block, recipient pre_block, recipient post_unblock,
    # donor post_unblock.
    fw_by_move = {}
    for ev in fw_events:
        fw_by_move.setdefault(ev.move_index, []).append(ev)
    for i, m in enumerate(moves):
        if m.from_node_id == "":
            continue  # Phase B handled separately
        bucket = fw_by_move.get(i, [])
        assert len(bucket) == 4, f"{spec.name} move {i}: {len(bucket)} fw events"
        assert bucket[0].phase == "pre_block"
        assert bucket[0].target_node == m.from_node_id
        assert bucket[1].phase == "pre_block"
        assert bucket[1].target_node == m.to_node_id
        assert bucket[2].phase == "post_unblock"
        assert bucket[2].target_node == m.to_node_id
        assert bucket[3].phase == "post_unblock"
        assert bucket[3].target_node == m.from_node_id

    # ---- Per-move bdev_nvme_attach_controller exactly once on holder
    # for every sec/tert move (Phase A or B). create-primary moves never
    # attach a controller — they create the lvstore locally.
    for i, m in enumerate(moves):
        attach_events = [ev for ev in events
                         if ev.move_index == i
                         and ev.rpc == "bdev_nvme_attach_controller"
                         and ev.target_node == m.to_node_id]
        if m.role == ROLE_PRIMARY:
            assert not attach_events, (
                f"{spec.name} move {i} (create-primary) emitted "
                f"bdev_nvme_attach_controller — primary should not connect "
                f"to a hublvol")
        else:
            assert len(attach_events) == 1, (
                f"{spec.name} move {i} ({m.role}, {m.from_node_id or '-'}"
                f" -> {m.to_node_id}): {len(attach_events)} attach events")

    # ---- Newcomer create-primary creates 1 hublvol subsystem +
    # lvols_per_primary lvol subsystems on the newcomer.
    for i, m in enumerate(moves):
        if m.role != ROLE_PRIMARY:
            continue
        create_subsys = [ev for ev in events
                         if ev.move_index == i
                         and ev.rpc == "nvmf_create_subsystem"
                         and ev.target_node == m.to_node_id]
        expected = 1 + spec.lvols_per_primary  # hublvol + per-lvol
        assert len(create_subsys) == expected, (
            f"{spec.name} create-primary move {i}: "
            f"{len(create_subsys)} subsystems on {m.to_node_id}, "
            f"expected {expected}")

    # ---- Sibling reattach for sec_1 re-home with a sibling sec_2 ----
    if spec.ftt >= 2:
        for i, m in enumerate(moves):
            if m.role != ROLE_SECONDARY or m.from_node_id == "":
                continue
            attach_events = [ev for ev in events
                             if ev.move_index == i
                             and ev.rpc == "bdev_nvme_attach_controller"]
            remove_events = [ev for ev in events
                             if ev.move_index == i
                             and ev.rpc == "bdev_nvme_remove_trid"]
            # Sibling reattach should produce ONE attach on the sibling
            # plus the recipient's own attach (= 2 attaches total) and
            # ONE remove_trid on the sibling.
            assert len(attach_events) == 2, (
                f"{spec.name} sec_1 re-home move {i}: "
                f"{len(attach_events)} attach events, expected 2 "
                f"(recipient + sibling)")
            assert len(remove_events) == 1, (
                f"{spec.name} sec_1 re-home move {i}: "
                f"{len(remove_events)} remove_trid events, expected 1 "
                f"(sibling)")
            # Additive-then-subtractive: sibling attach must precede
            # sibling remove in the timeline.
            sibling_attach = attach_events[1]  # second one is the sibling
            sibling_remove = remove_events[0]
            assert sibling_attach.t_ms < sibling_remove.t_ms, (
                f"{spec.name} move {i}: sibling remove_trid happens "
                f"before attach — violates additive-then-subtractive")

    # ---- Monotonic timestamps ----
    last = -1.0
    for ev in events:
        assert ev.t_ms >= last, (
            f"timestamp went backwards: {last} -> {ev.t_ms} at {ev.rpc}")
        last = ev.t_ms


def test_protocols_are_persisted_for_review():
    """All four protocol files exist after the parametrized test ran."""
    expected = {f"{name}.txt" for name in _EXPECTED_MOVES}
    actual = {p.name for p in _OUT_DIR.glob("*.txt")}
    missing = expected - actual
    assert not missing, f"protocol files missing: {missing}"


def test_timing_table_is_present_and_covers_critical_rpcs():
    """The extracted timing table covers the RPCs that dominate move
    cost. If this fails after a log-archive update, regenerate timing
    against a richer archive."""
    timing = load_timing(_TIMING_PATH)
    critical = [
        "bdev_nvme_attach_controller",
        "bdev_alceml_create",
        "bdev_distrib_create",
        "nvmf_create_subsystem",
        "nvmf_subsystem_add_listener",
        "nvmf_subsystem_listener_set_ana_state",
        "bdev_lvol_set_lvs_opts",
        "bdev_examine",
    ]
    missing = [r for r in critical if r not in timing]
    assert not missing, (
        f"timing table missing critical RPCs: {missing} "
        f"(re-extract from a richer log archive)")
