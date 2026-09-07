"""Tripwire: no bare (unfiltered, undeclared) ``get_bdevs()`` calls in
production code.

An unfiltered ``bdev_get_bdevs`` serializes EVERY bdev on the SPDK app
thread; its cost scales with lvol+snapshot count, not device count. Run
20260725 (mass_create_delete, 3k lvols + 18k snapshots): the periodic
health-check dump grew to 18s+ per call, starving keep-alive handling on
the app thread -> KATO storms -> JC/JM exclusions -> node aborts. This is
the SECOND full-dump regression to reach a scale test (2026-07-16 was
O(N^2) dumps), so the rule is now enforced mechanically:

- periodic / hot paths must pass a ``name`` or use
  ``bdev_nvme_controller_list`` (scales with attached controllers);
- cold paths that genuinely need the full inventory (node-add on a
  near-empty node) must declare it with ``get_bdevs(all_bdevs=True)``,
  which makes them grep-visible and keeps this test green.
"""
import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]

# Production packages the rule applies to. Tests / e2e / scripts define their
# own mocks and probes and are out of scope.
PRODUCTION_DIRS = ["simplyblock_core", "simplyblock_web", "simplyblock_cli"]

BARE_GET_BDEVS = re.compile(r"\.get_bdevs\(\s*\)")


def test_no_bare_get_bdevs_calls():
    offenders = []
    for top in PRODUCTION_DIRS:
        for path in (REPO_ROOT / top).rglob("*.py"):
            if "test" in path.parts:
                continue
            text = path.read_text(encoding="utf-8", errors="replace")
            for lineno, line in enumerate(text.splitlines(), 1):
                if BARE_GET_BDEVS.search(line):
                    offenders.append(f"{path.relative_to(REPO_ROOT)}:{lineno}: {line.strip()}")
    assert not offenders, (
        "bare get_bdevs() call(s) found — pass a name, use "
        "bdev_nvme_controller_list, or declare all_bdevs=True (cold paths "
        "only):\n" + "\n".join(offenders))
