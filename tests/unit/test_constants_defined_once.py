"""Every constant in simplyblock_core/constants.py is assigned once.

A second unconditional assignment silently wins over the first: whoever reads
the first one, its comment included, is reading a value the module never
exports. Ruff does not catch it (F811 covers functions and imports, not plain
module-level names), and a merge or a port is exactly where a second copy
arrives -- LVOL_MIG_RETRY_ON_FAILURE_WAIT_SEC came in twice when the R26.3
removal work was ported onto main (2026-10-01), and only the code-quality scan
noticed.

Only unconditional top-level assignments count. A reassignment under an
``if`` is a deliberate override (``NVMF_BASE_PORT`` from its environment
variable) and is left alone.
"""
import ast
from collections import Counter
from pathlib import Path

import simplyblock_core.constants as constants


def _top_level_names(tree):
    for node in tree.body:
        if isinstance(node, ast.Assign):
            targets = node.targets
        elif isinstance(node, ast.AnnAssign):
            targets = [node.target]
        else:
            continue
        for target in targets:
            for name in ast.walk(target):
                if isinstance(name, ast.Name):
                    yield name.id, node.lineno


def test_no_constant_is_assigned_twice():
    path = Path(constants.__file__)
    tree = ast.parse(path.read_text(encoding="utf-8"))
    lines: dict = {}
    counts: Counter = Counter()
    for name, lineno in _top_level_names(tree):
        counts[name] += 1
        lines.setdefault(name, []).append(lineno)
    twice = {name: lines[name] for name, n in counts.items() if n > 1}
    assert not twice, (
        "assigned more than once in constants.py (the last one wins; "
        f"keep one): {twice}")


def test_it_catches_a_second_assignment():
    tree = ast.parse("A = 1\nB = 2\nA = 3\nif B:\n    B = 4\n")
    names = Counter(name for name, _ in _top_level_names(tree))
    assert names["A"] == 2
    assert names["B"] == 1, "a reassignment under `if` is an override, not a duplicate"
