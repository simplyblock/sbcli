"""The backup controllers import at module level, and each imports cleanly as
the first module loaded.

group_backup and policy once depended on each other: policy calls the group
backup lifecycle, and group_backup reached back into policy for the merge
trigger. That cycle was survivable only through function-local imports, which
hide a module's real dependencies and break as soon as somebody hoists one.
The merge trigger now lives in the single-volume backup controller both
depend on, so neither needs a function-local import, and these tests keep it
that way.
"""
import ast
import pathlib
import subprocess
import sys

import pytest

from tests.unit.test_controller_import_order import _FDB_STUB

_ROOT = pathlib.Path(__file__).resolve().parents[3]

#: New backup code that must declare every dependency at module level.
MODULE_LEVEL_ONLY = [
    "simplyblock_core/controllers/backup/group_backup.py",
    "simplyblock_core/controllers/backup/policy.py",
]

BACKUP_MODULES = [
    "simplyblock_core.controllers.backup.group_backup",
    "simplyblock_core.controllers.backup.policy",
    "simplyblock_core.controllers.backup.controller",
    "simplyblock_core.services.tasks_runner_backup",
]


def _function_local_imports(path):
    tree = ast.parse(path.read_text())
    found = []
    for fn in ast.walk(tree):
        if isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
            for node in ast.walk(fn):
                if isinstance(node, (ast.Import, ast.ImportFrom)):
                    found.append(f"{path.name}:{node.lineno} {ast.unparse(node)}")
    return found


@pytest.mark.parametrize("relpath", MODULE_LEVEL_ONLY)
def test_imports_are_at_module_level(relpath):
    found = _function_local_imports(_ROOT / relpath)
    assert not found, "function-local imports:\n" + "\n".join(found)


@pytest.mark.parametrize("module", BACKUP_MODULES)
def test_backup_module_imports_first(module):
    proc = subprocess.run(
        [sys.executable, "-c", _FDB_STUB + f"\nimport {module}"],
        capture_output=True, text=True, timeout=120)
    assert proc.returncode == 0, (
        f"{module} does not import as the first module loaded:\n{proc.stderr}")
