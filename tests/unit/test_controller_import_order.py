"""Each controller in the lvol/snapshot/consistency-group cycle must import
cleanly as the FIRST module loaded.

These three import each other: snapshot_controller imports lvol_controller at
top level, and consistency_group_controller from-imports snapshot_controller
internals (_find_lvs_leader et al.). The cycle is only survivable while
lvol_controller reaches consistency_group_controller through function-local
imports. Hoisting that import to module level passed when lvol_controller or
consistency_group_controller was imported first, but entering through
snapshot_controller — the order every tasks-runner service uses — hit the
from-import against the partially initialized module and crashed the container
at boot (observed 2026-09-10, tasks-runner-sync-lvol-del CrashLoop).

Each module is imported in a fresh interpreter so this test pins ALL entry
orders, not just the one the test process happens to have loaded already.
"""
import subprocess
import sys

import pytest

#: fdb stub mirroring tests/unit/conftest.py, for interpreters without libfdb_c.
_FDB_STUB = """
import sys, types
_fdb = types.ModuleType('fdb')
_fdb.open = lambda *a, **k: None
_fdb.FDBError = Exception
_fdb.transactional = lambda f: f
sys.modules['fdb'] = _fdb
sys.modules['fdb.tuple'] = types.ModuleType('fdb.tuple')
"""

CYCLE_MODULES = [
    "simplyblock_core.controllers.snapshot_controller",
    "simplyblock_core.controllers.lvol_controller",
    "simplyblock_core.controllers.consistency_group_controller",
]


@pytest.mark.parametrize("module", CYCLE_MODULES)
def test_module_imports_first(module):
    proc = subprocess.run(
        [sys.executable, "-c", _FDB_STUB + f"\nimport {module}"],
        capture_output=True, text=True, timeout=120)
    assert proc.returncode == 0, (
        f"{module} does not import as the first module loaded:\n{proc.stderr}")
