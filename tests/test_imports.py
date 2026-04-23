# coding=utf-8
"""
test_imports.py – smoke tests that verify all packages import cleanly.

Catches circular imports, missing modules, and other import-time errors
that would prevent services from starting. These tests run on every Python
version and install mode (editable / non-editable).
"""

import importlib
import sys

import pytest


# Every top-level module that a service entrypoint imports.
# Add new modules here when new API routes or services are created.
_IMPORT_TARGETS = [
    # --- v2 API (imported by mgmt_webapp and indirectly by node_webapp) ---
    "simplyblock_web.api.v2",
    "simplyblock_web.api.v2.backup",
    "simplyblock_web.api.v2.cluster",
    "simplyblock_web.api.v2.device",
    "simplyblock_web.api.v2.migration",
    "simplyblock_web.api.v2.volume",
    "simplyblock_web.api.v2.management_node",
    "simplyblock_web.api.v2.pool",
    "simplyblock_web.api.v2.snapshot",
    "simplyblock_web.api.v2.storage_node",
    "simplyblock_web.api.v2.dtos",
    # --- Top-level API package (triggers v1 + v2 loading) ---
    "simplyblock_web.api",
    # --- Controllers ---
    "simplyblock_core.controllers.lvol_controller",
    "simplyblock_core.controllers.snapshot_controller",
    "simplyblock_core.controllers.migration_controller",
    "simplyblock_core.controllers.backup_controller",
    "simplyblock_core.controllers.tasks_controller",
    # --- Models ---
    "simplyblock_core.models.lvol_migration",
    "simplyblock_core.models.backup",
    "simplyblock_core.models.cluster",
    "simplyblock_core.models.storage_node",
    # --- CLI ---
    "simplyblock_cli.cli",
    "simplyblock_cli.clibase",
]


@pytest.mark.parametrize("module_path", _IMPORT_TARGETS)
def test_module_imports_cleanly(module_path):
    """Each critical module must import without errors (no circular imports).

    We intentionally do NOT del from sys.modules to force a fresh import:
    doing so breaks subsequent tests that already hold references to classes
    from the old module object (their methods close over the old module's
    globals, so mock.patch on the reimported module becomes a no-op for
    those callers). True "fresh import" semantics are covered by
    ``test_no_circular_import_in_subprocess`` below, which runs in its own
    process with a clean sys.modules.
    """
    mod = importlib.import_module(module_path)
    assert mod is not None


def test_v2_api_has_all_routers():
    """The assembled v2 API must expose the top-level router."""
    from simplyblock_web.api import v2
    assert hasattr(v2, "api"), "v2 package must expose 'api' router"


def test_node_webapp_import_chain():
    """Simulate the SNodeAPI startup import: api → internal."""
    from simplyblock_web.api import internal as internal_api  # noqa: F401


def test_no_circular_import_in_subprocess():
    """
    Run the v2 import in a clean subprocess with no pre-cached modules.

    This catches circular imports that are masked by module caching in the
    test process (the exact failure mode that breaks container startup).
    """
    import subprocess
    result = subprocess.run(
        [sys.executable, "-c", "from simplyblock_web.api.v2 import migration"],
        capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0, (
        f"Circular import detected in clean process:\n{result.stderr}"
    )
