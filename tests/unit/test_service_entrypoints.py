"""
test_service_entrypoints.py — guards the background services' invocation contract.

The services have historically been started by *file path*, e.g.
``python3 simplyblock_core/services/snapshot_monitor.py`` run from the source
tree. That form is baked into artifacts this repository does not own — the Helm
charts and CSI chart under ``kubernetes/``, the operator's generated init script
— and into Docker Swarm services created by an older control plane, which keep
their original command string across an image upgrade (``cluster_ops.py`` only
calls ``service.update(image=...)``).

``simplyblock_core/services/__main__.py`` adds a name-based entry point
(``simplyblock-service <name>``) so consumers can migrate off the paths, but the
paths have to keep working until they all have. These tests pin both halves:

* every name the dispatcher advertises resolves to a real module, and
* every module a consumer can invoke by path is reachable by name,

so a service renamed or moved on one side cannot silently drop the other.
"""

import ast
import importlib
import pathlib
import unittest
from typing import ClassVar

from simplyblock_core.services import __main__ as dispatcher


SERVICES_DIR = pathlib.Path(dispatcher.__file__).parent


def _module_path(name):
    return SERVICES_DIR / f"{name.replace('-', '_')}.py"


class TestServiceDispatcher(unittest.TestCase):

    def test_advertises_at_least_the_known_services(self):
        # A floor, not an exact count: adding a service should not fail this.
        self.assertGreaterEqual(len(dispatcher._service_names()), 30)

    def test_every_advertised_name_resolves_to_a_module(self):
        for name in dispatcher._service_names():
            with self.subTest(service=name):
                self.assertTrue(_module_path(name).is_file(), f"no module for {name}")

    def test_names_are_hyphenated(self):
        for name in dispatcher._service_names():
            self.assertNotIn("_", name, f"{name} should use hyphens, not underscores")

    def test_excluded_modules_are_libraries_with_no_entry_point(self):
        """The exclusion list must stay justified: these have no way to be run.

        If one of them ever grows a ``main()`` it is a service and belongs in the
        listing, so this fails rather than letting it stay hidden.
        """
        for name in dispatcher._NOT_SERVICES:
            with self.subTest(module=name):
                path = SERVICES_DIR / f"{name}.py"
                self.assertTrue(path.is_file())
                tree = ast.parse(path.read_text())
                has_main = any(
                    isinstance(node, ast.FunctionDef) and node.name == "main"
                    for node in tree.body
                )
                self.assertFalse(has_main, f"{name} has main(); it is a service")
                self.assertNotIn("__main__", path.read_text())

    def test_importing_the_package_is_side_effect_free(self):
        """``runpy`` imports the parent package, which plain ``python3 <file>``
        does not necessarily do. ``__init__`` builds ServiceObject instances at
        import; that must stay cheap and must not touch the network or FDB."""
        services = importlib.import_module("simplyblock_core.services")
        self.assertTrue(hasattr(services, "ServiceObject"))
        for attr in ("spdk_nvmf_tgt", "alloc_bdev", "ultra21", "distr"):
            self.assertIsInstance(getattr(services, attr), services.ServiceObject)


class TestPathInvocationContract(unittest.TestCase):
    """Every service a consumer starts by path must also be startable by name."""

    #: Kept explicit rather than globbed: this is the set of names other
    #: repositories and already-deployed Swarm services depend on, so a rename
    #: has to be a deliberate edit here plus a migration of those consumers.
    CONSUMER_INVOKED = (
        "cap_monitor", "capacity_and_stats_collector", "device_monitor",
        "health_check_service", "lvol_monitor", "lvol_stat_collector",
        "main_distr_event_collector", "mgmt_node_monitor", "snapshot_monitor",
        "snapshot_replication", "spdk_http_proxy_server", "storage_node_monitor",
        "tasks_cluster_status", "tasks_runner_backup", "tasks_runner_backup_merge",
        "tasks_runner_batch_migration", "tasks_runner_cluster_expand",
        "tasks_runner_failed_migration", "tasks_runner_fdb_backup",
        "tasks_runner_jc_comp", "tasks_runner_lvol_migration",
        "tasks_runner_migration", "tasks_runner_new_dev_migration",
        "tasks_runner_node_add", "tasks_runner_node_removal",
        "tasks_runner_port_allow", "tasks_runner_replication_final",
        "tasks_runner_restart", "tasks_runner_sync_lvol_del",
    )

    def test_module_files_exist(self):
        for module in self.CONSUMER_INVOKED:
            with self.subTest(module=module):
                self.assertTrue((SERVICES_DIR / f"{module}.py").is_file())

    def test_all_are_dispatchable_by_name(self):
        advertised = set(dispatcher._service_names())
        for module in self.CONSUMER_INVOKED:
            with self.subTest(module=module):
                self.assertIn(module.replace("_", "-"), advertised)


class TestServicesHaveMain(unittest.TestCase):
    """Each service should expose ``main()`` so it is callable and testable.

    ``spdk_http_proxy_server`` is deliberately exempt: its module body builds the
    shared state ``ServerHandler`` and ``rpc_call`` read as globals (``TIMEOUT``,
    ``MAX_CONCURRENT_SPDK``, ``spdk_semaphore``, ``rpc_sock``) and its
    ``get_env_var(..., is_required=True)`` calls raise at import time, so wrapping
    it changes binding semantics on the storage-node data path. The dispatcher
    runs it through ``runpy``, which needs no ``main()``.
    """

    EXEMPT: ClassVar[str] = {"spdk_http_proxy_server"}

    def test_every_service_defines_main(self):
        for name in dispatcher._service_names():
            module = name.replace("-", "_")
            if module in self.EXEMPT:
                continue
            with self.subTest(service=name):
                tree = ast.parse(_module_path(name).read_text())
                self.assertTrue(
                    any(isinstance(n, ast.FunctionDef) and n.name == "main" for n in tree.body),
                    f"{module} has no top-level main()",
                )
