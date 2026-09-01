# coding=utf-8
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
import io
import pathlib
import sys
import unittest
from typing import ClassVar
from unittest import mock

import yaml

from simplyblock_core.services import __main__ as dispatcher
from simplyblock_core.services import task_runners


SERVICES_DIR = pathlib.Path(dispatcher.__file__).parent
REPO_ROOT = SERVICES_DIR.parent.parent


def _module_path(name):
    return SERVICES_DIR / f"{name.replace('-', '_')}.py"


def _defines_main(path):
    return any(
        isinstance(node, ast.FunctionDef) and node.name == "main"
        for node in ast.parse(path.read_text()).body
    )


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

    def test_excluded_libraries_have_no_entry_point(self):
        """The exclusion list must stay justified: these have no way to be run.

        If one of them ever grows a ``main()`` it is a service and belongs in the
        listing, so this fails rather than letting it stay hidden.
        """
        for name in dispatcher._LIBRARIES:
            with self.subTest(module=name):
                path = SERVICES_DIR / f"{name}.py"
                self.assertTrue(path.is_file())
                self.assertFalse(_defines_main(path), f"{name} has main(); it is a service")
                self.assertNotIn("__main__", path.read_text())

    def test_excluded_dispatchers_are_entry_points_of_their_own(self):
        """The other half of the exclusion: a module kept out of the listing
        because it is a command in its own right, not because it is a library.
        One that lost its ``main()`` would be excluded for no reason."""
        for name in dispatcher._DISPATCHERS:
            with self.subTest(module=name):
                path = SERVICES_DIR / f"{name}.py"
                self.assertTrue(path.is_file())
                self.assertTrue(_defines_main(path), f"{name} has no main()")

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
        "tasks_cluster_status", "tasks_runner_backup", "backup_merge_service",
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

    EXEMPT: ClassVar[set] = {"spdk_http_proxy_server"}

    def test_every_service_defines_main(self):
        for name in dispatcher._service_names():
            module = name.replace("-", "_")
            if module in self.EXEMPT:
                continue
            with self.subTest(service=name):
                self.assertTrue(_defines_main(_module_path(name)),
                                f"{module} has no top-level main()")


class TestTaskRunnerDispatcher(unittest.TestCase):
    """The task runners' own entry point, ``simplyblock-task-runner <name>``.

    A task runner is fully described by the ``RunnerSpec`` its module exports, so
    this dispatcher serves the spec directly rather than executing a module. The
    name it takes is ``RunnerSpec.name`` — the deployment surface is the runner's
    identity, not a path inside the image.
    """

    def test_advertises_every_task_runner_module(self):
        modules = {path.stem for path in SERVICES_DIR.glob("tasks_runner_*.py")}
        self.assertEqual(
            {name.replace("-", "_") for name in task_runners.runner_names()},
            modules,
        )

    def test_names_are_hyphenated(self):
        for name in task_runners.runner_names():
            self.assertNotIn("_", name, f"{name} should use hyphens, not underscores")

    def test_every_advertised_name_loads_a_spec_of_that_name(self):
        """``load_spec`` raises on a mismatch; this walks every runner so a
        rename on either side is caught here rather than at deploy time."""
        for name in task_runners.runner_names():
            if name in task_runners._NOT_ON_DRIVER:
                continue
            with self.subTest(runner=name):
                self.assertEqual(task_runners.load_spec(name).name, name)

    def test_runners_listed_as_not_on_the_driver_really_export_no_spec(self):
        """Keeps the exemption honest: once one of these migrates it exports a
        SPEC, and leaving it listed would silently keep it on its own loop."""
        for name in task_runners._NOT_ON_DRIVER:
            with self.subTest(runner=name):
                module = importlib.import_module(
                    f"simplyblock_core.services.{name.replace('-', '_')}")
                self.assertFalse(hasattr(module, "SPEC"),
                                 f"{name} has a SPEC; drop it from _NOT_ON_DRIVER")

    def test_serves_the_named_spec(self):
        with mock.patch.object(task_runners, "serve") as serve, \
                mock.patch.object(sys, "argv", ["simplyblock-task-runner", "tasks-runner-fdb-backup"]):
            task_runners.main()

        import simplyblock_core.services.tasks_runner_fdb_backup as runner
        serve.assert_called_once_with(runner.SPEC)

    def test_a_runner_not_on_the_driver_is_started_through_its_own_main(self):
        import simplyblock_core.services.tasks_runner_lvol_migration as runner
        with mock.patch.object(runner, "main") as legacy_main, \
                mock.patch.object(task_runners, "serve") as serve, \
                mock.patch.object(sys, "argv",
                                  ["simplyblock-task-runner", "tasks-runner-lvol-migration"]):
            task_runners.main()

        legacy_main.assert_called_once_with()
        serve.assert_not_called()

    def test_an_unknown_runner_is_rejected(self):
        with mock.patch.object(sys, "argv", ["simplyblock-task-runner", "tasks-runner-nope"]), \
                mock.patch.object(sys, "stderr", io.StringIO()):
            with self.assertRaises(SystemExit):
                task_runners.main()

    def test_the_console_script_is_declared(self):
        pyproject = (REPO_ROOT / "pyproject.toml").read_text()
        self.assertIn(
            'simplyblock-task-runner = "simplyblock_core.services.task_runners:main"',
            pyproject,
        )


class TestDeploymentCommands(unittest.TestCase):
    """The consumers this repository owns must start runners by name.

    Everything else — the Helm charts, the operator, Swarm services created by an
    older control plane — still uses the paths, which is why
    ``TestPathInvocationContract`` above keeps them working. These two files are
    the ones a change here can actually fix, so they are pinned.
    """

    COMPOSE = pathlib.Path(
        SERVICES_DIR.parent / "scripts" / "docker-compose-swarm.yml")

    def _compose_commands(self):
        compose = yaml.safe_load(self.COMPOSE.read_text().replace("$", "_"))
        return {name: service.get("command", "")
                for name, service in compose["services"].items()}

    def test_every_task_runner_service_uses_the_entry_point(self):
        expected = {f"simplyblock-task-runner {name}"
                    for name in task_runners.runner_names()}
        found = {command for command in self._compose_commands().values()
                 if command.startswith("simplyblock-task-runner ")}
        self.assertEqual(found, expected)

    def test_no_compose_service_starts_a_runner_by_path(self):
        for name, command in self._compose_commands().items():
            with self.subTest(service=name):
                self.assertNotIn("services/tasks_runner_", command)

    def test_cluster_ops_creates_runner_services_by_name(self):
        source = (REPO_ROOT / "simplyblock_core" / "cluster_ops.py").read_text()
        self.assertNotIn("services/tasks_runner_", source)
        self.assertIn('"simplyblock-task-runner"', source)
