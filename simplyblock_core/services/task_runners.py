# coding=utf-8
"""Entry point for the task runners (``simplyblock-task-runner``).

After the rework a task runner *is* its :class:`~simplyblock_core.services.
task_runner_base.RunnerSpec`: the module exports one as ``SPEC`` and running it
is ``serve(SPEC)``. That makes a single shared entry point possible, rather than
one console script per runner::

    simplyblock-task-runner tasks-runner-restart

The name is the runner's own — ``RunnerSpec.name``, the same string its log
lines carry — not a file inside the image. Consumers that use it stop depending
on the source layout, which is what the historical form does::

    python3 simplyblock_core/services/tasks_runner_restart.py

That form keeps working, and has to: see ``tests/unit/test_service_entrypoints``
for the artifacts pinned to it. So does the generic ``simplyblock-service
<name>`` dispatcher, which reaches the runners as ordinary services. This is the
form new consumers should use.

Runner names are derived from module names rather than by importing every runner
and reading its ``SPEC.name``: a runner module pulls in the controllers,
``storage_node_ops`` and an FDB client at import time, and listing the runners
must not cost that. Only the runner being served is imported, and its spec name
is checked against the name it was asked for — the two drifting apart would make
the deployment name a lie.
"""
import argparse
import importlib
import pkgutil
from typing import List, Optional

from simplyblock_core.services.task_runner_base import RunnerSpec, serve


_PACKAGE = "simplyblock_core.services"
_MODULE_PREFIX = "tasks_runner_"

# Runners that still drive their own poll loop instead of exporting a spec, and
# are therefore started through their module's ``main()``. Both are under active
# upstream rewrite; they join the rest when they migrate onto the driver.
# Explicit rather than "no SPEC attribute means legacy", so a migrated runner
# that fails to export its spec is an error instead of a silent fallback.
_NOT_ON_DRIVER = frozenset({
    "tasks-runner-batch-migration",
    "tasks-runner-lvol-migration",
})


def _module_name(name: str) -> str:
    return name.replace("-", "_")


def runner_names() -> List[str]:
    """The names of every task runner in this package."""
    package = importlib.import_module(_PACKAGE)
    return sorted(
        name.replace("_", "-")
        for _, name, ispkg in pkgutil.iter_modules(package.__path__)
        if not ispkg and name.startswith(_MODULE_PREFIX)
    )


def load_spec(name: str) -> Optional[RunnerSpec]:
    """The spec ``name`` denotes, or None for a runner not on the driver yet.

    Importing the runner is the point where the name is validated, so a runner
    renamed on one side of the deployment contract fails loudly at start-up
    rather than serving the wrong tasks.
    """
    if name in _NOT_ON_DRIVER:
        return None

    module = importlib.import_module(f"{_PACKAGE}.{_module_name(name)}")
    spec = getattr(module, "SPEC", None)
    if spec is None:
        raise RuntimeError(f"task runner {name} exports no SPEC")
    if spec.name != name:
        raise RuntimeError(
            f"task runner {name} exports a spec named {spec.name!r}; "
            "the deployment name and the spec name must agree")
    return spec


def main():
    names = runner_names()
    parser = argparse.ArgumentParser(
        prog="simplyblock-task-runner",
        description="Run a simplyblock task runner.",
    )
    parser.add_argument("runner", choices=names, metavar="RUNNER",
                        help="one of: " + ", ".join(names))
    args = parser.parse_args()

    spec = load_spec(args.runner)
    if spec is None:
        importlib.import_module(f"{_PACKAGE}.{_module_name(args.runner)}").main()
        return
    serve(spec)


if __name__ == "__main__":
    main()
