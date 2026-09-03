"""Entry point for the background services.

The deployment contract has historically been ``python3
simplyblock_core/services/<name>.py`` run from the source tree, which is
hardcoded in the Helm charts, the Swarm compose file and the operator. That
form keeps working; this module adds a name-based entry point that does not
depend on a file layout, so consumers can migrate off the paths.

``runpy`` rather than importing and calling ``main()``: it reproduces the
semantics of running the file directly -- ``__name__ == "__main__"``, so a
module's own entry-point guard is what runs, and ``sys.argv[0]`` set to the
module's path -- so both invocations behave identically without this dispatcher
having to assume every service spells its entry point ``main()``.
"""
import argparse
import importlib
import pkgutil
import runpy
import sys


# Modules that live here but are libraries imported by the services, not
# services themselves -- they have no entry point and nothing invokes them as a
# command. Listing them would advertise names that do nothing when run.
_NOT_SERVICES = frozenset({
    "hub_controller_manager",
    "replication_final_step",
})


def _service_names():
    return sorted(
        name.replace("_", "-")
        for _, name, ispkg in pkgutil.iter_modules(importlib.import_module(__package__).__path__)
        if not ispkg and not name.startswith("_") and name not in _NOT_SERVICES
    )


def main():
    names = _service_names()
    parser = argparse.ArgumentParser(
        prog="simplyblock-service",
        description="Run a simplyblock background service.",
    )
    parser.add_argument("service", choices=names, metavar="SERVICE",
                        help="one of: " + ", ".join(names))
    args, rest = parser.parse_known_args()

    module = f"{__package__}.{args.service.replace('-', '_')}"
    sys.argv = [module, *rest]
    runpy.run_module(module, run_name="__main__", alter_sys=True)


if __name__ == "__main__":
    main()
