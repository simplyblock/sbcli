#!/usr/bin/env python3
"""Check that every test class can actually be reached by some --testname.

THE SUITE HAS FOUR RUNNERS, each with its own registry, and a class is only
runnable through the one that knows about it:

    e2e.py          ALL_TESTS           named classes
                    its own _GROUPS     group keys (security, lblk, migration, ...)
    stress.py       get_stress_tests, get_backup_stress_tests
    load.py         get_load_tests
    upgrade_e2e.py  get_upgrade_tests

The trap this guards against is specific to e2e.py: a group key and a named
class resolve against DIFFERENT lists. `--testname migration` looks in the
group function; `--testname MigrationSmoke` looks in ALL_TESTS. Register a
class in the group only and it runs fine as part of its lane and fails with
"Test not found" when named on its own -- which is exactly how a 20-class
lane was green by group and unrunnable one case at a time.

Worse, the failure message used to print the short default-suite list rather
than what it actually searched, so it sent you looking in the wrong place.

One second, no cluster. Run it with check_cli_usage.py before pushing a lane.
"""
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
E2E = os.path.dirname(HERE)
sys.path.insert(0, E2E)
os.environ.setdefault("KEY_NAME", "registry-check")
os.environ.setdefault("KEY_PATH", os.devnull)

import __init__ as suite  # noqa: E402


def groups_wired_into_e2e():
    """The group keys e2e.py's _GROUPS actually maps, read from the source."""
    src = open(os.path.join(E2E, "e2e.py"), encoding="utf-8").read()
    m = re.search(r"_GROUPS\s*=\s*\{(.*?)\n    \}", src, re.DOTALL)
    if not m:
        return {}
    return dict(re.findall(r'"([a-z0-9-]+)"\s*:\s*(get_[a-z0-9_]+)', m.group(1)))


def main():
    problems = []
    known = [c.__name__ for c in suite.ALL_TESTS]
    known_set = set(known)
    dupes = sorted({n for n in known_set if known.count(n) > 1})

    print(f"e2e.py      ALL_TESTS: {len(known)} entries, {len(known_set)} unique")
    if dupes:
        problems.append(("ALL_TESTS", f"duplicate entries: {dupes}"))

    wired = groups_wired_into_e2e()
    print(f"e2e.py      group keys: {len(wired)} "
          f"({', '.join(sorted(wired)) or 'none'})")

    # Only e2e.py's own groups need their classes in ALL_TESTS -- the other
    # runners resolve names against their own registries, so flagging those
    # would be noise about a problem that does not exist.
    for key, fn_name in sorted(wired.items()):
        fn = getattr(suite, fn_name, None)
        if fn is None:
            problems.append((key, f"_GROUPS maps to {fn_name}, which does not "
                                  f"exist in e2e/__init__.py"))
            continue
        try:
            classes = fn()
        except Exception as exc:                      # noqa: BLE001
            problems.append((key, f"{fn_name}() raised: {str(exc)[:160]}"))
            continue
        missing = [c.__name__ for c in classes if c.__name__ not in known_set]
        if missing:
            problems.append((
                key,
                f"{len(missing)} class(es) in this group are NOT in ALL_TESTS, "
                f"so they run as part of --testname {key} but fail when named "
                f"individually:\n        " + "\n        ".join(missing)))

    # The other runners: just confirm their registry resolves and is non-empty.
    for runner, fn_name in (("stress.py", "get_stress_tests"),
                            ("stress.py", "get_backup_stress_tests"),
                            ("load.py", "get_load_tests"),
                            ("upgrade_e2e.py", "get_upgrade_tests")):
        fn = getattr(suite, fn_name, None)
        if fn is None:
            problems.append((runner, f"{fn_name} is missing"))
            continue
        try:
            n = len(fn())
        except Exception as exc:                      # noqa: BLE001
            problems.append((runner, f"{fn_name}() raised: {str(exc)[:160]}"))
            continue
        print(f"{runner:<12}{fn_name}: {n} classes")

    if problems:
        print()
        print(f"{len(problems)} problem(s):")
        for where, msg in problems:
            print(f"  {where}: {msg}")
        print()
        print("Fix: add the classes to ALL_TESTS in e2e/__init__.py. Being in a")
        print("group function alone is not enough to name a class directly.")
        return 1

    print()
    print("every group class is also nameable individually")
    return 0


if __name__ == "__main__":
    sys.exit(main())
