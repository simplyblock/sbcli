"""A service module must be importable without starting its service.

tasks_runner_node_removal shipped its poll loop at module level, with no
``if __name__ == "__main__"`` guard. Importing it therefore *ran the daemon*:
``while True: time.sleep(...)`` inside the importing process, forever.

tests/unit/test_node_removal_wait_ceiling.py imports that module. So collection
of the unit tier never finished -- not the test, the *collection*. Locally it
looked like a slow suite; in CI the Unit tests job sat in_progress for hours
against a job timeout. Nothing failed, so nothing pointed at the cause, and
the tier silently stopped protecting the branch (2026-09-26).

The fifteen sibling runners were all guarded; this was the only one that was
not, which is why it reads as an oversight rather than a decision. Asserted
here over the whole services package, by source inspection rather than by
importing -- importing an unguarded module is the very thing that hangs, so a
test that did it would hang instead of failing.
"""
import ast
import pathlib
import unittest

SERVICES = pathlib.Path(__file__).resolve().parents[2] / "simplyblock_core" / "services"


def _module_level_forever_loops(path):
    """Return the line numbers of `while <truthy>:` statements at module scope."""
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    out = []
    for node in tree.body:                      # module scope only, not nested
        if not isinstance(node, ast.While):
            continue
        test = node.test
        is_forever = (
            (isinstance(test, ast.Constant) and bool(test.value))
            or (isinstance(test, ast.Name) and test.id == "True")
        )
        if is_forever:
            out.append(node.lineno)
    return out


class ServiceModulesDoNotRunOnImportTests(unittest.TestCase):

    def test_no_service_module_runs_a_poll_loop_at_import_time(self):
        offenders = []
        for path in sorted(SERVICES.glob("*.py")):
            for lineno in _module_level_forever_loops(path):
                offenders.append(f"{path.name}:{lineno}")

        self.assertEqual(
            offenders, [],
            "these service modules start their loop on import, so importing "
            "one (as a unit test does) hangs the process instead of failing: "
            + ", ".join(offenders)
            + ". Move the loop into main() behind `if __name__ == \"__main__\"`.")

    def test_every_runner_has_a_main_guard(self):
        """The positive form of the same rule -- a runner with neither a
        module-level loop nor a guard is one whose entry point moved somewhere
        this test can no longer see."""
        missing = [
            path.name for path in sorted(SERVICES.glob("tasks_runner_*.py"))
            if '__name__ == "__main__"' not in path.read_text(encoding="utf-8")
            and "__name__ == '__main__'" not in path.read_text(encoding="utf-8")
        ]
        self.assertEqual(missing, [], f"task runners without a main guard: {missing}")


if __name__ == '__main__':
    unittest.main()
