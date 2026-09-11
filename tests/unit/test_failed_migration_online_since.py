# coding=utf-8
"""The failed-migration runner must actually wait for a freshly-online node.

``online_since`` is stamped timezone-aware (storage_node_ops.py, via
``datetime.now(timezone.utc)``). Subtracting it from a NAIVE ``datetime.now()``
raises TypeError on every call; the surrounding ``except`` swallowed it, so the
"node is online < 1 min, retrying" guard never fired at all and failed-device
migrations -- the tasks node removal's phase 5 creates -- started immediately
against a node that had just come back online.

246 occurrences of "can't subtract offset-naive and offset-aware datetimes"
were logged across two node removals on 2026-09-11. The three sibling call
sites (tasks_runner_migration.py, tasks_runner_new_dev_migration.py,
storage_node_monitor.py) already passed timezone.utc; this one was missed.
"""
import ast
import inspect
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import ClassVar, List

import simplyblock_core.services.tasks_runner_failed_migration as runner


ONLINE_SINCE_STAMP_IS_AWARE = True  # storage_node_ops stamps datetime.now(timezone.utc)


class TestOnlineSinceComparison(unittest.TestCase):

    def _stamp(self, seconds_ago):
        return str(datetime.now(timezone.utc) - timedelta(seconds=seconds_ago))

    def test_an_aware_stamp_can_be_subtracted_without_raising(self):
        """The regression: this is exactly the expression the runner evaluates."""
        online_since = self._stamp(10)
        diff = datetime.now(timezone.utc) - datetime.fromisoformat(online_since)
        self.assertLess(diff.total_seconds(), 60)

    def test_a_naive_now_against_the_real_stamp_raises(self):
        """Guards the assumption above: if online_since ever became naive this
        test fails and the fix needs revisiting."""
        online_since = self._stamp(10)
        with self.assertRaises(TypeError):
            datetime.now() - datetime.fromisoformat(online_since)


class TestRunnerUsesAnAwareNow(unittest.TestCase):
    """Structural guard on the runner's source.

    Driving task_runner() needs FDB and a full task/node fixture; the defect
    is a one-token slip that a source assertion catches precisely.
    """

    def _online_since_calls(self):
        src = inspect.getsource(runner)
        tree = ast.parse(src)
        found = []
        for node in ast.walk(tree):
            if not isinstance(node, ast.BinOp) or not isinstance(node.op, ast.Sub):
                continue
            text = ast.dump(node)
            if "online_since" not in text:
                continue
            found.append(node)
        return found

    def test_the_online_since_subtraction_passes_a_timezone(self):
        calls = self._online_since_calls()
        self.assertTrue(calls, "no online_since subtraction found in the runner")
        for binop in calls:
            left = binop.left
            self.assertIsInstance(
                left, ast.Call,
                "left operand of the online_since subtraction should be datetime.now(...)")
            self.assertTrue(
                left.args or left.keywords,
                "datetime.now() is called with NO timezone: subtracting the "
                "timezone-aware online_since stamp raises TypeError, the except "
                "swallows it, and the 'node is online < 1 min' wait silently "
                "never happens")


class TestAllOnlineSinceCallSitesAgree(unittest.TestCase):
    """The same slip must not come back in a sibling runner.

    This bug existed because three of four call sites were corrected and one
    was not; nothing held them together.
    """

    SITES: ClassVar[List[str]] = [
        "simplyblock_core/services/tasks_runner_failed_migration.py",
        "simplyblock_core/services/tasks_runner_migration.py",
        "simplyblock_core/services/tasks_runner_new_dev_migration.py",
        "simplyblock_core/services/storage_node_monitor.py",
    ]

    def test_no_naive_now_is_subtracted_from_online_since(self):
        # .../simplyblock_core/services/<this>.py -> repo root
        root = Path(runner.__file__).resolve().parents[2]
        offenders = []
        checked = 0
        for rel in self.SITES:
            path = root / rel
            self.assertTrue(path.exists(), f"call site not found, fix SITES: {path}")
            checked += 1
            for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
                if "online_since" not in line or "datetime.now()" not in line:
                    continue
                offenders.append(f"{rel}:{lineno}: {line.strip()}")
        self.assertEqual(checked, len(self.SITES))
        self.assertEqual(
            offenders, [],
            "a naive datetime.now() is being subtracted from the aware "
            f"online_since stamp: {offenders}")


if __name__ == "__main__":
    unittest.main()
