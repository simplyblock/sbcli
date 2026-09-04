"""set_pool()'s size arguments: omitted vs explicitly zero vs positive.

`pool set <id> --pool-max 4TB` crashed with

    TypeError: '>' not supported between instances of 'NoneType' and 'int'

because the `set` subcommand declares no argparse default for --pool-max /
--lvol-max (unlike `add`, which defaults both to '0'), while
clibase.storage_pool__set passes args.pool_max/args.lvol_max positionally and
unconditionally. Omitting one therefore forwarded None, which fell through the
`== 0` guard straight into `> 0`.

None must mean "not specified, leave unchanged" rather than 0, because 0 is a
meaningful value here: it resets the limit to unlimited. Coercing None to 0
would have turned every partial update into a silent reset of the other size.

Also covers the field mix-up in the lvol_max branch, which assigned to
pool_max_size instead of lvol_max_size.
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.controllers import pool_controller
from simplyblock_core.models.pool import Pool

POOL_ID = "8ab9ca19-38a0-4fb6-ad71-4cfbe058db71"
TB4 = 4 * 1000 ** 4


def _pool(pool_max=1000, lvol_max=500):
    p = Pool()
    p.uuid = POOL_ID
    p.pool_name = "testpool"
    p.cluster_id = "cl-1"
    p.status = Pool.STATUS_ACTIVE
    p.pool_max_size = pool_max
    p.lvol_max_size = lvol_max
    return p


class _Ctx:
    """Patch set_pool's collaborators; expose the pool object it mutates."""

    def __enter__(self):
        self.pool = _pool()
        self._db = patch.object(pool_controller, "DBController")
        db_cls = self._db.start()
        db = db_cls.return_value
        db.get_pool_by_id.return_value = self.pool
        db.get_pools.return_value = [self.pool]
        db.get_lvols_by_pool_id.return_value = []      # no lvols -> no size floor
        db.get_hostnames_by_pool_id.return_value = []  # no QoS RPC fan-out
        db.kv_store = MagicMock()
        # pool_events -> events_controller keeps its OWN module-level
        # DBController, whose kv_store is None under test. Worse,
        # BaseModel.write_to_db calls exit(1) when the write fails, so an
        # unstubbed event turns a failing assertion into SystemExit.
        self._events = patch.object(pool_controller, "pool_events", MagicMock())
        self._events.start()
        # unsafe=True: the gate's method is named assert_object_ops_allowed,
        # and a safe MagicMock rejects any attribute starting with "assert".
        self._gate = patch.object(
            pool_controller, "ops_gate", MagicMock(unsafe=True), create=True)
        try:
            self._gate.start()
        except Exception:
            self._gate = None
        return self

    def __exit__(self, *a):
        if self._gate is not None:
            self._gate.stop()
        self._events.stop()
        self._db.stop()
        return False


class TestOmittedSizesAreLeftAlone(unittest.TestCase):
    """The reported crash, plus the reset-on-partial-update it was hiding."""

    def test_pool_max_only_does_not_raise(self):
        with _Ctx() as ctx:
            ok, err = pool_controller.set_pool(POOL_ID, TB4, None)
        self.assertTrue(ok, err)
        self.assertEqual(ctx.pool.pool_max_size, TB4)

    def test_pool_max_only_preserves_lvol_max(self):
        """Omitting --lvol-max must not reset it; 0 means unlimited, so
        coercing None to 0 would silently wipe the existing limit."""
        with _Ctx() as ctx:
            pool_controller.set_pool(POOL_ID, TB4, None)
        self.assertEqual(ctx.pool.lvol_max_size, 500)

    def test_lvol_max_only_does_not_raise_and_preserves_pool_max(self):
        with _Ctx() as ctx:
            ok, err = pool_controller.set_pool(POOL_ID, None, 2000)
        self.assertTrue(ok, err)
        self.assertEqual(ctx.pool.pool_max_size, 1000, "pool_max must be untouched")

    def test_both_omitted_changes_neither(self):
        with _Ctx() as ctx:
            ok, _ = pool_controller.set_pool(POOL_ID, None, None)
        self.assertTrue(ok)
        self.assertEqual((ctx.pool.pool_max_size, ctx.pool.lvol_max_size), (1000, 500))


class TestLvolMaxWritesItsOwnField(unittest.TestCase):
    """The lvol_max branch assigned to pool_max_size."""

    def test_lvol_max_sets_lvol_max_size(self):
        with _Ctx() as ctx:
            ok, err = pool_controller.set_pool(POOL_ID, None, 2000)
        self.assertTrue(ok, err)
        self.assertEqual(ctx.pool.lvol_max_size, 2000)

    def test_lvol_max_does_not_clobber_pool_max_size(self):
        with _Ctx() as ctx:
            pool_controller.set_pool(POOL_ID, None, 2000)
        self.assertNotEqual(ctx.pool.pool_max_size, 2000,
                            "lvol limit leaked into the pool-wide limit")


class TestExplicitZeroStillResets(unittest.TestCase):
    """0 is a real value -- 'unlimited' -- and must keep working."""

    def test_zero_pool_max_resets(self):
        with _Ctx() as ctx:
            pool_controller.set_pool(POOL_ID, 0, None)
        self.assertEqual(ctx.pool.pool_max_size, 0)

    def test_zero_lvol_max_resets(self):
        with _Ctx() as ctx:
            pool_controller.set_pool(POOL_ID, None, 0)
        self.assertEqual(ctx.pool.lvol_max_size, 0)


class TestNegativeStillRejected(unittest.TestCase):

    def test_negative_pool_max(self):
        with _Ctx():
            ok, err = pool_controller.set_pool(POOL_ID, -1, None)
        self.assertFalse(ok)
        self.assertIn("negative", err)

    def test_negative_lvol_max(self):
        with _Ctx():
            ok, err = pool_controller.set_pool(POOL_ID, None, -1)
        self.assertFalse(ok)
        self.assertIn("negative", err)
