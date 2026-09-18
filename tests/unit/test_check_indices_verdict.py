"""What `sbctl cluster check-indices` reports back to its caller.

The walk itself needs a real FoundationDB and lives in
``tests/integration/test_database_indices.py``. What is checkable here is the
verdict it turns into: the command's return value is its exit code, so it has to
mean "nothing is left to do" — not "something was repaired".
"""
import pytest

from simplyblock_core import cluster_ops, index_ops


def _findings(missing=0, duplicate=0, repaired=0, vanished=0, unresolved=0):
    return {
        'missing': [('k', 'e')] * missing,
        'stale': [],
        'orphaned': [],
        'duplicate': [('k', 'a', 'b')] * duplicate,
        'repaired': repaired,
        'vanished': vanished,
        'unresolved': unresolved,
    }


@pytest.fixture
def checked(monkeypatch):
    """Run `cluster_ops.check_indices` over a canned set of findings."""
    def run(findings, *, repair=False):
        monkeypatch.setattr(index_ops, 'check_indices',
                            lambda *a, **kw: findings)
        return cluster_ops.check_indices(repair=repair)

    return run


def test_a_clean_check_passes(checked):
    assert checked(_findings()) is True


def test_a_read_only_run_that_found_drift_fails(checked):
    """It settled nothing, so everything it found is still outstanding."""
    assert checked(_findings(missing=3, unresolved=3)) is False


def test_a_fully_repaired_run_passes(checked):
    assert checked(_findings(missing=3, repaired=3), repair=True) is True


def test_a_run_whose_findings_all_evaporated_passes(checked):
    """Neither walk is isolated from live traffic, so on a busy cluster a
    finding that is gone by the time the repair opens its transaction is the
    expected outcome — not something to send an operator after."""
    assert checked(_findings(missing=3, vanished=3), repair=True) is True


def test_a_partial_repair_fails(checked):
    """The regression: one repaired entry used to be enough to report success,
    while the duplicate that no repair can settle stayed in the data — and
    `build-indices` then went on refusing to flip the index over it."""
    findings = _findings(missing=4, duplicate=1, repaired=4, unresolved=1)

    assert checked(findings, repair=True) is False
