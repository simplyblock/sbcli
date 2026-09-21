import pytest

from simplyblock_core.db_controller import DBController
from simplyblock_core.utils import ttl_cache


@pytest.fixture
def db():
    return DBController()


@pytest.fixture(autouse=True)
def no_convergence_wait(monkeypatch):
    """Every state switch waits out the state cache fleet-wide; the tests that
    are not about that wait would each pay it in real seconds. The three that
    are about it set it back."""
    monkeypatch.setattr(ttl_cache, 'INDEX_STATE_CONVERGENCE_SEC', 0)
