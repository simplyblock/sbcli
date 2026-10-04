"""A blank id must never become a table scan.

read_from_db(kv, "") builds the TABLE prefix (get_db_id treats "" as absent and
falls back to a blank instance's id), so get_range_startswith returns every
record of that type. single_or_none then raises "Multiple values present" — a
confusing error that hides the real bug, and the failure mode that stalled every
fail-back task in the 2026-08-17/18 labs. Each by-id getter must reject a blank
id up front with KeyError, which is what its callers already handle.
"""
import pytest

from simplyblock_core.db_controller import DBController


@pytest.fixture
def db(monkeypatch):
    d = DBController.__new__(DBController)      # no FDB connection needed
    d.kv_store = object()
    return d


@pytest.mark.parametrize("getter", [
    "get_storage_node_by_id",
    "get_pool_by_id",
    "get_snapshot_by_id",
    "get_lvol_by_id",
    "get_mgmt_node_by_id",
    "get_cluster_by_id",
])
@pytest.mark.parametrize("blank", ["", None])
def test_blank_id_raises_keyerror(db, getter, blank):
    with pytest.raises(KeyError):
        getattr(db, getter)(blank)
