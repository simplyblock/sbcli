"""A pool deleted as part of bringing its cluster down. Its records go with
their keys: every encrypted volume's data-encryption key, which its snapshots
are encrypted under too, and the pool's key-encryption key, deleted through
the cluster's KMS while the cluster and its KMS configuration still exist."""

from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from simplyblock_core.controllers import pool_controller


def _record(uuid, **fields):
    record = SimpleNamespace(uuid=uuid, remove=MagicMock(), **fields)
    record.get_id = lambda: uuid
    return record


def test_the_keys_go_with_the_records():
    encrypted = _record("v1", crypto_bdev="crypto_v1")
    plain = _record("v2", crypto_bdev="")
    gone = _record("v3", crypto_bdev="crypto_v3")  # deleted; its snapshot remains
    snaps = [_record("s1", lvol=encrypted), _record("s2", lvol=gone)]
    pool = _record("p1", cluster_id="c1")
    db = MagicMock()
    db.get_pool_by_id.return_value = pool
    db.get_cluster_by_id.return_value = _record("c1")
    db.get_lvols_by_pool_id.return_value = [encrypted, plain]
    db.get_snapshots_by_pool_id.return_value = snaps
    kms = MagicMock()

    @contextmanager
    def connection(_cluster):
        yield kms

    with patch.object(pool_controller, "DBController", return_value=db), \
            patch.object(pool_controller, "create_kms_connection", connection), \
            patch.object(pool_controller, "pool_events") as events:
        pool_controller.force_delete_pool("p1")

    deleted_deks = sorted(c.args[0] for c in kms.delete_data_encryption_keys.call_args_list)
    assert deleted_deks == ["cluster/c1/lvol/v1", "cluster/c1/lvol/v3"]
    kms.delete_key_encryption_key.assert_called_once_with("pool-p1")
    for record in (encrypted, plain, *snaps, pool):
        record.remove.assert_called_once()
    events.pool_remove.assert_called_once_with(pool)
