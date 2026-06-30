# coding=utf-8
"""Regression tests for ``LocalKMS`` against a real FoundationDB.

The original bug: ``LocalKMS.get_data_encryption_keys`` called ``.wait()`` on
the result of ``Database.get(...)`` — but on a ``Database`` (as opposed to a
``Transaction``) ``get`` is synchronous and returns an already-resolved
``Value`` (a ``bytes`` subclass). The ``.wait()`` call therefore raised
``AttributeError: 'bytes' object has no attribute 'wait'`` mid-lvol-create.
"""
import uuid

import pytest

from simplyblock_core.db_controller import DBController
from simplyblock_core.kms import KMSException
from simplyblock_core.kms._fdb import LocalKMS
from tests._mocks import make_mock_cluster


@pytest.fixture()
def local_kms():
    if DBController().kv_store is None:
        pytest.skip("FoundationDB is not available")
    return LocalKMS(make_mock_cluster())


def _unique_path() -> str:
    return f"cluster/test/lvol/{uuid.uuid4()}"


def test_round_trip_returns_stored_keys(local_kms):
    path = _unique_path()
    key1, key2 = "deadbeef" * 8, "feedface" * 8

    local_kms.import_data_encryption_keys(path, "kek", (key1, key2))
    try:
        assert local_kms.get_data_encryption_keys(path, "kek") == (key1, key2)
    finally:
        local_kms.delete_data_encryption_keys(path)


def test_create_then_get_works(local_kms):
    """``create_data_encryption_keys`` must persist keys readable by ``get``.

    This is the exact sequence ``lvol_controller._create_crypto_bdev`` drives,
    and it is what previously crashed with ``'bytes' object has no attribute
    'wait'`` before the ``Database.get`` / ``Transaction.get`` mix-up was
    fixed.
    """
    path = _unique_path()

    local_kms.create_data_encryption_keys(path, "kek")
    try:
        key1, key2 = local_kms.get_data_encryption_keys(path, "kek")
        assert isinstance(key1, str) and len(key1) == 64
        assert isinstance(key2, str) and len(key2) == 64
    finally:
        local_kms.delete_data_encryption_keys(path)


def test_get_missing_raises_kms_exception(local_kms):
    with pytest.raises(KMSException):
        local_kms.get_data_encryption_keys(_unique_path(), "kek")


def test_delete_removes_keys(local_kms):
    path = _unique_path()
    local_kms.import_data_encryption_keys(path, "kek", ("aa" * 32, "bb" * 32))
    local_kms.delete_data_encryption_keys(path)

    with pytest.raises(KMSException):
        local_kms.get_data_encryption_keys(path, "kek")
