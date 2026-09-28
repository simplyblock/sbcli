"""Retry predicate for the ``bdev_lvol_snapshot`` create call.

SPDK reports every rejection of this RPC under the same JSON-RPC code
(-32602), so the code alone can't distinguish a genuinely transient
condition ("not leader, try again later") from a definitive one ("name
already exists") -- retrying the definitive one just re-collides with the
prior attempt's own leftover under the same name. Pure predicate, no DB/RPC,
so this stays in the unit tier.
"""

from simplyblock_core.controllers.snapshot_controller import _snapshot_create_is_transient


def test_retries_on_not_leader_try_again_later():
    result = (False, {"code": -32602, "message": "Cannot create snapshot; the lvol is not "
                                                   "leader, update in progress. try again later"})
    assert _snapshot_create_is_transient(result) is True


def test_does_not_retry_on_name_already_exists():
    result = (False, {"code": -32602, "message": "lvol with name SNAP_43 already exists"})
    assert _snapshot_create_is_transient(result) is False


def test_does_not_retry_on_name_being_already_created():
    result = (False, {"code": -32602, "message": "lvol with name SNAP_43 is being already created"})
    assert _snapshot_create_is_transient(result) is False


def test_does_not_retry_on_unrelated_code():
    result = (False, {"code": -32603, "message": "not leader, update in progress. try again later"})
    assert _snapshot_create_is_transient(result) is False


def test_does_not_retry_on_success():
    assert _snapshot_create_is_transient((True, None)) is False


def test_does_not_retry_when_no_error_object():
    assert _snapshot_create_is_transient((False, None)) is False
