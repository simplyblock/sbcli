"""SFAM-2834: RPCClient raises on every failure, so `if not ret:` failure
branches no longer run. These pin the call sites that now catch the raise
and run their original failure handling."""

from unittest.mock import MagicMock, patch

from requests.exceptions import ReadTimeout

from simplyblock_core import storage_node_ops
from simplyblock_core.controllers import lvol_controller
from simplyblock_core.rpc_client import (
    RPCConnectionError,
    RPCHTTPError,
    RPCProtocolError,
    RPCRemoteError,
    request_reached_remote,
)


def _conn_error(cause):
    try:
        raise RPCConnectionError("Could not reach remote") from cause
    except RPCConnectionError as e:
        return e


class TestRequestReachedRemote:

    def test_remote_error_reached(self):
        assert request_reached_remote(RPCRemoteError("File exists", code=-17))

    def test_protocol_error_reached(self):
        assert request_reached_remote(RPCProtocolError("Invalid result payload"))

    def test_read_timeout_reached(self):
        assert request_reached_remote(_conn_error(ReadTimeout("timed out")))

    def test_connect_failure_did_not(self):
        assert not request_reached_remote(_conn_error(ConnectionRefusedError()))

    def test_http_error_did_not(self):
        # A proxy 5xx (e.g. SPDK down) proves nothing about SPDK.
        assert not request_reached_remote(RPCHTTPError("HTTP 500", status_code=500))


class TestAddLvolOnNodeRollsBackOnRaise:

    def test_raise_after_bdev_rolls_back(self):
        lvol = MagicMock(namespace=None)
        snode = MagicMock()
        with patch.object(lvol_controller, "_create_bdev_stack", return_value=(True, None)), \
                patch.object(lvol_controller, "_attach_lvol_after_bdev",
                             side_effect=RPCConnectionError("Could not reach remote")), \
                patch.object(lvol_controller, "_fail_after_bdev",
                             return_value=(False, "rolled back")) as fail:
            assert lvol_controller.add_lvol_on_node(lvol, snode) == (False, "rolled back")
        fail.assert_called_once()
        assert "Could not reach remote" in fail.call_args.args[2]


class TestConnectLvolToPoolIsBestEffort:

    def test_raise_returns_false(self):
        db = MagicMock()
        pool = db.get_pool_by_id.return_value
        pool.status = "active"
        pool.has_qos.return_value = True
        rpc = db.get_storage_node_by_id.return_value.rpc_client.return_value
        rpc.bdev_lvol_add_to_group.side_effect = RPCRemoteError("exists", code=-17)
        with patch.object(lvol_controller, "DBController", return_value=db):
            assert lvol_controller.connect_lvol_to_pool("lv", "node") is False


class TestCreateOrNone:

    def test_raise_reads_as_failure(self):
        def create(*a, **kw):
            raise RPCConnectionError("Could not reach remote")
        assert storage_node_ops._create_or_none(create, "raid0_1", "raid0_1", ["d"]) is None

    def test_passes_result_through(self):
        assert storage_node_ops._create_or_none(lambda *a, **kw: {"x": 1}, "b", 1, k=2) == {"x": 1}
