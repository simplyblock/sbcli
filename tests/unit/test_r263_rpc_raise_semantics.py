"""SFAM-2834 on R26.3: _request returns None on an SPDK error and raises
RPCException("connection error") only on a transport failure, keeping the
requests exception on __context__. These pin the R26.3-specific halves of the
port."""

from unittest.mock import MagicMock, patch

from requests.exceptions import ConnectionError as RequestsConnectionError, ReadTimeout

from simplyblock_core.rpc_client import RPCException, request_reached_remote
from simplyblock_core.services import replication_final_step as rfs


def _raised_like_request2(original):
    # Mirrors _request2: `except Exception: raise RPCException(...)`.
    try:
        try:
            raise original
        except Exception:
            raise RPCException("connection error")
    except RPCException as e:
        return e


class TestRequestReachedRemote:

    def test_read_timeout_on_context_reached(self):
        assert request_reached_remote(_raised_like_request2(ReadTimeout("timed out")))

    def test_connect_failure_did_not(self):
        assert not request_reached_remote(_raised_like_request2(RequestsConnectionError("refused")))


def _cutover(final_step):
    src_node, tgt_node, lvol = MagicMock(), MagicMock(), MagicMock()
    src_rpc = src_node.rpc_client.return_value
    final_step(src_rpc)
    with patch.object(rfs, "ensure_hub_attached", return_value=("ctrl", "hubn1", None)), \
            patch.object(rfs, "fence_source_paths"), \
            patch.object(rfs, "restore_source_paths") as restore, \
            patch.object(rfs, "enable_target_paths") as enable, \
            patch.object(rfs, "_online_peers", return_value=[]):
        ok, _ = rfs.run_cutover(src_node, tgt_node, lvol, "LVS_2/LV_1", 7, "LVS_2/SNAP_1")
    return ok, restore, enable


class TestCutoverFinalStep:

    def test_failed_state_restores_the_source(self):
        # Was treated as success on R26.3 (only `ret is None` was checked).
        def final_step(rpc):
            rpc.bdev_lvol_transfer_final_step.return_value = {"transfer_state": "Failed"}
        ok, restore, enable = _cutover(final_step)
        assert ok is False
        restore.assert_called_once()
        enable.assert_not_called()

    def test_raise_with_done_stat_proceeds(self):
        def final_step(rpc):
            rpc.bdev_lvol_transfer_final_step.side_effect = _raised_like_request2(ReadTimeout("t"))
            rpc.bdev_lvol_transfer_stat.return_value = {"transfer_state": "Done"}
        ok, restore, enable = _cutover(final_step)
        assert ok is True
        restore.assert_not_called()
        enable.assert_called_once()

    def test_raise_without_done_restores_the_source(self):
        def final_step(rpc):
            rpc.bdev_lvol_transfer_final_step.side_effect = _raised_like_request2(ReadTimeout("t"))
            rpc.bdev_lvol_transfer_stat.side_effect = RPCException("connection error")
        ok, restore, enable = _cutover(final_step)
        assert ok is False
        restore.assert_called_once()
        enable.assert_not_called()

    def test_done_proceeds(self):
        def final_step(rpc):
            rpc.bdev_lvol_transfer_final_step.return_value = {"transfer_state": "Done"}
        ok, restore, enable = _cutover(final_step)
        assert ok is True
        enable.assert_called_once()
