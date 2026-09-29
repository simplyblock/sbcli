"""e2e harness: ``SbcliUtils.delete_lvol`` tells an absent lvol from a failed lookup.

A failed lookup (API down, bad response) is not "not found": a cleanup with
``skip_error=True`` must not report success while the lvol was never deleted.
``get_lvol_id`` reports absence as None; a failure propagates and no DELETE
is sent.

The harness is a black-box client normally run as PEP 723 scripts from
``e2e/``; its module is loaded here by path, with its harness-only imports
(``logger_config``, ``utils.common_utils``) stubbed for the duration of the load.
"""
import importlib.util
import sys
import types
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_MODULE_PATH = Path(__file__).resolve().parents[2] / "e2e" / "utils" / "sbcli_utils.py"


def _load_sbcli_utils():
    logger_config = types.ModuleType("logger_config")
    logger_config.setup_logger = lambda name: MagicMock()
    harness_utils = types.ModuleType("utils")
    common_utils = types.ModuleType("utils.common_utils")
    common_utils.sleep_n_sec = lambda seconds: None
    harness_utils.common_utils = common_utils
    stubs = {"logger_config": logger_config, "utils": harness_utils, "utils.common_utils": common_utils}
    with patch.dict(sys.modules, stubs):
        spec = importlib.util.spec_from_file_location("e2e_sbcli_utils_under_test", _MODULE_PATH)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    return module


@pytest.fixture()
def client():
    module = _load_sbcli_utils()
    utils = module.SbcliUtils("secret", "http://mgmt", "cluster-1")
    utils.delete_request = MagicMock()
    return utils


def test_absent_lvol_with_skip_error_is_a_successful_no_op(client):
    client.get_lvol_id = MagicMock(return_value=None)
    assert client.delete_lvol("vol-1", skip_error=True) is True
    client.delete_request.assert_not_called()


def test_absent_lvol_without_skip_error_raises(client):
    client.get_lvol_id = MagicMock(return_value=None)
    with pytest.raises(Exception, match="does not exist"):
        client.delete_lvol("vol-1")
    client.delete_request.assert_not_called()


@pytest.mark.parametrize("skip_error", [True, False])
def test_failed_lookup_propagates_without_a_delete(client, skip_error):
    failure = ConnectionError("API unreachable")
    client.get_lvol_id = MagicMock(side_effect=failure)
    with pytest.raises(ConnectionError) as raised:
        client.delete_lvol("vol-1", skip_error=skip_error)
    assert raised.value is failure
    client.delete_request.assert_not_called()
