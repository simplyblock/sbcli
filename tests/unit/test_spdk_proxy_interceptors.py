# coding=utf-8
"""Unit tests for the spdk_http_proxy_server RPC interceptors.

The keyring_add_key virtual method must write key material to the (memory-
backed) key dir and forward a rewritten keyring_file_add_key to SPDK without
the material ever reaching the forwarded payload or the logs.
"""

import json
import os
import stat
from unittest.mock import patch

import pytest

from tests.conftest_proxy import import_proxy_module

proxy_mod = import_proxy_module()

KEY_MATERIAL = "DHHC-1:00:c2VjcmV0LWtleS1tYXRlcmlhbA==:"


@pytest.fixture
def key_dir(tmp_path):
    d = tmp_path / "keys"
    with patch.object(proxy_mod, "SPDK_PROXY_KEY_DIR", str(d)):
        yield d


def _ok_response(req_id=7):
    return json.dumps({"jsonrpc": "2.0", "id": req_id, "result": True})


def _error_response(code, req_id=7):
    return json.dumps({"jsonrpc": "2.0", "id": req_id, "error": {"code": code, "message": "err"}})


def _add_request(name="key_1", key=KEY_MATERIAL, req_id=7):
    return {"id": req_id, "method": "keyring_add_key", "params": {"name": name, "key": key}}


def _remove_request(name="key_1", req_id=7):
    return {"id": req_id, "method": "keyring_file_remove_key", "params": {"name": name}}


class TestKeyringAddKey:

    def test_happy_path_rewrites_and_writes_file(self, key_dir):
        with patch.object(proxy_mod, "rpc_call", return_value=_ok_response()) as mock_rpc:
            response = proxy_mod._handle_keyring_add_key(_add_request(), None)

        forwarded = json.loads(mock_rpc.call_args[0][0].decode("ascii"))
        key_path = key_dir / "key_1"
        assert forwarded["method"] == "keyring_file_add_key"
        assert forwarded["id"] == 7
        assert forwarded["params"] == {"name": "key_1", "path": str(key_path)}
        assert KEY_MATERIAL not in json.dumps(forwarded)

        assert key_path.read_text() == KEY_MATERIAL
        assert stat.S_IMODE(os.stat(key_path).st_mode) == 0o600
        assert stat.S_IMODE(os.stat(key_dir).st_mode) == 0o700
        assert json.loads(response)["result"] is True

    @pytest.mark.parametrize("name", ["../evil", "a/b", "a b", "", None, 5])
    def test_invalid_name_rejected(self, key_dir, name):
        with patch.object(proxy_mod, "rpc_call") as mock_rpc:
            response = proxy_mod._handle_keyring_add_key(_add_request(name=name), None)

        assert json.loads(response)["error"]["code"] == -32602
        mock_rpc.assert_not_called()
        assert not key_dir.exists()

    @pytest.mark.parametrize("key", ["", None, 5, "ключ"])
    def test_invalid_key_material_rejected(self, key_dir, key):
        with patch.object(proxy_mod, "rpc_call") as mock_rpc:
            response = proxy_mod._handle_keyring_add_key(_add_request(key=key), None)

        assert json.loads(response)["error"]["code"] == -32602
        mock_rpc.assert_not_called()
        assert not key_dir.exists()

    def test_existing_file_different_content_is_error(self, key_dir):
        key_dir.mkdir(mode=0o700)
        (key_dir / "key_1").write_text("DHHC-1:00:b3RoZXI=:")

        with patch.object(proxy_mod, "rpc_call") as mock_rpc:
            response = proxy_mod._handle_keyring_add_key(_add_request(), None)

        assert json.loads(response)["error"]["code"] == -32000
        mock_rpc.assert_not_called()
        assert (key_dir / "key_1").read_text() == "DHHC-1:00:b3RoZXI=:"

    def test_existing_file_matching_content_spdk_eexist_passes_through(self, key_dir):
        key_dir.mkdir(mode=0o700)
        (key_dir / "key_1").write_text(KEY_MATERIAL)

        with patch.object(proxy_mod, "rpc_call", return_value=_error_response(-17)):
            response = proxy_mod._handle_keyring_add_key(_add_request(), None)

        assert json.loads(response)["error"]["code"] == -17
        assert (key_dir / "key_1").read_text() == KEY_MATERIAL

    def test_existing_file_matching_content_spdk_success_passes_through(self, key_dir):
        # SPDK restarted while the tmpfs survived: keyring empty, file present.
        key_dir.mkdir(mode=0o700)
        (key_dir / "key_1").write_text(KEY_MATERIAL)

        with patch.object(proxy_mod, "rpc_call", return_value=_ok_response()):
            response = proxy_mod._handle_keyring_add_key(_add_request(), None)

        assert json.loads(response)["result"] is True
        assert (key_dir / "key_1").read_text() == KEY_MATERIAL

    def test_fresh_write_spdk_eexist_keeps_file_and_passes_through(self, key_dir):
        # SPDK already has the key registered; the freshly written backing file
        # must stay (the client treats -17 as reuse), and -17 passes through.
        with patch.object(proxy_mod, "rpc_call", return_value=_error_response(-17)):
            response = proxy_mod._handle_keyring_add_key(_add_request(), None)

        assert json.loads(response)["error"]["code"] == -17
        assert (key_dir / "key_1").read_text() == KEY_MATERIAL

    def test_fresh_write_spdk_error_deletes_file(self, key_dir):
        with patch.object(proxy_mod, "rpc_call", return_value=_error_response(-22)):
            response = proxy_mod._handle_keyring_add_key(_add_request(), None)

        assert json.loads(response)["error"]["code"] == -22
        assert not (key_dir / "key_1").exists()

    def test_fresh_write_rpc_call_raises_deletes_file(self, key_dir):
        # The forward itself failed (e.g. SPDK timeout raises ValueError): the
        # freshly written secret must not linger, and the error propagates.
        with patch.object(proxy_mod, "rpc_call", side_effect=ValueError("SPDK response timeout")):
            with pytest.raises(ValueError):
                proxy_mod._handle_keyring_add_key(_add_request(), None)

        assert not (key_dir / "key_1").exists()

    def test_existing_file_spdk_error_keeps_file(self, key_dir):
        key_dir.mkdir(mode=0o700)
        (key_dir / "key_1").write_text(KEY_MATERIAL)

        with patch.object(proxy_mod, "rpc_call", return_value=_error_response(-22)):
            response = proxy_mod._handle_keyring_add_key(_add_request(), None)

        assert json.loads(response)["error"]["code"] == -22
        assert (key_dir / "key_1").read_text() == KEY_MATERIAL

    def test_existing_file_rpc_call_raises_keeps_file(self, key_dir):
        # We did not write the pre-existing file, so a failing forward must not
        # remove it.
        key_dir.mkdir(mode=0o700)
        (key_dir / "key_1").write_text(KEY_MATERIAL)

        with patch.object(proxy_mod, "rpc_call", side_effect=ValueError("boom")):
            with pytest.raises(ValueError):
                proxy_mod._handle_keyring_add_key(_add_request(), None)

        assert (key_dir / "key_1").read_text() == KEY_MATERIAL


class TestKeyringFileRemoveKey:

    def test_removal_deletes_file(self, key_dir):
        key_dir.mkdir(mode=0o700)
        (key_dir / "key_1").write_text(KEY_MATERIAL)

        with patch.object(proxy_mod, "rpc_call", return_value=_ok_response()) as mock_rpc:
            response = proxy_mod._handle_keyring_file_remove_key(_remove_request(), None)

        forwarded = json.loads(mock_rpc.call_args[0][0].decode("ascii"))
        assert forwarded == _remove_request()
        assert json.loads(response)["result"] is True
        assert not (key_dir / "key_1").exists()

    def test_key_unknown_to_spdk_still_deletes_file(self, key_dir):
        key_dir.mkdir(mode=0o700)
        (key_dir / "key_1").write_text(KEY_MATERIAL)

        with patch.object(proxy_mod, "rpc_call", return_value=_error_response(-2)):
            proxy_mod._handle_keyring_file_remove_key(_remove_request(), None)

        assert not (key_dir / "key_1").exists()

    def test_other_spdk_error_keeps_file(self, key_dir):
        key_dir.mkdir(mode=0o700)
        (key_dir / "key_1").write_text(KEY_MATERIAL)

        with patch.object(proxy_mod, "rpc_call", return_value=_error_response(-22)):
            response = proxy_mod._handle_keyring_file_remove_key(_remove_request(), None)

        assert json.loads(response)["error"]["code"] == -22
        assert (key_dir / "key_1").read_text() == KEY_MATERIAL

    def test_missing_file_is_tolerated(self, key_dir):
        with patch.object(proxy_mod, "rpc_call", return_value=_ok_response()):
            response = proxy_mod._handle_keyring_file_remove_key(_remove_request(), None)

        assert json.loads(response)["result"] is True

    def test_invalid_name_still_forwarded(self, key_dir):
        with patch.object(proxy_mod, "rpc_call", return_value=_error_response(-2)) as mock_rpc:
            proxy_mod._handle_keyring_file_remove_key(_remove_request(name="../evil"), None)

        mock_rpc.assert_called_once()


class TestProxyGetCapabilities:

    def test_answered_locally(self):
        with patch.object(proxy_mod, "rpc_call") as mock_rpc:
            response = proxy_mod._handle_proxy_get_capabilities({"id": 3, "method": "proxy_get_capabilities"}, None)

        mock_rpc.assert_not_called()
        parsed = json.loads(response)
        assert parsed["id"] == 3
        assert set(parsed["result"]["interceptors"]) == {
            "keyring_add_key", "keyring_file_remove_key", "proxy_get_capabilities",
        }


class TestDispatchRpc:

    def test_unknown_method_passes_through_unmodified(self):
        raw = json.dumps({"id": 1, "method": "bdev_get_bdevs"}).encode("ascii")
        with patch.object(proxy_mod, "rpc_call", return_value=_ok_response(1)) as mock_rpc:
            proxy_mod.dispatch_rpc(raw, "5")

        mock_rpc.assert_called_once_with(raw, "5")

    def test_raw_keyring_file_add_key_passes_through(self, key_dir):
        raw = json.dumps({"id": 1, "method": "keyring_file_add_key",
                          "params": {"name": "k", "path": "/etc/simplyblock/dhchap_keys/k"}}).encode("ascii")
        with patch.object(proxy_mod, "rpc_call", return_value=_ok_response(1)) as mock_rpc:
            proxy_mod.dispatch_rpc(raw, None)

        mock_rpc.assert_called_once_with(raw, None)

    def test_malformed_json_passes_through(self):
        raw = b"{not json"
        with patch.object(proxy_mod, "rpc_call", side_effect=ValueError("bad")) as mock_rpc:
            with pytest.raises(ValueError):
                proxy_mod.dispatch_rpc(raw, None)

        mock_rpc.assert_called_once_with(raw, None)

    def test_routes_keyring_add_key(self, key_dir):
        raw = json.dumps(_add_request()).encode("ascii")
        with patch.object(proxy_mod, "rpc_call", return_value=_ok_response()):
            proxy_mod.dispatch_rpc(raw, None)

        assert (key_dir / "key_1").read_text() == KEY_MATERIAL

    def test_oserror_becomes_jsonrpc_error(self, tmp_path):
        blocker = tmp_path / "not-a-dir"
        blocker.write_text("")
        with patch.object(proxy_mod, "SPDK_PROXY_KEY_DIR", str(blocker / "keys")):
            with patch.object(proxy_mod, "rpc_call") as mock_rpc:
                response = proxy_mod.dispatch_rpc(json.dumps(_add_request()).encode("ascii"), None)

        mock_rpc.assert_not_called()
        assert json.loads(response)["error"]["code"] == -32000

    def test_key_material_never_logged_or_forwarded(self, key_dir, caplog):
        forwarded_payloads = []

        def capture(req, client_timeout=None):
            forwarded_payloads.append(req.decode("ascii"))
            return _ok_response()

        with caplog.at_level("DEBUG"):
            with patch.object(proxy_mod, "rpc_call", side_effect=capture):
                proxy_mod.dispatch_rpc(json.dumps(_add_request()).encode("ascii"), None)

        assert forwarded_payloads and all(KEY_MATERIAL not in p for p in forwarded_payloads)
        assert KEY_MATERIAL not in caplog.text
