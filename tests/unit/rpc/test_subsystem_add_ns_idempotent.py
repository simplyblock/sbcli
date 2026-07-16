# coding=utf-8
"""
test_subsystem_add_ns_idempotent.py — unit tests for the idempotent
behavior of ``RPCClient.nvmf_subsystem_add_ns``.

Regression target: 2026-05-14 cluster_activate observation on cluster
3d4914e7-... emitted two identical ``nvmf_subsystem_add_ns`` requests
12 s apart for the same lvol (same nqn, bdev_name, uuid, nguid,
nsid=1). Idempotency is folded into the RPC method on the ERROR path:
the add fires directly (the duplicate re-entry is the rare case), and
only a rejected add triggers the ``nvmf_get_subsystems`` probe — whose
unfiltered response grows with total lvol count and used to be paid
before every single add. A rejected duplicate resolves to the existing
nsid; any other rejection propagates unchanged.
"""

import unittest
from unittest.mock import patch

from pydantic import SecretStr

from simplyblock_core.rpc_client import RPCClient


def _client():
    with patch("requests.session"):
        return RPCClient("127.0.0.1", 8081, "user", SecretStr("pass"), timeout=1, retry=0)


_DUP_ERR = {"code": -32602, "message": "Invalid parameters"}


class TestAddNsIdempotent(unittest.TestCase):

    def test_rejected_duplicate_resolves_to_existing_nsid(self):
        """A rejected add whose subsystem already holds a matching
        bdev/nsid/uuid is a duplicate re-entry: return the existing nsid."""
        c = _client()
        nqn = "nqn.test:lvol:abc"
        bdev = "LVS_345/LVOL_8403"
        uuid = "26cf808c-3c2e-472d-912f-b1da28d05350"

        with patch.object(c, "subsystem_get", return_value={
                "nqn": nqn,
                "namespaces": [{
                    "nsid": 1, "bdev_name": bdev, "uuid": uuid,
                }],
            }) as mock_get, \
             patch.object(c, "_request2", return_value=(None, _DUP_ERR)) as mock_req:
            ret = c.nvmf_subsystem_add_ns(nqn, bdev, uuid=uuid, nsid=1)

        self.assertEqual(ret, 1)
        # The add fires first; the probe runs only after the rejection.
        mock_req.assert_called_once()
        self.assertEqual(mock_req.call_args.args[0], "nvmf_subsystem_add_ns")
        mock_get.assert_called_once_with(nqn)

    def test_successful_add_never_probes(self):
        """The happy path pays no nvmf_get_subsystems dump at all."""
        c = _client()
        with patch.object(c, "subsystem_get") as mock_get, \
             patch.object(c, "_request2", return_value=(1, None)) as mock_req:
            ret = c.nvmf_subsystem_add_ns("nqn.test:lvol:abc", "bdev0", uuid="u1", nsid=1)

        self.assertEqual(ret, 1)
        mock_req.assert_called_once()
        mock_get.assert_not_called()

    def test_missing_namespace_fires_rpc(self):
        """When the bdev is not yet in the subsystem, the real RPC fires."""
        c = _client()
        nqn = "nqn.test:lvol:abc"
        bdev = "LVS_345/LVOL_8403"

        with patch.object(c, "subsystem_get", return_value={
                "nqn": nqn,
                "namespaces": [],
            }), \
             patch.object(c, "_request2", return_value=(1, None)) as mock_req:
            ret = c.nvmf_subsystem_add_ns(nqn, bdev, uuid="u1", nsid=1)

        self.assertEqual(ret, 1)
        mock_req.assert_called_once()
        # Confirm the underlying method name.
        self.assertEqual(mock_req.call_args.args[0], "nvmf_subsystem_add_ns")

    def test_rejection_without_match_propagates_error(self):
        """A rejected add with no matching namespace is a real failure — the
        original error must reach the caller (subsystem gone / full)."""
        c = _client()
        with patch.object(c, "subsystem_get", return_value=None), \
             patch.object(c, "_request2", return_value=(None, _DUP_ERR)) as mock_req:
            ret, err = c.nvmf_subsystem_add_ns2("nqn.test", "bdev0", uuid="u1")
        mock_req.assert_called_once()
        self.assertIsNone(ret)
        self.assertEqual(err, _DUP_ERR)

    def test_uuid_mismatch_keeps_error(self):
        """A bdev present at the same nsid but with a DIFFERENT uuid is a real
        conflict, not a duplicate; the rejection must not be swallowed."""
        c = _client()
        nqn = "nqn.test:lvol:abc"
        bdev = "LVS_345/LVOL_8403"

        with patch.object(c, "subsystem_get", return_value={
                "nqn": nqn,
                "namespaces": [{
                    "nsid": 1, "bdev_name": bdev, "uuid": "old-uuid",
                }],
            }), \
             patch.object(c, "_request2", return_value=(None, _DUP_ERR)) as mock_req:
            ret, err = c.nvmf_subsystem_add_ns2(nqn, bdev, uuid="new-uuid", nsid=1)

        mock_req.assert_called_once()
        self.assertIsNone(ret)
        self.assertEqual(err, _DUP_ERR)

    def test_idempotent_false_never_probes(self):
        """Callers can opt out: a rejected add returns the error directly."""
        c = _client()
        nqn = "nqn.test:lvol:abc"
        bdev = "LVS_345/LVOL_8403"

        with patch.object(c, "subsystem_get") as mock_get, \
             patch.object(c, "_request2", return_value=(None, _DUP_ERR)) as mock_req:
            ret, err = c.nvmf_subsystem_add_ns2(nqn, bdev, uuid="u1", nsid=1,
                                                idempotent=False)

        mock_get.assert_not_called()
        mock_req.assert_called_once()
        self.assertEqual(err, _DUP_ERR)

    def test_probe_failure_returns_original_error(self):
        """If the error-path probe itself raises, the original rejection is
        returned rather than the probe's exception."""
        c = _client()
        with patch.object(c, "subsystem_get", side_effect=RuntimeError("rpc down")), \
             patch.object(c, "_request2", return_value=(None, _DUP_ERR)) as mock_req:
            ret, err = c.nvmf_subsystem_add_ns2("nqn.test", "bdev0")
        mock_req.assert_called_once()
        self.assertIsNone(ret)
        self.assertEqual(err, _DUP_ERR)

    def test_rejected_same_bdev_different_nsid_resolves(self):
        """If the bdev is already attached at a different nsid (caller didn't
        pin nsid), a rejected add resolves to that nsid."""
        c = _client()
        nqn = "nqn.test:lvol:abc"
        bdev = "LVS_345/LVOL_8403"

        with patch.object(c, "subsystem_get", return_value={
                "nqn": nqn,
                "namespaces": [{"nsid": 2, "bdev_name": bdev, "uuid": "u1"}],
            }), \
             patch.object(c, "_request2", return_value=(None, _DUP_ERR)) as mock_req:
            ret = c.nvmf_subsystem_add_ns(nqn, bdev, uuid="u1")  # no nsid pinned

        self.assertEqual(ret, 2)
        mock_req.assert_called_once()


if __name__ == "__main__":
    unittest.main()
