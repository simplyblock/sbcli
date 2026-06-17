# coding=utf-8
"""
test_subsystem_add_ns_idempotent.py — unit tests for the idempotent
behavior of ``RPCClient.nvmf_subsystem_add_ns``.

Regression target: 2026-05-14 cluster_activate observation on cluster
3d4914e7-... emitted two identical ``nvmf_subsystem_add_ns`` requests
12 s apart for the same lvol (same nqn, bdev_name, uuid, nguid,
nsid=1). With idempotency folded into the RPC method, the second call
short-circuits and returns the existing nsid rather than firing a
duplicate JSON-RPC that SPDK would reject with -EEXIST.
"""

import unittest
from unittest.mock import patch

from simplyblock_core.rpc_client import RPCClient


def _client():
    with patch("requests.session"):
        return RPCClient("127.0.0.1", 8081, "user", "pass", timeout=1, retry=0)


class TestAddNsIdempotent(unittest.TestCase):

    def test_existing_namespace_short_circuits_rpc(self):
        """When the subsystem already has a matching bdev/nsid/uuid, no RPC fires."""
        c = _client()
        nqn = "nqn.test:lvol:abc"
        bdev = "LVS_345/LVOL_8403"
        uuid = "26cf808c-3c2e-472d-912f-b1da28d05350"

        with patch.object(c, "subsystem_list", return_value=[{
                "nqn": nqn,
                "namespaces": [{
                    "nsid": 1, "bdev_name": bdev, "uuid": uuid,
                }],
            }]) as mock_list, \
             patch.object(c, "_request2") as mock_req:
            ret = c.nvmf_subsystem_add_ns(nqn, bdev, uuid=uuid, nsid=1)

        self.assertEqual(ret, 1)
        mock_list.assert_called_once_with(nqn_name=nqn)
        # The real add_ns RPC must not fire.
        mock_req.assert_not_called()

    def test_missing_namespace_fires_rpc(self):
        """When the bdev is not yet in the subsystem, the real RPC fires."""
        c = _client()
        nqn = "nqn.test:lvol:abc"
        bdev = "LVS_345/LVOL_8403"

        with patch.object(c, "subsystem_list", return_value=[{
                "nqn": nqn,
                "namespaces": [],
            }]), \
             patch.object(c, "_request2", return_value=(1, None)) as mock_req:
            ret = c.nvmf_subsystem_add_ns(nqn, bdev, uuid="u1", nsid=1)

        self.assertEqual(ret, 1)
        mock_req.assert_called_once()
        # Confirm the underlying method name.
        self.assertEqual(mock_req.call_args.args[0], "nvmf_subsystem_add_ns")

    def test_subsystem_absent_fires_rpc(self):
        """When the subsystem itself is missing from the list, the real RPC fires."""
        c = _client()
        with patch.object(c, "subsystem_list", return_value=[]), \
             patch.object(c, "_request2", return_value=(1, None)) as mock_req:
            c.nvmf_subsystem_add_ns("nqn.test", "bdev0", uuid="u1")
        mock_req.assert_called_once()

    def test_uuid_mismatch_falls_through(self):
        """A bdev present at the same nsid but with a DIFFERENT uuid is a real
        conflict, not a duplicate; let SPDK reject it rather than swallow it.
        """
        c = _client()
        nqn = "nqn.test:lvol:abc"
        bdev = "LVS_345/LVOL_8403"

        with patch.object(c, "subsystem_list", return_value=[{
                "nqn": nqn,
                "namespaces": [{
                    "nsid": 1, "bdev_name": bdev, "uuid": "old-uuid",
                }],
            }]), \
             patch.object(c, "_request2", return_value=(False, None)) as mock_req:
            c.nvmf_subsystem_add_ns(nqn, bdev, uuid="new-uuid", nsid=1)

        mock_req.assert_called_once()

    def test_idempotent_false_always_fires_rpc(self):
        """Callers can opt out of the precheck with ``idempotent=False``."""
        c = _client()
        nqn = "nqn.test:lvol:abc"
        bdev = "LVS_345/LVOL_8403"

        with patch.object(c, "subsystem_list", return_value=[{
                "nqn": nqn,
                "namespaces": [{"nsid": 1, "bdev_name": bdev, "uuid": "u1"}],
            }]) as mock_list, \
             patch.object(c, "_request2", return_value=(False, None)) as mock_req:
            c.nvmf_subsystem_add_ns(nqn, bdev, uuid="u1", nsid=1, idempotent=False)

        # No precheck, real RPC fires regardless.
        mock_list.assert_not_called()
        mock_req.assert_called_once()

    def test_probe_failure_does_not_block_add(self):
        """If the idempotency probe itself raises, fall back to the real RPC."""
        c = _client()
        with patch.object(c, "subsystem_list", side_effect=RuntimeError("rpc down")), \
             patch.object(c, "_request2", return_value=(1, None)) as mock_req:
            ret = c.nvmf_subsystem_add_ns("nqn.test", "bdev0")
        self.assertEqual(ret, 1)
        mock_req.assert_called_once()

    def test_same_bdev_different_nsid_skips(self):
        """If the bdev is already attached at a different nsid (caller didn't pin
        nsid), treat as already-present and return that nsid.
        """
        c = _client()
        nqn = "nqn.test:lvol:abc"
        bdev = "LVS_345/LVOL_8403"

        with patch.object(c, "subsystem_list", return_value=[{
                "nqn": nqn,
                "namespaces": [{"nsid": 2, "bdev_name": bdev, "uuid": "u1"}],
            }]), \
             patch.object(c, "_request2") as mock_req:
            ret = c.nvmf_subsystem_add_ns(nqn, bdev, uuid="u1")  # no nsid pinned

        self.assertEqual(ret, 2)
        mock_req.assert_not_called()


if __name__ == "__main__":
    unittest.main()
