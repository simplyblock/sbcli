# coding=utf-8
"""
test_inline_checksum.py – unit tests for the per-cluster inline CRC checksum
validation feature (TD.100226.1).

Covers:
  * Cluster.inline_checksum / NVMeDevice.md_size / md_supported model defaults.
  * find_md_lbaf_id helper – LBAF selection from `nvme id-ns` JSON.
  * alceml_checksum_params helper – cluster flag + per-device md combo logic.
  * alceml_fallback_overhead_bytes helper – capacity-overhead math.
  * bdev_alceml_create RPC – correct param wire-up for each method.
  * addNvmeDevices – md_size flowing from SPDK bdev JSON onto NVMeDevice.
"""

import json
import unittest
from unittest.mock import patch, MagicMock

from simplyblock_core import utils
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.rpc_client import RPCClient


def _make_rpc_client():
    with patch("requests.session"):
        return RPCClient("127.0.0.1", 8081, "user", "pass", timeout=1, retry=0)


# ---------------------------------------------------------------------------
# Model defaults & persistence
# ---------------------------------------------------------------------------
class TestModelDefaults(unittest.TestCase):
    def test_cluster_inline_checksum_defaults_off(self):
        c = Cluster()
        self.assertFalse(c.inline_checksum)

    def test_cluster_inline_checksum_can_be_set(self):
        c = Cluster()
        c.inline_checksum = True
        self.assertTrue(c.inline_checksum)

    def test_nvme_device_md_fields_default_off(self):
        d = NVMeDevice()
        self.assertEqual(d.md_size, 0)
        self.assertFalse(d.md_supported)

    def test_nvme_device_md_fields_round_trip(self):
        d = NVMeDevice({'md_size': 16, 'md_supported': True})
        self.assertEqual(d.md_size, 16)
        self.assertTrue(d.md_supported)


# ---------------------------------------------------------------------------
# find_md_lbaf_id
# ---------------------------------------------------------------------------
class TestFindMdLbafId(unittest.TestCase):
    def _idns(self, lbafs):
        return json.dumps({'lbafs': lbafs})

    def test_returns_none_when_no_md_lbaf(self):
        # Only 4K-no-md available.
        s = self._idns([
            {'ms': 0, 'ds': 9},
            {'ms': 0, 'ds': 12},
        ])
        self.assertIsNone(utils.find_md_lbaf_id(s))

    def test_picks_smallest_ms_above_min(self):
        # 4K-with-8B is preferred over 4K-with-64B (waste less space).
        s = self._idns([
            {'ms': 0, 'ds': 12},     # idx 0 – no md
            {'ms': 64, 'ds': 12},    # idx 1
            {'ms': 8, 'ds': 12},     # idx 2 – preferred
            {'ms': 16, 'ds': 12},    # idx 3
        ])
        self.assertEqual(utils.find_md_lbaf_id(s), 2)

    def test_skips_non_matching_ds(self):
        # An LBAF with ms>=8 but ds!=12 must not be selected.
        s = self._idns([
            {'ms': 8, 'ds': 9},      # 512B with md – ignore
            {'ms': 0, 'ds': 12},
        ])
        self.assertIsNone(utils.find_md_lbaf_id(s))

    def test_below_min_ms_excluded(self):
        s = self._idns([
            {'ms': 4, 'ds': 12},     # below 8B threshold
            {'ms': 0, 'ds': 12},
        ])
        self.assertIsNone(utils.find_md_lbaf_id(s))

    def test_invalid_json_returns_none(self):
        self.assertIsNone(utils.find_md_lbaf_id("not json"))
        self.assertIsNone(utils.find_md_lbaf_id(None))

    def test_empty_lbafs_returns_none(self):
        self.assertIsNone(utils.find_md_lbaf_id(self._idns([])))


# ---------------------------------------------------------------------------
# alceml_checksum_params
# ---------------------------------------------------------------------------
class TestAlcemlChecksumParams(unittest.TestCase):
    def test_off_when_cluster_flag_off(self):
        c = Cluster({'inline_checksum': False})
        d = NVMeDevice({'md_supported': True})
        self.assertEqual(utils.alceml_checksum_params(c, d), (0, 0, 0))

    def test_method_1_when_md_supported(self):
        c = Cluster({'inline_checksum': True})
        d = NVMeDevice({'md_supported': True, 'md_size': 8})
        self.assertEqual(utils.alceml_checksum_params(c, d), (1, 0, 0))

    def test_method_2_when_md_unsupported(self):
        c = Cluster({'inline_checksum': True})
        d = NVMeDevice({'md_supported': False, 'md_size': 0})
        self.assertEqual(utils.alceml_checksum_params(c, d), (2, 0, 0))

    def test_off_for_cluster_without_attribute(self):
        # Old DB record (no inline_checksum field) must behave as off.
        class _Old:
            pass
        d = NVMeDevice({'md_supported': True})
        self.assertEqual(utils.alceml_checksum_params(_Old(), d), (0, 0, 0))


# ---------------------------------------------------------------------------
# alceml_fallback_overhead_bytes
# ---------------------------------------------------------------------------
class TestFallbackOverhead(unittest.TestCase):
    def test_zero_when_flag_off(self):
        c = Cluster({'inline_checksum': False, 'blk_size': 4096, 'page_size_in_blocks': 2 * 1024 * 1024})
        self.assertEqual(utils.alceml_fallback_overhead_bytes(c, 100 * 2 * 1024 * 1024), 0)

    def test_zero_for_zero_or_negative_size(self):
        c = Cluster({'inline_checksum': True, 'blk_size': 4096, 'page_size_in_blocks': 2 * 1024 * 1024})
        self.assertEqual(utils.alceml_fallback_overhead_bytes(c, 0), 0)
        self.assertEqual(utils.alceml_fallback_overhead_bytes(c, -1), 0)

    def test_six_blocks_per_page(self):
        # 100 pages × 2 MiB = 200 MiB device. Overhead = 100 × 6 × 4 KiB = 2400 KiB.
        c = Cluster({'inline_checksum': True, 'blk_size': 4096, 'page_size_in_blocks': 2 * 1024 * 1024})
        device_size = 100 * 2 * 1024 * 1024
        expected = 100 * 6 * 4096
        self.assertEqual(utils.alceml_fallback_overhead_bytes(c, device_size), expected)

    def test_partial_page_floored(self):
        # 1.5 pages → only 1 full page counts (page-granular accounting).
        c = Cluster({'inline_checksum': True, 'blk_size': 4096, 'page_size_in_blocks': 2 * 1024 * 1024})
        partial = (1 * 2 * 1024 * 1024) + (1 * 1024 * 1024)
        self.assertEqual(utils.alceml_fallback_overhead_bytes(c, partial), 1 * 6 * 4096)

    def test_overhead_is_about_1_17_percent(self):
        # Sanity: the design doc cites ~1.17% overhead in fallback mode.
        c = Cluster({'inline_checksum': True, 'blk_size': 4096, 'page_size_in_blocks': 2 * 1024 * 1024})
        size = 1024 * 2 * 1024 * 1024  # 2 GiB, 1024 pages
        ratio = utils.alceml_fallback_overhead_bytes(c, size) / size
        self.assertAlmostEqual(ratio, 6 / 512, places=6)


# ---------------------------------------------------------------------------
# bdev_alceml_create RPC params
# ---------------------------------------------------------------------------
class TestBdevAlcemlCreateRPC(unittest.TestCase):
    @patch.object(RPCClient, "_request")
    def test_no_checksum_params_when_method_zero(self, mock_req):
        mock_req.return_value = True
        client = _make_rpc_client()
        client.bdev_alceml_create("alc_x", "nvme0", "uuid-1")
        params = mock_req.call_args[0][1]
        self.assertNotIn("checksum_validation_method", params)
        self.assertNotIn("cache_size", params)
        self.assertNotIn("cache_eviction_threshold", params)

    @patch.object(RPCClient, "_request")
    def test_method_1_only_emits_method_field(self, mock_req):
        mock_req.return_value = True
        client = _make_rpc_client()
        client.bdev_alceml_create("alc_x", "nvme0", "uuid-1", checksum_method=1)
        params = mock_req.call_args[0][1]
        self.assertEqual(params["checksum_validation_method"], 1)
        # Defaults of 0 must not be sent so the data plane uses its own defaults.
        self.assertNotIn("cache_size", params)
        self.assertNotIn("cache_eviction_threshold", params)

    @patch.object(RPCClient, "_request")
    def test_method_2_with_explicit_cache_overrides(self, mock_req):
        mock_req.return_value = True
        client = _make_rpc_client()
        client.bdev_alceml_create(
            "alc_x", "nvme0", "uuid-1",
            checksum_method=2, cache_size=1500, cache_eviction_threshold=85,
        )
        params = mock_req.call_args[0][1]
        self.assertEqual(params["checksum_validation_method"], 2)
        self.assertEqual(params["cache_size"], 1500)
        self.assertEqual(params["cache_eviction_threshold"], 85)

    @patch.object(RPCClient, "_request")
    def test_existing_params_unchanged(self, mock_req):
        # Regression guard: the new kwargs must not perturb the well-known params
        # the data plane expects.
        mock_req.return_value = True
        client = _make_rpc_client()
        client.bdev_alceml_create(
            "alc_x", "nvme0", "uuid-1",
            pba_init_mode=2, pba_page_size=2 * 1024 * 1024,
            write_protection=True, full_page_unmap=True, checksum_method=1,
        )
        params = mock_req.call_args[0][1]
        self.assertEqual(params["name"], "alc_x")
        self.assertEqual(params["cntr_path"], "nvme0")
        self.assertEqual(params["uuid"], "uuid-1")
        self.assertEqual(params["pba_init_mode"], 2)
        self.assertEqual(params["pba_page_size"], 2 * 1024 * 1024)
        self.assertTrue(params["write_protection"])
        self.assertTrue(params["use_map_whole_page_on_1st_write"])
        self.assertEqual(params["checksum_validation_method"], 1)


# ---------------------------------------------------------------------------
# addNvmeDevices md detection
# ---------------------------------------------------------------------------
class TestAddNvmeDevicesMd(unittest.TestCase):
    def _make_rpc_with_bdev(self, *, md_size):
        rpc = MagicMock()
        rpc.bdev_nvme_controller_list.return_value = []
        rpc.bdev_nvme_controller_attach.return_value = ["nvmeX_n1"]
        rpc.bdev_examine.return_value = True
        rpc.bdev_wait_for_examine.return_value = True
        # SPDK bdev_get_bdevs payload – the only md-relevant field is the
        # top-level uint32 md_size set by spdk_bdev_get_md_size.
        bdev_payload = [{
            'name': 'nvmeX_n1',
            'block_size': 4096,
            'num_blocks': 100 * 1024 * 1024 // 4096,  # 100 MiB
            'md_size': md_size,
            'driver_specific': {
                'nvme': [{
                    'pci_address': '0000:00:01.0',
                    'ctrlr_data': {
                        'model_number': 'TEST_NVME',
                        'serial_number': 'SN-TEST-1',
                    },
                }],
            },
        }]
        rpc.get_bdevs.return_value = bdev_payload
        return rpc

    def _make_snode(self):
        snode = MagicMock()
        snode.physical_label = 0
        snode.id_device_by_nqn = False
        snode.get_id.return_value = "snode-1"
        snode.cluster_id = "cluster-1"
        return snode

    def test_md_size_zero_marks_unsupported(self):
        rpc = self._make_rpc_with_bdev(md_size=0)
        snode = self._make_snode()
        devs = utils.addNvmeDevices(rpc, snode, ["0000:00:01.0"])
        self.assertEqual(len(devs), 1)
        self.assertEqual(devs[0].md_size, 0)
        self.assertFalse(devs[0].md_supported)

    def test_md_size_8_marks_supported(self):
        rpc = self._make_rpc_with_bdev(md_size=8)
        snode = self._make_snode()
        devs = utils.addNvmeDevices(rpc, snode, ["0000:00:01.0"])
        self.assertEqual(devs[0].md_size, 8)
        self.assertTrue(devs[0].md_supported)

    def test_md_size_below_threshold_marks_unsupported(self):
        # Pre-existing PI-only formats expose ms=4 (T10 PI's reftag field
        # alone). That's < 8 bytes so checksums won't fit – treat as no-md.
        rpc = self._make_rpc_with_bdev(md_size=4)
        snode = self._make_snode()
        devs = utils.addNvmeDevices(rpc, snode, ["0000:00:01.0"])
        self.assertEqual(devs[0].md_size, 4)
        self.assertFalse(devs[0].md_supported)

    def test_missing_md_size_field_treated_as_zero(self):
        # Older SPDK builds that omit the field entirely must not crash.
        rpc = MagicMock()
        rpc.bdev_nvme_controller_list.return_value = []
        rpc.bdev_nvme_controller_attach.return_value = ["nvmeX_n1"]
        rpc.get_bdevs.return_value = [{
            'name': 'nvmeX_n1',
            'block_size': 4096,
            'num_blocks': 100 * 1024 * 1024 // 4096,
            'driver_specific': {
                'nvme': [{
                    'pci_address': '0000:00:01.0',
                    'ctrlr_data': {'model_number': 'M', 'serial_number': 'S'},
                }],
            },
        }]
        snode = self._make_snode()
        devs = utils.addNvmeDevices(rpc, snode, ["0000:00:01.0"])
        self.assertEqual(devs[0].md_size, 0)
        self.assertFalse(devs[0].md_supported)


if __name__ == "__main__":
    unittest.main()
