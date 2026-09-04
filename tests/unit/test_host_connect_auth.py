"""Tests for HostConnectAuth.from_entry — the single place that resolves the
credentials a client must present, shared by the lvol and migration connect
paths.
"""
import unittest
from types import SimpleNamespace

from pydantic import SecretStr

from simplyblock_core.utils.nvme import HostConnectAuth


def _pool(dhchap, dhchap_key="", dhchap_ctrlr_key=""):
    return SimpleNamespace(
        dhchap=dhchap,
        dhchap_key=SecretStr(dhchap_key),
        dhchap_ctrlr_key=SecretStr(dhchap_ctrlr_key),
    )


class TestHostConnectAuthFromEntry(unittest.TestCase):
    def test_dhchap_pool_uses_pool_keys_and_no_psk(self):
        # A DHCHAP pool holds the shared key pair; the entry carries only the nqn.
        entry = {"nqn": "nqn.host:a"}
        pool = _pool(True, dhchap_key="POOL-DH", dhchap_ctrlr_key="POOL-CTRL")

        auth = HostConnectAuth.from_entry(entry, pool)

        self.assertEqual(auth.nqn, "nqn.host:a")
        self.assertEqual(auth.dhchap_key.get_secret_value(), "POOL-DH")
        self.assertEqual(auth.dhchap_ctrlr_key.get_secret_value(), "POOL-CTRL")
        self.assertEqual(auth.psk.get_secret_value(), "")

    def test_dhchap_pool_ignores_per_host_keys(self):
        entry = {"nqn": "nqn.host:a", "psk": "HOST-PSK", "dhchap_key": "HOST-DH"}
        pool = _pool(True, dhchap_key="POOL-DH", dhchap_ctrlr_key="POOL-CTRL")

        auth = HostConnectAuth.from_entry(entry, pool)

        self.assertEqual(auth.dhchap_key.get_secret_value(), "POOL-DH")
        self.assertEqual(auth.psk.get_secret_value(), "")

    def test_non_dhchap_pool_uses_per_host_keys(self):
        entry = {
            "nqn": "nqn.host:a",
            "psk": "HOST-PSK",
            "dhchap_key": "HOST-DH",
            "dhchap_ctrlr_key": "HOST-CTRL",
        }
        pool = _pool(False)

        auth = HostConnectAuth.from_entry(entry, pool)

        self.assertEqual(auth.psk.get_secret_value(), "HOST-PSK")
        self.assertEqual(auth.dhchap_key.get_secret_value(), "HOST-DH")
        self.assertEqual(auth.dhchap_ctrlr_key.get_secret_value(), "HOST-CTRL")

    def test_no_pool_falls_back_to_per_host_keys(self):
        entry = {"nqn": "nqn.host:a", "psk": "HOST-PSK"}

        auth = HostConnectAuth.from_entry(entry, None)

        self.assertEqual(auth.psk.get_secret_value(), "HOST-PSK")
        self.assertEqual(auth.dhchap_key.get_secret_value(), "")

    def test_missing_keys_default_to_empty(self):
        auth = HostConnectAuth.from_entry({"nqn": "nqn.host:a"}, _pool(False))

        self.assertEqual(auth.psk.get_secret_value(), "")
        self.assertEqual(auth.dhchap_key.get_secret_value(), "")
        self.assertEqual(auth.dhchap_ctrlr_key.get_secret_value(), "")

    def test_secrets_are_masked_in_repr(self):
        auth = HostConnectAuth.from_entry(
            {"nqn": "nqn.host:a", "psk": "HOST-PSK", "dhchap_key": "HOST-DH"},
            _pool(False),
        )
        rendering = repr(auth)

        self.assertNotIn("HOST-PSK", rendering)
        self.assertNotIn("HOST-DH", rendering)


class TestHostConnectAuthResolve(unittest.TestCase):
    def _lvol(self, allowed_hosts, pool_uuid="pool-1", uuid="lvol-1"):
        return SimpleNamespace(allowed_hosts=allowed_hosts, pool_uuid=pool_uuid, get_id=lambda: uuid)

    def _db(self, pool):
        return SimpleNamespace(get_pool_by_id=lambda _uuid: pool)

    def test_no_allowed_hosts_returns_none(self):
        lvol = self._lvol(allowed_hosts=[])
        self.assertIsNone(HostConnectAuth.resolve(lvol, "nqn.host:a", self._db(_pool(False))))

    def test_no_allowed_hosts_ignores_host_nqn(self):
        lvol = self._lvol(allowed_hosts=[])
        self.assertIsNone(HostConnectAuth.resolve(lvol, None, self._db(_pool(False))))

    def test_allowed_hosts_without_host_nqn_raises(self):
        lvol = self._lvol(allowed_hosts=[{"nqn": "nqn.host:a"}])
        with self.assertRaises(ValueError):
            HostConnectAuth.resolve(lvol, None, self._db(_pool(False)))

    def test_unknown_host_nqn_raises(self):
        lvol = self._lvol(allowed_hosts=[{"nqn": "nqn.host:a"}])
        with self.assertRaises(ValueError):
            HostConnectAuth.resolve(lvol, "nqn.host:z", self._db(_pool(False)))

    def test_matching_host_delegates_to_from_entry(self):
        lvol = self._lvol(allowed_hosts=[{"nqn": "nqn.host:a", "psk": "HOST-PSK"}])
        auth = HostConnectAuth.resolve(lvol, "nqn.host:a", self._db(_pool(False)))
        self.assertEqual(auth.nqn, "nqn.host:a")
        self.assertEqual(auth.psk.get_secret_value(), "HOST-PSK")

    def test_matching_host_dhchap_pool_uses_pool_keys(self):
        lvol = self._lvol(allowed_hosts=[{"nqn": "nqn.host:a"}])
        pool = _pool(True, dhchap_key="POOL-DH", dhchap_ctrlr_key="POOL-CTRL")
        auth = HostConnectAuth.resolve(lvol, "nqn.host:a", self._db(pool))
        self.assertEqual(auth.dhchap_key.get_secret_value(), "POOL-DH")


if __name__ == "__main__":
    unittest.main()
