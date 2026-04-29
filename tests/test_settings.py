import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from pydantic import ValidationError

from simplyblock_core.settings import Settings


def _tls_env(**overrides):
    env = {
        "SB_TLS_ENABLED": "false",
        "SB_TLS_PROVIDER": "openshift",
    }
    env.update(overrides)
    return env


def _write_tls_file(directory: str, name: str) -> str:
    path = Path(directory) / name
    path.write_text("test\n", encoding="utf-8")
    return str(path)


class TestSettings(unittest.TestCase):

    def test_tls_disabled_does_not_infer_from_file_presence(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cert = _write_tls_file(tmpdir, "tls.crt")
            key = _write_tls_file(tmpdir, "tls.key")
            ca = _write_tls_file(tmpdir, "ca.crt")
            with patch.dict(
                os.environ,
                _tls_env(
                    SB_TLS_ENABLED="false",
                    SB_TLS_CERTIFICATE=cert,
                    SB_TLS_KEY=key,
                    SB_TLS_CERTIFICATE_AUTHORITY=ca,
                ),
                clear=True,
            ):
                settings = Settings()

        self.assertFalse(settings.tls_enabled)
        self.assertEqual(settings.tls_provider, "openshift")

    def test_tls_enabled_requires_all_configured_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cert = _write_tls_file(tmpdir, "tls.crt")
            with patch.dict(
                os.environ,
                _tls_env(
                    SB_TLS_ENABLED="true",
                    SB_TLS_CERTIFICATE=cert,
                    SB_TLS_KEY=str(Path(tmpdir) / "missing.key"),
                    SB_TLS_CERTIFICATE_AUTHORITY=str(Path(tmpdir) / "missing-ca.crt"),
                ),
                clear=True,
            ):
                with self.assertRaises(ValidationError) as exc_info:
                    Settings()

        msg = str(exc_info.exception)
        self.assertIn("SB_TLS_ENABLED=true requires TLS files to exist", msg)
        self.assertIn("tls_key=", msg)
        self.assertIn("tls_certificate_authority=", msg)

    def test_tls_enabled_accepts_existing_configured_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cert = _write_tls_file(tmpdir, "tls.crt")
            key = _write_tls_file(tmpdir, "tls.key")
            ca = _write_tls_file(tmpdir, "ca.crt")
            with patch.dict(
                os.environ,
                _tls_env(
                    SB_TLS_ENABLED="true",
                    SB_TLS_PROVIDER="cert-manager",
                    SB_TLS_CERTIFICATE=cert,
                    SB_TLS_KEY=key,
                    SB_TLS_CERTIFICATE_AUTHORITY=ca,
                ),
                clear=True,
            ):
                settings = Settings()

        self.assertTrue(settings.tls_enabled)
        self.assertEqual(settings.tls_provider, "cert-manager")
        self.assertEqual(settings.tls_certificate_authority, Path(ca))

    def test_tls_provider_rejects_mixed_case_openshift(self):
        with patch.dict(
            os.environ,
            _tls_env(SB_TLS_PROVIDER="OpenShift"),
            clear=True,
        ):
            with self.assertRaises(ValidationError):
                Settings()
