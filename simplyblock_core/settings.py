import ssl
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="sb_", case_sensitive=False)

    tls_certificate: Path = Path("/etc/simplyblock/tls/tls.crt")
    tls_key: Path = Path("/etc/simplyblock/tls/tls.key")
    certificate_authority: Path = Path("/etc/simplyblock/tls/ca.crt")

    @property
    def tls_enabled(self) -> bool:
        return all([
            self.tls_certificate.is_file(),
            self.tls_key.is_file(),
            self.certificate_authority.is_file(),
        ])

    def make_server_ssl_context(self):
        """Return an SSLContext requiring client certificates, or None if TLS is not configured."""
        if not self.tls_enabled:
            return None
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(self.tls_certificate, self.tls_key)
        ctx.verify_mode = ssl.CERT_REQUIRED
        ctx.load_verify_locations(self.certificate_authority)
        return ctx
