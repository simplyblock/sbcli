from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="sb_", case_sensitive=False)

    tls_certificate: Path = Path("/etc/simplyblock/tls/tls.crt")
    tls_key: Path = Path("/etc/simplyblock/tls/tls.key")

    @property
    def tls_enabled(self) -> bool:
        return self.tls_certificate.is_file() and self.tls_key.is_file()
