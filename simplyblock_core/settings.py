from pathlib import Path
from typing import Literal

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="sb_", case_sensitive=False)

    tls_enabled: bool = False
    tls_provider: Literal["openshift", "cert-manager"] = "openshift"
    tls_certificate: Path = Path("/etc/simplyblock/tls/tls.crt")
    tls_key: Path = Path("/etc/simplyblock/tls/tls.key")
    tls_certificate_authority: Path = Path("/etc/simplyblock/tls/ca.crt")

    @model_validator(mode="after")
    def validate_tls_files(self):
        if not self.tls_enabled:
            return self

        required_paths = ["tls_certificate", "tls_key", "tls_certificate_authority"]
        if (missing := [
            f"{name}={path}"
            for name in required_paths
            if not (path := getattr(self, name)).is_file()
        ]):
            raise ValueError(
                "SB_TLS_ENABLED=true requires TLS files to exist: "
                + ", ".join(missing)
            )
        return self
