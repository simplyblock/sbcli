from pathlib import Path
from typing import Annotated, Literal, Optional

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="sb_", case_sensitive=False)

    tls_serve: Annotated[
        bool,
        Field(
            description="Run servers in TLS mode. Requires certificate and key to be present."
        ),
    ] = False
    tls_connect: Annotated[
        Literal["disabled", "anonymous"],
        Field(description="Connect to internal services via TLS."),
    ] = "disabled"
    tls_provider: Annotated[
        Optional[Literal["openshift", "cert-manager"]],
        Field(description="Provider for TLS certificates in the cluster."),
    ] = None
    tls_certificate: Path = Path("/etc/simplyblock/tls/tls.crt")
    tls_key: Path = Path("/etc/simplyblock/tls/tls.key")
    tls_certificate_authority: Path = Path("/etc/simplyblock/tls/ca.crt")

    @model_validator(mode="after")
    def validate_tls_files(self):
        if not self.tls_serve and self.tls_connect == "disabled":
            return self

        if self.tls_serve and (
            missing := [
                name
                for name in ["tls_certificate", "tls_key"]
                if not getattr(self, name).is_file()
            ]
        ):
            raise ValueError(
                "SB_TLS_SERVE=true requires TLS files to exist: " + ", ".join(missing)
            )

        if (
            self.tls_connect != "disabled"
            and not self.tls_certificate_authority.is_file()
        ):
            raise ValueError(
                "SB_TLS_CONNECT != 'disabled' requires certificate authority to exist"
            )

        return self

    @model_validator(mode="after")
    def validate_tls_provider(self):
        if self.tls_connect != "disabled" and self.tls_provider is None:
            raise ValueError(
                "TLS provider needs to be configured for TLS connections to be used"
            )
        return self
