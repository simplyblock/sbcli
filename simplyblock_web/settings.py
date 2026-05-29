from typing import Annotated, Any

from pydantic import BeforeValidator, Field
from pydantic_settings import BaseSettings, EnvSettingsSource, SettingsConfigDict


def _parse_str_list(v: Any) -> list[str]:
    if isinstance(v, str):
        return [item.strip() for item in v.split(',') if item.strip()]
    return v


class _CommaSupportedEnvSource(EnvSettingsSource):
    """Env source that falls back to the raw string for list fields when the
    value is not valid JSON, allowing comma-separated env-var values."""

    def decode_complex_value(self, field_name: str, field: Any, value: Any) -> Any:
        try:
            return super().decode_complex_value(field_name, field, value)
        except Exception:
            return value


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="sb_", case_sensitive=False)

    k8s_admin_service_accounts: Annotated[
        list[str],
        Field(
            description=(
                "Kubernetes service accounts granted full admin access to the API. "
                "Comma-separated list of fully-qualified names "
                "(e.g. 'system:serviceaccount:default:my-operator'). "
            )
        ),
        BeforeValidator(_parse_str_list),
    ] = []

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: Any,
        env_settings: Any,
        dotenv_settings: Any,
        file_secret_settings: Any,
    ) -> tuple[Any, ...]:
        return (
            init_settings,
            _CommaSupportedEnvSource(settings_cls),
            dotenv_settings,
            file_secret_settings,
        )
