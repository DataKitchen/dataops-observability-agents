__all__ = ["CoreConfiguration", "DEFAULT_CONFIGURATION_FILE_PATHS"]


from pathlib import Path
from typing import Literal

from pydantic import HttpUrl, NonNegativeFloat, NonNegativeInt, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

DEFAULT_CONFIGURATION_FILE_PATHS: tuple[Path, ...] = (Path("agent.toml"), Path("/etc/observability/agent.toml"))


class CoreConfiguration(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="DK_")
    """Ensures all environment variables are read from DK_<key-name>. Environment variables are case insensitive."""

    agent_type: str
    agent_key: str
    observability_service_account_key: SecretStr
    observability_base_url: HttpUrl
    log_level: Literal["debug", "info", "warning", "error"] = "info"
    # zero implies infinite
    max_channel_capacity: NonNegativeInt = 0
    # in seconds
    heartbeat_period: NonNegativeFloat = 60.0

    @field_validator("observability_base_url", mode="before")
    @classmethod
    def url_append_slash(cls, base_url: str) -> str:
        if base_url:
            return base_url if str(base_url).endswith('/') else f"{base_url}/"
        return base_url
