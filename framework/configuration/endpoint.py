__all__ = ["EndpointConfiguration"]

from typing import Literal

from pydantic import HttpUrl, NonNegativeFloat, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class EndpointConfiguration(BaseSettings):
    endpoint: HttpUrl
    method: Literal["GET", "POST", "DELETE", "PATCH", "PUT", "HEAD", "OPTIONS"]
    timeout: NonNegativeFloat = 120.0
    period: NonNegativeFloat = 5.0

    @field_validator("method", mode="before")
    @classmethod
    def method_to_upper(cls, method: str) -> str:
        if method:
            return method.upper()
        return method

    model_config = SettingsConfigDict(env_prefix="DK_")
