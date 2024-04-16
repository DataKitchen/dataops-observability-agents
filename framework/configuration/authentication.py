__all__ = [
    "UsernamePasswordConfiguration",
    "ApiTokenConfiguration",
    "AzureServicePrincipalConfiguration",
    "AzureBasicOauthConfiguration",
]


from logging import getLogger

from pydantic import Field, HttpUrl, SecretStr
from pydantic_core import Url
from pydantic_settings import BaseSettings, SettingsConfigDict

LOGGER = getLogger(__name__)


class UsernamePasswordConfiguration(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="DK_", extra="ignore")

    agent_username: str = Field(min_length=1)
    agent_password: SecretStr = Field(min_length=1)


class ApiTokenConfiguration(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="DK_", extra="ignore")

    agent_token: SecretStr = Field(min_length=1)


class AzureServicePrincipalConfiguration(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="DK_AZURE_", extra="ignore")

    client_id: str = Field(min_length=1)
    client_secret: SecretStr = Field(min_length=1)
    tenant_id: str = Field(min_length=1)
    scope: str = ""


class AzureBasicOauthConfiguration(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="DK_AZURE_", extra="ignore")

    client_id: str = Field(min_length=1)
    username: str = Field(min_length=1)
    password: SecretStr = Field(min_length=1)
    tenant_id: str = Field(min_length=1)
    scope: str = ""
    authority: HttpUrl = Url("https://login.microsoftonline.com")
