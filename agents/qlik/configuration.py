from pydantic import Field, HttpUrl, NonNegativeFloat
from pydantic_settings import BaseSettings, SettingsConfigDict


class QlikConfiguration(BaseSettings):
    """
    Qlik configuration.
    """

    model_config = SettingsConfigDict(env_prefix="DK_QLIK_")
    tenant: str = ""
    api_key: str
    base_api_url: HttpUrl = HttpUrl("https://tenant.us.qlikcloud.com")
    apps: list[str] = Field(default_factory=list)
    """
    NOTE: to set the apps via the environment, just pass a JSON string. e.g., (mind the 's)
    e.g.: DK_QLIK_APPS='["abc123","abc321"]'
    """
    timeout: NonNegativeFloat = 120.0
    period: NonNegativeFloat = 30.0
