from pydantic import Field, HttpUrl, NonNegativeFloat
from pydantic_core import Url
from pydantic_settings import BaseSettings, SettingsConfigDict


class PowerBIConfiguration(BaseSettings):
    base_api_url: HttpUrl = Url("https://api.powerbi.com/v1.0/myorg/")
    groups: list[str] = Field(default_factory=list)
    """
    A comma separated list of Power BI group names, aka workspaces, to be monitored.

    If not set, it is defaulted to all groups.
    """
    datasets_fetching_period: NonNegativeFloat = 15.0
    period: NonNegativeFloat = 5.0
    model_config = SettingsConfigDict(env_prefix="DK_POWERBI_")
