import logging

from pydantic import HttpUrl, NonNegativeFloat
from pydantic_settings import BaseSettings, SettingsConfigDict

LOG = logging.getLogger(__name__)


class AirflowConfiguration(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="DK_AIRFLOW_")

    api_url: HttpUrl
    period: NonNegativeFloat = 5.0
