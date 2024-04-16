from typing import Literal

from pydantic import Field, HttpUrl, NonNegativeFloat
from pydantic_settings import BaseSettings, SettingsConfigDict


class DatabricksConfiguration(BaseSettings):
    """
    Databricks configuration.
    """

    databricks_host: HttpUrl
    databricks_jobs_version: Literal["2.1"] = "2.1"
    # Note: Pydantic does a deep copy, and thus handles the mutable class variable problem right.
    # We only use a Field class here to silence Ruff.
    databricks_jobs: list[str] = Field(default_factory=list)
    """
    NOTE: to set the jobs via the environment, just pass a JSON string. e.g., (mind the 's)
    e.g.: DK_DATABRICKS_JOBS='["abc123","abc321"]'
    """
    timeout: NonNegativeFloat = 120.0
    period: NonNegativeFloat = 5.0
    # when databricks job/task fails, watch it every <period> (default 10 minutes) for <max_time> (default 1 week)
    databricks_failed_watch_period: NonNegativeFloat = 600.0
    databricks_failed_watch_max_time: NonNegativeFloat = 604800.0

    model_config = SettingsConfigDict(env_prefix="DK_")
