from pydantic import NonNegativeInt, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

from toolkit.configuration.setting_types import NetworkPortNumber


class SsisConfiguration(BaseSettings):
    """Microsoft SQL Server Integration Services (SSIS) agent configuration."""

    model_config = SettingsConfigDict(env_prefix="DK_SSIS_")

    db_driver: str = "ODBC Driver 17 for SQL Server"
    db_host: str
    db_port: NetworkPortNumber = 1433
    db_name: str = "SSISDB"
    db_user: str
    db_password: SecretStr
    polling_interval: NonNegativeInt = 30
