from typing import Annotated

from pydantic import NonNegativeFloat, StringConstraints
from pydantic_settings import BaseSettings, SettingsConfigDict

from toolkit.configuration.setting_types import WebSocketUrl

COMPONENT_TYPE = Annotated[str, StringConstraints(to_upper=True, pattern=r"(?i)BATCH_PIPELINE|DATASET")]


class ExampleConfiguration(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="DK_EXAMPLE_")

    target_url: WebSocketUrl
    # You can't use constr directly because mypy complains. See
    # https://github.com/pydantic/pydantic/issues/156
    # https://github.com/pydantic/pydantic/issues/5006
    component_type: COMPONENT_TYPE
    # alternatively,
    timeout: NonNegativeFloat = 120.0
    period: NonNegativeFloat = 5.0
