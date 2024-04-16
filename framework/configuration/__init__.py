from typing import TypeVar

# shuts Ruff up.
from .core import DEFAULT_CONFIGURATION_FILE_PATHS, CoreConfiguration
from .endpoint import EndpointConfiguration
from .http import HTTPClientConfig

T_CONFIG = TypeVar("T_CONFIG", bound=CoreConfiguration)
