import logging
from typing import Any, Literal, TypedDict, TypeVar, cast

from pydantic import ValidationError

from agents.airflow.configuration import AirflowConfiguration
from agents.databricks.configuration import DatabricksConfiguration
from agents.eventhub.configuration import EventhubBlobConfiguration, EventhubConfiguration
from agents.powerbi.config import PowerBIConfiguration
from agents.qlik.configuration import QlikConfiguration
from agents.ssis.config import SsisConfiguration
from agents.synapse_analytics.config import SynapseAnalyticsConfiguration
from agents.target_example.configuration import ExampleConfiguration
from framework.configuration.authentication import (
    ApiTokenConfiguration,
    AzureBasicOauthConfiguration,
    AzureServicePrincipalConfiguration,
    UsernamePasswordConfiguration,
)
from framework.configuration.core import DEFAULT_CONFIGURATION_FILE_PATHS, CoreConfiguration
from framework.configuration.http import HTTPClientConfig, ObservabilityHTTPClientConfig
from toolkit.configuration.sources import read_configuration_file

LOGGER = logging.getLogger(__name__)


# NOTE: Every time you create a new agent, you will need to add to these types. Suggestions welcome on making
#       this less of a pain.
class ConfigurationDict(TypedDict, total=False):
    airflow: AirflowConfiguration
    auth_api_token: ApiTokenConfiguration
    auth_azure_basic_oauth: AzureBasicOauthConfiguration
    auth_azure_spn: AzureServicePrincipalConfiguration
    auth_username_password: UsernamePasswordConfiguration
    blob_storage: EventhubBlobConfiguration
    core: CoreConfiguration
    databricks: DatabricksConfiguration
    eventhubs: EventhubConfiguration
    example: ExampleConfiguration
    http: HTTPClientConfig
    observability: ObservabilityHTTPClientConfig
    powerbi: PowerBIConfiguration
    qlik: QlikConfiguration
    ssis: SsisConfiguration
    synapse_analytics: SynapseAnalyticsConfiguration


"""
Valid configurations to hold in the registry. Add in alphabetically.
"""

CONFIGURATION_ID = Literal[
    "airflow",
    "auth_api_token",
    "auth_azure_basic_oauth",
    "auth_azure_spn",
    "auth_username_password",
    "blob_storage",
    "core",
    "databricks",
    "eventhubs",
    "example",
    "http",
    "observability",
    "powerbi",
    "qlik",
    "ssis",
    "synapse_analytics",
]
"""
The section 'name' of your configuration. This is used to look up your particular configuration. Add in alphabetically.
"""

CONFIGURATION_TYPES = (
    AirflowConfiguration
    | ApiTokenConfiguration
    | AzureBasicOauthConfiguration
    | AzureServicePrincipalConfiguration
    | EventhubBlobConfiguration
    | CoreConfiguration
    | DatabricksConfiguration
    | EventhubConfiguration
    | ExampleConfiguration
    | HTTPClientConfig
    | ObservabilityHTTPClientConfig
    | PowerBIConfiguration
    | QlikConfiguration
    | SsisConfiguration
    | SynapseAnalyticsConfiguration
    | UsernamePasswordConfiguration
)

CONF_T = TypeVar(
    "CONF_T",
    AirflowConfiguration,
    ApiTokenConfiguration,
    AzureBasicOauthConfiguration,
    AzureServicePrincipalConfiguration,
    CoreConfiguration,
    EventhubBlobConfiguration,
    DatabricksConfiguration,
    EventhubConfiguration,
    ExampleConfiguration,
    HTTPClientConfig,
    ObservabilityHTTPClientConfig,
    PowerBIConfiguration,
    QlikConfiguration,
    SsisConfiguration,
    SynapseAnalyticsConfiguration,
    UsernamePasswordConfiguration,
)


class ConfigurationRegistry:
    __initialized_configurations__: ConfigurationDict = ConfigurationDict()

    def __init__(self) -> None:
        if not self.__initialized_configurations__:
            self.register("core", CoreConfiguration)
            self.register("http", HTTPClientConfig)
            self.register("observability", ObservabilityHTTPClientConfig)

    def _initialize(self, configuration_id: CONFIGURATION_ID, configuration_class: type[CONF_T]) -> CONF_T:
        raw_data = read_configuration_file(*DEFAULT_CONFIGURATION_FILE_PATHS, section=configuration_id, missing_ok=True)
        cls = configuration_class(**raw_data)
        self.__initialized_configurations__[configuration_id] = cls
        return cls

    def register(self, configuration_id: CONFIGURATION_ID, configuration_class: type[CONF_T]) -> None:
        """
        This method adds a configuration to the registry. It will raise a KeyError in case the configuration has already
        been registered.
        """
        if configuration_id not in self.__initialized_configurations__:
            self._initialize(configuration_id, configuration_class)
        else:
            # Calling this twice is probably an error. Instead, Use add() to explicitly overwrite the config.
            raise KeyError(f"Configuration {configuration_class.__name__} already registered as '{configuration_id}'")

    def add(self, configuration_id: CONFIGURATION_ID, configuration: CONF_T) -> None:
        """
        This method just force-adds a configuration to the registry. Useful for overriding configurations or refreshing.
        Prefer using register() though when possible.
        """
        self.__initialized_configurations__[configuration_id] = configuration

    def mutate(self, configuration_id: CONFIGURATION_ID, configuration_class: type[CONF_T], **kwargs: Any) -> CONF_T:
        """
        Creates a configuration from an existing configuration, with keys modified

        WARNING: This method should only be called with things that are already verified via models. If we start getting
        ValidationErrors deep into the agent, we will come and haunt you. Instead, prefer to register models at the
        top of your agent.
        """
        current_model = self.lookup(configuration_id, configuration_class)
        return configuration_class(**current_model.model_dump(exclude=set(kwargs.keys())), **kwargs)

    def lookup(self, configuration_id: CONFIGURATION_ID, configuration_type: type[CONF_T]) -> CONF_T:
        """
        Lookup by the configuration's ID. if the ID is known in the configuration file under the heading [tool], then
        your ID is 'tool'.

        if the configuration has not been set with register(), this function will throw a KeyError
        """
        LOGGER.debug("Loading %s: %s", configuration_id, configuration_type.__name__)
        try:
            return cast(CONF_T, self.__initialized_configurations__[configuration_id])
        except KeyError as k:
            raise KeyError(f"Unknown configuration {configuration_id}, register() configuration.") from k

    def available(self, configuration_id: CONFIGURATION_ID, configuration_class: type[CONF_T]) -> bool:
        """
        Check if requested configuration is available. It will register the configuration if it exists and return True,
        returns False if configuration doesn't exist.
        """
        if configuration_id in self.__initialized_configurations__:
            LOGGER.debug("Configuration %s available.", configuration_id)
            return True
        try:
            self._initialize(configuration_id, configuration_class)
        except (KeyError, ValidationError):
            LOGGER.debug("Configuration %s not available.", configuration_id, exc_info=True)
            return False
        else:
            LOGGER.debug("Configuration %s available and registered", configuration_id)
            return True
