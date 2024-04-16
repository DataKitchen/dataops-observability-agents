from logging import getLogger

from framework.authenticators import Auth, AzureBasicOauthAuth, AzureServicePrincipalAuth, BasicAuth, TokenAuth
from framework.configuration.authentication import (
    ApiTokenConfiguration,
    AzureBasicOauthConfiguration,
    AzureServicePrincipalConfiguration,
    UsernamePasswordConfiguration,
)
from registry.configuration_registry import ConfigurationRegistry

LOGGER = getLogger(__name__)

CREDENTIAL_CONFIG_TYPES = (
    UsernamePasswordConfiguration
    | ApiTokenConfiguration
    | AzureServicePrincipalConfiguration
    | AzureBasicOauthConfiguration
)


class CredentialsNotFoundError(Exception):
    def __init__(
        self,
        msg: str = "No suitable set of credentials found in configuration or environment",
        *args: object,
        **kwargs: object,
    ) -> None:
        super().__init__(msg, *args, **kwargs)


def load_agent_credentials() -> CREDENTIAL_CONFIG_TYPES:
    registry = ConfigurationRegistry()
    if registry.available("auth_username_password", UsernamePasswordConfiguration):
        return registry.lookup("auth_username_password", UsernamePasswordConfiguration)
    elif registry.available("auth_api_token", ApiTokenConfiguration):
        return registry.lookup("auth_api_token", ApiTokenConfiguration)
    elif registry.available("auth_azure_spn", AzureServicePrincipalConfiguration):
        return registry.lookup("auth_azure_spn", AzureServicePrincipalConfiguration)
    elif registry.available("auth_azure_basic_oauth", AzureBasicOauthConfiguration):
        return registry.lookup("auth_azure_basic_oauth", AzureBasicOauthConfiguration)
    else:
        raise CredentialsNotFoundError


def load_auth_class(spn_scope: str = "") -> Auth:
    auth_config = load_agent_credentials()
    match auth_config:
        case ApiTokenConfiguration():
            return TokenAuth(token=auth_config.agent_token.get_secret_value())
        case AzureServicePrincipalConfiguration():
            if not auth_config.scope and not spn_scope:
                raise ValueError(
                    "This agent does not have a scope configured for Azure Service Principal Authentication",
                )
            return AzureServicePrincipalAuth(
                tenant_id=auth_config.tenant_id,
                client_id=auth_config.client_id,
                client_secret=auth_config.client_secret.get_secret_value(),
                scope=auth_config.scope or spn_scope,
            )
        case UsernamePasswordConfiguration():
            return BasicAuth(
                username=auth_config.agent_username,
                password=auth_config.agent_password.get_secret_value(),
            )
        case AzureBasicOauthConfiguration():
            if not auth_config.scope and not spn_scope:
                raise ValueError("This agent does not have a scope configured for Azure Basic OAuth Authentication")
            return AzureBasicOauthAuth(
                tenant_id=auth_config.tenant_id,
                client_id=auth_config.client_id,
                username=auth_config.username,
                password=auth_config.password.get_secret_value(),
                scope=auth_config.scope or spn_scope,
                authority=auth_config.authority,
            )
        case _:
            raise ValueError("Couldn't load an appropriate Authenticator class.")
