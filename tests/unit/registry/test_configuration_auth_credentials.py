import os
from base64 import b64encode
from unittest.mock import patch

import pytest

from framework import authenticators
from framework.authenticators import AzureBasicOauthAuth, AzureServicePrincipalAuth, BasicAuth, TokenAuth
from framework.configuration.authentication import (
    ApiTokenConfiguration,
    AzureBasicOauthConfiguration,
    AzureServicePrincipalConfiguration,
    UsernamePasswordConfiguration,
)
from registry.configuration_auth_credentials import CredentialsNotFoundError, load_agent_credentials, load_auth_class
from registry.configuration_registry import ConfigurationRegistry


@pytest.fixture()
def mock_credentials_username_password_env_vars():
    environment_variables = {"DK_AGENT_USERNAME": "xxx", "DK_AGENT_PASSWORD": "yyy"}
    with patch.dict(os.environ, environment_variables):
        yield environment_variables


@pytest.fixture()
def mock_credentials_username_password_empty_env_vars():
    environment_variables = {"DK_AGENT_USERNAME": "", "DK_AGENT_PASSWORD": ""}
    with patch.dict(os.environ, environment_variables):
        yield environment_variables


@pytest.fixture()
def mock_credentials_api_token_env_vars():
    environment_variables = {"DK_AGENT_TOKEN": "xxxxx"}
    with patch.dict(os.environ, environment_variables):
        yield environment_variables


@pytest.fixture()
def mock_credentials_azure_service_principal_env_vars():
    environment_variables = {
        "DK_AZURE_CLIENT_ID": "xxxx",
        "DK_AZURE_CLIENT_SECRET": "yyyy",
        "DK_AZURE_TENANT_ID": "123",
    }
    with patch.dict(os.environ, environment_variables):
        yield environment_variables


@pytest.fixture()
def mock_credentials_azure_basic_oauth_env_vars():
    environment_variables = {
        "DK_AZURE_CLIENT_ID": "xxxx",
        "DK_AZURE_USERNAME": "xxx",
        "DK_AZURE_PASSWORD": "yyy",
        "DK_AZURE_TENANT_ID": "123",
        "DK_AZURE_AUTHORITY": "https://azure.login/",
    }
    with patch.dict(os.environ, environment_variables):
        yield environment_variables


@pytest.mark.unit()
def test_load_agent_credentials_none(mock_core_env_vars):
    with pytest.raises(CredentialsNotFoundError):
        load_agent_credentials()


@pytest.mark.unit()
def test_load_agent_credentials_empty_strings(mock_core_env_vars, mock_credentials_username_password_empty_env_vars):
    with pytest.raises(CredentialsNotFoundError):
        load_agent_credentials()


@pytest.mark.unit()
def test_load_agent_credentials_username_password(mock_core_env_vars, mock_credentials_username_password_env_vars):
    auth_conf = load_agent_credentials()
    assert type(auth_conf) == UsernamePasswordConfiguration
    lookup = ConfigurationRegistry().lookup("auth_username_password", UsernamePasswordConfiguration)
    assert type(lookup) == UsernamePasswordConfiguration


@pytest.mark.unit()
def test_load_auth_class_username_password(mock_core_env_vars, mock_credentials_username_password_env_vars):
    auth_class = load_auth_class()
    assert type(auth_class) == BasicAuth
    usernamepass_b64encoded = b64encode(
        b":".join(
            (
                mock_credentials_username_password_env_vars["DK_AGENT_USERNAME"].encode(),
                mock_credentials_username_password_env_vars["DK_AGENT_PASSWORD"].encode(),
            ),
        ),
    ).decode()
    assert auth_class._auth_header == f"Basic {usernamepass_b64encoded}"


@pytest.mark.unit()
def test_load_agent_credentials_api_token_config(mock_core_env_vars, mock_credentials_api_token_env_vars):
    auth_conf = load_agent_credentials()
    assert type(auth_conf) == ApiTokenConfiguration
    lookup = ConfigurationRegistry().lookup("auth_api_token", ApiTokenConfiguration)
    assert type(lookup) == ApiTokenConfiguration


@pytest.mark.unit()
def test_load_agent_credentials_ignore_empty(
    mock_core_env_vars,
    mock_credentials_api_token_env_vars,
    mock_credentials_username_password_empty_env_vars,
):
    auth_conf = load_agent_credentials()
    assert type(auth_conf) == ApiTokenConfiguration
    lookup = ConfigurationRegistry().lookup("auth_api_token", ApiTokenConfiguration)
    assert type(lookup) == ApiTokenConfiguration


@pytest.mark.unit()
def test_load_auth_class_api_token(mock_core_env_vars, mock_credentials_api_token_env_vars):
    auth_class = load_auth_class()
    assert type(auth_class) == TokenAuth
    assert auth_class.base_token == mock_credentials_api_token_env_vars["DK_AGENT_TOKEN"]


@pytest.mark.unit()
def test_load_agent_credentials_azure_service_principal(
    mock_core_env_vars,
    mock_credentials_azure_service_principal_env_vars,
):
    auth_conf = load_agent_credentials()
    assert type(auth_conf) == AzureServicePrincipalConfiguration
    lookup = ConfigurationRegistry().lookup("auth_azure_spn", AzureServicePrincipalConfiguration)
    assert type(lookup) == AzureServicePrincipalConfiguration


@pytest.mark.unit()
def test_load_auth_class_azure_service_principal(mock_core_env_vars, mock_credentials_azure_service_principal_env_vars):
    auth_class = load_auth_class(spn_scope="example.com/.default")
    assert type(auth_class) == AzureServicePrincipalAuth
    assert auth_class.client_secret == mock_credentials_azure_service_principal_env_vars["DK_AZURE_CLIENT_SECRET"]


@pytest.mark.unit()
def test_load_auth_class_azure_service_principal_without_scope(
    mock_core_env_vars,
    mock_credentials_azure_service_principal_env_vars,
):
    with pytest.raises(ValueError, match="scope"):
        load_auth_class()


@pytest.mark.unit()
def test_load_agent_credentials_azure_basic_oauth(mock_core_env_vars, mock_credentials_azure_basic_oauth_env_vars):
    auth_conf = load_agent_credentials()
    assert type(auth_conf) == AzureBasicOauthConfiguration
    lookup = ConfigurationRegistry().lookup("auth_azure_basic_oauth", AzureBasicOauthConfiguration)
    assert type(lookup) == AzureBasicOauthConfiguration


@pytest.mark.unit()
def test_load_auth_class_azure_basic_oauth_w_env_scope(mock_core_env_vars, mock_credentials_azure_basic_oauth_env_vars):
    os.environ["DK_AZURE_SCOPE"] = "example-scope"
    with patch.object(authenticators, "UsernamePasswordCredential") as mock_credential:
        auth_class = load_auth_class()
        assert type(auth_class) == AzureBasicOauthAuth
        mock_credential.assert_called_once_with(
            authority=mock_credentials_azure_basic_oauth_env_vars["DK_AZURE_AUTHORITY"],
            client_id=mock_credentials_azure_basic_oauth_env_vars["DK_AZURE_CLIENT_ID"],
            username=mock_credentials_azure_basic_oauth_env_vars["DK_AZURE_USERNAME"],
            password=mock_credentials_azure_basic_oauth_env_vars["DK_AZURE_PASSWORD"],
            tenant_id=mock_credentials_azure_basic_oauth_env_vars["DK_AZURE_TENANT_ID"],
        )
        assert auth_class.credential is not None
        assert auth_class.scope == os.environ["DK_AZURE_SCOPE"]


@pytest.mark.unit()
def test_load_auth_class_azure_basic_oauth_w_spn_scope(mock_core_env_vars, mock_credentials_azure_basic_oauth_env_vars):
    spn_scope = "example-scope"
    with patch.object(authenticators, "UsernamePasswordCredential") as mock_credential:
        auth_class = load_auth_class(spn_scope)
        assert type(auth_class) == AzureBasicOauthAuth
        mock_credential.assert_called_once_with(
            authority=mock_credentials_azure_basic_oauth_env_vars["DK_AZURE_AUTHORITY"],
            client_id=mock_credentials_azure_basic_oauth_env_vars["DK_AZURE_CLIENT_ID"],
            username=mock_credentials_azure_basic_oauth_env_vars["DK_AZURE_USERNAME"],
            password=mock_credentials_azure_basic_oauth_env_vars["DK_AZURE_PASSWORD"],
            tenant_id=mock_credentials_azure_basic_oauth_env_vars["DK_AZURE_TENANT_ID"],
        )
        assert auth_class.credential is not None
        assert auth_class.scope == spn_scope


@pytest.mark.unit()
def test_load_auth_class_azure_basic_oauth_without_scope(
    mock_core_env_vars,
    mock_credentials_azure_basic_oauth_env_vars,
):
    with pytest.raises(ValueError, match="scope"):
        load_auth_class()
