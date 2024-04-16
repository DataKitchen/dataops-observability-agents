import os
from datetime import timedelta
from unittest.mock import AsyncMock, patch

import pytest
from azure.identity.aio import ClientSecretCredential
from azure.synapse.artifacts.aio import ArtifactsClient
from azure.synapse.artifacts.models import (
    ActivityRun,
    ActivityRunsQueryResponse,
)

from agents.synapse_analytics.client import artifacts_client
from agents.synapse_analytics.config import (
    SynapseAnalyticsConfiguration,
)
from agents.synapse_analytics.types import SynapseActivityData, SynapseRunData
from framework.configuration.authentication import (
    AzureServicePrincipalConfiguration,
)
from registry import ConfigurationRegistry


@pytest.fixture()
def earlier(now):
    return now - timedelta(seconds=10)


@pytest.fixture()
def channel():
    return AsyncMock()


@pytest.fixture()
def run_data():
    return SynapseRunData(pipeline_name="a name", run_id="an id")


@pytest.fixture()
def empty_activity_response(synapse_client):
    response = ActivityRunsQueryResponse(value=[])
    response.continuation_token = None
    synapse_client.pipeline_run.query_activity_runs.return_value = response
    return response


@pytest.fixture()
def activity_run():
    activity_run = ActivityRun()
    activity_run.activity_name = "activity name"
    activity_run.activity_type = "activity type"
    activity_run.activity_run_id = "activity id"
    activity_run.pipeline_name = "a name"
    activity_run.pipeline_run_id = "an id"
    activity_run.status = "InProgress"
    return activity_run


@pytest.fixture()
def activity_data(activity_run):
    return SynapseActivityData.create(activity_run)


@pytest.fixture()
def azure_credentials():
    return {
        "client_id": "client id",
        "client_secret": "client secret",
        "tenant_id": "tenant id",
    }


@pytest.fixture()
def synapse_config_data():
    return {
        "workspace_name": "workspace name",
    }


@pytest.fixture()
def env_synapse_config(synapse_config_data, azure_credentials):
    environment_variables = {"DK_SYNAPSE_ANALYTICS_" + k.upper(): str(v) for k, v in synapse_config_data.items()}
    environment_variables.update({"DK_AZURE_" + k.upper(): str(v) for k, v in azure_credentials.items()})
    with patch.dict(os.environ, environment_variables):
        yield environment_variables


@pytest.fixture()
def register_synapse_config(env_synapse_config, mock_core_env_vars):
    registry = ConfigurationRegistry()
    registry.register("synapse_analytics", SynapseAnalyticsConfiguration)
    registry.register("auth_azure_spn", AzureServicePrincipalConfiguration)
    return registry.lookup("synapse_analytics", SynapseAnalyticsConfiguration)


@pytest.fixture()
async def synapse_client(env_synapse_config, register_synapse_config):
    with patch("agents.synapse_analytics.client.ArtifactsClient", autospec=ArtifactsClient), patch(
        "agents.synapse_analytics.client.ClientSecretCredential",
        autospec=ClientSecretCredential,
    ):
        async with artifacts_client as client:
            yield client
