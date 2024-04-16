import os
from datetime import timedelta
from unittest.mock import patch

import pytest
from azure.identity.aio import ClientSecretCredential
from azure.synapse.artifacts.aio import ArtifactsClient
from azure.synapse.artifacts.models import (
    ActivityRun,
    PipelineRun,
    PipelineRunInvokedBy,
)
from trio import open_memory_channel

from agents.synapse_analytics.client import artifacts_client
from agents.synapse_analytics.config import (
    SynapseAnalyticsConfiguration,
)
from agents.synapse_analytics.helpers import ActivityType
from agents.synapse_analytics.types import SynapseRunData
from framework.configuration.authentication import (
    AzureServicePrincipalConfiguration,
)
from registry import ConfigurationRegistry
from toolkit.more_typing import JSON_DICT


@pytest.fixture()
def channel_pair():
    return open_memory_channel[JSON_DICT](0)


@pytest.fixture()
async def outbound_channel(channel_pair):
    async with channel_pair[0] as channel:
        yield channel


@pytest.fixture()
async def inbound_channel(channel_pair):
    async with channel_pair[1] as channel:
        yield channel


@pytest.fixture()
def earlier(now):
    return now - timedelta(seconds=10)


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
def _register_synapse_config(env_synapse_config, mock_core_env_vars):
    ConfigurationRegistry().register("synapse_analytics", SynapseAnalyticsConfiguration)
    ConfigurationRegistry().register("auth_azure_spn", AzureServicePrincipalConfiguration)


@pytest.fixture()
async def synapse_client(env_synapse_config, _register_synapse_config):
    with patch("agents.synapse_analytics.client.ArtifactsClient", autospec=ArtifactsClient), patch(
        "agents.synapse_analytics.client.ClientSecretCredential",
        autospec=ClientSecretCredential,
    ):
        async with artifacts_client as client:
            yield client


@pytest.fixture()
def run_data():
    return SynapseRunData(pipeline_name="a name", run_id="an id")


@pytest.fixture()
def inprogress_run(run_data, earlier, now):
    invoked_by = PipelineRunInvokedBy()
    invoked_by.name = "Tengil"
    invoked_by.id = "an id"
    invoked_by.invoked_by_type = "a type"

    pipeline_run = PipelineRun()
    pipeline_run.pipeline_name = run_data.pipeline_name
    pipeline_run.run_id = run_data.run_id
    pipeline_run.status = "InProgress"
    pipeline_run.run_id = run_data.run_id
    pipeline_run.run_start = earlier
    pipeline_run.run_end = now
    pipeline_run.parameters = {"param1": 1, "param2": 2}
    pipeline_run.invoked_by = invoked_by
    pipeline_run.duration_in_ms = 1000
    return pipeline_run


@pytest.fixture()
def successful_run(run_data, earlier, now):
    invoked_by = PipelineRunInvokedBy()
    invoked_by.name = "Tengil"
    invoked_by.id = "an id"
    invoked_by.invoked_by_type = "a type"

    pipeline_run = PipelineRun()
    pipeline_run.status = "Succeeded"
    pipeline_run.run_id = run_data.run_id
    pipeline_run.run_start = earlier
    pipeline_run.run_end = now
    pipeline_run.parameters = {"param1": 1, "param2": 2}
    pipeline_run.invoked_by = invoked_by
    pipeline_run.duration_in_ms = 1000
    return pipeline_run


@pytest.fixture()
def start_activity1(earlier, run_data):
    activity_run = ActivityRun()
    activity_run.activity_name = "activity name 1"
    activity_run.activity_type = "activity type 1"
    activity_run.activity_run_id = "activity id 1"
    activity_run.pipeline_name = run_data.pipeline_name
    activity_run.pipeline_run_id = run_data.run_id
    activity_run.status = "InProgress"
    activity_run.activity_run_start = earlier
    return activity_run


@pytest.fixture()
def end_activity1(now, run_data):
    activity_run = ActivityRun()
    activity_run.activity_name = "activity name 1"
    activity_run.activity_type = "activity type 1"
    activity_run.activity_run_id = "activity id 1"
    activity_run.pipeline_name = run_data.pipeline_name
    activity_run.pipeline_run_id = run_data.run_id
    activity_run.status = "Succeeded"
    activity_run.activity_run_end = now
    return activity_run


@pytest.fixture()
def end_activity2(run_data):
    activity_run = ActivityRun()
    activity_run.activity_name = "activity name 2"
    activity_run.activity_type = "activity type 2"
    activity_run.activity_run_id = "activity id 2"
    activity_run.pipeline_name = run_data.pipeline_name
    activity_run.pipeline_run_id = run_data.run_id
    activity_run.status = "Failed"
    return activity_run


@pytest.fixture()
def copy_activity(run_data):
    activity_run = ActivityRun()
    activity_run.activity_name = "copy activity name"
    activity_run.activity_type = ActivityType.COPY.value
    activity_run.activity_run_id = "copy activity id"
    activity_run.pipeline_name = run_data.pipeline_name
    activity_run.pipeline_run_id = run_data.run_id
    activity_run.status = "Succeeded"
    activity_run.additional_properties = {"userProperties": {"Source": "source table", "Destination": "dest table"}}
    return activity_run
