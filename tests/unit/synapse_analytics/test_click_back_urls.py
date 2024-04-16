import os
from unittest.mock import patch

import pytest

from agents.synapse_analytics.click_back_urls import (
    activity_click_back_url,
    pipeline_click_back_url,
)
from agents.synapse_analytics.config import (
    SynapseAnalyticsConfiguration,
)
from agents.synapse_analytics.types import (
    SynapseActivityData,
)
from registry import ConfigurationRegistry


@pytest.fixture()
def activity_data():
    return SynapseActivityData(
        activity_name="a name",
        activity_type="a type",
        activity_run_id="a run id",
        pipeline_name="a pipeline name",
        pipeline_run_id="a pipeline id",
    )


@pytest.fixture()
def env_workspace_id():
    environment_variables = {
        "DK_SYNAPSE_ANALYTICS_SUBSCRIPTION_ID": "sub id",
        "DK_SYNAPSE_ANALYTICS_RESOURCE_GROUP_NAME": "group name",
    }
    with patch.dict(os.environ, environment_variables):
        yield environment_variables


@pytest.mark.unit()
def test_pipeline_click_back_url_without_workspace_id(register_synapse_config):
    assert pipeline_click_back_url("run id") is None


@pytest.mark.unit()
def test_pipeline_click_back_url_with_workspace_id(env_workspace_id, env_synapse_config, mock_core_env_vars):
    ConfigurationRegistry().register("synapse_analytics", SynapseAnalyticsConfiguration)
    assert pipeline_click_back_url("run id") is not None


@pytest.mark.unit()
def test_activity_click_back_url_without_workspace_id(activity_data, register_synapse_config):
    assert activity_click_back_url(activity_data, "run id") is None


@pytest.mark.unit()
def test_activity_click_back_url_with_workspace_id(
    env_workspace_id,
    env_synapse_config,
    mock_core_env_vars,
    activity_data,
):
    ConfigurationRegistry().register("synapse_analytics", SynapseAnalyticsConfiguration)
    assert activity_click_back_url(activity_data, "run id") is not None


@pytest.mark.unit()
@pytest.mark.parametrize(
    "activity_type",
    [
        "SynapseNotebook",
        "ExecuteDataFlow",
        "some_other_type",
    ],
)
def test_activity_click_back_url_synapsenotebook(
    env_workspace_id,
    env_synapse_config,
    mock_core_env_vars,
    activity_data,
    activity_type,
):
    ConfigurationRegistry().register("synapse_analytics", SynapseAnalyticsConfiguration)
    activity_data.activity_type = activity_type
    assert activity_click_back_url(activity_data, "run id") is not None
