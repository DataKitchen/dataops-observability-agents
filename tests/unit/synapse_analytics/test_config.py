import pytest

from agents.synapse_analytics.config import (
    BASE_CLICK_BACK_URL,
    CLIENT_ENDPOINT,
    SYNAPSE_WORKSPACE_ID_TEMPLATE,
    SynapseAnalyticsConfiguration,
)


@pytest.fixture()
def default_config(synapse_config_data):
    return SynapseAnalyticsConfiguration(**synapse_config_data)


@pytest.mark.unit()
def test_default_values(default_config):
    assert default_config.period == 5
    assert default_config.subscription_id is None
    assert default_config.resource_group_name is None
    assert default_config.pipelines_filter == []

    assert default_config.workspace_id is None


@pytest.mark.unit()
def test_client_endpoint_empty(default_config):
    default_config.workspace_name = ""

    assert default_config.client_endpoint == ""


@pytest.mark.unit()
def test_client_endpoint_not_empty(default_config):
    assert default_config.client_endpoint == CLIENT_ENDPOINT.format(workspace_name=default_config.workspace_name)


@pytest.mark.unit()
def test_workspace_id(default_config):
    config = default_config
    config.subscription_id = "s"
    config.resource_group_name = "r"

    assert config.workspace_id == SYNAPSE_WORKSPACE_ID_TEMPLATE.format(
        subscription=config.subscription_id,
        resource_group=config.resource_group_name,
        workspace=config.workspace_name,
    )


@pytest.mark.unit()
def test_base_click_back_url(default_config):
    assert default_config.base_click_back_url == BASE_CLICK_BACK_URL


@pytest.mark.unit()
def test_valid_click_back_url_config(synapse_config_data):
    SynapseAnalyticsConfiguration(**synapse_config_data)
    SynapseAnalyticsConfiguration(subscription_id="na", resource_group_name="na", **synapse_config_data)


@pytest.mark.unit()
def test_invalid_click_back_url_config(synapse_config_data):
    with pytest.raises(ValueError, match="both"):
        SynapseAnalyticsConfiguration(subscription_id="na", **synapse_config_data)
    with pytest.raises(ValueError, match="both"):
        SynapseAnalyticsConfiguration(resource_group_name="na", **synapse_config_data)
