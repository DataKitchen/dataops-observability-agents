from urllib.parse import urlencode

from registry import ConfigurationRegistry

from .config import SynapseAnalyticsConfiguration
from .helpers import ActivityType
from .types import SynapseActivityData


def pipeline_click_back_url(run_id: str) -> str | None:
    config = ConfigurationRegistry().lookup("synapse_analytics", SynapseAnalyticsConfiguration)
    if workspace_id := config.workspace_id:
        query = {"workspace": workspace_id}
        return f"{config.base_click_back_url}/{run_id}?{urlencode(query=query, doseq=True)}"
    return None


def activity_click_back_url(activity_data: SynapseActivityData, run_id: str) -> str | None:
    config = ConfigurationRegistry().lookup("synapse_analytics", SynapseAnalyticsConfiguration)
    if not config.workspace_id:
        return None

    query = {"workspace": config.workspace_id}
    match activity_data.activity_type:
        case ActivityType.SYNAPSE_NOTEBOOK.value:
            query["snapshotId"] = activity_data.activity_run_id
            url = f"{config.base_click_back_url}/{run_id}?{urlencode(query=query, doseq=True)}"
        case ActivityType.EXECUTE_DATA_FLOW.value:
            url = f"{config.base_click_back_url}/{run_id}/{activity_data.activity_run_id}?{urlencode(query=query, doseq=True)}"
        case _:
            url = f"{config.base_click_back_url}/{run_id}?{urlencode(query=query, doseq=True)}"
    return url
