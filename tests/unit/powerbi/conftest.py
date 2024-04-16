import copy
import json
import os
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, Mock, patch

import pytest
from pydantic_core import Url
from pytest_httpx import HTTPXMock

from agents.powerbi.config import PowerBIConfiguration
from agents.powerbi.tasks import (
    PowerBIDataset,
    PowerBIDatasetRefresh,
    PowerBIFetchDatasetsTask,
    PowerBIGroup,
    PowerBIMonitorRunTask,
    PowerBIReport,
)
from framework import authenticators
from framework.configuration.authentication import AzureBasicOauthConfiguration
from registry import ConfigurationRegistry


@pytest.fixture()
def powerbi_client_id():
    return "DUMMY-CLIENT-ID"


@pytest.fixture()
def powerbi_tenant_id():
    return "DUMMY-TENANT-ID"


@pytest.fixture()
def powerbi_username():
    return "USER-NAME"


@pytest.fixture()
def powerbi_password():
    return "USER-PASSWORD"


@pytest.fixture()
def powerbi_scope():
    return Url("https://scope.url")


@pytest.fixture()
def powerbi_base_config():
    return {"groups": [], "base_api_url": "https://api.powerbi.com/v1.0/myorg"}


@pytest.fixture()
def powerbi_configuration(powerbi_base_config):
    return PowerBIConfiguration(**powerbi_base_config)


@pytest.fixture()
def env_powerbi_config(powerbi_base_config):
    environment_variables = {"DK_POWERBI_" + k.upper(): str(v) for k, v in powerbi_base_config.items()}
    with patch.dict(os.environ, environment_variables):
        yield environment_variables


@pytest.fixture()
def register_powerbi_config(env_powerbi_config, mock_core_env_vars):
    registry = ConfigurationRegistry()
    registry.register("powerbi", PowerBIConfiguration)
    return registry.lookup("powerbi", PowerBIConfiguration)


@pytest.fixture()
def basic_oauth_config(powerbi_client_id, powerbi_tenant_id, powerbi_username, powerbi_password, powerbi_scope):
    return {
        "azure_client_id": powerbi_client_id,
        "azure_tenant_id": powerbi_tenant_id,
        "azure_username": powerbi_username,
        "azure_password": powerbi_password,
        "azure_scope": powerbi_scope,
    }


@pytest.fixture()
def env_basic_oauth_config(basic_oauth_config):
    environment_variables = {"DK_" + k.upper(): str(v) for k, v in basic_oauth_config.items()}
    with patch.dict(os.environ, environment_variables):
        yield environment_variables


@pytest.fixture()
def register_basic_oauth_config(env_basic_oauth_config, mock_core_env_vars):
    registry = ConfigurationRegistry()
    registry.register("auth_azure_basic_oauth", AzureBasicOauthConfiguration)
    return registry.lookup("auth_azure_basic_oauth", AzureBasicOauthConfiguration)


@pytest.fixture()
def group():
    return PowerBIGroup(group_id="g1", group_name="group1")


@pytest.fixture()
def groups_data():
    return [{"id": f"g{i}", "name": f"group{i}"} for i in range(5)]


@pytest.fixture()
def groups(groups_data):
    return [PowerBIGroup(group_id=g["id"], group_name=g["name"]) for g in groups_data]


@pytest.fixture()
def dataset_data():
    return {"id": "d1", "name": "dataset1", "isRefreshable": True, "webUrl": "https://powerbi.com"}


@pytest.fixture()
def dataset(dataset_data):
    return PowerBIDataset(
        dataset_id=dataset_data["id"],
        dataset_name=dataset_data["name"],
        is_refreshable=dataset_data["isRefreshable"],
        web_url=dataset_data["webUrl"],
    )


@pytest.fixture()
def dataset_refresh_data(str_timestamp_now):
    return {
        "requestId": "dr1",
        "status": "Unknown",
        "startTime": str_timestamp_now,
        "refreshType": "Manual",
        "endTime": None,
    }


@pytest.fixture()
def dataset_refresh(dataset_refresh_data):
    return PowerBIDatasetRefresh(
        request_id=dataset_refresh_data["requestId"],
        refresh_type=dataset_refresh_data["refreshType"],
        status=dataset_refresh_data["status"],
        start_time=dataset_refresh_data["startTime"],
    )


@pytest.fixture()
def past_dataset_refresh(str_timestamp_now):
    return {
        "id": "dr1",
        "requestId": "dr1",
        "status": "Unknown",
        "startTime": str_timestamp_now,
        "refreshType": "Manual",
        "endTime": None,
    }


@pytest.fixture()
def reports_data(dataset_data):
    return [
        {
            "id": f"r{i}",
            "name": f"report{i}",
            "datasetId": dataset_data["id"],
            "reportType": "Manual",
            "webUrl": "https://powerbi.com",
        }
        for i in range(1, 3)
    ]


@pytest.fixture()
def reports(reports_data):
    return [
        PowerBIReport(
            report_id=r["id"],
            report_name=r["name"],
            report_type=r["reportType"],
            dataset_id=r["datasetId"],
            web_url=r["webUrl"],
        )
        for r in reports_data
    ]


@pytest.fixture()
def fetch_runs_task():
    with patch.object(authenticators, "UsernamePasswordCredential"):
        return PowerBIFetchDatasetsTask(
            nursery=Mock(),
            outbound_channel=AsyncMock(),
        )


@pytest.fixture()
def monitor_run_task(group, dataset):
    with patch.object(authenticators, "UsernamePasswordCredential"):
        return PowerBIMonitorRunTask(
            nursery=Mock(),
            outbound_channel=AsyncMock(),
            group_id=group.group_id,
            dataset=dataset,
        )


@pytest.fixture()
def mock_list_groups_response(groups_data, httpx_mock: HTTPXMock):
    httpx_mock.add_response(status_code=200, json={"value": groups_data})
    return httpx_mock


@pytest.fixture()
def mock_datasets_response(dataset_data, httpx_mock):
    httpx_mock.add_response(status_code=200, json={"value": [dataset_data]})
    return httpx_mock


@pytest.fixture()
def mock_active_refresh_response(dataset_refresh_data, httpx_mock):
    httpx_mock.add_response(status_code=200, json={"value": [dataset_refresh_data]})
    return httpx_mock


@pytest.fixture()
def completed_refresh_data(dataset_refresh_data, str_timestamp_now):
    data = copy.copy(dataset_refresh_data)
    data.update({"status": "Completed", "endTime": str_timestamp_now})
    return data


@pytest.fixture()
def mock_completed_refresh_response(completed_refresh_data, httpx_mock, str_timestamp_now):
    httpx_mock.add_response(
        url="https://api.powerbi.com/v1.0/groups/g1/datasets/d1/refreshes?%24top=1",
        status_code=200,
        json={"value": [completed_refresh_data]},
    )
    return httpx_mock


@pytest.fixture()
def failed_refresh_error_code():
    return {"errorCode": "some error code"}


@pytest.fixture()
def failed_refresh_data(dataset_refresh_data, failed_refresh_error_code, str_timestamp_now):
    data = copy.copy(dataset_refresh_data)
    data.update(
        {
            "status": "Failed",
            "endTime": str_timestamp_now,
            "serviceExceptionJson": json.dumps(failed_refresh_error_code),
        },
    )
    return data


@pytest.fixture()
def mock_failed_refresh_response(failed_refresh_data, httpx_mock):
    httpx_mock.add_response(
        status_code=200,
        json={"value": [failed_refresh_data]},
    )
    return httpx_mock


@pytest.fixture()
def cancelled_refresh_error_code():
    return {"errorCode": "cancelled by user"}


@pytest.fixture()
def cancelled_refresh_data(cancelled_refresh_error_code, dataset_refresh_data, str_timestamp_now):
    data = copy.deepcopy(dataset_refresh_data)
    data.update(
        {
            "status": "Cancelled",
            "endTime": str_timestamp_now,
            "serviceExceptionJson": json.dumps(cancelled_refresh_error_code),
        },
    )
    return data


@pytest.fixture()
def mock_cancelled_refresh_response(cancelled_refresh_data, dataset_refresh_data, httpx_mock, str_timestamp_now):
    httpx_mock.add_response(
        url="https://api.powerbi.com/v1.0/groups/g1/datasets/d1/refreshes?%24top=1",
        status_code=200,
        json={"value": [cancelled_refresh_data]},
    )
    return httpx_mock


@pytest.fixture()
def inactive_dataset_refresh_data(dataset_refresh_data):
    data = copy.deepcopy(dataset_refresh_data)
    data.update({"status": "Completed", "startTime": "2017-06-13T09:25:43.153Z", "endTime": "2017-06-13T09:26:43.153Z"})
    return data


@pytest.fixture()
def mock_inactive_refresh_response(inactive_dataset_refresh_data, httpx_mock):
    httpx_mock.add_response(
        status_code=200,
        json={"value": [inactive_dataset_refresh_data]},
    )
    return httpx_mock


@pytest.fixture()
def mock_empty_response(httpx_mock):
    httpx_mock.add_response(status_code=200, json={"value": []})
    return httpx_mock


@pytest.fixture()
def mock_reports_response(httpx_mock, reports_data):
    httpx_mock.add_response(
        url=r"https://api.powerbi.com/v1.0/groups/g1/reports",
        status_code=200,
        json={"value": reports_data},
    )
    return httpx_mock


@pytest.fixture()
def timestamp_now():
    return datetime.now(UTC)


@pytest.fixture()
def str_timestamp_now(timestamp_now):
    return timestamp_now.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


@pytest.fixture()
def timestamp_past(timestamp_now):
    return timestamp_now - timedelta(minutes=1)


@pytest.fixture()
def str_timestamp_past(timestamp_past):
    return timestamp_past.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


@pytest.fixture()
def pipeline_event_data(dataset, dataset_refresh):
    return {
        "pipeline_key": dataset.dataset_id,
        "pipeline_name": dataset.dataset_name,
        "run_key": dataset_refresh.request_id,
    }
