import json
import os
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from agents.airflow.configuration import AirflowConfiguration
from agents.airflow.job_runs import AirflowListRunsTask, AirflowWatchTaskStatus
from framework.configuration.authentication import UsernamePasswordConfiguration
from framework.core.channels import NullSendChannel
from framework.core.handles import get_client
from registry import ConfigurationRegistry

from .mock_data import (
    DAG_IDS,
    DAG_IDS_BAD,
    DAG_RUN_SUCCESS,
    DAG_RUNS_SINGLE_FAILED,
    DAG_RUNS_SINGLE_SUCCESS,
    TASK_INSTANCES_FINISHED,
)


def make_response(*, status_code=200, data=None, headers=None):
    """Simple method to make a httpx response object. TODO: move to testlib."""
    _headers = {"Content-Type": "application/json"}
    if headers:
        _headers.update(headers)
    content = json.dumps(data, indent=4).encode("utf-8") if data else None
    return httpx.Response(status_code=status_code, content=content, headers=_headers)


@pytest.fixture()
def airflow_config_data():
    config = {
        "api_url": "http://example.com",
    }
    return config


@pytest.fixture()
def env_airflow_config(airflow_config_data):
    environment_variables = {"DK_AIRFLOW_" + k.upper(): str(v) for k, v in airflow_config_data.items()}
    with patch.dict(os.environ, environment_variables):
        yield environment_variables


@pytest.fixture()
def register_airflow_config(env_airflow_config, mock_core_env_vars):
    registry = ConfigurationRegistry()
    registry.register("airflow", AirflowConfiguration)
    return registry.lookup("airflow", AirflowConfiguration)


@pytest.fixture()
def basic_auth_config():
    return {
        "agent_username": "username",
        "agent_password": "password",
    }


@pytest.fixture()
def env_basic_auth_config(basic_auth_config):
    environment_variables = {"DK_" + k.upper(): str(v) for k, v in basic_auth_config.items()}
    with patch.dict(os.environ, environment_variables):
        yield environment_variables


@pytest.fixture()
def register_basic_auth_config(env_basic_auth_config, mock_core_env_vars):
    registry = ConfigurationRegistry()
    registry.register("auth_username_password", UsernamePasswordConfiguration)
    return registry.lookup("auth_username_password", UsernamePasswordConfiguration)


@pytest.fixture()
def response_execute():
    return make_response(data={"status": "executed"})


@pytest.fixture()
def list_dag_ids_endpoint():
    response = make_response(data=DAG_IDS)
    with patch("agents.airflow.job_runs.AirflowListDagIDsEndpoint.handle", AsyncMock(return_value=response)) as handle:
        yield handle


@pytest.fixture()
def list_dag_ids_endpoint_bad_response():
    response = make_response(data=DAG_IDS_BAD)
    with patch("agents.airflow.job_runs.AirflowListDagIDsEndpoint.handle", AsyncMock(return_value=response)) as handle:
        yield handle


@pytest.fixture()
def list_dags_endpoint_single_failed():
    response = make_response(data=DAG_RUNS_SINGLE_FAILED)
    with patch("agents.airflow.job_runs.AirflowListRunsEndpoint.handle", AsyncMock(return_value=response)) as handle:
        yield handle


@pytest.fixture()
def list_dags_endpoint_single_success():
    response = make_response(data=DAG_RUNS_SINGLE_SUCCESS)
    with patch("agents.airflow.job_runs.AirflowListRunsEndpoint.handle", AsyncMock(return_value=response)) as handle:
        yield handle


@pytest.fixture()
def list_dags_endpoint_empty():
    with patch("agents.airflow.job_runs.AirflowListRunsEndpoint.handle", AsyncMock(return_value=None)) as handle:
        yield handle


@pytest.fixture()
def list_nursery(nursery):
    nursery.start_soon = AsyncMock()
    return nursery


@pytest.fixture()
def airflow_task(
    register_airflow_config,
    register_basic_auth_config,
    response_execute,
    list_nursery,
) -> AirflowListRunsTask:
    response = make_response(data={"dags": []})
    with patch("agents.airflow.job_runs.AirflowListDagIDsEndpoint.handle", AsyncMock(return_value=response)):
        airflow_list_runs_task = AirflowListRunsTask(list_nursery, NullSendChannel())
        yield airflow_list_runs_task


@pytest.fixture()
def task_status_finished(register_airflow_config, register_basic_auth_config) -> AirflowWatchTaskStatus:
    tasks_response = make_response(data=TASK_INSTANCES_FINISHED)
    run_response = make_response(data=DAG_RUN_SUCCESS)
    with patch(
        "agents.airflow.job_runs.AirflowListTaskInstancesEndpoint.handle",
        AsyncMock(return_value=tasks_response),
    ):
        with patch("agents.airflow.job_runs.AirflowGetRunEndpoint.handle", AsyncMock(return_value=run_response)):
            task = AirflowWatchTaskStatus(
                pipeline_key="test",
                run_key="test",
                client=get_client(),
                outbound_channel=NullSendChannel(),
            )
            yield task
