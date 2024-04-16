from datetime import UTC, datetime, timedelta
from unittest.mock import ANY, call, patch

import pytest

from agents.airflow.lib import get_status
from toolkit.observability import Status

CURR_DT = datetime.now(tz=UTC)
PREV_DT = CURR_DT - timedelta(days=1)


@pytest.mark.unit()
@pytest.mark.parametrize(
    ("test_input", "expected"),
    [
        ("queued", Status.UNKNOWN),
        ("running", Status.RUNNING),
        ("success", Status.COMPLETED),
        ("failed", Status.FAILED),
        ("any other", Status.UNKNOWN),
    ],
)
def test_get_status(test_input, expected):
    assert expected == get_status(test_input)


@pytest.mark.unit()
async def test_airflow_get_dag_ids(airflow_task, list_dag_ids_endpoint):
    with patch("agents.airflow.job_runs.AirflowListDagIDsEndpoint.handle", list_dag_ids_endpoint):
        expected = ["example_sla_dag", "simple_bash_dag"]
        actual = await airflow_task.get_dag_ids()
        airflow_task.dag_list_endpoint.handle.assert_called_once()
        assert expected == actual


@pytest.mark.unit()
async def test_airflow_get_dag_ids_bad_response(airflow_task, list_dag_ids_endpoint_bad_response):
    with patch("agents.airflow.job_runs.AirflowListDagIDsEndpoint.handle", list_dag_ids_endpoint_bad_response):
        with pytest.raises(KeyError):
            await airflow_task.get_dag_ids()


@pytest.mark.unit()
@patch("agents.airflow.job_runs.AirflowListRunsTask.send")
async def test_airflow_execute(mock_send, airflow_task, list_dags_endpoint_single_success, list_dag_ids_endpoint):
    with patch("agents.airflow.job_runs.AirflowListDagIDsEndpoint.handle", list_dag_ids_endpoint):
        await airflow_task.execute(CURR_DT, PREV_DT)
        airflow_task.endpoint.handle.assert_called_once_with(
            payload={
                "dag_ids": ["example_sla_dag", "simple_bash_dag"],
                "execution_date_gte": PREV_DT.isoformat(),
                "execution_date_lte": CURR_DT.isoformat(),
            },
        )


@pytest.mark.unit()
@patch("logging.Logger.warning")
async def test_airflow_execute_empty_response(mock_log, list_dags_endpoint_empty, list_dag_ids_endpoint, airflow_task):
    await airflow_task.execute(CURR_DT, PREV_DT)
    airflow_task.endpoint.handle.assert_called_once()
    mock_log.assert_called_once_with("Failed to list job runs")


@pytest.mark.unit()
async def test_parse_get_dag_ids(airflow_task, list_dag_ids_endpoint):
    with patch("agents.airflow.job_runs.AirflowListDagIDsEndpoint.handle", list_dag_ids_endpoint):
        expected = ["example_sla_dag", "simple_bash_dag"]
        actual = await airflow_task.get_dag_ids()
        assert expected == actual


@pytest.mark.unit()
@patch("agents.airflow.job_runs.AirflowWatchTaskStatus.send")
async def test_airflow_send_success_events(
    mock_event_send,
    task_status_finished,
):
    await task_status_finished.execute(CURR_DT, PREV_DT)

    expected_task_payload = {
        "EVENT_TYPE": "run-status",
        "event_timestamp": ANY,
        "status": ANY,
        "pipeline_key": "test",
        "run_key": "test",
        "task_key": ANY,
        "metadata": ANY,
        "component_tool": "airflow",
    }
    expected_run_payload = {
        "EVENT_TYPE": "run-status",
        "event_timestamp": ANY,
        "status": ANY,
        "pipeline_key": "test",
        "run_key": "test",
        "metadata": ANY,
        "component_tool": "airflow",
    }
    mock_event_send.assert_has_calls(
        [
            call(payload=expected_task_payload),
            call(payload=expected_task_payload),
            call(payload=expected_run_payload),
        ],
    )
