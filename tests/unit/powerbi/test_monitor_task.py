from datetime import datetime
from unittest.mock import patch

import pytest

from agents.powerbi import tasks
from agents.powerbi.tasks import GenericApiError
from toolkit.observability import Status


@pytest.mark.unit()
@pytest.mark.usefixtures("register_powerbi_config", "register_basic_oauth_config")
async def test_get_refresh_history_200(dataset_refresh_data, mock_active_refresh_response, monitor_run_task) -> None:
    result = await monitor_run_task.get_refresh_history()
    assert result == dataset_refresh_data


@pytest.mark.unit()
@pytest.mark.usefixtures("register_powerbi_config", "register_basic_oauth_config")
async def test_get_refresh_history_empty_result(monitor_run_task, mock_empty_response) -> None:
    result = await monitor_run_task.get_refresh_history()
    assert result is None


@pytest.mark.unit()
@pytest.mark.usefixtures("register_powerbi_config", "register_basic_oauth_config")
async def test_get_refresh_history_non_200_response_raise(httpx_mock, monitor_run_task, powerbi_configuration) -> None:
    httpx_mock.add_response(502)
    with pytest.raises(GenericApiError):
        await monitor_run_task.get_refresh_history()


@pytest.mark.unit()
@pytest.mark.usefixtures("register_powerbi_config", "register_basic_oauth_config")
async def test_get_reports_200(monitor_run_task, mock_reports_response, reports) -> None:
    # with patch.object(PowerBIListReportsEndpoint, "handle", return_value=mock_reports_response):
    result = await monitor_run_task.get_reports()
    assert result == reports


@pytest.mark.unit()
@pytest.mark.usefixtures("register_powerbi_config", "register_basic_oauth_config")
async def test_get_reports_empty_result(monitor_run_task, mock_empty_response) -> None:
    result = await monitor_run_task.get_reports()
    assert result == []


@pytest.mark.unit()
@pytest.mark.usefixtures("register_powerbi_config", "register_basic_oauth_config")
async def test_get_reports_non_200_response_raise(httpx_mock, monitor_run_task, powerbi_configuration) -> None:
    httpx_mock.add_response(502)
    with pytest.raises(GenericApiError):
        await monitor_run_task.get_reports()


@pytest.mark.unit()
@pytest.mark.usefixtures("register_powerbi_config", "register_basic_oauth_config")
async def test_monitor_run_task_execute_empty_result(
    timestamp_now,
    mock_empty_response,
    monitor_run_task,
    timestamp_past,
) -> None:
    with patch.object(tasks, "send_run_status_event") as send_event:
        await monitor_run_task.execute(timestamp_now, timestamp_past)
        send_event.assert_not_called()
        assert monitor_run_task.finished_refreshes == []
        assert monitor_run_task.is_done is False


@pytest.mark.unit()
@pytest.mark.usefixtures("register_powerbi_config", "register_basic_oauth_config")
async def test_monitor_run_task_execute_existing_finished_refresh(
    dataset_refresh,
    mock_completed_refresh_response,
    monitor_run_task,
    timestamp_now,
    timestamp_past,
) -> None:
    monitor_run_task.finished_refreshes = [dataset_refresh.request_id]
    with patch.object(tasks, "send_run_status_event") as send_event:
        await monitor_run_task.execute(timestamp_now, timestamp_past)
        send_event.assert_not_called()
        assert monitor_run_task.finished_refreshes == [dataset_refresh.request_id]
        assert monitor_run_task.is_done is False


@pytest.mark.unit()
@pytest.mark.usefixtures("register_powerbi_config", "register_basic_oauth_config")
async def test_monitor_run_task_execute_past_refresh(
    inactive_dataset_refresh_data,
    mock_inactive_refresh_response,
    monitor_run_task,
    timestamp_now,
    timestamp_past,
) -> None:
    assert datetime.fromisoformat(inactive_dataset_refresh_data["startTime"]) < monitor_run_task.start_time
    assert datetime.fromisoformat(inactive_dataset_refresh_data["endTime"]) < monitor_run_task.start_time
    with patch.object(tasks, "send_run_status_event") as send_event:
        await monitor_run_task.execute(timestamp_now, timestamp_past)
        send_event.assert_not_called()
        assert monitor_run_task.finished_refreshes == []
        assert monitor_run_task.is_done is False


@pytest.mark.unit()
@pytest.mark.usefixtures("register_powerbi_config", "register_basic_oauth_config")
async def test_get_dataset_refreshes_running_send_event(
    timestamp_now,
    dataset,
    dataset_refresh_data,
    mock_active_refresh_response,
    monitor_run_task,
    pipeline_event_data,
    timestamp_past,
) -> None:
    # send_run_status_event
    with patch.object(tasks, "send_run_status_event") as send_event:
        await monitor_run_task.execute(timestamp_now, timestamp_past)
        metadata = {
            "dataset_refresh_id": dataset_refresh_data["requestId"],
            "dataset_refresh_type": dataset_refresh_data["refreshType"],
            "dataset": vars(dataset),
        }
        pipeline_event_data.update(
            {
                "event_timestamp": dataset_refresh_data["startTime"],
                "metadata": metadata,
                "external_url": dataset.web_url,
            },
        )

        # Run task status event
        task_event_data = pipeline_event_data.copy()
        task_event_data.update({"task_key": dataset_refresh_data["requestId"], "task_name": dataset.dataset_name})
        assert send_event.call_args_list[0].args == (monitor_run_task.outbound_channel, Status.RUNNING, task_event_data)
        # Run status event
        assert send_event.call_args_list[1].args == (
            monitor_run_task.outbound_channel,
            Status.RUNNING,
            pipeline_event_data,
        )
        assert monitor_run_task.finished_refreshes == []
        assert monitor_run_task.is_done is False


@pytest.mark.unit()
@pytest.mark.usefixtures("register_powerbi_config", "register_basic_oauth_config")
async def test_get_dataset_refreshes_completed_send_event(
    completed_refresh_data,
    dataset,
    mock_completed_refresh_response,
    mock_reports_response,
    monitor_run_task,
    pipeline_event_data,
    reports,
    timestamp_now,
    timestamp_past,
) -> None:
    monitor_run_task.start_time = timestamp_past
    with patch.object(tasks, "send_run_status_event") as send_run_status_event, patch.object(
        tasks,
        "send_dataset_operation_event",
    ) as send_dataset_operation_event:
        await monitor_run_task.execute(timestamp_now, timestamp_past)
        refresh_data = completed_refresh_data
        metadata = {
            "dataset_refresh_id": refresh_data["requestId"],
            "dataset_refresh_type": refresh_data["refreshType"],
            "dataset": vars(dataset),
        }
        pipeline_event_data.update(
            {"event_timestamp": refresh_data["endTime"], "metadata": metadata, "external_url": dataset.web_url},
        )

        # Close run task
        task_event_data = pipeline_event_data.copy()
        task_event_data.update({"task_key": refresh_data["requestId"], "task_name": dataset.dataset_name})
        assert send_run_status_event.call_args_list[0].args == (
            monitor_run_task.outbound_channel,
            Status.COMPLETED,
            task_event_data,
        )
        # Dataset operation event
        for i, report in enumerate(reports):
            dataset_event_data = {
                "event_timestamp": refresh_data["endTime"],
                "dataset_key": report.report_id,
                "dataset_name": report.report_name,
                "metadata": {"reportType": report.report_type, "dataset": vars(dataset)},
                "external_url": dataset.web_url,
            }
            assert send_dataset_operation_event.call_args_list[i].args == (
                monitor_run_task.outbound_channel,
                "WRITE",
                dataset_event_data,
            )
        # Close run
        assert send_run_status_event.call_args_list[1].args == (
            monitor_run_task.outbound_channel,
            Status.COMPLETED,
            pipeline_event_data,
        )
        assert monitor_run_task.finished_refreshes == [refresh_data["requestId"]]
        assert monitor_run_task.is_done is False


@pytest.mark.unit()
@pytest.mark.usefixtures("register_powerbi_config", "register_basic_oauth_config")
async def test_get_dataset_refreshes_cancelled_send_event(
    cancelled_refresh_data,
    cancelled_refresh_error_code,
    dataset,
    mock_cancelled_refresh_response,
    monitor_run_task,
    pipeline_event_data,
    timestamp_now,
    timestamp_past,
) -> None:
    monitor_run_task.start_time = timestamp_past
    with patch.object(tasks, "send_run_status_event") as send_run_status_event, patch.object(
        tasks,
        "send_message_log_event",
    ) as send_message_log_event:
        await monitor_run_task.execute(timestamp_now, timestamp_past)
        metadata = {
            "dataset_refresh_id": cancelled_refresh_data["requestId"],
            "dataset_refresh_type": cancelled_refresh_data["refreshType"],
            "dataset": vars(dataset),
        }
        pipeline_event_data.update(
            {
                "event_timestamp": cancelled_refresh_data["endTime"],
                "metadata": metadata,
                "external_url": dataset.web_url,
            },
        )

        # Close run task
        task_event_data = pipeline_event_data.copy()
        task_event_data.update({"task_key": cancelled_refresh_data["requestId"], "task_name": dataset.dataset_name})
        assert send_run_status_event.call_args_list[0].args == (
            monitor_run_task.outbound_channel,
            Status.COMPLETED_WITH_WARNINGS,
            task_event_data,
        )
        # Message log event
        assert send_message_log_event.call_args_list[0].args == (
            monitor_run_task.outbound_channel,
            "WARNING",
            cancelled_refresh_error_code["errorCode"],
            task_event_data,
        )
        # Close run
        assert send_run_status_event.call_args_list[1].args == (
            monitor_run_task.outbound_channel,
            Status.COMPLETED_WITH_WARNINGS,
            pipeline_event_data,
        )
        assert monitor_run_task.finished_refreshes == [cancelled_refresh_data["requestId"]]
        assert monitor_run_task.is_done is False


@pytest.mark.unit()
@pytest.mark.usefixtures("register_powerbi_config", "register_basic_oauth_config")
async def test_get_dataset_refreshes_failed_send_event(
    dataset,
    failed_refresh_error_code,
    failed_refresh_data,
    mock_failed_refresh_response,
    monitor_run_task,
    pipeline_event_data,
    timestamp_now,
    timestamp_past,
) -> None:
    monitor_run_task.start_time = timestamp_past
    with patch.object(tasks, "send_run_status_event") as send_run_status_event, patch.object(
        tasks,
        "send_message_log_event",
    ) as send_message_log_event:
        await monitor_run_task.execute(timestamp_now, timestamp_past)
        refresh_data = failed_refresh_data
        metadata = {
            "dataset_refresh_id": refresh_data["requestId"],
            "dataset_refresh_type": refresh_data["refreshType"],
            "dataset": vars(dataset),
        }
        pipeline_event_data.update(
            {"event_timestamp": refresh_data["endTime"], "metadata": metadata, "external_url": dataset.web_url},
        )

        # Close run task
        task_event_data = pipeline_event_data.copy()
        task_event_data.update({"task_key": refresh_data["requestId"], "task_name": dataset.dataset_name})
        assert send_run_status_event.call_args_list[0].args == (
            monitor_run_task.outbound_channel,
            Status.FAILED,
            task_event_data,
        )
        # Message log event
        assert send_message_log_event.call_args_list[0].args == (
            monitor_run_task.outbound_channel,
            "ERROR",
            failed_refresh_error_code["errorCode"],
            task_event_data,
        )
        # Close run
        assert send_run_status_event.call_args_list[1].args == (
            monitor_run_task.outbound_channel,
            Status.FAILED,
            pipeline_event_data,
        )
        assert monitor_run_task.finished_refreshes == [refresh_data["requestId"]]
        assert monitor_run_task.is_done is False
