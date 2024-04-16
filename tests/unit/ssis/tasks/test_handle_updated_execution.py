from unittest.mock import AsyncMock

import pytest

from agents.ssis.agent_state import ExecutionState, StateMonitoring
from agents.ssis.core import Execution, ExecutionStatus
from agents.ssis.tasks import SsisHandleUpdatedExecutionTask
from toolkit.observability import Status


@pytest.mark.unit()
@pytest.mark.parametrize(
    ("initial_status", "target_status", "expected_events", "stop_monitoring"),
    [
        (ExecutionStatus.NEW, ExecutionStatus.SUCCEEDED, (Status.RUNNING, Status.COMPLETED), True),
        (ExecutionStatus.RUNNING, ExecutionStatus.COMPLETED, (Status.COMPLETED_WITH_WARNINGS,), True),
        (ExecutionStatus.STOPPING, ExecutionStatus.ENDED_UNEXPECTEDLY, (Status.FAILED,), True),
        (ExecutionStatus.NEW, ExecutionStatus.RUNNING, (Status.RUNNING,), False),
    ],
)
async def test_handle_updated_execution(
    initial_status,
    target_status,
    expected_events,
    stop_monitoring,
    package_data,
    agent_state_mock,
    execution_start_time,
    execution_end_time,
):
    execution = Execution(
        **package_data,
        execution_id=42,
        status=target_status.value,
        start_time=execution_start_time,
        end_time=execution_end_time,
    )

    agent_state_mock.monitored_executions = {
        execution.execution_id: ExecutionState(execution_id=execution.execution_id, last_seen_status=initial_status),
    }

    channel_receive = AsyncMock()
    task = SsisHandleUpdatedExecutionTask(outbound_channel=channel_receive)

    async with task:
        await task.execute(execution)

    if stop_monitoring:
        agent_state_mock.stop_monitoring.assert_called_once_with(42, StateMonitoring.STATUS_CHANGE)
    else:
        agent_state_mock.stop_monitoring.assert_not_called()
    assert channel_receive.send.call_count == len(expected_events)
    for call, expected_status in zip(channel_receive.send.call_args_list, expected_events, strict=True):
        emitted_event = call[0][0]
        assert emitted_event["status"] == expected_status.name
        if expected_status is Status.RUNNING:
            assert emitted_event["event_timestamp"] == execution_start_time.isoformat()
        else:
            assert emitted_event["event_timestamp"] == execution_end_time.isoformat()
        assert emitted_event["pipeline_key"] == "folder_1/project_1/package_1"
        assert emitted_event["pipeline_name"] == "package_1"
        assert emitted_event["run_key"] == "folder_1/project_1/package_1:42"
        assert emitted_event["component_tool"] == "ssis"
        assert emitted_event["EVENT_TYPE"] == "run-status"
