from unittest.mock import AsyncMock

import pytest

from agents.ssis.agent_state import ExecutionState
from agents.ssis.core import ExecutableStatistic
from agents.ssis.tasks import SsisHandleNewExecutableStatisticsTask
from toolkit.observability import Status


@pytest.mark.unit()
@pytest.mark.parametrize(
    ("execution_path", "container_before", "container_after", "skipped", "expected_task_name"),
    [
        ("Package\\node", {"Package"}, {"Package"}, False, "node"),
        ("Package\\container\\node", {"Package"}, {"Package", "Package\\container"}, False, "node"),
        ("Package\\container", {"Package", "Package\\container"}, None, True, None),
    ],
)
async def test_handle_new_executable_statistics(
    execution_path,
    container_before,
    container_after,
    skipped,
    expected_task_name,
    package_data,
    agent_state_mock,
    execution_start_time,
    execution_end_time,
):
    exec_stat = ExecutableStatistic(
        **package_data,
        execution_id=42,
        statistics_id=43,
        execution_path=execution_path,
        execution_result=0,
        start_time=execution_start_time,
        end_time=execution_end_time,
    )

    execution_state = ExecutionState(
        execution_id=exec_stat.execution_id,
        container_executables=container_before.copy(),
    )
    agent_state_mock.monitored_executions = {execution_state.execution_id: execution_state}

    channel_receive = AsyncMock()
    task = SsisHandleNewExecutableStatisticsTask(outbound_channel=channel_receive)

    async with task:
        await task.execute(exec_stat)

    if skipped:
        channel_receive.send.assert_not_called()
        return

    assert execution_state.container_executables == container_after

    expected_events = (Status.RUNNING, Status.COMPLETED)
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
        assert emitted_event["task_key"] == "folder_1/project_1/package_1:43"
        assert emitted_event["task_name"] == expected_task_name
        assert emitted_event["component_tool"] == "ssis"
        assert emitted_event["EVENT_TYPE"] == "run-status"
