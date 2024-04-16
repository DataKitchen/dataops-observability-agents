import datetime
from unittest.mock import AsyncMock

import pytest

from agents.ssis.agent_state import ExecutionState
from agents.ssis.core import Execution, ExecutionStatus
from agents.ssis.tasks import SsisFindUpdatedExecutionsTask
from tests.utils import iterate_async


@pytest.mark.unit()
async def test_find_updated_executions(package_data, execution_start_time, agent_state_mock, async_conn_mock):
    agent_state_mock.get_monitored_executions.return_value = (
        ExecutionState(execution_id=1, last_seen_status=ExecutionStatus.RUNNING),
        ExecutionState(execution_id=2, last_seen_status=ExecutionStatus.NEW),
        ExecutionState(execution_id=3, last_seen_status=ExecutionStatus.RUNNING),
    )

    updated_exec_1 = Execution(
        **package_data,
        execution_id=2,
        status=ExecutionStatus.CANCELED.value,
        start_time=execution_start_time + datetime.timedelta(minutes=1),
        end_time=execution_start_time + datetime.timedelta(minutes=2),
    )

    updated_exec_2 = Execution(
        **package_data,
        execution_id=3,
        status=ExecutionStatus.SUCCEEDED.value,
        start_time=execution_start_time,
        end_time=execution_start_time + datetime.timedelta(minutes=2),
    )

    async_conn_mock.exec_and_fetch_all.side_effect = (
        iterate_async(updated_exec_1),
        iterate_async(updated_exec_2),
    )

    channel_receiver = AsyncMock()
    task = SsisFindUpdatedExecutionsTask(async_conn_mock, channel_receiver)

    async with task:
        await task.execute(None, None)

    assert channel_receiver.send.call_args_list[0][0][0] == updated_exec_1
    assert channel_receiver.send.call_args_list[1][0][0] == updated_exec_2
    assert channel_receiver.send.call_count == 2

    db_calls = async_conn_mock.exec_and_fetch_all.call_args_list
    assert "IN (2)" in db_calls[0][0][0]
    assert db_calls[0][0][1][0] == ExecutionStatus.NEW.value
    assert "IN (1, 3)" in db_calls[1][0][0]
    assert db_calls[1][0][1][0] == ExecutionStatus.RUNNING.value
    assert async_conn_mock.exec_and_fetch_all.call_count == 2
