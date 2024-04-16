import datetime
from unittest.mock import AsyncMock

import pytest

from agents.ssis.agent_state import ExecutionState
from agents.ssis.core import ExecutableStatistic, ExecutableStatisticResult
from agents.ssis.tasks import SsisFindAddedExecutableStatisticsTask
from tests.utils import iterate_async


@pytest.mark.unit()
@pytest.mark.parametrize(("monitored_count", "added_count", "db_access_count"), [(99, 50, 1), (120, 110, 2)])
async def test_find_added_executable_statistics(
    monitored_count,
    added_count,
    db_access_count,
    package_data,
    execution_start_time,
    agent_state_mock,
    async_conn_mock,
):
    agent_state_mock.get_monitored_executions.return_value = (
        ExecutionState(execution_id=i, last_seen_statistic_id=i * 10) for i in range(monitored_count)
    )

    added_executable_statistic = [
        ExecutableStatistic(
            **package_data,
            execution_id=i,
            statistics_id=1000 + i,
            execution_path="Node1",
            execution_result=ExecutableStatisticResult.COMPLETED.value,
            start_time=execution_start_time + datetime.timedelta(minutes=i * 10),
            end_time=execution_start_time + datetime.timedelta(minutes=(i + 1) * 10, seconds=-1),
        )
        for i in range(added_count)
    ]

    batch_size = SsisFindAddedExecutableStatisticsTask.QUERY_BATCH_SIZE
    async_conn_mock.exec_and_fetch_all.side_effect = [
        iterate_async(*added_executable_statistic[idx * batch_size : (idx + 1) * batch_size])
        for idx in range(db_access_count)
    ]

    channel_receiver = AsyncMock()
    task = SsisFindAddedExecutableStatisticsTask(async_conn_mock, channel_receiver)

    async with task:
        await task.execute(None, None)

    assert async_conn_mock.exec_and_fetch_all.call_count == db_access_count
    assert channel_receiver.send.call_count == added_count
    assert [call[0][0] for call in channel_receiver.send.call_args_list] == added_executable_statistic
