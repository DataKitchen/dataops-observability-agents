from unittest.mock import AsyncMock

import pytest
import trio
from trio.testing import assert_checkpoints

from agents.powerbi.tasks import PowerBIFetchDatasetsTask, PowerBIMonitorRunTask
from framework.core.loops import PeriodicLoop


@pytest.mark.unit()
async def test_main_loop(powerbi_configuration, autojump_clock) -> None:
    """
    This test mocks the agent runs for "22 seconds".
    Set up:
    - Fetch runs, i.e. groups and datasets, every 10s. When it runs for the first time, it will start a monitor run task.
    - Monitor runs, i.e. dataset refresh history, runs every 5s.
    Expectations:
    - Fetch runs task will execute 2 times @0s, 10s and 20s.
    - Monitor runs task will execute 2 times @0s, 10s, 15s and 20s.
    """
    powerbi_configuration.datasets_fetching_period = 10.0
    powerbi_configuration.period = 5.0
    mock_fetch_task = AsyncMock(spec=PowerBIFetchDatasetsTask)
    mock_fetch_task.is_done = False
    mock_fetch_task.refresh_loop_period.return_value = None
    mock_monitor_task = AsyncMock(spec=PowerBIMonitorRunTask)
    mock_monitor_task.is_done = False
    mock_monitor_task.refresh_loop_period.return_value = None
    time_out = powerbi_configuration.datasets_fetching_period * 2 + 2

    with trio.move_on_after(time_out):  # noqa: TRIO100
        with assert_checkpoints():
            async with trio.open_nursery() as n:

                def spawn_monitor_task(*_args, **_kwargs):
                    if mock_fetch_task.execute_task.call_count == 1:
                        n.start_soon(
                            PeriodicLoop(
                                period=powerbi_configuration.period,
                                task=mock_monitor_task,
                            ).run,
                        )

                mock_fetch_task.execute_task.side_effect = spawn_monitor_task
                n.start_soon(
                    PeriodicLoop(
                        period=powerbi_configuration.datasets_fetching_period,
                        task=mock_fetch_task,
                    ).run,
                )
    assert trio.current_time() == time_out
    # TODO: extend tests to check expected groups, datasets, and refresh values
    assert len(mock_fetch_task.execute_task.call_args_list) == 3
    assert len(mock_monitor_task.execute_task.call_args_list) == 5
