import datetime
from unittest.mock import AsyncMock, Mock, PropertyMock, patch

import pytest
from trio import current_time

from framework.core.loops.periodic_loop import PeriodicLoop, _periodic_loop
from framework.core.tasks import PeriodicTask

NOW = datetime.datetime(2021, 10, 27, 20, 50, 46, 221427, tzinfo=datetime.UTC)
"""
Timestamp for our datetime fixture
"""


@pytest.fixture()
def mock_datetime_now():
    with patch("framework.core.loops.periodic_loop.datetime") as m:
        now = datetime.datetime(2020, 3, 11, 14, 0, 0, tzinfo=datetime.UTC)
        m.now.return_value = now
        yield now


@pytest.mark.unit()
async def test_internal_periodic_loop(autojump_clock):
    period = 5.0
    loop = _periodic_loop(period)
    cur_time = current_time()

    result = [await anext(loop) for i in range(3)]
    assert result == [cur_time + (period * i) for i in range(3)]


@pytest.mark.unit()
async def test_periodic_class(autojump_clock, mock_datetime_now):
    mock = AsyncMock(auto_spec=PeriodicTask)
    type(mock).is_done = PropertyMock(side_effect=[True])
    await PeriodicLoop(task=mock, period=5).run()
    mock.__aenter__.assert_called_once()
    mock.__aexit__.assert_called_once()
    mock.execute_task.assert_called_with(mock_datetime_now, mock_datetime_now)
    # we can assume is_done is called because that's the only thing
    # that can break the loop


@pytest.mark.unit()
async def test_periodic_loop_update_time(autojump_clock, mock_datetime_now):
    mock = AsyncMock(auto_spec=PeriodicTask)
    mock.refresh_loop_period = Mock(return_value=10)
    type(mock).is_done = PropertyMock(side_effect=[False, True])
    loop = PeriodicLoop(task=mock, period=5)
    assert loop.period == 5
    await loop.run()
    mock.__aenter__.assert_called_once()
    mock.__aexit__.assert_called_once()
    mock.execute_task.assert_called_with(mock_datetime_now, mock_datetime_now)
    mock.refresh_loop_period.assert_called_once()
    assert loop.period == 10
