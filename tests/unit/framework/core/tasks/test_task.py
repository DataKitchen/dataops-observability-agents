from unittest.mock import AsyncMock

import pytest
import trio

from framework.core.channels import NullSendChannel
from framework.core.tasks import Task


class TestTask(Task):
    async def execute(self, *args, **kwargs):
        raise RuntimeError()


@pytest.mark.unit()
def test_task_inits_with_null(core_config):
    task = TestTask(outbound_channel=None)
    assert isinstance(task.outbound_channel, NullSendChannel)

    s, r = trio.open_memory_channel[str](100)
    with s, r:
        task = TestTask(outbound_channel=s)
        assert isinstance(task.outbound_channel, trio.MemorySendChannel)


@pytest.mark.unit()
async def test_task_context(core_config):
    channel = AsyncMock()
    task = TestTask(outbound_channel=channel)
    assert not task._channel_open
    async with task:
        channel.__aenter__.assert_called_once()
        channel.__aexit__.assert_not_called()
        assert task._channel_open
    assert not task._channel_open
    channel.__aexit__.assert_called_once()


@pytest.mark.unit()
async def test_task_send(core_config):
    channel = AsyncMock()
    payload = {"hello": "world"}
    task = TestTask(outbound_channel=channel)
    # task unopened
    with pytest.raises(RuntimeError):
        await task.send(payload)
    channel.send.assert_not_called()
    async with task:
        await task.send(payload)
    channel.send.assert_called_once_with(payload)


@pytest.mark.unit()
async def test_task_execute_task(core_config):
    task = TestTask()
    task.execute = AsyncMock()

    await task.execute_task()
    task.execute.assert_called_once()
