from unittest.mock import AsyncMock

import pytest
from trio import ClosedResourceError, EndOfChannel, open_memory_channel

from framework.core.loops.channel_receive_loop import ChannelReceiveLoop, _channel_receive_loop


@pytest.mark.unit()
async def test_internal_receive_loop(autojump_clock):
    s, r = open_memory_channel[str](100)
    async with s, r:
        loop = _channel_receive_loop(r)
        contents = ["foo", "bar", "baz"]
        for c in contents:
            await s.send(c)

        read = [await anext(loop) for _ in range(len(contents))]
        assert read == contents
    with pytest.raises(ClosedResourceError):
        await anext(loop)


@pytest.mark.unit()
async def test_receive_loop_class(autojump_clock):
    task = AsyncMock()
    channel_receive = AsyncMock()
    channel_receive.receive.side_effect = ["Value", EndOfChannel]

    await ChannelReceiveLoop(channel_receive, task=task).run()

    assert channel_receive.receive.call_count == 2
    assert task.__aenter__.called
    task.execute_task.assert_called_with("Value")
