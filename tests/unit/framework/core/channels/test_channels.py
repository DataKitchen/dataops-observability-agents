import pytest
from trio.testing import assert_checkpoints, assert_no_checkpoints

from framework.core.channels import NullSendChannel


@pytest.mark.unit()
async def test_null_channel_send():
    channel = NullSendChannel()
    with assert_checkpoints():
        await channel.send(10)


@pytest.mark.unit()
async def test_null_channel_send_nowait():
    channel = NullSendChannel()
    with assert_no_checkpoints():
        channel.send_nowait(10)


@pytest.mark.unit()
async def test_null_channel_send_context():
    channel = NullSendChannel()
    with assert_checkpoints():
        async with channel:
            pass
