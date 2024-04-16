import logging
from collections.abc import AsyncGenerator
from contextlib import suppress
from pprint import pformat
from typing import Generic

from trio import EndOfChannel, MemoryReceiveChannel

from framework.core.tasks.channel_task import ChannelTask
from toolkit.more_typing import T_RECEIVABLE

from .loop import Loop

LOGGER = logging.getLogger(__name__)


async def _channel_receive_loop(channel: MemoryReceiveChannel[T_RECEIVABLE]) -> AsyncGenerator[T_RECEIVABLE, None]:
    try:
        while True:
            result: T_RECEIVABLE = await channel.receive()
            yield result
    except EndOfChannel:
        LOGGER.warning("Under production, channel_receive_loop is not supposed to terminate.")


class ChannelReceiveLoop(Loop[ChannelTask], Generic[T_RECEIVABLE]):
    """
    A loop which drains a memory channel (infinitely receives). This run blocks on the inbound_channel.
    """

    def __init__(self, inbound_channel: MemoryReceiveChannel[T_RECEIVABLE], task: ChannelTask) -> None:
        super().__init__(task=task)
        self.inbound_channel = inbound_channel

    async def run(self) -> None:
        with suppress(EndOfChannel):
            async with self.task, self.inbound_channel:
                async for receivable in _channel_receive_loop(self.inbound_channel):
                    LOGGER.debug("Executing Task. Received: %s", pformat(receivable))
                    await self.task.execute_task(receivable)
        LOGGER.warning("Receivable channel was closed.")
