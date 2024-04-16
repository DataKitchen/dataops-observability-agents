import logging
from typing import final

from trio import MemorySendChannel, TooSlowError
from trio_websocket import WebSocketConnection

from framework.core.channels import NullSendChannel
from framework.core.handles import Handle
from framework.core.tasks import Task
from framework.timing import timeout_scope_log
from registry import ConfigurationRegistry
from toolkit.more_typing import JSON_DICT

from .configuration import ExampleConfiguration
from .handles import WebsocketHandle

LOGGER = logging.getLogger(__name__)


@final
class WebsocketTask(Task):
    def __init__(self, outbound_channel: MemorySendChannel | NullSendChannel):
        super().__init__(outbound_channel=outbound_channel)
        example_configuration: ExampleConfiguration = ConfigurationRegistry().lookup("example", ExampleConfiguration)
        self.timeout = example_configuration.timeout

    async def execute(self, client: WebSocketConnection) -> None:
        try:
            handle: Handle = WebsocketHandle(client)
            async with timeout_scope_log(self.timeout, f"{WebsocketTask.__name__}.pre_hook"):
                await handle.pre_hook()
            async with timeout_scope_log(self.timeout, f"{WebsocketTask.__name__}.handle"):
                value = await handle.handle()
            async with timeout_scope_log(self.timeout, f"{WebsocketTask.__name__}.post_hook"):
                payload: JSON_DICT = await handle.post_hook(value)
            await self.send(payload)
        except TooSlowError:
            # already logged
            return
