import logging

from trio import open_memory_channel, open_nursery

from framework.configuration import CoreConfiguration
from framework.core.loops import ChannelReceiveLoop
from framework.observability import EventSenderTask, create_heartbeat_loop
from registry import ConfigurationRegistry
from toolkit.more_typing import JSON_DICT

from .configuration import ExampleConfiguration
from .loop import WebsocketLoop
from .task import WebsocketTask

LOGGER = logging.getLogger(__name__)


async def main() -> None:
    registry = ConfigurationRegistry()
    agent_config = registry.lookup("example", ExampleConfiguration)
    core_config = registry.lookup("core", CoreConfiguration)

    max_channel_capacity = int(core_config.max_channel_capacity)
    event_queue_send, event_queue_receive = open_memory_channel[JSON_DICT](max_channel_capacity)
    async with open_nursery() as n:
        n.start_soon(
            WebsocketLoop(
                connect_timeout=agent_config.timeout,
                target_url=agent_config.target_url,
                task=WebsocketTask(outbound_channel=event_queue_send),
            ).run,
        )
        n.start_soon(ChannelReceiveLoop(inbound_channel=event_queue_receive, task=EventSenderTask()).run)
        n.start_soon(create_heartbeat_loop(tool="example tool"), name="Heartbeat")
