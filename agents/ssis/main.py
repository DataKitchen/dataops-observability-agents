from trio import open_memory_channel, open_nursery

from framework.configuration import CoreConfiguration
from framework.core.loops import ChannelReceiveLoop, PeriodicLoop
from framework.observability import EventSenderTask, create_heartbeat_loop
from registry import ConfigurationRegistry
from toolkit.more_typing import JSON_DICT

from .config import SsisConfiguration
from .core import COMPONENT_TOOL, ExecutableStatistic, Execution
from .database import AsyncConn
from .tasks import (
    SsisFetchNewExecutionsTask,
    SsisFindAddedExecutableStatisticsTask,
    SsisFindUpdatedExecutionsTask,
    SsisHandleNewExecutableStatisticsTask,
    SsisHandleUpdatedExecutionTask,
)


async def main() -> None:
    registry = ConfigurationRegistry()
    registry.register("ssis", SsisConfiguration)
    agent_config = registry.lookup("ssis", SsisConfiguration)
    core_config = registry.lookup("core", CoreConfiguration)

    max_channel_capacity = int(core_config.max_channel_capacity)
    event_queue_send, event_queue_receive = open_memory_channel[JSON_DICT](max_channel_capacity)
    exec_update_queue_send, exec_update_queue_receive = open_memory_channel[Execution](max_channel_capacity)
    stat_update_queue_send, stat_update_queue_receive = open_memory_channel[ExecutableStatistic](max_channel_capacity)

    db_conn = AsyncConn(agent_config)
    async with open_nursery() as n:
        n.start_soon(
            PeriodicLoop(
                period=agent_config.polling_interval,
                task=SsisFetchNewExecutionsTask(db_conn=db_conn),
            ).run,
        )
        n.start_soon(
            PeriodicLoop(
                period=agent_config.polling_interval,
                task=SsisFindUpdatedExecutionsTask(db_conn=db_conn, outbound_channel=exec_update_queue_send),
            ).run,
        )
        n.start_soon(
            ChannelReceiveLoop(
                inbound_channel=exec_update_queue_receive,
                task=SsisHandleUpdatedExecutionTask(outbound_channel=event_queue_send),
            ).run,
        )
        n.start_soon(
            PeriodicLoop(
                period=agent_config.polling_interval,
                task=SsisFindAddedExecutableStatisticsTask(db_conn=db_conn, outbound_channel=stat_update_queue_send),
            ).run,
        )
        n.start_soon(
            ChannelReceiveLoop(
                inbound_channel=stat_update_queue_receive,
                task=SsisHandleNewExecutableStatisticsTask(outbound_channel=event_queue_send),
            ).run,
        )
        n.start_soon(ChannelReceiveLoop(inbound_channel=event_queue_receive, task=EventSenderTask()).run)
        n.start_soon(create_heartbeat_loop(tool=COMPONENT_TOOL), name="Heartbeat")
