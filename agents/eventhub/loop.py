import asyncio
import logging
import queue
import threading

import trio
from azure.eventhub import EventData
from azure.eventhub.aio import EventHubConsumerClient, PartitionContext
from azure.eventhub.extensions.checkpointstoreblobaio import BlobCheckpointStore
from pydantic import ValidationError

from framework.core.loops import Loop
from framework.core.loops.loop import T_TASK
from registry import ConfigurationRegistry

from .configuration import EventhubBlobConfiguration, EventhubConfiguration

LOG = logging.getLogger(__name__)


class EventHubLoop(Loop):
    AZURE_CLIENT_MAX_WAIT_TIME: int = 5
    """Sets the maximum waiting time for an event to arrive. When reached, the callback will be called with None."""

    def __init__(self, task: T_TASK, queue_size: int = 10, queue_pop_sleep: int = 2, queue_put_sleep: int = 2) -> None:
        super().__init__(task)
        self.events_queue: queue.Queue[None | EventData] = queue.Queue(queue_size)
        self.queue_pop_sleep = queue_pop_sleep
        self.queue_put_sleep = queue_put_sleep
        self.client: EventHubConsumerClient

    async def run(self) -> None:
        aio_loop = asyncio.new_event_loop()
        aio_thread = threading.Thread(target=lambda: aio_loop.run_until_complete(self.run_in_asyncio()))
        aio_thread.start()

        LOG.debug("AsyncIO thread started.")
        try:
            async with self.task:
                while True:
                    try:
                        event = self.events_queue.get(False)
                    except queue.Empty:
                        await trio.sleep(self.queue_pop_sleep)
                        continue
                    try:
                        await self.task.execute(event)
                    except Exception:
                        LOG.exception("Error executing %r", self.task)

        finally:
            LOG.debug("Cancelled. Terminating the AsyncIO thread.")
            # The client will effectively close when the on_event callback is done, which takes up to
            # AZURE_CLIENT_MAX_WAIT_TIME seconds plus the callback execution time to happen.
            aio_loop.create_task(self.client.close())
            aio_thread.join()
            LOG.debug("AsyncIO thread joined.")

    async def process_event(self, partition_context: PartitionContext, event: EventData | None) -> None:
        while True:
            try:
                self.events_queue.put_nowait(event)
            except queue.Full:
                await asyncio.sleep(self.queue_put_sleep)
            else:
                break
            finally:
                await partition_context.update_checkpoint(event)

    async def run_in_asyncio(self) -> None:
        registry = ConfigurationRegistry()
        eventhub_config = registry.lookup("eventhubs", EventhubConfiguration)

        try:
            registry.register("blob_storage", EventhubBlobConfiguration)
            blob_config: EventhubBlobConfiguration = registry.lookup("blob_storage", EventhubBlobConfiguration)
            # Mypy complains about
            # Invalid self argument "type[BlobCheckpointStore]" to attribute function
            #   "from_connection_string" with type "Callable[[str, str, Any,
            #   DefaultNamedArg(Any | None, 'credential'), KwArg(Any)], BlobCheckpointStore]".
            blob_store = BlobCheckpointStore.from_connection_string(eventhub_config.connection_string, blob_config.name)  # type: ignore[misc]
        except ValidationError:
            # If none, no load balancing or checkpointing will be done.
            blob_store = None

        self.client = EventHubConsumerClient.from_connection_string(
            eventhub_config.connection_string,
            consumer_group=eventhub_config.consumer_group,
            eventhub_name=eventhub_config.name,
            checkpoint_store=blob_store,
        )

        async with self.client:
            LOG.debug("Starting the Azure Eventhub client.")

            await self.client.receive(
                starting_position=eventhub_config.starting_position,
                on_event=self.process_event,
                max_wait_time=self.AZURE_CLIENT_MAX_WAIT_TIME,
            )
