import ast
import asyncio
import logging
import os
import sys
import traceback
from pathlib import Path
from typing import Any, List, Optional

from azure.eventhub.aio import EventHubConsumerClient
from azure.eventhub.extensions.checkpointstoreblobaio import BlobCheckpointStore
from events_ingestion_client import ApiClient, Configuration, EventsApi

from common.events_publisher import EventsPublisher
from common.plugin_utils import fetch_plugins
from listener_agents.abstract_event_handler import AbstractEventHandler

EVENT_HUB_CONN_STR = os.getenv("EVENT_HUB_CONN_STR", "")
EVENT_HUB_NAME = os.getenv("EVENT_HUB_NAME", "")
AZURE_STORAGE_CONN_STR = os.getenv("AZURE_STORAGE_CONN_STR", "")
BLOB_CONTAINER_NAME = os.getenv("BLOB_CONTAINER_NAME", "")

NATIVE_PLUGINS_PATH: Path = Path(__file__).parent / "plugins"
PLUGINS_PATHS: List[str] = [str(NATIVE_PLUGINS_PATH)]
EXTERNAL_PLUGINS_PATH: str = os.getenv("EXTERNAL_PLUGINS_PATH", "")
if EXTERNAL_PLUGINS_PATH:
    PLUGINS_PATHS.append(EXTERNAL_PLUGINS_PATH)
EVENT_HANDLERS: List[AbstractEventHandler] = []
PUBLISH_EVENTS = os.getenv("PUBLISH_EVENTS", "true").lower() in ["true", "1"]

logger = logging.getLogger()
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(logging.INFO)


def main() -> None:
    """
    This method runs the listening coroutine in an asyncio event loop. In addition, it configures
    and instantiates a client for connecting and sending events to the `Events Ingestion API
    <https://api.docs.datakitchen.io/production/events.html>`_ and retrieves and registers plugins.

    This module is based on an `example receiver python script
    <https://github.com/Azure/azure-sdk-for-python/blob/main/sdk/eventhub/azure-eventhub/samples/async_samples/recv_with_checkpoint_store_async.py>`_
    that receives events and does checkpointing using a blob checkpoint store. This example and
    others are found in the `azure-sdk-for-python repository
    <https://github.com/Azure/azure-sdk-for-python>`_

    Returns
    -------
    None
    """
    global EVENT_HANDLERS

    try:
        # Configure API key authorization: SAKey
        configuration = Configuration()
        configuration.api_key["ServiceAccountAuthenticationKey"] = os.getenv("EVENTS_API_KEY")
        configuration.host = os.getenv("EVENTS_API_HOST")
        configuration.verify_ssl = {"true": True, "false": False}[
            os.getenv("DK_EVENTS_VERIFY_SSL", "true").lower()
        ]
        events_api_client = EventsApi(ApiClient(configuration))
        events_publisher = EventsPublisher(
            events_api_client=events_api_client, publish_events=PUBLISH_EVENTS
        )

        logger.info(f"Plugins search paths: {PLUGINS_PATHS}")
        fetch_plugins(AbstractEventHandler, PLUGINS_PATHS)

        EVENT_HANDLERS = [
            event_handler.create_event_handler(events_publisher)
            for event_handler in AbstractEventHandler.plugins
        ]
        logger.info(f"Event Handlers: {EVENT_HANDLERS}")
        asyncio.run(listen())
    except KeyboardInterrupt:
        logger.info("\nStopped receiving")


async def listen() -> None:
    """
    Create an EventHubConsumerClient instance with a CheckpointStore.The client will load-balance
    partition assignment with other EventHubConsumerClient instances which also try to receive
    events from all partitions and use the same storage resource.

    Returns
    -------
    None
    """
    checkpoint_store: Optional[BlobCheckpointStore] = None
    if BLOB_CONTAINER_NAME:
        checkpoint_store = BlobCheckpointStore.from_connection_string(  # type: ignore
            AZURE_STORAGE_CONN_STR, BLOB_CONTAINER_NAME
        )
    client: EventHubConsumerClient = EventHubConsumerClient.from_connection_string(
        EVENT_HUB_CONN_STR,
        consumer_group="$Default",
        eventhub_name=EVENT_HUB_NAME,
        # For load-balancing and checkpoint. Leave None for no load-balancing.
        checkpoint_store=checkpoint_store,
    )
    async with client:
        await receive(client)


async def receive(client: EventHubConsumerClient) -> None:
    """Receive Event Hubs events"""
    await client.receive(
        on_event=on_event,
        starting_position="-1",  # "-1" is from the beginning of the partition.
    )


async def on_event(partition_context: Any, event: Any) -> None:
    # Try catch block to parse the Azure event correctly
    try:
        event_data = event.body_as_json(encoding="UTF-8")
    except:
        event_data = event.body_as_str(encoding="UTF-8")
        event_data = ast.literal_eval(event_data)

    loop = asyncio.get_event_loop()
    for record in event_data["records"]:
        for event_handler in EVENT_HANDLERS:
            # Workaround for calling a non-async function inside an async function
            # Passing event_handler.handle_event_record directly fails - lambda is a workaround
            try:
                success = await loop.run_in_executor(
                    None, lambda r: event_handler.handle_event_record(r), record
                )
                if success:
                    print(f"SUCCESS: {record['category']}")
                    # Process next record once an event handler successfully handles the event
                    continue
            except Exception:
                logger.error(f"Error handling event: {traceback.format_exc()}")

    await partition_context.update_checkpoint(event)


if __name__ == "__main__":
    main()
