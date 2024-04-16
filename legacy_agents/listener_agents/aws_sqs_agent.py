import json
import logging
import os
import sys
import traceback
from pathlib import Path
from typing import List

import boto3
from events_ingestion_client import ApiClient, Configuration, EventsApi

from common.events_publisher import EventsPublisher
from common.plugin_utils import fetch_plugins
from listener_agents.abstract_event_handler import AbstractEventHandler

# Need to be set as env variables
EVENTS_API_HOST = os.getenv("EVENTS_API_HOST", "")
EVENTS_API_KEY = os.getenv("EVENTS_API_KEY", "")
ACCESS_KEY = os.getenv("ACCESS_KEY", "")
SECRET_KEY = os.getenv("SECRET_KEY", "")
SESSION_TOKEN = os.getenv("SESSION_TOKEN", "")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", "")
SQS_QUEUE_NAME = os.getenv("SQS_QUEUE_NAME", "")

NATIVE_PLUGINS_PATH: Path = Path(__file__).parent / "plugins"
PLUGINS_PATHS: List[str] = [str(NATIVE_PLUGINS_PATH)]
EXTERNAL_PLUGINS_PATH: str = os.getenv("EXTERNAL_PLUGINS_PATH", "")
if EXTERNAL_PLUGINS_PATH:
    PLUGINS_PATHS.append(EXTERNAL_PLUGINS_PATH)
EVENT_HANDLERS: List[AbstractEventHandler] = []
PUBLISH_EVENTS = os.getenv("PUBLISH_EVENTS", "True").lower() in ["true", "1"]

logger = logging.getLogger()
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(logging.INFO)


def main() -> None:
    """
    This method runs the listening coroutine in an asyncio event loop. In addition, it configures
    and instantiates a client for connecting and sending events to the `Events Ingestion API
    <https://api.docs.datakitchen.io/production/events.html>`_ and retrieves and registers plugins.

    Returns
    -------
    None
    """
    global EVENT_HANDLERS

    try:
        # Configure API key authorization: SAKey
        configuration = Configuration()
        configuration.api_key["ServiceAccountAuthenticationKey"] = EVENTS_API_KEY
        configuration.host = EVENTS_API_HOST
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
        logger.info(f"EVENT_HANDLERS: {EVENT_HANDLERS}")

        if len(EVENT_HANDLERS) > 0:
            # Connect to AWS SQS
            try:
                sqs = boto3.resource(
                    "sqs",
                    region_name=AWS_DEFAULT_REGION,
                    aws_access_key_id=ACCESS_KEY,
                    aws_secret_access_key=SECRET_KEY,
                    aws_session_token=SESSION_TOKEN,
                )
            except Exception:
                logger.error(f"Error connecting to SQS: {traceback.format_exc()}")

            # Get SQS Queue
            try:
                queue = sqs.get_queue_by_name(QueueName=SQS_QUEUE_NAME)
                while True:
                    messages = queue.receive_messages(WaitTimeSeconds=5)
                    # Long poll message from SQS and delete after processing
                    for message in messages:
                        print("Message received: {0}".format(message.body))
                        message_body = json.loads(message.body)
                        if "Records" in message_body:
                            record = message_body["Records"][0]
                            for event_handler in EVENT_HANDLERS:
                                # Workaround for calling a non-async function inside an async function
                                # Passing event_handler.handle_event_record directly fails - lambda is a workaround
                                try:
                                    success = event_handler.handle_event_record(record)
                                    if success:
                                        message.delete()
                                        # Process next record once an event handler successfully handles the event
                                        continue
                                    else:
                                        print("No event handled!")
                                except Exception:
                                    logger.error(f"Error handling event: {traceback.format_exc()}")
            except Exception:
                logger.error(f"Error getting Queue by name : {traceback.format_exc()}")
        else:
            print("No plugin enabled!")

    except KeyboardInterrupt:
        logger.info("\nStopped receiving")


if __name__ == "__main__":
    main()
