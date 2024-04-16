import logging
import os
from datetime import datetime
from typing import Optional, Tuple

from attrs import define
from dateutil import parser

from common.events_publisher import EventsPublisher
from common.status import Status
from listener_agents.abstract_event_handler import AbstractEventHandler

logger = logging.getLogger(__name__)

VALID_CATEGORIES = ["aws:s3/ObjectCreated:Put"]


@define(kw_only=True, slots=False)
class AWSS3EventHandler(AbstractEventHandler):
    @classmethod
    def create_event_handler(cls, events_publisher: EventsPublisher) -> AbstractEventHandler:
        return AWSS3EventHandler(events_publisher=events_publisher)

    @property
    def name(self) -> str:
        return self.__class__.__name__

    _component_tool = "sqs"

    @property
    def component_tool(self) -> str:
        return self._component_tool

    def _get_category(self, event_record: dict) -> Optional[str]:
        if "eventName" in event_record and "eventSource" in event_record:
            category = f'{str(event_record["eventSource"])}/{str(event_record["eventName"])}'
            if category in VALID_CATEGORIES:
                return category
            else:
                logger.debug(f"Invalid category found by {self.name}: {category}")
                logger.debug(f"Category not in event_record")
                return None
        else:
            return None

    @staticmethod
    def _get_timestamp(event_record: dict) -> Tuple[Status, Optional[datetime]]:
        status = Status.COMPLETED
        return Status.COMPLETED, parser.parse(event_record["eventTime"])

    def handle_event_record(self, event_record: dict) -> bool:
        """
        Return False if the event is NOT handled by this handler. This indicates that other plugins
        should attempt to handle this event. Otherwise, handle the event and return True to indicate
        that the event needs no further processing by other plugins.
        """
        category = self._get_category(event_record)

        if category is None:
            return False

        logger.info(f"Processing Event from AWS S3 - Log Event Record: {event_record}")
        event_timestamp = parser.parse(event_record["eventTime"])
        object_key = event_record["s3"]["object"]["key"]
        bucket = event_record["s3"]["bucket"]["name"]
        file_name = os.path.basename(object_key)
        file_dir = os.path.dirname(object_key)
        file_path_prefix = f"s3://{bucket}"
        object_path = os.path.join(file_path_prefix, object_key)
        if (drop_dataset_name_date_suffix := os.getenv("DROP_DATASET_NAME_DATE_SUFFIX")) == "True":
            file_name_no_date = file_name.rsplit("_", 1)[0]
            dataset_key = os.path.join(file_path_prefix, file_dir, file_name_no_date)
            logger.info(f"Dropping dataset name date suffix : {dataset_key}")
        else:
            dataset_key = object_path

        dataset_name = dataset_key
        operation = "WRITE"
        path = object_path
        metadata = event_record
        external_url = f"https://{bucket}.s3.amazonaws.com/{object_key}"
        self.publish_dataset_event(
            event_timestamp=event_timestamp,
            dataset_key=dataset_key,
            dataset_name=dataset_name,
            operation=operation,
            path=path,
            metadata=metadata,
            external_url=external_url,
            component_tool=self.component_tool,
        )
        return True
