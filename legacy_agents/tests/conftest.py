import datetime
from unittest.mock import Mock

import attrs
import pytest
from requests import Session

from common.events_publisher import EventsPublisher
from common.message_event_log_level import MessageEventLogLevel


@pytest.fixture
def events_api_client():
    yield Mock()


@pytest.fixture
def events_publisher(events_api_client):
    # Mock events_api_client fails attrs validation, so disable it for object creation
    attrs.validators.set_disabled(True)
    ep = EventsPublisher(events_api_client=events_api_client, publish_events=True)
    attrs.validators.set_disabled(False)
    yield ep


@pytest.fixture
def message_log_event():
    event_info = {
        "event_timestamp": datetime.datetime.now(),
        "task_key": "task_key",
        "log_level": MessageEventLogLevel.ERROR,
        "message": "message",
        "pipeline_name": "pipeline_name",
        "task_name": "task_name",
        "metadata": None,
        "external_url": None,
    }
    yield event_info


@pytest.fixture
def metric_log_event():
    event_info = {
        "event_timestamp": datetime.datetime.now(),
        "task_key": "task_key",
        "metric_key": "fileSize",
        "metric_value": 100,
        "pipeline_name": "pipeline_name",
        "task_name": "task_name",
        "metadata": None,
        "external_url": None,
    }
    yield event_info
