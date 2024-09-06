from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from attrs import define, field, validators
from events_ingestion_client import (
    DatasetOperationApiSchema,
    EventsApi,
    MessageLogEventApiSchema,
    MetricLogApiSchema,
    RunStatusApiSchema,
    TestOutcomesApiSchema,
)
from events_ingestion_client.rest import ApiException

from .message_event_log_level import MessageEventLogLevel
from .status import Status

logger = logging.getLogger(__name__)


@define(kw_only=True)
class EventsPublisher:
    """
    All events published to the `Events Ingestion API
    <https://api.docs.datakitchen.io/production/events.html>`_ should be sent via an instance of
    this class. This class leverages the `events-ingestion-client PyPI package
    <https://pypi.org/project/events-ingestion-client/>`_. The source code of this package is
    publicly available and documented in the `DKEventsIngestionClient repository
    <https://gitlab.com/dkinternal/DKEventsIngestionClient>`_.
    """

    events_api_client: EventsApi = field(validator=validators.instance_of(EventsApi))
    """
    Instance of the `python client
    <https://gitlab.com/dkinternal/DKEventsIngestionClient/-/blob/master/events_ingestion_client/api/events_api.py>`_
    for making API requests to the `Events Ingestion API
    <https://api.docs.datakitchen.io/production/events.html>`_.
    """
    publish_events: bool = field(default=True, validator=validators.instance_of(bool))
    """
    If True, publish events to the Events Ingestion API. Setting to False is mainly for testing
    purposes (default is True).
    """

    def publish_run_status_event(
        self,
        event_timestamp: datetime,
        pipeline_key: str,
        run_key: str,
        task_key: str | None,
        status: Status,
        pipeline_name: str | None = None,
        task_name: str | None = None,
        metadata: Any = None,
        external_url: str | None = None,
        component_tool: str | None = None,
    ) -> None:
        """
        Call this method when the status of a task changes. In this context, a task is a single node
        within a pipeline. For instance, the basic unit of execution in Azure Data Factory (ADF) is
        an activity and activities are arranged into pipelines. Therefore, this method should be
        called at least twice for every activity in an ADF pipeline - once when the activity starts
        and again when the activity finishes (either successfully or not).

        Parameters
        ----------
        event_timestamp: datetime
            A timezone aware timestamp indicating when the status of the task changed.
        pipeline_key: str
            Pipeline key that will be passed to the Events Ingestion API endpoints.
        run_key: str
            Pipeline run key passed to the Events Ingestion API endpoints.
        task_key: str
            The identification key for the run task. Typically, this is the name of a node within a pipeline DAG that the
            task status event is associated with.
        status: Status
            New status of the associated task.
        pipeline_name: str
            Human readable display value for a pipeline.
        task_name: str
            Human readable display value for a task.
        metadata: object
            Arbitrary key-value information, supplied by the user, to apply to the event.
        external_url: str
            A link to source information.
        component_tool: str
            A certain tool type

        Returns
        -------
        None
        """
        if not self.publish_events:
            logger.debug(
                f"publish_run_status_event() called with task name '{task_key}', status '{status.name}', and timestamp '{event_timestamp.astimezone().isoformat()}'"
            )
            return
        try:
            event_info = {
                "event_timestamp": event_timestamp.astimezone().isoformat(),
                "pipeline_key": pipeline_key,
                "run_key": run_key,
                "task_key": task_key,
                "status": status.name,
                "pipeline_name": pipeline_name,
                "task_name": task_name,
                "metadata": metadata,
                "external_url": external_url,
                "component_tool": component_tool,
            }
            logger.debug(f"Publishing task event: {event_info}")
            self.events_api_client.post_run_status(RunStatusApiSchema(**event_info), event_source="API")

        except ApiException:
            logger.exception("Exception when calling EventsApi->post_run_status\n")
            raise

    def publish_metric_log_event(
        self,
        event_timestamp: datetime,
        pipeline_key: str,
        run_key: str,
        task_key: str | None,
        metric_key: str,
        metric_value: float,
        pipeline_name: str | None = None,
        task_name: str | None = None,
        metadata: Any = None,
        external_url: str | None = None,
        component_tool: str | None = None,
    ) -> None:
        """
        Call this method to log any desired metrics of a task. For example, a metric could be file size,
        byte read, byte written, etc.

        Parameters
        ----------
        event_timestamp: datetime
            A timezone aware timestamp indicating when the status of the task changed.
        pipeline_key: str
            Pipeline key that will be passed to the Events Ingestion API endpoints.
        run_key: str
            Pipeline run key passed to the Events Ingestion API endpoints.
        task_key: str
            The identification key for the run task. Typically, this is the name of a node within a pipeline DAG that the
            task status event is associated with.
        metric_key: str
            Metric name to be posted.
        metric_value: float
            Metric value to be posted.
        pipeline_name: str
            Human readable display value for a pipeline.
        task_name: str
            Human readable display value for a task.
        metadata: object
            Arbitrary key-value information, supplied by the user, to apply to the event.
        external_url: str
            A link to source information.

        Returns
        -------
        None
        """
        if not self.publish_events:
            logger.debug(
                f"publish_metric_log_event() called with metric name '{metric_key}', metric value '{metric_value}', and timestamp '{event_timestamp.astimezone().isoformat()}'"
            )
            return
        try:
            event_info = {
                "event_timestamp": event_timestamp.astimezone().isoformat(),
                "pipeline_key": pipeline_key,
                "run_key": run_key,
                "task_key": task_key,
                "metric_key": metric_key,
                "metric_value": metric_value,
                "pipeline_name": pipeline_name,
                "task_name": task_name,
                "metadata": metadata,
                "external_url": external_url,
                "component_tool": component_tool,
            }
            logger.debug(f"Publishing metric log event: {event_info}")
            self.events_api_client.post_metric_log(MetricLogApiSchema(**event_info), event_source="API")

        except ApiException:
            logger.exception("Exception when calling EventsApi->post_metric_log\n")
            raise

    def publish_message_log_event(
        self,
        event_timestamp: datetime,
        pipeline_key: str,
        run_key: str,
        task_key: str | None,
        log_level: MessageEventLogLevel,
        message: str,
        pipeline_name: str | None = None,
        task_name: str | None = None,
        metadata: Any = None,
        external_url: str | None = None,
        component_tool: str | None = None,
    ) -> None:
        """
        Call this method to log any information or message.

        Parameters
        ----------
        event_timestamp: datetime
            A timezone aware timestamp indicating when the status of the task changed.
        pipeline_key: str
            Pipeline key that will be passed to the Events Ingestion API endpoints.
        run_key: str
            Pipeline run key passed to the Events Ingestion API endpoints.
        task_key: str
            The identification key for the run task. Typically, this is the name of a node within a pipeline DAG that the
            task status event is associated with.
        log_level: str
            Log level for the associated message.
        message: str
            Message to be logged
        pipeline_name: str
            Human readable display value for a pipeline.
        task_name: str
            Human readable display value for a task.
        metadata: object
            Arbitrary key-value information, supplied by the user, to apply to the event.
        external_url: str
            A link to source information.

        Returns
        -------
        None
        """
        if not self.publish_events:
            logger.debug(
                f"publish_message_log_event() called with log level '{log_level.value}' and message '{message}'"
            )
            return
        try:
            event_info = {
                "event_timestamp": event_timestamp.astimezone().isoformat(),
                "pipeline_key": pipeline_key,
                "run_key": run_key,
                "task_key": task_key,
                "log_level": log_level.value,
                "message": message,
                "pipeline_name": pipeline_name,
                "task_name": task_name,
                "metadata": metadata,
                "external_url": external_url,
                "component_tool": component_tool,
            }
            logger.debug(f"Publishing message log event: {event_info}")
            self.events_api_client.post_message_log(MessageLogEventApiSchema(**event_info), event_source="API")

        except ApiException:
            logger.exception("Exception when calling EventsApi->post_message_log\n")
            raise

    def publish_test_outcomes_event_dataset(
        self,
        event_timestamp: datetime,
        dataset_key: str,
        test_outcomes: list[dict],
        dataset_name: str | None = None,
        metadata: Any = None,
        external_url: str | None = None,
        component_tool: str | None = None,
    ) -> None:
        """
        Call this method to publish test outcome information for Dataset Components. There is a separate method for
        Pipeline components.

        Parameters
        ----------
        event_timestamp: datetime
            A timezone aware timestamp indicating when the status of the task changed.
        dataset_key: str
            The key identifier of the target dataset component for the event action.
        test_outcomes: list
            A list of dictionaries with the name of the test and its outcomes.
        dataset_name: str
            Optional. Human readable display name for the dataset component.
        metadata: object
            Arbitrary key-value information, supplied by the user, to apply to the event.
        external_url: str
            A link to source information.

        Returns
        -------
        None
        """
        if not self.publish_events:
            logger.debug(f"publish_test_outcomes_event() called with test outcomes '{test_outcomes}'")
            return
        try:
            event_info = {
                "event_timestamp": event_timestamp.astimezone().isoformat(),
                "dataset_key": dataset_key,
                "test_outcomes": test_outcomes,
                "dataset_name": dataset_name,
                "metadata": metadata,
                "external_url": external_url,
                "component_tool": component_tool,
            }
            logger.debug(f"Publishing test outcome event: {event_info}")
            self.events_api_client.post_test_outcomes(TestOutcomesApiSchema(**event_info), event_source="API")

        except ApiException:
            logger.exception("Exception when calling EventsApi->post_test_outcomes\n")
            raise

    def publish_test_outcomes_event(
        self,
        event_timestamp: datetime,
        pipeline_key: str,
        run_key: str,
        test_outcomes: list[dict],
        task_key: str | None,
        pipeline_name: str | None = None,
        task_name: str | None = None,
        metadata: Any = None,
        external_url: str | None = None,
        component_tool: str | None = None,
    ) -> None:
        """
        Call this method to publish test outcome information.

        Parameters
        ----------
        event_timestamp: datetime
            A timezone aware timestamp indicating when the status of the task changed.
        pipeline_key: str
            Pipeline key that will be passed to the Events Ingestion API endpoints.
        run_key: str
            Pipeline run key passed to the Events Ingestion API endpoints.
        test_outcomes: list
            A list of dictionaries with the name of the test and its outcomes.
        pipeline_name: str
            Human readable display value for a pipeline.
        task_key: str
            The identification key for the run task. Typically, this is the name of a node within a pipeline DAG that the
            task status event is associated with.
        task_name: str
            Human readable display value for a task.
        metadata: object
            Arbitrary key-value information, supplied by the user, to apply to the event.
        external_url: str
            A link to source information.

        Returns
        -------
        None
        """
        if not self.publish_events:
            logger.debug(f"publish_test_outcomes_event() called with test outcomes '{test_outcomes}'")
            return
        try:
            event_info = {
                "event_timestamp": event_timestamp.astimezone().isoformat(),
                "pipeline_key": pipeline_key,
                "run_key": run_key,
                "test_outcomes": test_outcomes,
                "task_key": task_key,
                "pipeline_name": pipeline_name,
                "task_name": task_name,
                "metadata": metadata,
                "external_url": external_url,
                "component_tool": component_tool,
            }
            logger.debug(f"Publishing test outcome event: {event_info}")
            self.events_api_client.post_test_outcomes(TestOutcomesApiSchema(**event_info), event_source="API")

        except ApiException:
            logger.exception("Exception when calling EventsApi->post_test_outcomes\n")
            raise

    def publish_dataset_event(
        self,
        event_timestamp: datetime,
        dataset_key: str,
        dataset_name: str | None,
        operation: str,
        path: str | None,
        metadata: Any = None,
        external_url: str | None = None,
        pipeline_key: str | None = None,
        pipeline_name: str | None = None,
        stream_key: str | None = None,
        stream_name: str | None = None,
        server_key: str | None = None,
        server_name: str | None = None,
        run_key: str | None = None,
        run_name: str | None = None,
        task_key: str | None = None,
        task_name: str | None = None,
        component_tool: str | None = None,
    ) -> None:
        """
            Call this method to log any desired dataset event. For example, a file upload.

        Parameters
        ----------
        event_timestamp: datetime
            A timezone aware timestamp indicating when the status of the task changed.
        dataset_key: str
            A dataset key that will be passed to the Events Ingestion API endpoints.
        dataset_name: str
            Human readable display value for a dataset.
        operation: str
             The read or write operation performed. Accepts "READ" or "WRITE".
        path: str
            Path within the dataset where the operation took place.
        metadata: object
            Arbitrary key-value information, supplied by the user, to apply to the event.
        external_url: str
            A link to source information.
        pipeline_key: str
            Pipeline key that will be passed to the Events Ingestion API endpoints.
        pipeline_name: str
            Human readable display value for a pipeline.
        stream_key: str
            The key identifier of the target streaming-pipeline for the event action. Only one component key can be provided at a time.
        stream_name: str
            Human readable display name for the streaming-pipeline.
        server_key: str
            The key identifier of the target server component for the event action. Only one component key can be provided at a time.
        server_name: str
            Human readable display name for the server.
        run_key: str
            The identifier of the target run for the event action. This key is created and managed by the user. Required if the target component is a batch-pipeline.
        run_name: str
            Human readable display name for the run.
        task_key: str
            The identification key for the run task. Typically, this is the name of a node within a pipeline DAG that the
            task status event is associated with.
        task_name: str
            Human readable display value for a task.
        component_tool: str
            A certain tool type

        Returns
        -------
        None
        """
        if not self.publish_events:
            logger.debug(
                f"publish_dataset_event() called with dataset key '{dataset_key}', operation '{operation}', and timestamp '{event_timestamp.astimezone().isoformat()}'"
            )
            return
        try:
            event_info = {
                "event_timestamp": event_timestamp.astimezone().isoformat(),
                "dataset_key": dataset_key,
                "dataset_name": dataset_name,
                "operation": operation,
                "path": path,
                "metadata": metadata,
                "external_url": external_url,
                "pipeline_key": pipeline_key,
                "pipeline_name": pipeline_name,
                "stream_key": stream_key,
                "stream_name": stream_name,
                "server_key": server_key,
                "server_name": server_name,
                "run_key": run_key,
                "run_name": run_name,
                "task_key": task_key,
                "task_name": task_name,
                "component_tool": component_tool,
            }
            logger.debug(f"Publishing dataset event: {event_info}")
            self.events_api_client.post_dataset_operation(DatasetOperationApiSchema(**event_info), event_source="API")

        except ApiException:
            logger.exception("Exception when calling EventsApi->post_dataset_operation\n")
            raise
