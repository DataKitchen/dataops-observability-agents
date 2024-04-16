import hashlib
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from logging import Logger
from typing import Any, List, Optional, Union

import requests
from attrs import define, field, validators
from databricks import sql  # type: ignore

from common.component_helper import ComponentHelper
from common.events_publisher import EventsPublisher
from common.message_event_log_level import MessageEventLogLevel
from common.status import Status
from poller_agents.abstract_run import AbstractRun
from poller_agents.abstract_runs_fetcher import AbstractRunsFetcher

last_sent_event_label: str = "last_sent_event"

fivetran_fetcher_description = "System Managed Component to track log fetches"

logger: Logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@define(kw_only=True, slots=False)
class FiveTranSync(AbstractRun):
    # Send to Observability API
    pipeline_name: str = field(validator=validators.instance_of(str))

    # All the events that we got for this sync
    events: List = field(validator=validators.instance_of(list))
    max_time_stamp: datetime = field(validator=validators.instance_of(datetime))

    _component_tool = "fivetran"

    def _external_url(self) -> str:
        return f"https://fivetran.com/dashboard/connectors/{self.pipeline_key}/logs"

    @staticmethod
    def _is_monitor_event(event: str) -> bool:
        if event == "records_modified":
            return True
        return False

    @staticmethod
    def _is_sync_event(event: str) -> bool:
        if event == "sync_end" or event == "sync_start":
            return True
        return False

    @staticmethod
    def _is_write_event(event: str) -> bool:
        if event in ("write_to_table_end", "write_to_table_start"):
            return True
        return False

    @staticmethod
    def _compose_metadata(event: dict) -> Optional[Union[dict, None]]:
        if (
            "DEBUG_OMIT_METADATA" not in os.environ
            or os.environ["DEBUG_OMIT_METADATA"].upper() == "FALSE"
        ):
            metadata = {
                "event": event["message_event"] if "message_event" in event else None,
                "data": event["message_data"] if "message_data" in event else None,
                "time_stamp": event["time_stamp"] if "time_stamp" in event else None,
                "connector_id": event["connector_id"] if "connector_id" in event else None,
                "connector_name": event["connector_name"] if "connector_name" in event else None,
                "connector_type": event["official_connector_name"]
                if "official_connector_name" in event
                else None,
                "sync_id": event["sync_id"] if "sync_id" in event else None,
            }
            return metadata
        return None

    @property
    def component_tool(self) -> str:
        return self._component_tool

    def update(self) -> None:
        event_count = 0

        for event in self.events:
            event_count += 1
            try:
                if event["message_event"] == "records_modified":
                    # logger.info(f"{self.pipeline_key} - {event['message_data']} - {self.run_key}")
                    task_key = f"write_to_table.{event['message_data']['table']}"
                    self.publish_metric_log_event(
                        event["time_stamp"],
                        task_key=task_key,
                        metric_key=f"{event['message_data']['table']}_count",
                        metric_value=event["message_data"]["count"],
                        pipeline_name=f'{event["connector_name"]} - FiveTran',
                        metadata=self._compose_metadata(event),
                        external_url=self._external_url(),
                        component_tool=self.component_tool,
                    )
                    continue
                elif self._is_write_event(event["message_event"]):
                    task_key = f"write_to_table.{event['message_data']['table']}"
                    task_name = task_key
                    if event["message_event"] == "write_to_table_start":
                        status = Status.RUNNING
                    elif event["message_event"] == "write_to_table_end":
                        status = Status.COMPLETED
                    else:
                        status = Status.UNKNOWN
                    # logger.info(
                    #    f"{self.pipeline_key} - {task_key} - {event['message_event']} - {self.run_key}"
                    # )
                    self.publish_run_status_event(
                        event["time_stamp"],
                        task_key=task_key,
                        status=status,
                        pipeline_name=f'{event["connector_name"]} - FiveTran',
                        metadata=self._compose_metadata(event),
                        task_name=task_name,
                        external_url=self._external_url(),
                        component_tool=self.component_tool,
                    )
                    continue
                elif self._is_sync_event(event["message_event"]):
                    #  Sync Start and Sync end should be 1 task. give it the same task_key
                    if event["message_event"] == "sync_end":
                        if event["message_data"]["status"] == "FAILURE_WITH_TASK":
                            status = Status.FAILED
                            # logger.info(
                            #    f"{self.pipeline_key} - {event['message_event']} - {self.run_key}"
                            # )
                            self.publish_message_log_event(
                                event_timestamp=event["time_stamp"],
                                log_level=MessageEventLogLevel.ERROR,
                                message=f"reason: {event['message_data']['reason']} ; taskType: {event['message_data']['taskType']}".replace(
                                    '"', '\\"'
                                ).replace(
                                    "\n", "\\n"
                                ),
                                pipeline_name=f'{event["connector_name"]} - FiveTran',
                                task_key=None,
                                task_name=None,
                                external_url=self._external_url(),
                                component_tool=self.component_tool,
                                metadata=self._compose_metadata(event),
                            )
                        else:
                            status = Status.COMPLETED
                        # logger.info(
                        # f"{self.pipeline_key} - {event['message_event']} - {self.run_key}"
                        # )
                        self.publish_run_status_event(
                            event["time_stamp"],
                            task_key=None,
                            status=status,
                            pipeline_name=f'{event["connector_name"]} - FiveTran',
                            metadata=self._compose_metadata(event),
                            task_name=None,
                            external_url=self._external_url(),
                            component_tool=self.component_tool,
                        )
                        continue
                    elif event["message_event"] == "sync_start":
                        status = Status.RUNNING
                        # logger.info(
                        #    f"{self.pipeline_key} - {event['message_event']} - {self.run_key}"
                        # )
                        self.publish_run_status_event(
                            event["time_stamp"],
                            task_key=None,
                            status=status,
                            pipeline_name=f'{event["connector_name"]} - FiveTran',
                            metadata=self._compose_metadata(event),
                            task_name=None,
                            external_url=self._external_url(),
                            component_tool=self.component_tool,
                        )
                        continue
                    else:
                        continue
            except ValueError as err:
                logger.info(err)
                continue

        # Store out latest event time_stamp
        helper = ComponentHelper(
            key=f"FiveTranLogsFetcher - {self.agent_key}",
            name=f"FiveTranLogsFetcher - {self.agent_name}",
            description=fivetran_fetcher_description,
            tool=self.component_tool,
        )

        helper.set_label(
            last_sent_event_label, self.max_time_stamp.strftime("%Y-%m-%dT%H:%M:%S.%f%z")
        )

        self.publish_message_log_event(
            event_timestamp=datetime.now(timezone.utc),
            log_level=MessageEventLogLevel.INFO,
            message=f"Found {event_count} events",
            pipeline_name=self.agent_name,
            task_key=None,
            task_name=None,
            external_url=None,
            component_tool=self.component_tool,
            metadata=None,
        )

        # We are done processing the events that we were given.
        # Even if the FiveTran Sync is not finished, this object is done.
        self.finished = True


@define(kw_only=True, slots=False)
class FiveTranLogsFetcher(AbstractRunsFetcher):
    server_hostname: str = field(validator=validators.instance_of(str))
    http_path: str = field(validator=validators.instance_of(str))
    fivetran_log_schema: str = field(validator=validators.instance_of(str))
    connection: object = field(validator=validators.instance_of(object))

    _component_tool: str = "fivetran"

    @property
    def agent_name(self) -> str:
        return f"{self.server_hostname}_{self.http_path}_{self.fivetran_log_schema}"

    @property
    def agent_key(self) -> str:
        md5 = hashlib.md5()
        md5.update(self.agent_name.encode("utf-8"))
        return md5.hexdigest()

    @property
    def component_tool(self) -> str:
        return self._component_tool

    #
    # def get_agent_key(self) -> str:
    #     name = self.get_agent_name()
    #
    #     # Convert the string to bytes and hash it
    #     md5 = hashlib.md5()
    #     md5.update(name.encode("utf-8"))
    #
    #     # Get the hexadecimal representation of the hash
    #     encrypted_string = md5.hexdigest()
    #     return encrypted_string
    #
    # def get_agent_name(self) -> str:
    #     name_root = f"{self.server_hostname}_{self.http_path}_{self.fivetran_log_schema}"
    #     agent_name = f"FiveTranLogsFetcher_{name_root}"
    #     return agent_name

    @classmethod
    def create_runs_fetcher(cls, events_publisher: EventsPublisher) -> AbstractRunsFetcher:
        cls._check_parameters()

        connection = sql.connect(
            server_hostname=os.environ["FIVETRAN_DB_SERVER_HOSTNAME"],
            http_path=os.environ["FIVETRAN_DB_HTTP_PATH"],
            access_token=os.environ["FIVETRAN_DB_PERSONAL_ACCESS_TOKEN"],
        )

        return FiveTranLogsFetcher(
            events_publisher=events_publisher,
            connection=connection,
            server_hostname=os.environ["FIVETRAN_DB_SERVER_HOSTNAME"],
            http_path=os.environ["FIVETRAN_DB_HTTP_PATH"],
            fivetran_log_schema=os.environ["FIVETRAN_DB_LOG_SCHEMA"],
        )

    @classmethod
    def _check_parameters(cls) -> None:
        err_message = ""
        required_envvars = [
            "FIVETRAN_DB_SERVER_HOSTNAME",
            "FIVETRAN_DB_HTTP_PATH",
            "FIVETRAN_DB_PERSONAL_ACCESS_TOKEN",
            "FIVETRAN_DB_LOG_SCHEMA",
        ]
        for required_envvar in required_envvars:
            if required_envvar not in os.environ:
                err_message = (
                    err_message + f"{required_envvar} is not set in the environment variables.\n"
                )
        if len(err_message) != 0:
            logger.info(err_message)
            raise ValueError(err_message)

    def fetch_runs(self, execution_date_gte: datetime, execution_date_lte: datetime) -> Any:
        # The fetch_runs method must return a list of AbstractRun subclass instances

        start_time_utc = self.get_start_time_utc()
        logger.info(f"Looking for Sync events after {start_time_utc}")

        max_time_stamp: datetime = start_time_utc
        results = []

        # self.events_publisher

        # First figure out all of the tasks that we need to work on.
        with self.connection.cursor() as cursor:  # type: ignore
            sql = f"""
            SELECT a.id,
            a.time_stamp,
            a.connector_id,
            a.transformation_id,
            a.event,
            a.message_event,
            a.message_data,
            a.sync_id,
            a._fivetran_synced,
            b.connector_name,
            c.official_connector_name
            FROM {self.fivetran_log_schema}.log a
            inner join {self.fivetran_log_schema}.connector b on a.connector_id = b.connector_id
            inner join {self.fivetran_log_schema}.connector_type c on b.connector_type_id = c.id
            WHERE a._fivetran_synced > '{start_time_utc}'
            ORDER BY a._fivetran_synced asc;
            """
            logger.info(sql)
            cursor.execute(sql)
            result = cursor.fetchall()

            for row in result:
                new_event = row.asDict()

                if new_event["_fivetran_synced"] > max_time_stamp:
                    max_time_stamp = new_event["_fivetran_synced"]

                if "message_data" in new_event and new_event["message_data"] is not None:
                    new_event["message_data"] = json.loads(new_event["message_data"])
                results.append(new_event)

            cursor.close()

        self.connection.close()  # type: ignore
        syncs = self._separate_runs(results, max_time_stamp)
        logger.info(f"{len(syncs)} new syncs found")
        return syncs.values()

    def get_start_time_utc(self) -> datetime:
        """Let's figure out a lookback period that is not greater than the environment variable setting or the default.

        First figure out a proposed lookback period from the environment variable and default.

        Now, let's see if a last_sent_event timestamp can be found in the component metadata. If last_sent_event is
        found, then take the newest of lookback period and last_sent_event. If last_sent_event is not found, then
        use the lookback period."""

        # See if we have any anything stored in the component metadata. It might be shorted than the lookback.
        helper = ComponentHelper(
            key=f"FiveTranLogsFetcher - {self.agent_key}",
            name=f"FiveTranLogsFetcher - {self.agent_name}",
            tool=self.component_tool,
            description=fivetran_fetcher_description,
        )
        last_sent_event_string = helper.get_label(last_sent_event_label)

        # No figure out a look back based on the environment variable and default.
        lookback_period_utc = self._get_lookback_period()
        logger.info(f"Default/Environment Lookback: {lookback_period_utc}")

        # We always want to get the most recent time that we find.
        # This is to ensure that we don't try and get too many events for a very busy system.
        if last_sent_event_string is None:
            start_time = lookback_period_utc
            logger.info("No component metadata found. Using Default/Environment Lookback ")
        else:
            last_sent_event = datetime.strptime(last_sent_event_string, "%Y-%m-%dT%H:%M:%S.%f%z")

            logger.info(f"last_sent_event: {last_sent_event}")
            if last_sent_event > lookback_period_utc:
                start_time = last_sent_event
                logger.info(
                    f"Component metadata found, and it is newer than lookback. {last_sent_event} > {lookback_period_utc}"
                )
            else:
                start_time = lookback_period_utc
                logger.info(
                    f"Lookback is equal to or newer than component metadata. {lookback_period_utc} >= {last_sent_event}"
                )
        logger.info(f"Using start time of {start_time}")
        return start_time

    @staticmethod
    def _get_lookback_period() -> datetime:
        """Get the lookback period from the environment variable or default to 10 minutes. Provide the time in UTC"""
        start_time = datetime.now(timezone.utc) - timedelta(
            minutes=int(os.getenv("FIVETRAN_DB_LOOKBACK", 10))
        )
        return start_time

    def _separate_runs(self, results: list, max_time_stamp: datetime) -> dict[Any, FiveTranSync]:
        syncs = {}
        for event in results:
            if "sync_id" in event and event["sync_id"] is not None:
                if event["sync_id"] not in syncs:
                    logger.info(
                        f"Found new sync {event['sync_id']} for name: {event['connector_name']} , id: {event['connector_id']}"
                    )
                    syncs[event["sync_id"]] = FiveTranSync(
                        events_publisher=self.events_publisher,
                        pipeline_key=event["connector_id"],
                        pipeline_name=f'{event["connector_name"]} - FiveTran',
                        run_key=event["sync_id"],
                        max_time_stamp=max_time_stamp,
                        agent_key=self.agent_key,
                        agent_name=self.agent_name,
                        events=[],
                    )

                syncs[event["sync_id"]].events.append(event)
        return syncs
