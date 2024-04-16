import hashlib
import logging
import os
from datetime import datetime, timedelta, timezone
from logging import Logger
from typing import List

import sqlalchemy
from attrs import define, field, validators
from events_ingestion_client.rest import ApiException
from retry.api import retry_call
from sqlalchemy import MetaData, create_engine, exc
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import sessionmaker

from common.component_helper import ComponentHelper
from common.events_publisher import EventsPublisher
from poller_agents.abstract_run import AbstractRun
from poller_agents.abstract_runs_fetcher import AbstractRunsFetcher

logger: Logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

API_TRIES: int = int(os.getenv("API_TRIES", 1))
API_DELAY_SECS: int = int(os.getenv("API_DELAY_SECS", 1))
API_BACKOFF_MULTIPLIER: int = int(os.getenv("API_BACKOFF_MULTIPLIER", 1))


@define(kw_only=True, slots=False)
class SqlTestOutcomesCustom01Run(AbstractRun):
    events: List = field(validator=validators.instance_of(List))
    _component_tool = "sql_test_outcomes_custom_01"

    _state_component_prefix = "RawAuditFetcher - "
    _state_component_description = "System Managed Component to track log fetches"
    _last_sent_event_label = "last_sent_event"

    def update(self) -> None:
        if len(self.events) > 0:
            hundred_years_ago = datetime.now(timezone.utc) - timedelta(days=365 * 100)
            max_load_date_time = hundred_years_ago
            for event in self.events:
                retry_call(
                    self.publish_dataset_event,
                    exceptions=ApiException,
                    tries=API_TRIES,
                    backoff=API_BACKOFF_MULTIPLIER,
                    delay=API_DELAY_SECS,
                    logger=logger,
                    fargs=[
                        event["event_timestamp"],
                        event["dataset_key"],
                        event["dataset_name"],
                        "WRITE",
                        event["dataset_name"],
                    ],
                    fkwargs={"metadata": event["metadata"]},
                )
                retry_call(
                    self.publish_test_outcomes_event_dataset,
                    exceptions=ApiException,
                    tries=API_TRIES,
                    backoff=API_BACKOFF_MULTIPLIER,
                    delay=API_DELAY_SECS,
                    logger=logger,
                    fargs=[
                        event["event_timestamp"] + timedelta(milliseconds=1),
                        event["dataset_key"],
                        event["test_outcomes"],
                    ],
                    fkwargs={"dataset_name": event["dataset_name"], "metadata": event["metadata"]},
                )
                load_datetime_utc = event["metadata"]["RawTable_Loaddatetime"].replace(
                    tzinfo=timezone.utc
                )
                if load_datetime_utc > max_load_date_time:
                    max_load_date_time = load_datetime_utc

            # All events loaded. Now let's update our state.
            # Store out latest event time_stamp
            helper = ComponentHelper(
                key=f"{self._state_component_prefix}{self.agent_key}",
                name=f"{self._state_component_prefix}{self.agent_name}",
                description=self._state_component_description,
                tool=self.component_tool,
            )

            helper.set_label(
                self._last_sent_event_label, max_load_date_time.strftime("%Y-%m-%dT%H:%M:%S")
            )

        self.finished = True

    @property
    def component_tool(self) -> str:
        return self._component_tool


class SQLServerHelper:
    def __init__(self) -> None:
        self._check_parameters()
        self._assign_parameters()
        self._connect()

    username: str = field(validator=validators.instance_of(str))
    password: str = field(validator=validators.instance_of(str))
    host: str = field(validator=validators.instance_of(str))
    port: int = field(validator=validators.instance_of(int))
    database: str = field(validator=validators.instance_of(str))

    table: str = field(validator=validators.instance_of(str))
    schema: str = field(validator=validators.instance_of(str))

    engine: Engine = field(validator=validators.instance_of(Engine))
    connection: Connection = field(validator=validators.instance_of(Connection))
    metadata: MetaData = field(validator=validators.instance_of(MetaData))
    reflected_table: sqlalchemy.Table = field(validator=validators.instance_of(sqlalchemy.Table))
    session: sqlalchemy.orm.session.Session = field(
        validator=validators.instance_of(sqlalchemy.orm.session.Session)
    )

    _driver_name: str = "mssql+pyodbc"

    @classmethod
    def _check_parameters(cls) -> None:
        err_message = ""
        required_envvars = [
            "DK_USERNAME",
            "DK_PASSWORD",
            "DK_HOST",
            "DK_PORT",
            "DK_DATABASE",
            "DK_TABLE",
            "DK_SCHEMA",
        ]
        for required_envvar in required_envvars:
            if required_envvar not in os.environ:
                err_message = (
                    err_message + f"{required_envvar} is not set in the environment variables.\n"
                )
        if len(err_message) != 0:
            logger.info(err_message)
            raise ValueError(err_message)

    def create_mssql_engine(self) -> Engine:
        conn_str = (
            "DRIVER={ODBC Driver 18 for SQL Server};"
            + f"SERVER={self.host},{self.port};"
            + f"DATABASE={self.database};"
            + f"UID={self.username};"
            + f"PWD={self.password};"
        )
        connection_url = URL("mssql+pyodbc", query={"odbc_connect": conn_str})
        engine = create_engine(connection_url, connect_args={"TrustServerCertificate": "yes"})
        return engine

    def _assign_parameters(self) -> None:
        self.username = os.environ["DK_USERNAME"]
        self.password = os.environ["DK_PASSWORD"]
        self.host = os.environ["DK_HOST"]
        self.port = int(os.environ["DK_PORT"])
        self.database = os.environ["DK_DATABASE"]
        self.table = os.environ["DK_TABLE"]
        self.schema = os.environ["DK_SCHEMA"]
        self._lookback_minutes = int(os.environ.get("DK_LOOKBACK_MINUTES", 720 * 60))

        self._batch_size = int(os.environ.get("DK_BATCH_SIZE", 500))

    def _connect(self) -> None:
        self.engine = self.create_mssql_engine()
        logger.info(f"Connecting to {self.host}:{self.port} database {self.database}")
        try:
            self.connection = self.engine.connect()
        except exc.OperationalError as e:
            logger.error(f"Failed to connect to the database. Error: {e}")
            raise e
        except exc.ArgumentError as e:
            logger.error(f"Failed to connect to the database. ArgumentError. Error: {e}")
            raise e
        logger.info(f"Connected to {self.host}:{self.port} database {self.database}")
        self.metadata = MetaData(schema=self.schema)  # extracting the metadata
        self.metadata.reflect(bind=self.engine)
        self.reflected_table = self.metadata.tables[f"{self.schema}.{self.table}"]
        Session = sessionmaker(bind=self.connection)
        self.session = Session()


@define(kw_only=True, slots=False)
class SqlTestOutcomesCustom01Fetcher(AbstractRunsFetcher):
    sqlserver_helper: SQLServerHelper = field(validator=validators.instance_of(SQLServerHelper))

    _component_tool: str = "sql_test_outcomes_custom_01"
    _lookback_minutes: int = 720 * 60

    _state_component_prefix: str = "RawAuditFetcher - "
    _state_component_description: str = "System Managed Component to track log fetches"
    _last_sent_event_label: str = "last_sent_event"

    @property
    def component_tool(self) -> str:
        return self._component_tool

    @classmethod
    def create_runs_fetcher(cls, events_publisher: EventsPublisher) -> AbstractRunsFetcher:
        sqlserver_helper = SQLServerHelper()
        return SqlTestOutcomesCustom01Fetcher(
            events_publisher=events_publisher, sqlserver_helper=sqlserver_helper
        )

    def _get_lookback_period(self) -> datetime:
        """Get the lookback period from the environment variable or default to 10 minutes. Provide the time in UTC"""
        start_time = datetime.now(timezone.utc) - timedelta(minutes=self._lookback_minutes)
        return start_time

    def get_start_time_utc(self) -> datetime:
        """Let's figure out a lookback period that is not greater than the environment variable setting or the default.

        First figure out a proposed lookback period from the environment variable and default.

        Now, let's see if a last_sent_event timestamp can be found in the component metadata. If last_sent_event is
        found, then take the newest of lookback period and last_sent_event. If last_sent_event is not found, then
        use the lookback period."""

        # See if we have any anything stored in the component metadata. It might be shorted than the lookback.
        helper = ComponentHelper(
            key=f"{self._state_component_prefix}{self.agent_key}",
            name=f"{self._state_component_prefix}{self.agent_name}",
            tool=self.component_tool,
            description=self._state_component_description,
        )
        last_sent_event_string = helper.get_label(self._last_sent_event_label)

        # No figure out a look back based on the environment variable and default.
        lookback_period_utc = self._get_lookback_period()
        logger.info(f"Default/Environment Lookback: {lookback_period_utc}")

        # We always want to get the most recent time that we find.
        # This is to ensure that we don't try and get too many events for a very busy system.
        if last_sent_event_string is None:
            start_time = lookback_period_utc
            logger.info("No component metadata found. Using Default/Environment Lookback ")
        else:
            last_sent_event = datetime.strptime(last_sent_event_string, "%Y-%m-%dT%H:%M:%S")
            last_sent_event = last_sent_event.replace(tzinfo=timezone.utc)
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

    def fetch_runs(
        self, execution_date_gte: datetime, execution_date_lte: datetime
    ) -> List[AbstractRun]:
        # self.sqlserver_helper._check_parameters()
        # self._assign_parameters()
        # self._connect()
        start_time = self.get_start_time_utc()
        filtered_query = (
            self.sqlserver_helper.session.query(self.sqlserver_helper.reflected_table)
            .filter(self.sqlserver_helper.reflected_table.c.RawTable_Loaddatetime > start_time)
            .order_by(self.sqlserver_helper.reflected_table.c.RawTable_Loaddatetime)
        )
        row_count = filtered_query.count()
        logger.info(
            f"Found {row_count} rows in {self.sqlserver_helper.schema}.{self.sqlserver_helper.table} to process"
        )

        runs: List[AbstractRun] = []
        if row_count != 0:
            events = []
            max_load_date_time = start_time
            for row in filtered_query.yield_per(self.sqlserver_helper._batch_size):
                new_event = self.create_test_outcome_event(row)
                events.append(new_event)
                load_datetime_utc = new_event["metadata"]["RawTable_Loaddatetime"].replace(
                    tzinfo=timezone.utc
                )
                if load_datetime_utc > max_load_date_time:
                    max_load_date_time = load_datetime_utc

            faux_run_id = f'{max_load_date_time.strftime("%Y%m%dT%H%M%S.%f%z")}'
            this_run = SqlTestOutcomesCustom01Run(
                events_publisher=self.events_publisher,
                events=events,
                run_key=faux_run_id,  # This is ignored because we are doing dataset events
                agent_key=self.agent_key,
                agent_name=self.agent_name,
                pipeline_key=f"{self.sqlserver_helper.database}.{self.sqlserver_helper.schema}.{self.sqlserver_helper.table}",  # This is ignored because we are doing dataset events
            )
            runs.append(this_run)

        self.sqlserver_helper.session.close()
        self.sqlserver_helper.connection.close()
        self.sqlserver_helper.engine.dispose()
        return runs

    def create_test_outcome_event(self, row: dict) -> dict:
        dataset_name = f"{row['SchemaName']}.{row['RawTableName']}"
        dataset_key = f"{row['SchemaName']}.{row['RawTableName']}"
        event_timestamp = row["RawTable_Loaddatetime"]
        metadata = {
            "AuditId": row["AuditId"],
            "SourceFileName": row["SourceFileName"],
            "SourceFile_RowCount": row["SourceFile_RowCount"],
            "SourceFile_LastModifiedDateTime": row["SourceFile_LastModifiedDateTime"],
            "SchemaName": row["SchemaName"],
            "RawTableName": row["RawTableName"],
            "RawTable_LoadedRowCount": row["RawTable_LoadedRowCount"],
            "RawTable_NotLoadedRowCount": row["RawTable_NotLoadedRowCount"],
            "MatchedRowCount_SrcFile_RawTable_Y_N": row["MatchedRowCount_SrcFile_RawTable_Y_N"],
            "RawTable_Loaddatetime": row["RawTable_Loaddatetime"],
            "SourceFile_Processed_Y_N": row["SourceFile_Processed_Y_N"],
        }
        if row["MatchedRowCount_SrcFile_RawTable_Y_N"] == "Y":
            status = "PASSED"
        else:
            status = "FAILED"
        test_outcomes = {}
        test_outcomes["description"] = "Compare the row count in the raw table "
        "with the rows that were loaded. All rows must load."
        test_outcomes["start_time"] = row["RawTable_Loaddatetime"]
        test_outcomes["end_time"] = row["RawTable_Loaddatetime"]
        test_outcomes["metric_name"] = "Rows Not Loaded"
        test_outcomes["status"] = status
        test_outcomes["name"] = "Row Count Compare"
        test_outcomes["metric_value"] = row["RawTable_NotLoadedRowCount"]
        new_event = dict(
            dataset_name=dataset_name,
            dataset_key=dataset_key,
            event_timestamp=event_timestamp,
            status=status,
            test_outcomes=[test_outcomes],
            metadata=metadata,
        )
        return new_event

    @property
    def agent_name(self) -> str:
        return f"{self.sqlserver_helper.host}/{self.sqlserver_helper.database}"

    @property
    def agent_key(self) -> str:
        md5 = hashlib.md5()
        md5.update(self.agent_name.encode("utf-8"))
        return md5.hexdigest()
