import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from types import TracebackType
from typing import Any, Self, cast

from httpx import URL, AsyncClient
from trio import MemorySendChannel, Nursery

from framework.configuration.http import HTTPClientConfig
from framework.core.handles import get_client
from framework.core.loops import PeriodicLoop
from framework.core.tasks import PeriodicTask
from framework.observability.message_event_log_level import MessageEventLogLevel
from registry import ConfigurationRegistry
from registry.configuration_auth_credentials import load_auth_class
from toolkit.more_typing import JSON_DICT
from toolkit.observability import EVENT_TYPE_KEY, EventType, Status

from .configuration import DatabricksConfiguration
from .constants import COMPONENT_TOOL, DATABRICKS_SPN_SCOPE
from .handles import DatabricksGetRunEndpoint, DatabricksListRunsEndpoint
from .helpers import get_status, is_a_repair_run

LOGGER = logging.getLogger(__name__)


class DatabricksWatchRunTask(PeriodicTask):
    def __init__(
        self,
        run: JSON_DICT,
        client: AsyncClient,
        outbound_channel: MemorySendChannel[JSON_DICT],
    ) -> None:
        super().__init__(outbound_channel=outbound_channel)
        self.configuration = ConfigurationRegistry().lookup("databricks", DatabricksConfiguration)
        self.jobs_version = str(self.configuration.databricks_jobs_version)
        self.endpoint = DatabricksGetRunEndpoint(base_url=URL(str(self.configuration.databricks_host)), client=client)
        self.pipeline_key = str(run["job_id"])
        self.pipeline_name = cast(str, run["run_name"])
        self.run_key = str(run["run_id"])
        self.tasks: dict[int, DatabricksRunTaskData] = {}
        self.start_time = datetime.now(tz=UTC)
        self.status = Status.UNKNOWN

    async def execute(self, current_dt: datetime, previous_dt: datetime) -> None:
        params = {"run_id": self.run_key}
        response = await self.endpoint.handle(
            query_params=params,
            path_args={"jobs_version": self.configuration.databricks_jobs_version},
        )
        # TODO: Better error handling
        if not response:
            LOGGER.warning(f"Failed to get run id {self.run_key}")
            return

        json = response.json()
        repair_run = is_a_repair_run(json)
        metadata = {
            "job_id": json.get("job_id"),
            "run_id": json.get("run_id"),
            "creator_user_name": json.get("creator_user_name"),
            "number_in_job": json.get("number_in_job"),
            "original_attempt_run_id": json.get("original_attempt_run_id"),
            "schedule": json.get("schedule"),
            "trigger": json.get("trigger"),
            "run_name": json.get("run_name"),
            "run_type": json.get("run_type"),
            "repair_run": repair_run,
            # TODO "prev_run_end_time_ms": json.get("prev_run_end_time_ms", int(datetime.now(tz=UTC).timestamp() * 1000)),
        }
        await self.update_run_status(json, metadata)
        await self.update_tasks(json, metadata)
        if self.start_time + timedelta(seconds=self.configuration.databricks_failed_watch_max_time) < datetime.now(
            tz=UTC,
        ):
            LOGGER.info("Finishing extended watch for run id %s ", self.run_key)
            self.finish()

    async def update_run_status(self, json: dict, metadata: dict) -> None:
        external_url = json.get("run_page_url")
        status = get_status(json["state"])
        if status == self.status:
            return
        self.status = status
        if self.status.finished:
            event_timestamp = datetime.fromtimestamp(json["end_time"] / 1000, UTC).astimezone().isoformat()
            if self.status == Status.FAILED:
                state_message = json["state"]["state_message"]
                error_message = state_message if len(state_message) > 0 else f"Run {self.pipeline_name} failed!"
                LOGGER.debug(
                    "Log Run failed message -> Run-key: '%s', message: '%s', name: '%s'",
                    self.run_key,
                    error_message,
                    self.pipeline_name,
                )
                failed_message_log: JSON_DICT = {
                    EVENT_TYPE_KEY: EventType.MESSAGE_LOG.value,
                    "event_timestamp": event_timestamp,
                    "task_key": None,
                    "log_level": MessageEventLogLevel.ERROR.value,
                    "message": error_message,
                    "pipeline_name": self.pipeline_name,
                    "pipeline_key": self.pipeline_key,
                    "run_key": self.run_key,
                    "task_name": None,
                    "metadata": metadata,
                    "external_url": external_url,
                    "component_tool": COMPONENT_TOOL,
                }
                await self.send(failed_message_log)
            if metadata["repair_run"]:
                repair_run_message = "This is a repaired run."
                repair_run_message_log: JSON_DICT = {
                    EVENT_TYPE_KEY: EventType.MESSAGE_LOG.value,
                    "event_timestamp": event_timestamp,
                    "task_key": None,
                    "log_level": MessageEventLogLevel.INFO.value,
                    "message": repair_run_message,
                    "pipeline_name": self.pipeline_name,
                    "pipeline_key": self.pipeline_key,
                    "run_key": self.run_key,
                    "task_name": None,
                    "metadata": metadata,
                    "external_url": external_url,
                    "component_tool": COMPONENT_TOOL,
                }
                await self.send(repair_run_message_log)
            LOGGER.debug(
                "Log Job End: pipeline_name: '%s', metadata: '%s', external_url: '%s'",
                self.pipeline_name,
                metadata,
                external_url,
            )
            update_run_status_event: JSON_DICT = {
                EVENT_TYPE_KEY: EventType.RUN_STATUS.value,
                "event_timestamp": event_timestamp,
                "task_key": None,
                "status": self.status.value,
                "pipeline_key": self.pipeline_key,
                "pipeline_name": self.pipeline_name,
                "run_key": self.run_key,
                "task_name": None,
                "metadata": metadata,
                "external_url": external_url,
                "component_tool": COMPONENT_TOOL,
            }
            await self.send(payload=update_run_status_event)
            if self.status in (Status.COMPLETED, Status.COMPLETED_WITH_WARNINGS):
                self.finish()
            else:
                LOGGER.info("Run id %s did not complete successfully, watching at lower period.", self.run_key)
                self.update_loop_period(self.configuration.databricks_failed_watch_period)

    async def update_tasks(self, json: JSON_DICT, metadata: dict) -> None:
        tasks = cast(list[JSON_DICT], json.get("tasks", []))
        if not tasks:
            LOGGER.info("No tasks found for run %s", self.run_key)
            return
        for task in tasks:
            # original agent does this but is slightly confusing
            # our task_key is the databricks response run_id
            # our name is the databricks response task_key
            task_key = cast(int, task.get("run_id", 0))
            if task_key not in self.tasks:
                attempt_number = cast(int, task.get("attempt_number", 0))
                self.tasks[task_key] = DatabricksRunTaskData(
                    status=Status.UNKNOWN,
                    task_key=task_key,
                    name=cast(str, task.get("task_key", "missing_task_key")),
                    pipeline_name=self.pipeline_name,
                    pipeline_key=self.pipeline_key,
                    run_key=self.run_key,
                    external_url=cast(str, task.get("run_page_url", "missing_task_run_page_url")),
                    attempt_number=attempt_number,
                )
                # missing: publish metric event in case attempt_number > 0
            updates = self.tasks[task_key].update_task(task, metadata)
            if updates:
                for payload in updates:
                    await self.send(payload=payload)
                # missing: if existing_task.status.finshed then parse notebook to send metrics and test events


@dataclass
class DatabricksRunTaskData:
    status: Status
    task_key: int
    name: str
    pipeline_name: str
    pipeline_key: str
    run_key: str
    external_url: str
    attempt_number: int

    def update_task(self, task_json: dict, metadata: dict) -> list[JSON_DICT] | None:
        status = get_status(task_json["state"])
        if status not in (Status.UNKNOWN, self.status):
            if status.finished:
                time_property = "end_time"
            else:
                time_property = "start_time"
            event_timestamp = datetime.fromtimestamp(task_json[time_property] / 1000).astimezone().isoformat()
            event_payload: JSON_DICT = {
                EVENT_TYPE_KEY: EventType.RUN_STATUS.value,
                "event_timestamp": event_timestamp,
                "pipeline_name": self.pipeline_name,
                "pipeline_key": self.pipeline_key,
                "run_key": self.run_key,
                "task_name": self.name,
                "task_key": str(self.task_key),
                "status": status.value,
                "metadata": metadata,
                "external_url": self.external_url,
                "component_tool": COMPONENT_TOOL,
            }
            self.status = status
            events_to_send = [event_payload]
            if status == Status.FAILED:
                state_message = task_json["state"]["state_message"]
                error_message = state_message if len(state_message) > 0 else f"Task {self.name} failed!"
                failed_message_log: JSON_DICT = {
                    EVENT_TYPE_KEY: EventType.MESSAGE_LOG.value,
                    "event_timestamp": event_timestamp,
                    "log_level": MessageEventLogLevel.ERROR.value,
                    "message": error_message,
                    "pipeline_name": self.pipeline_name,
                    "pipeline_key": self.pipeline_key,
                    "run_key": self.run_key,
                    "task_name": self.name,
                    "task_key": str(self.task_key),
                    "metadata": metadata,
                    "external_url": self.external_url,
                    "component_tool": COMPONENT_TOOL,
                }
                events_to_send.append(failed_message_log)
            return events_to_send
        return None


class DatabricksListRunsTask(PeriodicTask):
    def __init__(self, nursery: Nursery, outbound_channel: MemorySendChannel[JSON_DICT], **kwargs: Any):
        super().__init__(outbound_channel=outbound_channel, **kwargs)
        registry = ConfigurationRegistry()
        self.configuration = registry.lookup("databricks", DatabricksConfiguration)
        auth = load_auth_class(spn_scope=DATABRICKS_SPN_SCOPE)
        self.http_config = registry.mutate("http", HTTPClientConfig, auth=auth)
        self.client = get_client(self.http_config)
        self.endpoint = DatabricksListRunsEndpoint(
            base_url=URL(str(self.configuration.databricks_host)),
            client=self.client,
        )
        self.nursery = nursery
        self.runs_watched: dict[str, DatabricksWatchRunTask] = {}
        if self.configuration.databricks_jobs:
            # Logic for filtering jobs to be observed if DK_DATABRICKS_JOBS is defined
            # TODO (non-parity): should we change this to using JOB Id's and query job runs with that filter instead?
            LOGGER.debug(
                "jobs/DK_DATABRICKS_JOBS is defined! Observing databricks for the following jobs: '%s'",
                self.configuration.databricks_jobs,
            )
        else:
            LOGGER.debug("jobs/DK_DATABRICKS_JOBS is not defined! Observing databricks all databricks jobs!")

        self.jobs_filter = self.configuration.databricks_jobs

    async def __aenter__(self) -> Self:
        await self.client.__aenter__()
        return await super().__aenter__()

    async def __aexit__(self, exc_type: type[BaseException], exc_val: BaseException, exc_tb: TracebackType) -> None:
        await self.client.__aexit__(exc_type, exc_val, exc_tb)
        return await super().__aexit__(exc_type, exc_val, exc_tb)

    async def execute(self, current_dt: datetime, previous_dt: datetime) -> None:
        await self.refresh_tasks_watched()
        has_more = True
        page_token: str | None = None
        params = {
            "expand_tasks": "true",
            "start_time_from": int(previous_dt.timestamp() * 1000),
            "start_time_to": int(current_dt.timestamp() * 1000),
        }
        while has_more:
            if page_token:
                params["page_token"] = page_token
            response = await self.endpoint.handle(
                query_params=params,
                path_args={"jobs_version": self.configuration.databricks_jobs_version},
            )
            if not response:
                LOGGER.warning("Failed to list job runs")
                return

            json = response.json()
            if json["has_more"]:
                page_token = json["next_page_token"]
            else:
                has_more = False

            if "runs" not in json:
                LOGGER.debug("No runs for jobs found in page for period.")
                return

            runs = cast(list[JSON_DICT], json["runs"])
            if self.jobs_filter:
                new_runs = [
                    run
                    for run in runs
                    if run["run_name"] in self.jobs_filter and run["run_id"] not in self.runs_watched
                ]
            else:
                new_runs = [run for run in runs if run["run_id"] not in self.runs_watched]
            for run in new_runs:
                get_run_task = DatabricksWatchRunTask(
                    run=run,
                    # cast handles a mypy bug; see https://github.com/python/mypy/issues/16659
                    outbound_channel=cast(MemorySendChannel[JSON_DICT], self.outbound_channel.clone()),
                    client=self.client,
                )
                self.nursery.start_soon(PeriodicLoop(period=self.configuration.period, task=get_run_task).run)

                self.runs_watched[cast(str, run["run_id"])] = get_run_task
                LOGGER.debug("Log Run to be observed: %s", run.get("run_name"))

    async def refresh_tasks_watched(self) -> None:
        runs_finished: list[str] = []
        for key, run_task in self.runs_watched.items():
            if run_task.is_done:
                runs_finished.append(key)
        for finished_run in runs_finished:
            self.runs_watched.pop(finished_run)
