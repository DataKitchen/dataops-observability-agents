from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from http import HTTPMethod, HTTPStatus
from types import TracebackType
from typing import Any, Self, cast

from dateutil.parser import parse as parse_datetime
from httpx import URL, AsyncClient
from trio import MemorySendChannel, Nursery

from framework.configuration.http import HTTPClientConfig
from framework.core.handles import HTTPAPIRequestHandle, HTTPRetryConfig, get_client
from framework.core.loops import PeriodicLoop
from framework.core.tasks import PeriodicTask
from registry import ConfigurationRegistry
from registry.configuration_auth_credentials import load_auth_class
from toolkit.more_typing import JSON_DICT, JSON_LIST
from toolkit.observability import EVENT_TYPE_KEY, EventType, Status

from .configuration import AirflowConfiguration
from .constants import COMPONENT_TOOL
from .lib import get_status

LOG = logging.getLogger(__name__)

RETRY_CONFIG = HTTPRetryConfig(status_code=HTTPStatus.SERVICE_UNAVAILABLE, retry_count=5)


class AirflowListDagIDsEndpoint(HTTPAPIRequestHandle):
    path = "dags"
    method = HTTPMethod.GET


class AirflowListRunsEndpoint(HTTPAPIRequestHandle):
    path = "dags/~/dagRuns/list"
    method = HTTPMethod.POST


class AirflowGetRunEndpoint(HTTPAPIRequestHandle):
    path = "dags/{dag_id}/dagRuns/{dag_run_id}"
    method = HTTPMethod.GET


class AirflowListTaskInstancesEndpoint(HTTPAPIRequestHandle):
    path = "dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances"
    method = HTTPMethod.GET


@dataclass(kw_only=True, slots=True, frozen=True)
class AirflowTask:
    name: str
    timestamp: datetime
    finished: bool
    status: Status

    @classmethod
    def new(cls, json_data: JSON_DICT) -> AirflowTask:
        name = str(json_data["task_id"])
        status = get_status(str(json_data.get("state", "")))
        if status.finished:
            timestamp = parse_datetime(str(json_data["end_date"]))
        else:
            timestamp = parse_datetime(str(json_data["start_date"]))
        return cls(name=name, status=status, finished=status.finished, timestamp=timestamp)

    def key(self) -> int:
        """
        Returns a unique identifier of this task instance within a run.

        This is used to identify the task regardless of it's status.
        """
        return hash(("AirflowTask", self.name, self.timestamp))


class AirflowWatchTaskStatus(PeriodicTask):
    def __init__(
        self,
        *,
        pipeline_key: str,
        run_key: str,
        client: AsyncClient,
        outbound_channel: MemorySendChannel[JSON_DICT],
    ) -> None:
        super().__init__(outbound_channel=outbound_channel)
        registry = ConfigurationRegistry()
        self.configuration = registry.lookup("airflow", AirflowConfiguration)
        self.run_endpoint = AirflowGetRunEndpoint(
            base_url=URL(str(self.configuration.api_url)),
            client=client,
            retry_config=RETRY_CONFIG,
        )
        self.tasks_endpoint = AirflowListTaskInstancesEndpoint(
            base_url=URL(str(self.configuration.api_url)),
            client=client,
            retry_config=RETRY_CONFIG,
        )
        self.pipeline_key = pipeline_key
        self.run_key = run_key
        self.task_instances: dict[object, AirflowTask] = {}
        self.client = client

    async def execute(self, current_dt: datetime, previous_dt: datetime) -> None:
        await self.update_task_instances()
        await self.update_run_status()

    async def update_task_instances(self) -> None:
        response = await self.tasks_endpoint.handle(
            path_args={"dag_id": self.pipeline_key, "dag_run_id": self.run_key},
        )

        # TODO: Better error handling
        if not response:
            LOG.warning("Failed to get task instance for pipeline_key=%s run_key=%s", self.pipeline_key, self.run_key)
            return

        data = response.json()["task_instances"]
        tasks = [AirflowTask.new(x) for x in data]
        for task in tasks:
            if prev_instance := self.task_instances.get(task.key):
                # Don't resend the event if the task is already done or the status hasn't changed
                if prev_instance.finished is True or prev_instance.status == task.status:
                    continue
            else:
                self.task_instances[task.key] = task

            event_payload: JSON_DICT = {
                EVENT_TYPE_KEY: EventType.RUN_STATUS.value,
                "event_timestamp": task.timestamp.isoformat(),
                "status": task.status.value,
                "pipeline_key": self.pipeline_key,
                "run_key": self.run_key,
                "task_key": task.name,
                "metadata": {},
                "component_tool": COMPONENT_TOOL,
            }
            await self.send(payload=event_payload)

    async def update_run_status(self) -> None:
        response = await self.run_endpoint.handle(
            path_args={"dag_id": self.pipeline_key, "dag_run_id": self.run_key},
        )

        # TODO: Better error handling
        if not response:
            LOG.warning("Failed to get dag run for pipeline_key=%s run_key=%s", self.pipeline_key, self.run_key)
            return

        data = response.json()
        status = get_status(str(data.get("state", "")))
        if status.finished:
            LOG.info("Finishing run %s %s", self.pipeline_key, self.run_key)
            timestamp = parse_datetime(str(data.get("end_date")))
            event_payload: JSON_DICT = {
                EVENT_TYPE_KEY: EventType.RUN_STATUS.value,
                "event_timestamp": timestamp.isoformat(),
                "status": status.value,
                "pipeline_key": self.pipeline_key,
                "run_key": self.run_key,
                "metadata": {},
                "component_tool": COMPONENT_TOOL,
            }
            await self.send(payload=event_payload)
            self.finish()


class AirflowListRunsTask(PeriodicTask):
    def __init__(
        self,
        nursery: Nursery,
        outbound_channel: MemorySendChannel[JSON_DICT],
        **kwargs: Any,
    ):
        super().__init__(outbound_channel=outbound_channel, **kwargs)
        registry = ConfigurationRegistry()
        self.configuration = registry.lookup("airflow", AirflowConfiguration)
        auth = load_auth_class()
        self.http_config = registry.mutate(
            "http",
            HTTPClientConfig,
            auth=auth,
        )
        self.client = get_client(self.http_config)
        LOG.info("Setting up Airflow Agent with HTTP Config: %s", self.http_config)
        self.endpoint = AirflowListRunsEndpoint(
            base_url=URL(str(self.configuration.api_url)),
            client=self.client,
            retry_config=RETRY_CONFIG,
        )
        self.dag_list_endpoint = AirflowListDagIDsEndpoint(
            base_url=URL(str(self.configuration.api_url)),
            client=self.client,
            retry_config=RETRY_CONFIG,
        )
        self.nursery = nursery
        self.watched_tasks: dict[str, AirflowWatchTaskStatus] = {}

    async def __aenter__(self) -> Self:
        await self.client.__aenter__()
        return await super().__aenter__()

    async def __aexit__(self, exc_type: type[BaseException], exc_val: BaseException, exc_tb: TracebackType) -> None:
        await self.client.__aexit__(exc_type, exc_val, exc_tb)
        return await super().__aexit__(exc_type, exc_val, exc_tb)

    async def get_dag_ids(self) -> list[str]:
        response = await self.dag_list_endpoint.handle()
        try:
            ids = [d["dag_id"] for d in response.json()["dags"]]
        except Exception:
            LOG.exception("Unable to parse response: %s", response.text)
            raise
        else:
            LOG.debug("DAG IDs: %s", ", ".join(ids))
        return sorted(ids)

    async def execute(self, current_dt: datetime, previous_dt: datetime) -> None:
        dag_ids = await self.get_dag_ids()
        prev_dt = previous_dt.astimezone(UTC).isoformat()
        curr_dt = current_dt.astimezone(UTC).isoformat()
        payload: JSON_DICT = {
            "dag_ids": cast(JSON_LIST, dag_ids),
            "execution_date_gte": prev_dt,
            "execution_date_lte": curr_dt,
        }

        response = await self.endpoint.handle(payload=payload)
        if not response:
            LOG.warning("Failed to list job runs")
            return

        data = response.json()
        for run_data in data.get("dag_runs", ()):
            pipeline_key = run_data.get("dag_id")
            run_key = run_data.get("dag_run_id")
            task = AirflowWatchTaskStatus(
                pipeline_key=pipeline_key,
                run_key=run_key,
                client=self.client,
                outbound_channel=cast(MemorySendChannel[JSON_DICT], self.outbound_channel.clone()),
            )
            self.nursery.start_soon(PeriodicLoop(period=self.configuration.period, task=task).run)
            watch_key = f"{pipeline_key}|{run_key}"
            self.watched_tasks[watch_key] = task

        finished: list[str] = []
        for key, task in self.watched_tasks.items():
            if task.is_done:
                finished.append(key)
        for finished_key in finished:
            self.watched_tasks.pop(finished_key)
            LOG.info("Removed finished watch task: %s", finished_key)
