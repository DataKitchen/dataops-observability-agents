from __future__ import annotations

import hashlib
import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List
from urllib.parse import urljoin

from attrs import define, field, validators
from dateutil import parser
from requests import Session

from common.events_publisher import EventsPublisher
from common.message_event_log_level import MessageEventLogLevel
from common.status import Status
from poller_agents.abstract_run import AbstractRun
from poller_agents.abstract_runs_fetcher import AbstractRunsFetcher

logger = logging.getLogger(__name__)

VALID_STATUS = ["executing", "execution_successful", "dispatching", "execution_failed"]


def get_status(record: dict) -> Status:
    status = record["status"]
    if status == "dispatching":
        return Status.RUNNING
    elif status == "executing":
        return Status.RUNNING
    elif status == "execution_successful":
        return Status.COMPLETED
    elif status == "execution_failed":
        return Status.FAILED
    elif status == "deploy_failed":
        return Status.FAILED
    else:
        logger.error(f"Unrecognized status: {status}. Setting status to {Status.UNKNOWN.name}")
        return Status.UNKNOWN


@define(kw_only=True, slots=False)
class TalendRunsFetcher(AbstractRunsFetcher):
    base_api_url: str = field(validator=validators.instance_of(str))
    session: Session = field(validator=validators.instance_of(Session))

    _component_tool: str = "talend"

    @property
    def agent_name(self) -> str:
        return f"{self.base_api_url}"

    @property
    def agent_key(self) -> str:
        md5 = hashlib.md5()
        md5.update(self.agent_name.encode("utf-8"))
        return md5.hexdigest()

    @property
    def component_tool(self) -> str:
        return self._component_tool

    @classmethod
    def create_runs_fetcher(cls, events_publisher: EventsPublisher) -> AbstractRunsFetcher:
        base_api_url = os.getenv("TALEND_BASE_API_URL")
        token = os.getenv("TALEND_TOKEN")
        if base_api_url is None:
            raise ValueError(f"TALEND_BASE_API_URL environment variable not found.")
        if token is None:
            raise ValueError(f"TALEND_TOKEN environment variable not found.")
        session = Session()
        session.verify = {"true": True, "false": False}[
            os.getenv("TARGET_VERIFY_SSL", "true").lower()
        ]
        session.headers.update({"Authorization": "Bearer {}".format(token)})
        return TalendRunsFetcher(
            events_publisher=events_publisher, base_api_url=base_api_url, session=session
        )

    # TODO retry decorator and handling of failed api requests
    def fetch_runs(
        self, execution_date_gte: datetime, execution_date_lte: datetime
    ) -> List[AbstractRun]:
        if not self.base_api_url.endswith("/"):
            self.base_api_url += "/"
        api_path = urljoin(self.base_api_url, "/executables/tasks/executions".lstrip("/"))
        execution_date_lte_milliseconds = int(execution_date_lte.timestamp() * 1000)
        execution_date_gte_milliseconds = int(execution_date_gte.timestamp() * 1000)
        logger.debug(
            f"Finding executions between - {execution_date_gte}: {execution_date_gte_milliseconds} and {execution_date_lte} : {execution_date_lte_milliseconds}"
        )
        response = self.session.get(
            api_path,
            params={"from": execution_date_gte_milliseconds, "to": execution_date_lte_milliseconds},
        )
        response.raise_for_status()
        return [self._create_talend_run(r) for r in response.json()["items"]]

    def _create_talend_run(self, record: dict) -> TalendRun:
        return TalendRun(
            events_publisher=self.events_publisher,
            pipeline_key=record[
                "taskId"
            ],  # it is jobId for the endpoint "/executions/{executionId}
            run_key=record["executionId"],
            base_api_url=self.base_api_url,
            session=self.session,
            agent_key=self.agent_key,
            agent_name=self.agent_name,
        )


@define(kw_only=True)
class TalendRun(AbstractRun):
    session: Session = field(validator=validators.instance_of(Session))
    base_api_url: str = field(validator=validators.instance_of(str))
    tasks: List[TalendTask] = field(default=[])
    external_url: str = field(default="", validator=validators.instance_of(str))

    _component_tool = "talend"

    @property
    def component_tool(self) -> str:
        return self._component_tool

    def update(self) -> None:
        logger.debug(f"Updating pipeline_key :{self.pipeline_key} run {self.run_key}....")
        api_path = f"{self.base_api_url}/executions/{self.run_key}"
        response = self.session.get(api_path)
        response.raise_for_status()
        response_json = response.json()

        workspace_id = response_json["workspaceId"]
        external_url_api_path = f"{self.base_api_url}/workspaces?query=id=={workspace_id}"
        response_external_url = (self.session.get(external_url_api_path)).json()
        environment_id = response_external_url[0]["environment"]["id"]
        self.external_url = f"https://tmc.us.cloud.talend.com/jobs-and-plans/{environment_id}/workspace/{workspace_id}/standard/{self.pipeline_key}/execution/{self.run_key}/run-overview/logs"
        self.update_tasks(response_json)
        status = get_status(response_json)

        if status.finished:
            self.finished = True
            event_timestamp = parser.parse(response_json["finishTimestamp"])
            logger.debug(f"Log Run finished ->  status : {status} ")
            self.publish_run_status_event(event_timestamp + timedelta(milliseconds=2), None, status)

    def update_tasks(self, record: dict) -> None:
        if len(self.tasks) == 0:
            # call create_task() for for each task (node) in the talend pipeline
            self.tasks = [self.create_task(r) for r in self.get_task_records_dict(record).values()]
        else:
            # this is when there are multiple tasks withing a pipeline for e.g. steps in an airflow DAG.  For talend just create one
            task_records_dict = self.get_task_records_dict(record)
            [t.update(task_records_dict[t.name]) for t in self.tasks if not t.finished]

    def get_task_records_dict(self, record: dict) -> dict:
        # Add logic to get details for multiple tasks(nodes) in the pipeline here
        output = {record["jobId"]: record}
        return output

    def create_task(self, record: dict) -> TalendTask:
        talend_task = TalendTask(talend_run=self, name=record["jobId"], metadata=record)
        talend_task.update(record)
        return talend_task


@define(kw_only=True)
class TalendTask:
    talend_run: TalendRun = field(validator=validators.instance_of(TalendRun))
    name: str = field(validator=validators.instance_of(str))
    event_published: bool = field(default=False, validator=validators.instance_of(bool))
    status: Status = field(default=Status.UNKNOWN, validator=validators.instance_of(Status))
    metadata: dict = field(default={}, validator=validators.instance_of(dict))

    @property
    def finished(self) -> bool:
        return self.status.finished

    def update(self, record: dict) -> str:
        prev_status = self.status
        self.status = get_status(record)
        logger.debug(
            f"Log previous state and current state of task : prev_status {prev_status} , current_status : {self.status} :"
        )
        self.metadata = {
            "userID": record["userId"],
            "jobVersion": record["jobVersion"],
            "executionType": record["executionType"],
            "executionDestination": record["executionDestination"],
            "accountId": record["accountId"],
            "workspaceId": record["workspaceId"],
        }
        task_key = self.name
        task_name = self.name

        if not self.event_published and self.status != Status.UNKNOWN:
            # if the event has not already been published and status is valid, publish task events
            event_timestamp = parser.parse(record["startTimestamp"])
            self.talend_run.publish_run_status_event(
                event_timestamp,
                task_key,
                Status.RUNNING,
                self.name,
                self.name,
                self.metadata,
                self.talend_run.external_url,
            )
            self.event_published = True
            if self.status.finished:
                if self.status == Status.FAILED:
                    # if job has failed, also send a message log event
                    error_message = record["errorMessage"]
                    logger.debug(
                        f"Log Task failed -> task_key : {task_key} , message : {error_message} , "
                        f"task_name : {task_name} , metadata : {self.metadata} , external_url : {self.talend_run.external_url}"
                    )
                    self.talend_run.publish_message_log_event(
                        event_timestamp,
                        task_key,
                        MessageEventLogLevel.ERROR,
                        error_message,
                        self.name,
                        self.name,
                        self.metadata,
                        self.talend_run.external_url,
                    )
                logger.debug(
                    f"Log Task finished -> task_key : {task_key} , status : {self.status} , "
                    f"task_name : {task_name} , metadata : {self.metadata} , external_url : {self.talend_run.external_url}"
                )
                self.talend_run.publish_run_status_event(
                    event_timestamp,
                    task_key=task_key,
                    status=self.status,
                    task_name=task_key,
                    metadata=self.metadata,
                    external_url=self.talend_run.external_url,
                )
        elif self.status != Status.UNKNOWN and prev_status != self.status:
            event_timestamp = parser.parse(record["finishTimestamp"])
            logger.debug(
                f"Log Task for new status -> task_key : {task_key} , status : {self.status} , "
                f"task_name : {task_name} , metadata : {self.metadata} , external_url : {self.talend_run.external_url}"
            )
            self.talend_run.publish_run_status_event(
                event_timestamp,
                task_key,
                self.status,
                None,
                task_key,
                self.metadata,
                self.talend_run.external_url,
            )
            if self.status == Status.FAILED:
                # if job has failed, also send a message log event
                error_message = record["errorMessage"]
                logger.debug(
                    f"Log Task failed -> task_key : {task_key} , message : {error_message} , "
                    f"task_name : {task_name} , metadata : {self.metadata} , external_url : {self.talend_run.external_url}"
                )
                self.talend_run.publish_message_log_event(
                    event_timestamp,
                    task_key,
                    MessageEventLogLevel.ERROR,
                    error_message,
                    self.name,
                    self.name,
                    self.metadata,
                    self.talend_run.external_url,
                )
        return self.name
