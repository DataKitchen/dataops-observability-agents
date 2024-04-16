import logging
import re
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from types import EllipsisType
from typing import cast

from toolkit.observability import Status

LOGGER = logging.getLogger(__name__)

COMPONENT_TOOL: str = "ssis"


class ExecutionStatus(Enum):
    """
    Valid statuses for an Execution.

    Includes a NEW status that does not exist at the SSIS catalog database, but is
    useful as a default status for our internal representation of an Execution.
    """

    NEW = 0  # DK added status
    CREATED = 1
    RUNNING = 2
    CANCELED = 3
    FAILED = 4
    PENDING = 5
    ENDED_UNEXPECTEDLY = 6
    SUCCEEDED = 7
    STOPPING = 8
    COMPLETED = 9


class ExecutableStatisticResult(Enum):
    """Valid statuses for a ExecutableStatistic."""

    SUCCEEDED = 0
    FAILED = 1
    COMPLETED = 2
    CANCELED = 3


EXPECTED_STATUS_TRANSITIONS: tuple[
    tuple[Status, tuple[ExecutionStatus, ...] | EllipsisType, tuple[ExecutionStatus, ...]],
    ...,
] = (
    (
        Status.RUNNING,
        (ExecutionStatus.NEW, ExecutionStatus.CREATED, ExecutionStatus.PENDING),
        (
            ExecutionStatus.RUNNING,
            ExecutionStatus.FAILED,
            ExecutionStatus.SUCCEEDED,
            ExecutionStatus.COMPLETED,
            ExecutionStatus.ENDED_UNEXPECTEDLY,
        ),
    ),
    (
        Status.COMPLETED,
        ...,
        (ExecutionStatus.SUCCEEDED,),
    ),
    (
        Status.COMPLETED_WITH_WARNINGS,
        ...,
        (ExecutionStatus.COMPLETED,),
    ),
    (
        Status.FAILED,
        ...,
        (ExecutionStatus.FAILED, ExecutionStatus.ENDED_UNEXPECTEDLY),
    ),
)
"""
This tuple lists potential translations between Execution status and Observability Pipeline status transitions.

Each entry is a tuple consisting of:
- New Observability Pipeline status
- Previous Executable statuses tuple or Ellipses when the previous status does not matter
- Captured Executable statuses tuple
"""


STAT_RESULT_TO_RUN_STATUS_MAP: dict[ExecutableStatisticResult, Status] = {
    ExecutableStatisticResult.SUCCEEDED: Status.COMPLETED,
    ExecutableStatisticResult.COMPLETED: Status.COMPLETED_WITH_WARNINGS,
    ExecutableStatisticResult.CANCELED: Status.FAILED,
    ExecutableStatisticResult.FAILED: Status.FAILED,
}
"""Maps an ExecutableStatistic result to a Observability Task status."""


def calculate_status_transitions(prev_status: ExecutionStatus, reported_status: ExecutionStatus) -> Iterable[Status]:
    """
    Calculates the necessary Pipeline status that should be reported to Observability to properly represent
    the Execution captured status.

    As an example, an Execution found for the first time at the SUCCEEDED status requires the agent to sent a RunStatus
    event with RUNNING status (including the start time) and then another RunStatus with COMPLETED status (including
    end time).
    """
    yield from (
        status
        for status, expected_prev, expected_reported in EXPECTED_STATUS_TRANSITIONS
        if (expected_prev is ... or prev_status in cast(tuple, expected_prev)) and reported_status in expected_reported
    )


@dataclass
class ExecutionInfoMixIn:
    """MixIn class containing common fields for the entities based at the [executions] catalog table."""

    execution_id: int
    folder_name: str
    project_name: str
    package_name: str

    @property
    def pipeline_key(self) -> str:
        return f"{self.folder_name}/{self.project_name}/{self.package_name}"

    @property
    def pipeline_name(self) -> str:
        return re.sub(r"\.dtsx$", "", self.package_name)

    @property
    def run_key(self) -> str:
        return f"{self.pipeline_key}:{self.execution_id}"


@dataclass(kw_only=True)
class Execution(ExecutionInfoMixIn):
    """Entity based at the [executions] catalog table, matches the Observability Pipeline entity."""

    status: int
    start_time: datetime
    end_time: datetime | None

    @property
    def status_obj(self) -> ExecutionStatus:
        return ExecutionStatus(self.status)


@dataclass(kw_only=True)
class ExecutableStatistic(ExecutionInfoMixIn):
    """
    Entity based at the [executable_statistics] catalog table, matches the Observability Task entity. It includes some
    columns from the [executions] catalog table for processing convenience.
    """

    statistics_id: int
    execution_path: str
    start_time: datetime
    end_time: datetime
    execution_result: int

    @property
    def result_obj(self) -> ExecutableStatisticResult:
        return ExecutableStatisticResult(self.execution_result)

    @property
    def task_key(self) -> str:
        return f"{self.pipeline_key}:{self.statistics_id}"

    @property
    def task_name(self) -> str:
        return self.execution_path.rpartition("\\")[-1] or self.execution_path
