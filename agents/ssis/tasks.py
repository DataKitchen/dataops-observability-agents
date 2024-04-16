import logging
import re
from datetime import datetime
from itertools import groupby, islice
from typing import cast

from trio import MemorySendChannel

from framework.core.tasks import ChannelTask, PeriodicTask
from toolkit.more_typing import JSON_DICT
from toolkit.observability import Status

from .agent_state import AGENT_STATE, StateMonitoring
from .core import (
    COMPONENT_TOOL,
    STAT_RESULT_TO_RUN_STATUS_MAP,
    ExecutableStatistic,
    Execution,
    calculate_status_transitions,
)
from .database import AsyncConn

LOGGER = logging.getLogger(__name__)


class SsisFetchNewExecutionsTask(PeriodicTask):
    """
    Polls the SSIS catalog looking for not-yet-known Executions, based on their IDs. New Executions are registered
    within the internal agent state to be monitored.

    This task do not send the Executions found downstream to prevent race conditions.
    """

    def __init__(
        self,
        db_conn: AsyncConn,
    ) -> None:
        super().__init__(outbound_channel=None)
        self.db_conn = db_conn

    async def execute(self, current_dt: datetime, previous_dt: datetime) -> None:
        if AGENT_STATE.last_known_execution_id is None:
            LOGGER.info("Agent is starting from an empty state.")
            query = "SELECT MAX([execution_id]) FROM [catalog].[executions]"
            res = self.db_conn.exec_and_fetch_all(query)
            try:
                AGENT_STATE.last_known_execution_id = (await anext(res))[0]
            except StopAsyncIteration:
                LOGGER.info("No Executions found.")
            else:
                LOGGER.info("Execution ID %d marked as being the last known.", AGENT_STATE.last_known_execution_id)
        else:
            LOGGER.info("Searching for new Executions, newer than ID %d.", AGENT_STATE.last_known_execution_id)
            count = 0
            last_id = 0

            query = "SELECT [execution_id] FROM [catalog].[executions] WHERE [execution_id] > ?"
            async for execution_id, in self.db_conn.exec_and_fetch_all(query, [AGENT_STATE.last_known_execution_id]):
                AGENT_STATE.start_monitoring(execution_id)
                count += 1
                last_id = max(last_id, execution_id)

            if count:
                LOGGER.info("Found %d new Executions to monitor. New last Execution ID is %d.", count, last_id)
                AGENT_STATE.last_known_execution_id = last_id
            else:
                LOGGER.info("No new Executions were found.")


class SsisFindUpdatedExecutionsTask(PeriodicTask):
    """
    Iterate through the monitored Executions, polling the SSIS catalog for Executions that changed status since the
    previous iteration. Updated Executions are sent downstream for event processing.
    """

    def __init__(
        self,
        db_conn: AsyncConn,
        outbound_channel: MemorySendChannel[Execution],
    ) -> None:
        super().__init__(outbound_channel=outbound_channel)
        self.db_conn = db_conn

    async def execute(self, current_dt: datetime, previous_dt: datetime) -> None:
        exec_states = AGENT_STATE.get_monitored_executions(StateMonitoring.STATUS_CHANGE)
        sorted_exec_states = sorted(exec_states, key=lambda es: es.last_seen_status.value)
        for status, exec_state_list in groupby(sorted_exec_states, key=lambda es: es.last_seen_status):
            LOGGER.info("Searching %s Executions for updates.", status.name)
            exec_ids = ", ".join([str(es.execution_id) for es in exec_state_list])
            query = f"""
                SELECT [execution_id], [status], [start_time], [end_time], [folder_name], [project_name], [package_name]
                FROM [catalog].[executions]
                WHERE [execution_id] IN ({exec_ids}) AND [status] != ?
            """
            async for execution in self.db_conn.exec_and_fetch_all(query, [status.value], model=Execution):
                LOGGER.info("Updates detected for Execution ID %d", execution.execution_id)
                await self.send(execution)


class SsisHandleUpdatedExecutionTask(ChannelTask[Execution]):
    """
    Receives Executions that have had a recent status transition and generate events according to the transition. The
    events are then sent downstream for posting.

    Executions are updated as the SSIS package runs, so this task has to build the Execution history from sparse
    snapshots. Additionally, removes the Executions from the status change monitoring when they reach a final status.
    """

    async def execute(self, receivable: Execution) -> None:
        execution = receivable
        exec_state = AGENT_STATE.monitored_executions[execution.execution_id]

        LOGGER.info("Updating Execution ID %d", exec_state.execution_id)

        common_event_payload: JSON_DICT = {
            "EVENT_TYPE": "run-status",
            "pipeline_key": execution.pipeline_key,
            "pipeline_name": execution.package_name,
            "component_tool": COMPONENT_TOOL,
            "run_key": execution.run_key,
        }

        status: Status | None = None
        LOGGER.info("Handling %s -> %s status transition.", exec_state.last_seen_status.name, execution.status_obj.name)
        for status in calculate_status_transitions(exec_state.last_seen_status, execution.status_obj):
            event_payload = common_event_payload.copy()
            event_payload["status"] = status.value
            match status:
                case Status.RUNNING:
                    event_payload["event_timestamp"] = execution.start_time.isoformat()
                case s if s.finished:
                    event_payload["event_timestamp"] = cast(datetime, execution.end_time).isoformat()
                case _:
                    LOGGER.error("Could not handle %s status transition.", status.name)

            LOGGER.info("Emitting %s event.", status.name)
            await self.send(event_payload)

        if status is None:
            LOGGER.info("Transition did not generate events.")  # type: ignore[unreachable]

        if status and status.finished:
            AGENT_STATE.stop_monitoring(execution.execution_id, StateMonitoring.STATUS_CHANGE)
        else:
            exec_state.last_seen_status = execution.status_obj


class SsisFindAddedExecutableStatisticsTask(PeriodicTask):
    """
    Polls the SSIS catalog looking for new ExecutableStatistics, sending them downstream for event processing.

    A ExecutableStatistics is considered _new_ when its ID is greater than the last seen statistics ID, which is
    part of the ExecutionState. Additionally, removes Executions from the added statistics monitoring when
    they are regarded as done.
    """

    QUERY_BATCH_SIZE = 100
    """Determines how many Execution criteria are OR'ed together to build the database query."""

    def __init__(
        self,
        db_conn: AsyncConn,
        outbound_channel: MemorySendChannel[ExecutableStatistic],
    ) -> None:
        super().__init__(outbound_channel=outbound_channel)
        self.db_conn = db_conn

    async def execute(self, current_dt: datetime, previous_dt: datetime) -> None:
        monitored_executions_list = list(AGENT_STATE.get_monitored_executions(StateMonitoring.STATISTICS_ADDED))
        monitoring_to_stop = {state.execution_id for state in monitored_executions_list}
        monitored_executions = iter(monitored_executions_list)
        while True:
            clauses = [
                f"[es].[execution_id] = {exec_state.execution_id} AND "
                f"[es].[statistics_id] > {exec_state.last_seen_statistic_id}"
                for exec_state in islice(monitored_executions, self.QUERY_BATCH_SIZE)
            ]

            if not clauses:
                LOGGER.info("Done searching new Tasks")
                break

            LOGGER.info("Searching for new Tasks among %d monitored Executions.", len(clauses))

            query = f"""
                SELECT
                    [statistics_id], [execution_path], [es].[start_time], [es].[end_time], [execution_result],
                    [e].[execution_id], [folder_name], [project_name], [package_name]
                FROM [catalog].[executable_statistics] AS [es]
                JOIN [catalog].[executions] as [e]  ON [es].[execution_id] = [e].[execution_id]
                WHERE (({") OR (".join(clauses)}))
                ORDER BY [statistics_id] ASC
            """

            async for stats in self.db_conn.exec_and_fetch_all(query, model=ExecutableStatistic):
                exec_state = AGENT_STATE.monitored_executions[stats.execution_id]
                exec_state.set_last_stat_id(stats.statistics_id)
                monitoring_to_stop.discard(stats.statistics_id)
                await self.send(stats)

            for state in monitored_executions_list:
                # When an Execution reached a final status and no new ExecutableStatistics are found, we assume that
                # all the ExecutableStatistics were previously capture by earlier iterations and therefore we
                # stop monitoring the Execution for new ExecutableStatistics.
                if not state.monitoring & StateMonitoring.STATUS_CHANGE and state.execution_id in monitoring_to_stop:
                    AGENT_STATE.stop_monitoring(state.execution_id, StateMonitoring.STATISTICS_ADDED)


class SsisHandleNewExecutableStatisticsTask(ChannelTask[ExecutableStatistic]):
    """
    Receives newly added ExecutableStatistics and generates the RunStatus events (for a Task), reporting the
    observed ExecutableStatistics status.

    In opposition to how Executions works, all found ExecutableStatistics are final and will not change.

    Given Observability does not support nesting Tasks, we try to detect and ignore container executables, based
    on their _execution path_. This logic assumes that all the inner executables will have their execution traces
    (the ExecutableStatistics) exposed on the catalog _before_ their containers, so we ignore statistics whose
    execution paths were previously collected.
    """

    async def execute(self, receivable: ExecutableStatistic) -> None:
        stat = receivable

        common_event_payload: JSON_DICT = {
            "EVENT_TYPE": "run-status",
            "pipeline_key": stat.pipeline_key,
            "pipeline_name": stat.pipeline_name,
            "component_tool": COMPONENT_TOOL,
            "run_key": stat.run_key,
            "task_key": stat.task_key,
            "task_name": stat.task_name,
        }

        container_executables = AGENT_STATE.monitored_executions[stat.execution_id].container_executables

        # If the executable path is already known as a container path, we ignore the ExecutableStatistics
        if stat.execution_path in container_executables:
            return

        # Adding the execution paths from container executables to the registry
        container_path, sep, _ = stat.execution_path.rpartition("\\")
        if sep:
            # When coming from a loop, the container path has a numeric index for the iteration. We remove it.
            container_executables.add(re.sub(r"\[\d+\]", "", container_path))

        async def emit_event(status: Status, timestamp: datetime) -> None:
            event_payload = common_event_payload.copy()
            event_payload["status"] = status.value
            event_payload["event_timestamp"] = timestamp.isoformat()
            LOGGER.info("Emitting %s Task event.", status.name)
            await self.send(event_payload)

        await emit_event(Status.RUNNING, stat.start_time)
        await emit_event(STAT_RESULT_TO_RUN_STATUS_MAP[stat.result_obj], stat.end_time)
