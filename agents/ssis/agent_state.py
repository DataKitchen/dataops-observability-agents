__all__ = ["StateMonitoring", "ExecutionState", "AGENT_STATE"]

from collections.abc import Iterable
from dataclasses import dataclass, field
from enum import Flag, auto

from .core import LOGGER, ExecutionStatus


class StateMonitoring(Flag):
    """Flag that indicates what to monitor regarding an Execution."""

    STATUS_CHANGE = auto()
    STATISTICS_ADDED = auto()
    ALL = STATUS_CHANGE | STATISTICS_ADDED


@dataclass(kw_only=True)
class ExecutionState:
    """Holds the state of an Execution as per the agent concern."""

    execution_id: int
    monitoring: StateMonitoring = StateMonitoring.ALL
    last_seen_status: ExecutionStatus = ExecutionStatus.NEW
    last_seen_statistic_id: int = 0
    container_executables: set[str] = field(default_factory=set)

    def set_last_stat_id(self, stat_id: int) -> None:
        self.last_seen_statistic_id = max(self.last_seen_statistic_id, stat_id)


class AgentState:
    """
    This class is responsible for managing the global agent state, that affects multiple
    tasks.

    As a side effect of having all the state at the same place is that this class can evolve
    to also be responsible for storing and retrieving the state data.
    """

    def __init__(self) -> None:
        self.monitored_executions: dict[int, ExecutionState] = {}
        self.last_known_execution_id: int | None = None

    def start_monitoring(self, execution_id: int, monitoring: StateMonitoring = StateMonitoring.ALL) -> None:
        LOGGER.info("Execution ID %d added to the monitoring.", execution_id)
        self.monitored_executions[execution_id] = ExecutionState(execution_id=execution_id, monitoring=monitoring)

    def stop_monitoring(self, execution_id: int, monitoring: StateMonitoring) -> None:
        exec_state = self.monitored_executions[execution_id]
        exec_state.monitoring ^= exec_state.monitoring & monitoring

        if exec_state.monitoring:
            LOGGER.info("No longer monitoring %s for Execution ID %d.", monitoring.name, execution_id)
        else:
            LOGGER.info("No longer monitoring Execution ID %d.", execution_id)
            del self.monitored_executions[execution_id]

    def get_monitored_executions(self, monitoring: StateMonitoring) -> Iterable[ExecutionState]:
        if monitoring is StateMonitoring.ALL:
            raise ValueError("StateMonitoring.ALL should not be used to retrieve the monitored Executions.")
        yield from (state for state in self.monitored_executions.values() if state.monitoring & monitoring)


AGENT_STATE = AgentState()
"""Singleton instance of AgentState."""
