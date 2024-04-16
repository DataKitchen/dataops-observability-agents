import pytest

from agents.ssis.agent_state import AgentState, StateMonitoring


@pytest.mark.unit()
def test_execution_state(execution_state):
    execution_state.set_last_stat_id(20)
    assert execution_state.last_seen_statistic_id == 20

    execution_state.set_last_stat_id(10)
    assert execution_state.last_seen_statistic_id == 20


@pytest.mark.unit()
def test_agent_state(execution_state):
    agent_state = AgentState()

    agent_state.start_monitoring(execution_id=42)
    exec_state = agent_state.monitored_executions[42]

    with pytest.raises(ValueError, match="ALL should not be used to retrieve the monitored Executions"):
        list(agent_state.get_monitored_executions(StateMonitoring.ALL))

    assert list(agent_state.get_monitored_executions(StateMonitoring.STATUS_CHANGE)) == [exec_state]
    assert list(agent_state.get_monitored_executions(StateMonitoring.STATISTICS_ADDED)) == [exec_state]

    agent_state.stop_monitoring(42, StateMonitoring.STATUS_CHANGE)

    assert list(agent_state.get_monitored_executions(StateMonitoring.STATUS_CHANGE)) == []
    assert list(agent_state.get_monitored_executions(StateMonitoring.STATISTICS_ADDED)) == [exec_state]

    agent_state.stop_monitoring(42, StateMonitoring.STATISTICS_ADDED)

    assert len(agent_state.monitored_executions) == 0
    assert list(agent_state.get_monitored_executions(StateMonitoring.STATUS_CHANGE)) == []
    assert list(agent_state.get_monitored_executions(StateMonitoring.STATISTICS_ADDED)) == []
