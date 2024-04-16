import pytest

from agents.ssis.tasks import SsisFetchNewExecutionsTask


@pytest.mark.unit()
@pytest.mark.parametrize(
    ("query_result", "last_id"),
    [((42,), 42), (None, None)],
    ids=["non-empty-table", "empty-table"],
)
async def test_empty_state(query_result, last_id, agent_state_mock, async_conn_mock):
    async def db_exec(*_):
        if query_result:
            yield query_result

    async_conn_mock.exec_and_fetch_all.side_effect = db_exec
    task = SsisFetchNewExecutionsTask(async_conn_mock)

    await task.execute(None, None)

    assert agent_state_mock.last_known_execution_id == last_id
    async_conn_mock.exec_and_fetch_all.assert_called_once()


@pytest.mark.unit()
async def test_no_new_execution_found(agent_state_mock, async_conn_mock):
    async def db_exec(*_):
        if False:
            yield

    async_conn_mock.exec_and_fetch_all.side_effect = db_exec
    agent_state_mock.last_known_execution_id = 42
    task = SsisFetchNewExecutionsTask(async_conn_mock)

    await task.execute(None, None)

    assert agent_state_mock.last_known_execution_id == 42
    agent_state_mock.start_monitoring.assert_not_called()
    async_conn_mock.exec_and_fetch_all.assert_called_once()


@pytest.mark.unit()
async def test_new_execution_found(agent_state_mock, async_conn_mock):
    async def db_exec(*_):
        yield (43,)

    async_conn_mock.exec_and_fetch_all.side_effect = db_exec
    agent_state_mock.last_known_execution_id = 42
    task = SsisFetchNewExecutionsTask(async_conn_mock)

    await task.execute(None, None)

    assert agent_state_mock.last_known_execution_id == 43
    agent_state_mock.start_monitoring.assert_called_once_with(43)
    async_conn_mock.exec_and_fetch_all.assert_called_once()
