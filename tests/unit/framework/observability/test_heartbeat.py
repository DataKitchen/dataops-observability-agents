from datetime import UTC, datetime
from http import HTTPStatus

import pytest
from httpx import HTTPStatusError
from trio import open_nursery

from framework.observability import HeartbeatTask, create_heartbeat_loop
from toolkit.exceptions import UnrecoverableError


@pytest.fixture()
def now():
    return datetime.now(tz=UTC)


@pytest.mark.unit()
async def test_send_heartbeat_ok(now, httpx_mock, mock_core_env_vars):
    httpx_mock.add_response(HTTPStatus.ACCEPTED)
    h = HeartbeatTask(tool="test tool")
    await h.execute(now, now)


@pytest.mark.unit()
async def test_send_heartbeat_bad_request(now, httpx_mock, mock_core_env_vars):
    httpx_mock.add_response(HTTPStatus.BAD_REQUEST)
    h = HeartbeatTask(tool="test tool")
    with pytest.raises(HTTPStatusError):
        await h.execute(now, now)


@pytest.mark.unit()
def test_create_heartbeat_loop(mock_core_env_vars):
    assert create_heartbeat_loop(tool="test tool")


@pytest.mark.unit()
async def test_send_heartbeat_unauthorized(autojump_clock, now, httpx_mock, mock_core_env_vars):
    httpx_mock.add_response(HTTPStatus.UNAUTHORIZED)
    with pytest.raises(UnrecoverableError):
        async with open_nursery() as n:
            n.start_soon(create_heartbeat_loop(tool="test tool"))
