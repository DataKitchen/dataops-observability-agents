from http import HTTPStatus

import pytest
from trio import open_nursery

from framework.observability import EventSenderTask
from toolkit.exceptions import UnrecoverableError
from toolkit.observability import EVENT_TYPE_KEY


@pytest.mark.unit()
async def test_send_event_unauthorized(autojump_clock, now, httpx_mock, mock_core_env_vars):
    httpx_mock.add_response(HTTPStatus.UNAUTHORIZED)
    task = EventSenderTask()
    with pytest.raises(UnrecoverableError):
        async with open_nursery():
            await task.execute({EVENT_TYPE_KEY: "a"})
