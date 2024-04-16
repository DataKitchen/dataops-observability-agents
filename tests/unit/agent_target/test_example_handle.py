from unittest.mock import AsyncMock

import pytest

from agents.target_example.handles import WebsocketHandle
from toolkit.observability import EVENT_TYPE_KEY


@pytest.fixture()
async def connection():
    return AsyncMock()


@pytest.mark.unit()
async def test_example_handle_call(connection):
    connection.get_message.return_value = "foo"
    handle = WebsocketHandle(connection=connection)
    result = await handle.handle()
    assert result == "foo"
    connection.get_message.assert_called_once_with()


@pytest.mark.unit()
async def test_example_post_hook(connection):
    handle = WebsocketHandle(connection=connection)
    payload = '{"foo": "bar"}'
    result = await handle.post_hook(payload)
    assert result["foo"] == "bar"
    assert EVENT_TYPE_KEY in result
