import json

from trio_websocket import WebSocketConnection

from framework.core.handles import Handle
from toolkit.more_typing import JSON_DICT
from toolkit.observability import EVENT_TYPE_KEY, EventType


class WebsocketHandle(Handle[str, JSON_DICT]):
    def __init__(self, connection: WebSocketConnection):
        self.connection = connection

    async def pre_hook(self) -> None:
        pass

    async def handle(self) -> str:
        message: str = await self.connection.get_message()
        return message

    async def post_hook(self, value: str) -> JSON_DICT:
        payload: JSON_DICT = json.loads(value)
        payload[EVENT_TYPE_KEY] = EventType.RUN_STATUS.value
        return payload
