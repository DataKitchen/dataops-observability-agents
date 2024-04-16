import logging

from trio import TooSlowError
from trio_websocket import ConnectionClosed, WebSocketConnection, open_websocket_url

from framework.core.loops import Loop
from toolkit.configuration.setting_types import WebSocketUrl

from .task import WebsocketTask

LOGGER = logging.getLogger(__name__)


class WebsocketLoop(Loop):
    def __init__(self, task: WebsocketTask, target_url: WebSocketUrl, connect_timeout: float):
        super().__init__(task)
        self.target_url: str = str(target_url)
        self.connect_timeout: float = connect_timeout

    async def run(self) -> None:
        with self.task:
            while True:
                try:
                    ws: WebSocketConnection
                    async with open_websocket_url(self.target_url, connect_timeout=self.connect_timeout) as ws:
                        while True:
                            try:
                                await self.task.execute_task(ws)
                            except TooSlowError:
                                continue
                except ConnectionClosed:
                    LOGGER.warning("Connection Closed! attempting to reconnect...")
