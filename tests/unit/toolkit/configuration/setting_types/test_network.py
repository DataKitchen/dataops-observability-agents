import pytest
from pydantic import BaseModel, ValidationError

from toolkit.configuration.setting_types import WebSocketUrl


class WebsocketModel(BaseModel):
    url: WebSocketUrl


@pytest.mark.unit()
def test_websocket_url():
    WebsocketModel(url="ws://example.com")
    WebsocketModel(url="wss://example.com")

    with pytest.raises(ValidationError):
        WebsocketModel(url="http://example.com")
