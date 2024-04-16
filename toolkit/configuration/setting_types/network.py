from typing import Annotated

from pydantic import AnyUrl, Field, UrlConstraints

WebSocketUrl = Annotated[AnyUrl, UrlConstraints(allowed_schemes=["wss", "ws"])]
"""Valid Websocket URL."""

NetworkPortNumber = Annotated[int, Field(ge=1, lt=2**16)]
"""Integer field limited accordingly to the valid range for network port numbers."""
