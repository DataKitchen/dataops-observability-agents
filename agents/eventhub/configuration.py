from datetime import datetime
from typing import Any

import dateutil
from annotated_types import Annotated, Len
from dateutil.parser import ParserError
from pydantic import BaseModel, ValidationError, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class EventhubConfiguration(BaseSettings):
    name: str
    """Eventhub name"""
    connection_string: str
    """Eventhub connection string."""
    message_types: Annotated[set[str], Len(min_length=1)]
    """The list of types / messages that the agent will attempt to handle"""
    consumer_group: str = "$Default"
    """Name of the eventhub consumer group."""
    starting_position: str | datetime | int = "-1"
    """The starting position of the partition. "-1" means 'from the beginning of the partition.'"""

    model_config = SettingsConfigDict(env_prefix="DK_AZURE_EVENTHUB_")

    @field_validator("starting_position", mode="before")
    @classmethod
    def validate_starting_position(cls, value: Any) -> datetime | int | str:
        """
        According to the documentation I can glean, starting position can be a bunch of different things.

        * a "-1" or "@latest"
        * an integer index
        * A datetime
        * A dictionary of partition_id: starting_position

        We'll only support the first 3 for now.
        """
        if isinstance(value, datetime):
            return value
        elif isinstance(value, int):
            if value > 0:
                return value
            elif value == -1:
                # It's unclear if negative values are accepted, as "-1" (the string) is called out as a special
                # value. We'll just accept > -1 and cast -1 to a string to be ultra-sure.
                return str(value)
            else:
                raise ValidationError("starting position must be > -1")
        elif isinstance(value, str):
            # It's unclear from the documentation what other strings may be accepted. These are called out specifically.
            if value in ("@latest", "-1"):
                return value
            try:
                # Could possibly be a  datetime string. If so, we'll just parse it to a datetime. Pydantic
                # may also handle this in some cases.
                return dateutil.parser.parse(value)
            except ParserError:
                raise ValidationError(f"Invalid string: {value}") from None
        raise ValidationError(f"Unknown input type: {type(value)!s}")


class EventhubBlobConfiguration(BaseModel):
    model_config = SettingsConfigDict(env_prefix="DK_AZURE_BLOB_")
    name: str
