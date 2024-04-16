__all__ = ["parse_validation_error", "level_from_string"]

import logging
from pprint import pformat

from pydantic import ValidationError


def level_from_string(level_name: str) -> int:
    """
    Get the integer log-level from the name.
    """
    # we cast it in case someone gets the nasty idea of injecting a string here.
    log_level = int(getattr(logging, level_name.strip().upper()))
    return log_level


def parse_validation_error(ex: ValidationError) -> str:
    """
    The traceback of a validationError can be verbose and hard to read on the commandline. This just provides
    a cleaned up string.
    """
    errors = ex.errors()
    msg = "Configuration validation error\n"
    msg += "\n".join(f"{','.join(str(loc) for loc in e['loc'])} - {e['msg']}" for e in errors)
    msg += "\nInput: " + pformat(errors[0]["input"])
    return msg
