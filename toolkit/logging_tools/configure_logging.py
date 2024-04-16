__all__ = ("logging_init",)

import os
from logging.config import dictConfig
from pathlib import Path


def logging_init(
    *,
    level: str = "INFO",
    agent_level: str = "WARNING",
    library_level: str = "ERROR",
    logfile: str | Path | None = None,
) -> None:
    """
    Given the log level and an optional logging file location, configure all logging.

    - level sets the default log level
    - library_level sets the level for libraries
    """

    # If we have a log make sure we can write to it
    if logfile:
        _path = Path(logfile).resolve()
        if not _path.parent.exists():
            _path.parent.mkdir(parents=True, exist_ok=True)
        if not os.access(_path.parent, os.W_OK):
            raise PermissionError(f"Logfile parent folder is not writable: {_path.parent}")
        log_handlers = ["console", "file"]
        handlers = {
            "console": {"level": level, "class": "logging.StreamHandler", "formatter": "basic"},
            "file": {"level": level, "class": "logging.FileHandler", "filename": str(_path), "formatter": "basic"},
        }
    else:
        log_handlers = ["console"]
        handlers = {"console": {"level": level, "class": "logging.StreamHandler", "formatter": "basic"}}

    # Setup the loggers
    dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": True,
            "formatters": {"basic": {"format": "%(asctime)s %(levelname)8s %(message)s - %(name)s:%(lineno)s"}},
            "handlers": handlers,
            "loggers": {
                "agents": {"handlers": log_handlers, "level": level, "propagate": False},
                "toolkit": {"handlers": log_handlers, "level": agent_level, "propagate": False},
                "framework": {"handlers": log_handlers, "level": agent_level, "propagate": False},
                "registry": {"handlers": log_handlers, "level": agent_level, "propagate": False},
                "urllib3": {"handlers": log_handlers, "level": library_level, "propagate": False},
                "requests": {"handlers": log_handlers, "level": library_level, "propagate": False},
                "httpx": {"handlers": log_handlers, "level": library_level, "propagate": False},
                "trio": {"handlers": log_handlers, "level": library_level, "propagate": False},
                "pydantic": {"handlers": log_handlers, "level": library_level, "propagate": False},
                "": {"handlers": log_handlers, "level": library_level, "propagate": False},
            },
        },
    )
