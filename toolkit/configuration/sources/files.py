import tomllib
from functools import cache
from logging import getLogger
from pathlib import Path
from typing import cast

LOGGER = getLogger(__name__)


@cache
def read_configuration_file(*potential_paths: Path | str, section: str, missing_ok: bool = False) -> dict:
    """
    Reads a configuration from the list of potential paths. The first one found is the one which is read.
    Therefore, the ordering of the arguments will matter.

    missing_ok: returns an empty dictionary if it fails to find any files or the relevant section.
    """
    result = {}
    for p in potential_paths:
        if (f := Path(p)).exists():
            result = tomllib.loads(f.read_text())
            LOGGER.info("Loaded toml file: %s", f.absolute())
            break
    if missing_ok:
        return cast(dict, result.get(section, {}))
    else:
        try:
            return cast(dict, result[section])
        except KeyError as k:
            if any(Path(p).exists() for p in potential_paths):
                raise KeyError(f"section '{section}' is not found in first-found configuration") from k
            else:
                raise FileNotFoundError(
                    f"Could not find configuration file. Searched: '{','.join(str(p) for p in potential_paths)}'",
                ) from k
