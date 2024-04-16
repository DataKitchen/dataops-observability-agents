"""
Contains premade type-hints related to JSONs.
"""
from typing import TypeAlias

JSON: TypeAlias = dict[str, "JSON"] | list["JSON"] | str | int | float | bool | None
# see: https://github.com/kevinheavey/jsonalias

JSON_DICT: TypeAlias = dict[str, "JSON"]
"""
A JSON type that is a complete object, i.e., {"foo": "bar"}
"""

JSON_LIST: TypeAlias = list["JSON"]
"""
A type-hint for lists of JSONs. i.e., "[{"foo": "bar"}]"
"""
