from contextlib import AbstractContextManager, contextmanager

from pydantic import ValidationError

from toolkit import parse_validation_error


@contextmanager
def better_validation_message() -> AbstractContextManager[None]:
    try:
        yield None
    except ValidationError as e:
        print(parse_validation_error(e))  # noqa: T201
        raise
