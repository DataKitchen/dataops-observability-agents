import logging

import pytest
from pydantic import BaseModel, ValidationError

from toolkit.logging_tools import level_from_string, parse_validation_error


class TestModel(BaseModel):
    foo: str
    bar: str


@pytest.mark.unit()
def test_parse_validation_error_empty_input():
    with pytest.raises(ValidationError) as f:
        TestModel()

    result = parse_validation_error(f.value)
    assert "foo" in result
    assert "bar" in result


@pytest.mark.unit()
def test_parse_validation_error_missing():
    with pytest.raises(ValidationError) as f:
        TestModel()
    with pytest.raises(ValidationError) as f:
        TestModel(bar="baz")

    result = parse_validation_error(f.value)
    assert "bar" in result
    assert "Field required" in result


@pytest.mark.unit()
def test_parse_validation_error_type():
    with pytest.raises(ValidationError) as f:
        TestModel()
    with pytest.raises(ValidationError) as f:
        TestModel(bar=10)

    result = parse_validation_error(f.value)
    assert "bar" in result
    assert "Input should be a valid string" in result


@pytest.mark.unit()
def test_log_levels():
    assert logging.WARNING == level_from_string("warning")
    assert logging.DEBUG == level_from_string("deBug")
    assert logging.INFO == level_from_string("INFO")
    assert logging.ERROR == level_from_string("Error")
    assert logging.INFO == level_from_string("  info  ")
