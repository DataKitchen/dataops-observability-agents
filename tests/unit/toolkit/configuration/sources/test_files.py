import tempfile
from pathlib import Path
from uuid import uuid4

import pytest
import tomli_w

from toolkit.configuration.sources import read_configuration_file


@pytest.fixture()
def example_toml_data() -> dict:
    return {
        "core": {"name": "My App", "version": "1.0", "author": "John Doe"},
        "noncore": {"name": "Not My App", "version": "2.0", "author": "Jane Doe"},
    }


@pytest.fixture()
def temporary_toml(example_toml_data):
    with tempfile.NamedTemporaryFile(mode="wb") as temp:
        # Write some example TOML data to the temporary file
        tomli_w.dump(example_toml_data, temp)
        temp.flush()
        temp.seek(0)
        yield temp


def _compare_simple_configuration(original: dict, actual: dict) -> None:
    assert len(original) == len(actual)
    for key in original:
        assert original[key] == actual[key]


@pytest.mark.unit()
@pytest.mark.parametrize("acceptable_type", [str, Path])
def test_read_configuration_file_types(temporary_toml, example_toml_data, acceptable_type):
    configuration_contents = read_configuration_file(acceptable_type(temporary_toml.name), section="noncore")
    _compare_simple_configuration(example_toml_data["noncore"], configuration_contents)


@pytest.mark.unit()
def test_read_configuration_multiple_paths(temporary_toml, example_toml_data):
    paths = [f"/tmp/{uuid4()}", f"/tmp/{uuid4()}", temporary_toml.name]
    configuration_contents = read_configuration_file(*paths, section="core")
    _compare_simple_configuration(example_toml_data["core"], configuration_contents)

    configuration_contents = read_configuration_file(*paths, section="core", missing_ok=True)
    _compare_simple_configuration(example_toml_data["core"], configuration_contents)


@pytest.mark.unit()
def test_read_configuration_no_files():
    paths = [f"/tmp/{uuid4()}", f"/tmp/{uuid4()}"]
    with pytest.raises(FileNotFoundError):
        read_configuration_file(*paths, section="core")
    configuration_contents = read_configuration_file(*paths, section="core", missing_ok=True)
    assert configuration_contents == {}


@pytest.mark.unit()
def test_configuration_missing_section(temporary_toml):
    with pytest.raises(KeyError):
        read_configuration_file(temporary_toml.name, section="doesnotexist", missing_ok=False)
