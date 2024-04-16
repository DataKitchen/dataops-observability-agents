# See: https://medium.com/@nicolaikozel/modularizing-pytest-fixtures-fd40315c5a93
from glob import glob

pytest_plugins = [
    fixture_file.replace("/", ".").replace(".py", "")
    for fixture_file in glob("testlib/**/fixtures/[!__]*.py", recursive=True)
]
