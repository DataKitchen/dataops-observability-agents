# type: ignore
__all__ = ["clean", "dev_install", "mypy", "precommit", "unittest", "validate", "docs"]

import sys
from os.path import exists
from sys import stderr

from invoke import UnexpectedExit, call, task


@task
def is_venv(ctx):
    """
    Check if one is an environment. Note: Invoke de-duplicates redundant tasks. Do not worry if this is
    in multiple consecutive tasks.
    """
    if not (
        hasattr(sys, "real_prefix")
        or (hasattr(sys, "base_prefix") and sys.base_prefix != sys.prefix)
    ):
        raise RuntimeError("You have not sourced a virtual environment.")


@task(name="dev-install", pre=(is_venv,))
def dev_install(ctx, quiet_pip=False):
    """Installs the package as a developer (editable, all optional dependencies)."""
    if quiet_pip:
        print("observability-legacy-agents package is being re-installed.")
    ctx.run("pip install -e .[dev,build]", hide=quiet_pip)


@task
def clean(ctx):
    """Deletes old python files and build artifacts"""
    ctx.run("find . -type f -name '*.py[co]' -delete -o -type d -name __pycache__ -delete")
    ctx.run("make -C docs clean")
    if exists("dist"):
        ctx.run("rm -rf dist")
    if exists("observability-legacy-agents.egg-info"):
        ctx.run("rm -rf observability-legacy-agents.egg-info")
    if exists("build"):
        ctx.run("rm -rf build")
    if exists("type-check-coverage.xml"):
        ctx.run("rm -rf type-check-coverage.xml")
    if exists("index.txt"):
        ctx.run("rm -rf index.txt")
    if exists("unit-tests-coverage.xml"):
        ctx.run("rm -rf unit-tests-coverage.xml")
    if exists("unit-tests-results.xml"):
        ctx.run("rm -rf unit-tests-results.xml")


@task(pre=(is_venv,))
def precommit(ctx):
    """Run pre-commit on all files"""
    try:
        ctx.run("pre-commit run --all-files")
    except UnexpectedExit:
        print("pre-commit has failed - stopping validation.", file=stderr)
        raise


@task(pre=(is_venv,))
def mypy(ctx):
    """
    Run mypy and output a Type Check Coverage Summary (i.e. index.txt) and junit.xml report
    (i.e. type-check-coverage.xml)
    """
    ctx.run(
        "mypy . --txt-report . --junit-xml type-check-coverage.xml --config-file=./pyproject.toml"
    )


@task(pre=(is_venv,))
def unittest(ctx):
    """Run all unit tests"""
    ctx.run(
        "pytest -m 'unit' --cov --cov-report=term --cov-report=xml:unit-tests-coverage.xml --junitxml=unit-tests-results.xml ."
    )


@task(pre=(precommit, mypy, unittest))
def validate(ctx):
    """Run precommit, mypy, and unittests task functions all in one go"""
    print("Validation passed!")


@task(pre=(is_venv,))
def docs(ctx):
    """Build documentation"""
    ctx.run("make -C docs html")
