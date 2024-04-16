"""
Day-to-day developer tasks belong here.
"""

__all__ = ["install", "lint", "clean", "mypy", "precommit", "build_public_image", "build_image", "mk_note", "fix_quotes"]

import os
import sys
import termios
import tty
from datetime import UTC, datetime
from os.path import exists
from shutil import rmtree, which

from invoke import Context, task, Exit
from rich.prompt import IntPrompt, Prompt

from .common import is_venv
from .toolbox import ensure_tools, get_project_name

DOCKER_BUILDER_NAME = "dk-builder"
DOCKER_BUILDER_PLATFORMS = "linux/amd64,linux/arm64"


@task
def required_tools(ctx: Context) -> None:
    ensure_tools("git", "find", "docker")


@task(pre=(is_venv,))
def install(ctx: Context, quiet_pip: bool = False) -> None:
    name = get_project_name(ctx)
    """Installs the package as a developer (editable, all optional dependencies)."""
    if quiet_pip:
        print(f"{name} package is being re-installed.")
    ctx.run("pip install -e .[dev,release]", hide=quiet_pip)


@task(pre=(is_venv,))
def mk_note(ctx: Context) -> None:
    """Makes a release note with the proper names."""
    ticket_type = Prompt.ask(
        "What type of change is it?",
        choices=["fixed", "changed", "deprecated", "removed", "added", "chore"],
    )
    board_choices = ["AG", "OBS", "AUTO", "TG", "ITSD"]
    if ticket_type == "chore":
        # chore's don't explicity require ticket
        board_choices.append("")
    board = Prompt.ask("What is your ticket board?: ", choices=board_choices)
    if board:
        number = IntPrompt.ask("What is your ticket number? (e.g., 12): ")
        filename = f"{board}-{number}.{ticket_type}.md"
    else:
        filename = f"NA-{datetime.now(tz=UTC).isoformat()}.{ticket_type}.md"
    fd = sys.stdin.fileno()
    settings = termios.tcgetattr(fd)

    try:
        tty.setraw(sys.stdin.fileno())
        ctx.run(f"towncrier create {filename} --edit", pty=True)
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, settings)
    ctx.run(f"git add changelog.d/{filename}")


@task(pre=(is_venv,))
def lint(ctx: Context) -> None:
    """Runs the standard suite of quality/linting tools."""
    ctx.run("black .")
    ctx.run("ruff . --fix --show-fixes")
    print("Lint complete!")


@task(pre=(is_venv,))
def precommit(ctx: Context, all_files: bool = False) -> None:
    """Runs pre-commit."""
    if which("pre-commit") is None:
        install(ctx)
    if not exists(".git/hooks/pre-commit"):
        ctx.run("pre-commit install")

    command = "pre-commit run --all-files" if all_files else "pre-commit run"
    ctx.run(command)


@task(pre=(is_venv,))
def mypy(ctx: Context) -> None:
    ctx.run("mypy . --config-file=./pyproject.toml")


@task(iterable=["tag"])
def build_image(ctx: Context, tag: list[str]) -> None:
    """Builds the observability-agent image. Does not push."""
    if not tag:
        tags = " -t observability-agent "
    else:
        tags = " -t " + " -t ".join(t for t in tag)
    ctx.run(f"docker build . -f deploy/docker/observability-agent.dockerfile {tags}")


@task(pre=(required_tools,))
def build_public_image(ctx, version: str, push=False, local=False):
    """Builds and pushes the observability agent image"""
    use_cmd = f"docker buildx use {DOCKER_BUILDER_NAME}"
    if push and local:
        raise Exit("Cannot use --local and --push at the same time.")

    if not ctx.run(use_cmd, hide=True, warn=True).ok:
        ctx.run(f"docker buildx create --name {DOCKER_BUILDER_NAME} --platform {DOCKER_BUILDER_PLATFORMS}")
        ctx.run(use_cmd)

    extra_args = []
    if push:
        extra_args.append("--push")
    elif local:
        extra_args.extend(("--load", "--set=*.platform=$BUILDPLATFORM"))
    ctx.run(
        f"docker buildx bake -f deploy/docker/docker-bake.json agents {' '.join(extra_args)} ",
        env={"AGENT_VERSION": version},
        echo=True,
    )


@task(pre=(required_tools,))
def clean(ctx: Context) -> None:
    """deletes old python files and build artifacts"""
    project_name = get_project_name(ctx)

    ctx.run("find . -type f -name '*.py[co]' -delete -o -type d -name __pycache__ -delete")
    for d in ("dist", "build", f"{project_name}.egg-info", "deploy/pages/public/", "public/"):
        if exists(d):
            rmtree(d)
    for f in ("docs/PENDING_CHANGELOG.md", "docs/dev-README.md"):
        if exists(f):
            os.remove(f)

    print("Cleaning finished!")


@task(pre=(required_tools,))
def fix_quotes(ctx: Context) -> None:
    if os.path.exists("/etc/observability/agent.toml"):
        path = "/etc/observability/agent.toml"
    elif os.path.exists("agent.toml"):
        path = "agent.toml"
    else:
        raise FileNotFoundError("Could not find an agent.toml!")

    print(f"Converting smart quotes in {path} to normal quotes... ", end="")
    with open(path, "+r", encoding="utf8") as f:
        config = f.read()
    config = config.replace("”", '"')
    config = config.replace("“", '"')
    config = config.replace("‘", "'")  # noqa: RUF001
    config = config.replace("’", "'")  # noqa: RUF001
    with open(path, "+w", encoding="utf8") as f:
        f.write(config)
    print("Done.")
