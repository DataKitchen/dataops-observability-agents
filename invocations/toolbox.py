import tomllib  # isort: skip
from os.path import join
from shutil import which

from invoke import Context, Exit


def ensure_tools(*tools: str) -> None:
    """
    Check the PATH to see if the required tools exist. e.g.,

    ensure_tools("git", "bash")
    """
    result = [f"ERROR: Required tool '{tool}' is not installed on your path." for tool in tools if which(tool) is None]
    if result:
        raise Exit(message="\n".join(msg for msg in result), code=1)


def get_project_name(ctx: Context) -> str:
    repo_root = get_repo_root(ctx)
    with open(join(repo_root, "pyproject.toml"), "rb") as f:
        return str(tomllib.load(f)["project"]["name"])


def get_repo_root(ctx: Context) -> str:
    result = ctx.run("git rev-parse --show-toplevel", hide=True)
    if result is not None:
        return result.stdout.strip()
    else:
        raise Exit("Could not find repository root.", code=1)
