"""
Tasks shared between the various work-modes.
"""
__all__ = ["is_venv"]

import sys

from invoke import Context, task


@task
def is_venv(ctx: Context) -> None:
    """
    Check if one is an environment. Note: Invoke de-duplicates redundant tasks. Do not worry if this is
    in multiple consecutive tasks.
    """
    if not (hasattr(sys, "real_prefix") or (hasattr(sys, "base_prefix") and sys.base_prefix != sys.prefix)):
        raise RuntimeError("You have not sourced a virtual environment.")
