# Observability Legacy Agent Framework

## Table of Contents
1. [Project Structure](#project-structure)
2. [Developer Setup](#developer-setup)
3. [Developer Experience](#developer-experience)

## Project Structure

**TODO:  FILL THIS OUT**

## Developer Setup

**This repository requires `Python 3.9` at minimum -- prefer the latest `3.9.X`.**

### Installation

Prefer using a virtual Python environment when installing. Tools such as
[virtualenv](https://virtualenv.pypa.io) can be used to set up the environment
using a specific Python version. [pyenv](https://github.com/pyenv/pyenv) can be
used to install the desired Python version if your choice of OS does not provide
it for you.

Example install
```bash
python3.9 -m venv venv
source venv/bin/activate
# Install platform and developer extra packages
pip install --editable '.[dev]'
```

### Testing

`pytest` is used to run test.
```bash
cd /to/legacy_framework
pytest
```

## Developer Experience

### Pre-commit + Linting

We enforce the use of certain linting tools. To not get caught by the build-system's checks, you should use
`pre-commit` to scan your commits before they go upstream.

The following hooks are enabled in pre-commit:

- `black`: The black formatter is enforced on the project. We use a basic configuration. Ideally this should solve any
and all formatting questions we might encounter.
- `isort`: the isort import-sorter is enforced on the project. We use it with the `black` profile.

To enable pre-commit from within your virtual environment, simply run:

```bash
pip install pre-commit
pre-commit install
```

### Additional tools

These tools should be used by the developer because the build-system will enforce that the code complies with them.
These tools are pinned in the `dev` extra-requirements of `pyproject.toml`, so you can acquire them with

```sh
# within environment
pip install .[dev]
```

We use the following additional tools:

- `pytest`: This tool is used to run the test e.g. `pytest .`
- `mypy`: This is a static and dynamic type-checking tool. This also checks for unreachable and non-returning code. See
`pyproject.toml` for its settings. This tool is not included in pre-commit because doing so would require installing
this repo's package and additional stubs into the pre-commit environment, which is not advised by pre-commit, and poorly
supported.
- `invoke` (shorthand `inv`): This is a `make` replacement.
  - Run `invoke --list` to see available commands and e.g. `invoke restart --help` for additional info on command `restart`.
  - [Shell tab completion](https://docs.pyinvoke.org/en/stable/invoke.html#shell-tab-completion)

### FAQ: mypy errors

#### I've encountered 'Unused "type: ignore" comment'

Good news, this means that `mypy` has found symbols for the thing which you are ignoring. That means its time to enable
type-checking on these code-paths.

To resolve this error, do two things:

1. Remove the ignore and fix any type errors.
2. run `mypy . --install-types` and add any newly installed `types-*` packages installed to our `dev` dependencies.
