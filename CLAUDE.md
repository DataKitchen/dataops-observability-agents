# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

DataOps Observability Agents — async integration framework that connects data tools (Airflow, Databricks, EventHub, Power BI, Qlik, SSIS, Synapse Analytics) to DataKitchen's DataOps Observability platform. Sends logs, metrics, runtime events, and scheduling info. Python 3.12+, built on **trio** (structured concurrency) and **pydantic** (config validation).

## Common Commands

```bash
# Setup
pip install -e '.[dev,release]'
pre-commit install

# Quality
inv lint          # black + ruff --fix
inv mypy          # type-check entire repo

# Testing (trio_mode=true, run from repo root)
pytest .                              # all tests
pytest tests/unit/                    # unit tests only
pytest -m unit                        # by marker
pytest tests/unit/framework/test_states.py  # single file
pytest -k "test_name"                 # single test by name

# Docker
inv build-image                       # local single-platform build
inv build-image --tag mytag           # with custom tag
inv build-public-image --version X.Y.Z --push   # multi-platform (amd64+arm64) build & push to Docker Hub
inv build-public-image --version X.Y.Z --local   # multi-platform build, load locally (no push)

# Release notes
towncrier create <issue>.added.md     # categories: fixed, added, deprecated, removed, changed, chore
```

## Test Requirements

- Every test must have a marker: `@pytest.mark.unit`, `@pytest.mark.integration`, `@pytest.mark.functional`, or `@pytest.mark.slow` — enforced by pre-commit hook.
- Test class naming: `Test_*`, test function naming: `test_*`.
- Shared fixtures live in `testlib/**/fixtures/` and are auto-discovered.
- Async tests work out of the box (pytest-trio enabled globally).

## Architecture

**Entry point**: `observability-agent` CLI → `framework.__main__:cli()` → `trio.run(main())`

**Four main packages**:
- **framework/** — Core async infrastructure: tasks, loops, handles, channels, authenticators, observability event sending, heartbeat, state management.
- **agents/** — Individual agent implementations. Each agent has: `agent.py` (async main), `configuration.py` (pydantic settings), `loop.py`, `task.py`, `handles.py`.
- **registry/** — Singleton configuration registry. Reads from `agent.toml` (local) or `/etc/observability/agent.toml` (deployed). TOML sections map to config types: `[core]`, `[databricks]`, `[auth_*]`, `[http]`, `[observability]`.
- **toolkit/** — Shared utilities: logging, typing, constants, exceptions.

**Agent lifecycle** (typical pattern):
1. Load config from registry
2. Create trio memory channel pair (producer/consumer event queue)
3. Open trio nursery for concurrent tasks:
   - Polling/collection loop → writes events to channel
   - ChannelReceiveLoop → reads events, feeds to EventSenderTask (HTTP POST to Observability)
   - Heartbeat loop

**Key patterns**:
- `Task` (ABC) — orchestrates Handles, sends JSON payloads via outbound channel, async context manager.
- `Loop` — runs a Task repeatedly. `ChannelReceiveLoop` drains a MemoryReceiveChannel.
- `UnrecoverableError` — fatal error (e.g., bad credentials), causes agent exit with code 1.
- `ConfigurationRegistry` — singleton with lazy init; `register()`, `lookup()`, `available()`.

## Docker

- **Only active image**: `datakitchen/dataops-observability-agents` on Docker Hub (multi-platform: amd64 + arm64).
- **Dockerfile**: `deploy/docker/observability-agent.dockerfile` — multi-stage build (build stage compiles deps, runtime stage is minimal).
- **Docker bake config**: `deploy/docker/docker-bake.json` — tags `latest`, `vX.Y.Z`, `vX.Y`, `vX`.
- **ODBC install script**: `deploy/docker/install_linuxodbc.sh` — installs MS ODBC driver for SQL Server.
- **Invoke tasks**: `inv build-image` (local), `inv build-public-image` (multi-platform + Docker Hub).
- **Security scanning**: `docker scout cves datakitchen/dataops-observability-agents` to check for vulnerabilities.
- Transitive Python deps with security lower-bound pins are in `pyproject.toml` (urllib3, aiohttp, azure-core, cryptography).
- pip is uninstalled in the runtime stage since it's not needed and carries CVEs.
- `legacy_agents/` and `dbt-core-connector/` have their own Dockerfiles but are not actively published.

## Code Style

- **black**: line-length=120, target Python 3.11+
- **ruff**: 30+ rule categories enabled (see pyproject.toml `[tool.ruff.select]`)
- **mypy**: strict mode — `check_untyped_defs`, `disallow_untyped_defs`, `disallow_untyped_decorators`, `warn_return_any`
