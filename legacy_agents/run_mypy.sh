#!/usr/bin/env bash

mypy . --txt-report . --junit-xml type-check-coverage.xml --config-file="./pyproject.toml"
