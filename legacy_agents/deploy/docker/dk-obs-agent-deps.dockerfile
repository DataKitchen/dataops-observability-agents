# Image that contains all the dependencies needed by the legacy agents. It uses pip to
# install an empty project
# DEV NOTE:  YOU MUST RUN `docker build` FROM THE `legacy_framework` FOLDER AND POINT TO THIS FILE.
# e.g., cd ~/../../legacy_framework && docker build . -f deploy/docker/dk-obs-agent-deps.dockerfile

# TODO: This image was pinned to bullseye on 2023-06-15. It has support for one more year from date.
FROM python:3.9-slim-bullseye AS build-image
LABEL maintainer="DataKitchen"

ARG DEBIAN_FRONTEND=noninteractive
RUN mkdir -p /tmp/dk && \
    apt-get update &&  \
    apt-get upgrade -y && \
    rm -rf /var/lib/apt/lists/*

# Copying only the dependencies file, so the project will be empty
# and only the dependencies will be built and installed
COPY ./pyproject.toml /tmp/dk/
RUN python3 -m pip install /tmp/dk --prefix=/dk
