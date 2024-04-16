ARG BASE_IMAGE_URL
FROM ${BASE_IMAGE_URL}python:3.11-slim-bookworm AS build-image
LABEL maintainer="DataKitchen"

RUN apt-get update && \
    apt-get install -y curl gnupg2 && \
    curl -sS https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg && \
    curl -sS https://packages.microsoft.com/config/debian/12/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
    echo msodbcsql18 msodbcsql/ACCEPT_EULA boolean true | debconf-set-selections && \
    apt-get update &&  \
    DEBIAN_FRONTEND="noninteractive" ACCEPT_EULA=Y apt-get install -y msodbcsql18 && \
    apt-get remove -y curl gnupg2 && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p /tmp/dk
RUN mkdir /dk

# Installing the dependencies first, so that this layer wont change too often
COPY pyproject.toml /tmp/dk/
# The __init__.py of the framework package has the agent version string and is referenced in pyproject.toml
COPY framework/__init__.py /tmp/dk/framework/__init__.py
RUN python3 -O -m pip install /tmp/dk --prefix=/dk

# Now installing eveything else. We use --no-deps to avoid resolving the dependencies a second time
COPY . /tmp/dk/
RUN python3 -O -m pip install --no-deps /tmp/dk --prefix=/dk

# Cleaning up unused files
RUN rm -Rf /tmp/dk

ENV PYTHONPATH=${PYTHONPATH}:/dk/lib/python3.11/site-packages \
    PATH=${PATH}:/dk/bin

ARG USERNAME=dkagent
ARG USER_UID=10001
ARG USER_GID=10002

RUN groupadd --gid $USER_GID $USERNAME && \
    useradd --uid $USER_UID --gid $USER_GID -m $USERNAME

USER $USERNAME:$USERNAME

ENTRYPOINT ["observability-agent"]
