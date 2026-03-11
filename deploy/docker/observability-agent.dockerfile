ARG BASE_IMAGE_URL
FROM ${BASE_IMAGE_URL}python:3.12-alpine3.22 AS build-image
LABEL maintainer="DataKitchen"

RUN apk update && apk upgrade && apk add --no-cache \
    g++ \
    unixodbc-dev \
    curl \
    gnupg

COPY --chmod=775 ./deploy/docker/install_linuxodbc.sh /tmp/dk/install_linuxodbc.sh
RUN /tmp/dk/install_linuxodbc.sh

# Installing the dependencies first, so that this layer wont change too often
COPY pyproject.toml /tmp/dk/
# The __init__.py of the framework package has the agent version string and is referenced in pyproject.toml
COPY framework/__init__.py /tmp/dk/framework/__init__.py
RUN python3 -O -m pip install /tmp/dk --prefix=/dk

# Now installing eveything else. We use --no-deps to avoid resolving the dependencies a second time
COPY . /tmp/dk/
RUN python3 -O -m pip install --no-deps /tmp/dk --prefix=/dk

# --- Runtime stage: only what's needed to run the agent ---
FROM ${BASE_IMAGE_URL}python:3.12-alpine3.22 AS runtime-image

RUN apk update && apk upgrade && apk add --no-cache \
    unixodbc \
    libstdc++ \
    && pip uninstall -y pip

# Copy ODBC driver and tools installed by install_linuxodbc.sh
COPY --from=build-image /opt/microsoft /opt/microsoft
# Copy ODBC driver config registered via apk
COPY --from=build-image /etc/odbcinst.ini /etc/odbcinst.ini

# Copy pip-installed packages
COPY --from=build-image /dk /dk

ENV PYTHONPATH="/dk/lib/python3.12/site-packages" \
    PATH="/usr/local/bin:${PATH}:/dk/bin:/opt/microsoft/mssql-tools18/bin"

RUN addgroup -S dkagent && adduser -S dkagent -G dkagent
USER dkagent

ENTRYPOINT ["observability-agent"]
