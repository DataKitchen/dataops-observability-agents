# DEV NOTE:  YOU MUST RUN `docker build` FROM THE `legacy_framework` FOLDER AND POINT TO THIS FILE.
# e.g., cd ~/../../legacy_framework && docker build . -f deploy/docker/dk-agent-obs-agent-base.dockerfile

ARG tag
FROM dk-obs-agent-deps:$tag AS build-image
LABEL maintainer="DataKitchen"

# Copy and build the actual application
# TODO: ARE THE NEXT 3 LAYERS REALLY NECESSARY?
# TODO: CAN FOLLOWING LINES SIMPLY BE ADDED TO THE DEPS IMAGE AND THIS ONE REMOVED?
COPY . /tmp/dk/
ENV PYTHONPATH ${PYTHONPATH}:/dk/lib/python3.9/site-packages
RUN python3 -m pip install --no-cache-dir /tmp/dk --prefix=/dk

# TODO: This image was pinned to bullseye on 2023-06-15. It has support for one more year from date.
FROM python:3.9-slim-bullseye AS runtime-image

# This is to install ODBC-18 for pyodbc.
ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update && \
    apt-get install -y curl gnupg2 && \
    curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
    echo msodbcsql18 msodbcsql/ACCEPT_EULA boolean true | debconf-set-selections && \
    mkdir -p /tmp/dk && \
    apt-get update &&  \
    apt-get upgrade -y && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql18=18.3.1.1-1 unixodbc=2.3.11-1 && \
    apt-get remove -y curl gnupg2 && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/*

# Grab the pre-built app from the build-image. This way we don't have
# excess laying around in the final image.
COPY --from=build-image /dk /dk

ENV PYTHONPATH ${PYTHONPATH}:/dk/lib/python3.9/site-packages
