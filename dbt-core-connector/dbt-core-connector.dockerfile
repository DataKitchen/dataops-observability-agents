ARG BASE_IMAGE_URL
FROM ${BASE_IMAGE_URL}python:3.12-alpine3.21 AS build-image
LABEL maintainer="devops@datakitchen.io"

RUN mkdir -p /dk

COPY ./pyproject.toml /tmp/dk/
RUN python3 -m pip install /tmp/dk --prefix=/dk


FROM ${BASE_IMAGE_URL}python:3.12-alpine3.21 AS release-image

COPY . /tmp/dk/
COPY --from=build-image /dk /dk
ENV PYTHONPATH ${PYTHONPATH}:/dk/lib/python3.10/site-packages
ENV PATH="$PATH:/dk/bin"
RUN python3 -m pip install --no-deps /tmp/dk --prefix=/dk

RUN apk add --no-cache shadow
ARG USERNAME=dkapp
ARG USER_UID=10001
ARG USER_GID=10002

RUN groupadd --gid $USER_GID $USERNAME && \
    useradd --uid $USER_UID --gid $USER_GID -m $USERNAME

USER $USERNAME

USER $USERNAME:$USERNAME

ENTRYPOINT ["action_observer"]
