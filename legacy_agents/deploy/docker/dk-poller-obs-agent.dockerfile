# DEV NOTE:  YOU MUST RUN `docker build` FROM THE `legacy_framework` FOLDER AND POINT TO THIS FILE.
# e.g., cd ~/../../legacy_framework && docker build . -f deploy/docker/dk-poller-obs-agent.dockerfile
ARG tag
FROM dk-obs-agent-base:$tag
LABEL maintainer="DataKitchen"
ARG USERNAME=agentapp
ARG USER_UID=1023
ARG USER_GID=$USER_UID

RUN groupadd --gid $USER_GID $USERNAME && \
    useradd --uid $USER_UID --gid $USER_GID -m $USERNAME

USER $USERNAME

CMD ["/dk/bin/polling-agent"]
