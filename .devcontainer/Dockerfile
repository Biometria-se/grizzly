# See here for image contents: https://github.com/microsoft/vscode-dev-containers/tree/v0.154.2/containers/python-3/.devcontainer/base.Dockerfile

# [Choice] Python version: 3, 3.9, 3.8, 3.7, 3.6
ARG VARIANT="3"

# IBM MQ libraries dependencies
FROM alpine:latest as dependencies

USER root

ENV MQ_VERSION="9.2.2.0"

RUN mkdir /root/ibm && cd /root/ibm && \
    wget https://public.dhe.ibm.com/ibmdl/export/pub/software/websphere/messaging/mqdev/redist/${MQ_VERSION}-IBM-MQC-Redist-LinuxX64.tar.gz -O - | tar xzf -

# End IBM MQ libraries dependencies


FROM mcr.microsoft.com/vscode/devcontainers/python:0-${VARIANT}

ARG NONROOT_USER="vscode"

# Copy/setup IBM MQ stuff
RUN mkdir -p /opt/mqm/lib64 && mkdir /opt/mqm/lib && mkdir -p /opt/mqm/gskit8/lib64

COPY --from=dependencies /root/ibm/inc /opt/mqm/inc
COPY --from=dependencies /root/ibm/lib/libcurl.so /opt/mqm/lib/
COPY --from=dependencies /root/ibm/lib/ccsid_part2.tbl /opt/mqm/lib/
COPY --from=dependencies /root/ibm/lib/ccsid.tbl /opt/mqm/lib/
COPY --from=dependencies /root/ibm/lib64/libmqic_r.so /opt/mqm/lib64/
COPY --from=dependencies /root/ibm/lib64/libmqe_r.so /opt/mqm/lib64/
COPY --from=dependencies /root/ibm/gskit8/lib64 /opt/mqm/gskit8/lib64/

ENV LD_LIBRARY_PATH="/opt/mqm/lib64:${LD_LIBRARY_PATH}"
# End Copy/setup IBM MQ stuff


ENV DEBIAN_FRONTEND="noninteractive"

# run github workflows locally
RUN curl https://raw.githubusercontent.com/nektos/act/master/install.sh | sudo bash

RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
     && apt-get -y install --no-install-recommends vim

# Install Docker CE CLI
RUN apt-get update \
    && apt-get install -y apt-transport-https ca-certificates curl gnupg2 lsb-release \
    && curl -fsSL https://download.docker.com/linux/$(lsb_release -is | tr '[:upper:]' '[:lower:]')/gpg | apt-key add - 2>/dev/null \
    && echo "deb [arch=amd64] https://download.docker.com/linux/$(lsb_release -is | tr '[:upper:]' '[:lower:]') $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list \
    && apt-get update \
    && apt-get install -y docker-ce-cli

# Install Docker Compose
RUN LATEST_COMPOSE_VERSION=$(curl -sSL "https://api.github.com/repos/docker/compose/releases/latest" | grep -o -P '(?<="tag_name": ").+(?=")') \
    && curl -sSL "https://github.com/docker/compose/releases/download/${LATEST_COMPOSE_VERSION}/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose \
    && chmod +x /usr/local/bin/docker-compose

# Create /usr/local/share/docker-init.sh entrypoint script, that creates docker group with correct GID (host docker group GID)
RUN echo "#!/bin/sh\n\
    sudoIf() { if [ \"\$(id -u)\" -ne 0 ]; then sudo \"\$@\"; else \"\$@\"; fi }\n\
    SOCKET_GID=\$(stat -c '%g' /var/run/docker.sock) \n\
    if [ \"${SOCKET_GID}\" != '0' ]; then\n\
        if [ \"\$(cat /etc/group | grep :\${SOCKET_GID}:)\" = '' ]; then sudoIf groupadd --gid \${SOCKET_GID} docker-host; fi \n\
        if [ \"\$(id ${NONROOT_USER} | grep -E \"groups=.*(=|,)\${SOCKET_GID}\(\")\" = '' ]; then sudoIf usermod -aG \${SOCKET_GID} ${NONROOT_USER}; fi\n\
    fi\n\
    exec \"\$@\"" > /usr/local/share/docker-init.sh \
    && chmod +x /usr/local/share/docker-init.sh

# VS Code overrides ENTRYPOINT and CMD when executing `docker run` by default.
# Setting the ENTRYPOINT to docker-init.sh will configure non-root access to
# the Docker socket if "overrideCommand": false is set in devcontainer.json.
# The script will also execute CMD if you need to alter startup behaviors.
ENTRYPOINT [ "/usr/local/share/docker-init.sh" ]
CMD [ "sleep", "infinity" ]
