ARG BUILD_FROM
FROM $BUILD_FROM

# Python 3.13 + uv for fast deterministic installs. Using the python:alpine
# base via HA's "addon-base" arg means BUILD_FROM is automatically the right
# arch (aarch64 / amd64). We add Python on top of the alpine init image.
RUN apk add --no-cache \
    python3 \
    py3-pip \
    py3-virtualenv \
    sqlite \
    && python3 -m venv /opt/venv

ENV PATH="/opt/venv/bin:$PATH"

# Pin everything for reproducibility — no network at runtime except the
# WebSocket to HA Supervisor and HTTPS to NeoHelio.
COPY requirements.txt /tmp/requirements.txt
RUN /opt/venv/bin/pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /app
COPY app/ /app/
COPY themes/ /app/themes/
COPY run.sh /run.sh
RUN chmod a+x /run.sh

CMD [ "/run.sh" ]
