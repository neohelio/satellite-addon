ARG BUILD_FROM
FROM $BUILD_FROM

# Stdlib-only Python — no pip install, no native deps, no toolchain.
# Refresh loop is ~150 lines of stdlib HTTP + JSON; deliberately simple
# so the Pi-side local build takes seconds, not minutes.
RUN apk add --no-cache python3

WORKDIR /app
COPY app/ /app/
COPY run.sh /run.sh
RUN chmod a+x /run.sh

CMD [ "/run.sh" ]
