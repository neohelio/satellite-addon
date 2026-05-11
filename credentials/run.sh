#!/usr/bin/with-contenv bashio
# NeoHelio Credentials — startup wrapper invoked by HA Supervisor.
# bashio gives us config-fetching + structured logging.

set -euo pipefail

bashio::log.info "NeoHelio Credentials starting…"

export NEOHELIO_SITE_TOKEN="$(bashio::config 'site_token')"
export NEOHELIO_GATEWAY_SERIAL="$(bashio::config 'gateway_serial')"
export NEOHELIO_URL="$(bashio::config 'neohelio_url')"
export NEOHELIO_REGISTRY_HOST="$(bashio::config 'registry_host')"
export NEOHELIO_REFRESH_BUFFER_SEC="$(bashio::config 'refresh_buffer_sec')"
export NEOHELIO_LOG_LEVEL="$(bashio::config 'log_level')"

# HA Supervisor injects SUPERVISOR_TOKEN; the daemon uses it to POST to
# http://supervisor/docker/registries with the freshly-minted AR token.
if [[ -z "${SUPERVISOR_TOKEN:-}" ]]; then
  bashio::log.fatal "SUPERVISOR_TOKEN not set — addon needs hassio_api: true in config.yaml"
  exit 1
fi

if [[ -z "$NEOHELIO_SITE_TOKEN" || -z "$NEOHELIO_GATEWAY_SERIAL" ]]; then
  bashio::log.fatal "site_token and gateway_serial must be set in the add-on options"
  exit 1
fi

cd /app
exec python3 -u main.py
