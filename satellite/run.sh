#!/usr/bin/with-contenv bashio
# NeoHelio Satellite — startup wrapper invoked by HA Supervisor.
# bashio gives us config-fetching + structured logging.

set -euo pipefail

bashio::log.info "NeoHelio Satellite starting…"

# ── Install the NeoHelio HA theme (idempotent) ────────────────────────────
# /config is mapped read+write per config.yaml. Copy our packaged theme
# into HA's themes folder so the user can pick "NeoHelio" under Profile →
# Themes. Refreshes on every boot — operators editing the file directly
# will see their changes overwritten next restart, which is intentional
# (the add-on owns its theme).
THEME_SRC="/app/themes/neohelio.yaml"
THEME_DST="/config/themes/neohelio.yaml"
if [[ -f "$THEME_SRC" ]]; then
  mkdir -p "$(dirname "$THEME_DST")"
  cp -f "$THEME_SRC" "$THEME_DST"
  bashio::log.info "installed NeoHelio theme to $THEME_DST"
else
  bashio::log.warning "theme not found at $THEME_SRC — skipping"
fi

export NEOHELIO_SITE_TOKEN="$(bashio::config 'site_token')"
export NEOHELIO_GATEWAY_SERIAL="$(bashio::config 'gateway_serial')"
export NEOHELIO_URL="$(bashio::config 'neohelio_url')"
export NEOHELIO_INGEST_URL="$(bashio::config 'ingest_url')"
export NEOHELIO_REALTIME_RELAY_URL="$(bashio::config 'realtime_relay_url')"
export NEOHELIO_POLL_INTERVAL_SEC="$(bashio::config 'poll_interval_sec')"
export NEOHELIO_LOG_LEVEL="$(bashio::config 'log_level')"

# HA Supervisor injects SUPERVISOR_TOKEN; the add-on uses it to talk to the
# Home Assistant Core WebSocket via the Supervisor proxy.
export NEOHELIO_HASS_URL="ws://supervisor/core/websocket"
export NEOHELIO_HASS_TOKEN="$SUPERVISOR_TOKEN"

if [[ -z "$NEOHELIO_SITE_TOKEN" || -z "$NEOHELIO_GATEWAY_SERIAL" ]]; then
  bashio::log.fatal "site_token and gateway_serial must be set in the add-on options"
  exit 1
fi

cd /app
exec python3 -u -m main
