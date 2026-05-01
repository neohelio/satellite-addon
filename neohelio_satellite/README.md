# Orbit Satellite

Home Assistant add-on that pushes telemetry to **Orbit** — the energy-performance platform for commercial solar.

This is the source code repository. End-user installation instructions live in [DOCS.md](DOCS.md).

## Local development

```bash
# Mount this folder into your HA dev instance's /addons/local/
# (replace USER@HOST with your HA SSH target)
rsync -avz --exclude '.git' --exclude '__pycache__' \
  ./ root@homeassistant.local:/addons/local/neohelio_satellite/

# In HA UI: Settings → Add-ons → ⋮ → Reload, then install "Orbit Satellite (local)"
```

## Layout

```
config.yaml             HA add-on manifest
Dockerfile              Python runtime + deps
requirements.txt        Python deps (aiohttp, websockets)
run.sh                  Supervisor entrypoint, fetches options via bashio
app/
  config.py             Settings dataclass loaded from env
  blueprint.py          Cloud-pushed entity mapping cache
  ha_client.py          HA WebSocket subscriber with reconnect
  mapper.py             Entity state → CommonTelemetryReading buffer
  outbox.py             SQLite write-ahead buffer
  uplink.py             NDJSON HTTPS drain to Orbit cloud
  main.py               TaskGroup orchestration
translations/en.yaml    Add-on UI strings
themes/                 Orbit HA frontend theme (Stage 1 branding)
```

## Architecture

```
HA WebSocket ──► state_changed events ──► mapper (apply Blueprint)
                                              │
                                              ▼
                             SQLite outbox (per-device buffer, 24h retention)
                                              │
                                              ▼
                       HTTPS POST NDJSON ──► functions/ingestion/edge (Orbit cloud)
                                              │
                                              ▼
                          writeTenantBronze ──► bronze-to-silver ──► Live Dashboard
```

The add-on is intentionally narrow — it doesn't know what `battery_soc` *means*. The Blueprint, served by Orbit cloud per gateway, is the source of truth for which HA entities map to which Silver columns. This lets us evolve the mapping without redeploying the add-on.

## Licence

Proprietary — NeoHelio Pty Ltd. © 2026.
