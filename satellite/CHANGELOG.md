# Changelog

## 0.1.7 — 2026-05-12

- **Feature**: cloud-driven flush cadence. The slow-path NDJSON push
  interval (`flush_interval_sec`) is now sent by the cloud as part of the
  blueprint payload (`polling.flush_interval_sec`). Operators set it in
  the NeoHelio admin UI; the addon hot-reloads on its next blueprint
  refresh (≤5 min) with no restart. Falls back to the local addon option
  on cold start or when the cloud field is missing/out-of-bounds.
- **Feature**: HA re-snapshot on blueprint change. When the cloud sends a
  blueprint whose entity set or polling cadence differs from the cached
  one, the addon fetches a fresh `/api/states` from HA and replays each
  state through the bucket + live tee. Newly-mapped entities now flow
  immediately instead of waiting for each one's next HA `state_changed`
  event — a real pain point for slow-moving accumulators
  (`today_*_kwh`, temperatures, voltages).

## 0.1.6 — 2026-05-11

- **Fix**: discovery loop tolerates HA REST 404 on
  `/api/config/{entity,device}_registry/list`. Those endpoints are
  WebSocket-only in current HA, so the previous REST GETs aborted the
  entire refresh and no manifest ever reached the cloud. Treating 404 as
  empty list keeps the states + services data flowing; manifest is
  shallower (no manufacturer / model / integration_platform) until we
  add the WS-based fetcher.
- **Feature**: `realtime_relay_url` addon option. Empty derives wss://
  from `neohelio_url` (prod-correct). In dev where the relay is a
  separate Cloud Run service, paste its wss:// URL. Set to "disabled"
  to skip the live tee task entirely.

## 0.1.5 — 2026-05-11

- **Fix**: register / blueprint / manifest URLs now include the `/api/v1`
  prefix matching api-gateway's route mounts (`/api/v1/edge-gateways/*`).
  Previously the addon constructed `{neohelio_url}/v1/edge-gateways/...`
  which 404'd at the gateway. Operators must set `neohelio_url` to the
  API host without an `/api` suffix (e.g. `https://api.neohelio.io`).
- **Fix**: hardcoded `agent_version` + log line now read v0.1.5 instead
  of the stale v0.1.0 string.

## 0.1.4 — 2026-05-11

Phase 2 discovery + addon stabilization.

- HA entity discovery loop (`discovery.py`) wired into the asyncio TaskGroup —
  on boot + every 5 min, scans HA's REST API (`/api/states`,
  `/api/config/device_registry/list`, `/api/config/entity_registry/list`,
  `/api/services`), builds a manifest, POSTs to
  `/v1/edge-gateways/:serial/manifest` when the SHA-256 hash changes.
  Cloud-side classifier then proposes Blueprint mappings the operator can
  accept in the editor.
- Manifest hash echoed through to LiveUplink's `hello` frame so the realtime
  relay can surface stale-classification banners in the browser.
- Phase 0 stabilization complete (jono's work — landed independently).

## 0.1.0 — 2026-04-29

Initial spike. Telemetry-only.

- HA WebSocket subscription with auto-reconnect and exponential backoff.
- Cloud-pushed Blueprint (entity → Silver column mapping) refreshed every 5 min.
- SQLite outbox buffer with 24h vacuum.
- HTTPS uplink to `/v1/ingest/edge` with site-token bearer auth.
- HA Supervisor add-on packaging (aarch64 + amd64).
