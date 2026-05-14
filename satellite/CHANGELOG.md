# Changelog

## 0.2.0 — 2026-05-14

- **Feature (Phase C)**: HA control commands over the live-uplink WebSocket.
  NeoHelio cloud can now flip switches, lights, and HVAC setpoints from the
  Loads drilldown. Inbound `command` frames arrive on the existing realtime
  socket; the new `commands.CommandExecutor` calls
  `POST /api/services/{domain}/{service}` against HA and replies with a
  `command_ack` frame carrying the result. Domain allowlist: `switch`,
  `light`, `climate`, `fan`, `cover` — anything else is rejected at the
  satellite for defence-in-depth even though cloud already gates by
  per-device `dashboard_control_enabled`.

## 0.1.10 — 2026-05-13

- **Feature**: string field support in blueprint mapper. Enum fields
  (`battery_charge_status`, `device_alarm`, `device_fault`) now flow through
  the mapper unchanged instead of being silently dropped by the numeric coerce
  path. Blueprint entries declare `"field_type": "string"` to opt in; numeric
  is the default and unchanged.
- **Blueprint**: 12 new entity→Silver mappings for sat-dpzwsj covering battery
  capacity, inverter losses, lifetime energy totals, external CT clamp power,
  generator output, and alarm/fault strings. Three new device records added
  (inverter_external_ct1, inverter_external_ct2, inverter_generator).

## 0.1.9 — 2026-05-12

- **Feature**: HA WebSocket registry fetcher. Entity and device registries
  (`config/entity_registry/list`, `config/device_registry/list`) are now
  fetched via a short-lived WebSocket connection instead of the REST API,
  which removed those endpoints. The manifest now carries full
  `integration_platform`, `ha_device_manufacturer`, and `ha_device_model`
  fields for every entity — the classifier uses these to propose more
  accurate Blueprint mappings (e.g. Deye inverter entities get PV_PRODUCTION
  + correct device grouping). Degrades gracefully: on WS failure the manifest
  is still uploaded with states + services data and no device metadata.

## 0.1.8 — 2026-05-12

- **Breaking-good**: the NeoHelio Credentials bootstrap addon is no longer
  required. The Satellite image is now hosted publicly on GHCR
  (`ghcr.io/neohelio/satellite-{arch}`) so HA Supervisor can pull it without
  any registry authentication. Operators who installed the Credentials addon
  can safely uninstall it after updating Satellite to this version.

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
