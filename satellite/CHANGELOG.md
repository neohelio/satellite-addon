# Changelog

## 0.1.0 — 2026-04-29

Initial spike. Telemetry-only.

- HA WebSocket subscription with auto-reconnect and exponential backoff.
- Cloud-pushed Blueprint (entity → Silver column mapping) refreshed every 5 min.
- SQLite outbox buffer with 24h vacuum.
- HTTPS uplink to `/v1/ingest/edge` with site-token bearer auth.
- HA Supervisor add-on packaging (aarch64 + amd64).
