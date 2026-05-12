# Orbit Satellite

Pushes Home Assistant entity state to **Orbit** for fleet-level performance, financial, and SLA analytics. Runs as a Home Assistant Add-on alongside your existing HA install — no separate hardware required.

## What it does

The add-on subscribes to your HA's state-change events, maps configured entities (e.g. `sensor.deye_battery_soc`) to Orbit's telemetry schema using a cloud-pushed Blueprint, and forwards readings to the Orbit cloud over HTTPS. Buffers locally during outages and drains automatically when connectivity returns.

## Prerequisites

- Home Assistant OS, HA Supervised, or any Supervisor-aware HA install (HA Container is not supported — it has no Supervisor).
- An Orbit account and a registered Site.
- A **site token** and **gateway serial** issued during Orbit onboarding.

## Installation

1. In Home Assistant: **Settings → Add-ons → Add-on Store → ⋮ → Repositories**.
2. Add `https://github.com/neohelio/satellite-addon` (placeholder — public repo coming soon).
3. Find **Orbit Satellite** in the list, click **Install**.
4. Open the **Configuration** tab and enter:
   - `site_token`: from your Orbit onboarding screen
   - `gateway_serial`: the unique ID for this Satellite (also from onboarding)
   - `neohelio_url`: leave the default unless you're on a staging/self-hosted Orbit
5. Save → **Start** the add-on.

## How the Blueprint works

Orbit cloud serves a per-gateway Blueprint that maps your HA entities to columns in Orbit's Silver telemetry schema. Example:

```json
{
  "schema_version": 1,
  "site_id": "uuid",
  "device_id": "uuid",
  "subordinate_devices": { "deye-2787155991": "uuid" },
  "entities": [
    {
      "entity_id": "sensor.deye_battery_soc",
      "device_external_id": "deye-2787155991",
      "device_type": "INVERTER",
      "field": "battery_soc_pct"
    }
  ]
}
```

You configure these mappings in Orbit's onboarding wizard / admin UI. The add-on refreshes the Blueprint every 5 minutes; changes apply automatically.

## Logs

In HA: **Settings → Add-ons → Orbit Satellite → Log**. Set `log_level` to `debug` if you need verbose output for troubleshooting.

## Privacy

- Readings flow over TLS to Orbit cloud. No third parties.
- The site token is the only authentication credential; revoke any time in Orbit admin.
- Local SQLite buffer (`/data/satellite.sqlite`) holds at most a few hours of unsent data; sent rows are vacuumed after 24h.

## Support

- Documentation: https://docs.neohelio.io/satellite (placeholder)
- Issues: https://github.com/neohelio/satellite-addon/issues (placeholder)
