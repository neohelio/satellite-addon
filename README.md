# NeoHelio Add-ons for Home Assistant

The official Home Assistant add-on repository for the **NeoHelio energy-performance platform**.

## Available add-ons

- **[NeoHelio Satellite](satellite/)** — pushes Home Assistant telemetry to NeoHelio for fleet-level performance, financial, and SLA analytics. Pre-built public image — no registry authentication needed.

## Install

In Home Assistant:

1. **Settings → Apps → ⋮ (top right) → Repositories**
2. Paste this URL:
   ```
   https://github.com/neohelio/satellite-addon
   ```
3. Click **Add**.
4. **NeoHelio Satellite** now appears in your Add-on Store. Click it → **Install**.
5. Open the **Configuration** tab and fill in:
   - **Site token** — from your NeoHelio onboarding email
   - **Gateway serial** — from your NeoHelio onboarding email
6. Click **Start**. The add-on will register with NeoHelio cloud, fetch its blueprint, and begin streaming telemetry.

> **Upgrading from v0.1.7 or earlier?** You can uninstall the NeoHelio Credentials add-on after updating Satellite to v0.1.8+. The Credentials add-on handled private-registry authentication that is no longer needed.

## What it does

NeoHelio Satellite subscribes to your HA instance's state-change events, maps configured entities (e.g. `sensor.deye_battery_soc`) to NeoHelio's telemetry schema using a cloud-pushed Blueprint, and forwards readings to the NeoHelio cloud over HTTPS. Buffers locally during outages and drains automatically when connectivity returns.

This means:

- Any device HA can read, NeoHelio can ingest. Solarman/Deye/Sunsynk, SMA, Fronius, Pylontech BMS, Tasmota, Shelly, Zigbee thermostats, EV chargers — anything with an HA integration becomes a NeoHelio data source.
- One Bronze contract for both prosumer (HA) and commercial (Moxa) tiers.
- Branded HA frontend theme installed alongside the add-on.

## Support

- **Documentation**: https://docs.neohelio.io/satellite
- **Issues**: https://github.com/neohelio/satellite-addon/issues
- **NeoHelio account / onboarding**: https://app.neohelio.io

## Licence

Proprietary — © 2026 NeoHelio Pty Ltd. The add-on is distributed for use only with a valid NeoHelio account.
