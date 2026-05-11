# NeoHelio Add-ons for Home Assistant

The official Home Assistant add-on repository for the **NeoHelio energy-performance platform**.

## Available add-ons

- **[NeoHelio Credentials](credentials/)** — bootstrap addon. Fetches short-lived Artifact Registry pull tokens from NeoHelio and registers them with HA Supervisor. Install **before** Satellite. Builds locally on the Pi (no auth required to pull this one).
- **[NeoHelio Satellite](satellite/)** — pushes Home Assistant telemetry to NeoHelio for fleet-level performance, financial, and SLA analytics. Pulled as a pre-built private image — install **after** Credentials is running.

## Install

In Home Assistant:

1. **Settings → Apps → ⋮ (top right) → Repositories**
2. Paste this URL:
   ```
   https://github.com/neohelio/satellite-addon
   ```
3. Click **Add**.
4. Both **NeoHelio Credentials** and **NeoHelio Satellite** now appear in your Add-on Store.
5. **Install NeoHelio Credentials first.** Click it → Install → open the **Configuration** tab and paste your **site token** + **gateway serial** from your NeoHelio onboarding email → **Start**. Confirm logs show `registered credentials with HA Supervisor`.
6. **Then install NeoHelio Satellite.** Paste the *same* values you used for Credentials → **Start**. The Satellite will register with NeoHelio cloud, fetch its blueprint, and start streaming telemetry.

If the Satellite install fails with `denied: permission denied` or similar pull errors, the Credentials addon either isn't running yet or hasn't successfully registered its token with Supervisor. Check its logs first.

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
