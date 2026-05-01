# NeoHelio Satellite — Home Assistant Add-on

Streams Home Assistant telemetry to [NeoHelio](https://neohelio.io) for
fleet-level performance, financial, and SLA analytics.

## Add this repository to Home Assistant

In Home Assistant: **Settings → Add-ons → Add-on store → ⋮ (top right)
→ Repositories → Add**:

```
https://github.com/neohelio/satellite-addon
```

The **NeoHelio Satellite** add-on appears in the store. Install it,
configure with your site token + gateway serial from NeoHelio site
settings, and start.

## What it does

The add-on opens a WebSocket to Home Assistant and streams entity
state changes to NeoHelio. Per-gateway *Blueprints* (managed in
NeoHelio admin) declare which HA entities feed which NeoHelio
telemetry fields, so the add-on stays vendor-agnostic — Solarman,
SunSpec, Goodwe, and so on are all just different blueprints
against the same code.

Telemetry is buffered locally and shipped to NeoHelio's edge
ingestion endpoint with a per-site HMAC token.

## Adding to a NeoHelio site

The add-on requires a NeoHelio site to be set up first:

1. NeoHelio admin → site settings → Data Sources → Add → **Orbit Satellite**
2. Mint a site token; copy it
3. Note the gateway serial NeoHelio assigns
4. Paste both into this add-on's configuration in HA
5. Start the add-on

Once running, the add-on registers with NeoHelio and pulls down its
blueprint. From the blueprint editor in NeoHelio, you can apply a
template (e.g. *Deye Solarman — single-phase*) to wire common HA
integrations to NeoHelio fields automatically.

## Development

The canonical source for this add-on lives in the
[NeoHelio monorepo](https://github.com/neohelio/neohelio.io) at
`edge/satellite-addon/`. This repo is the distribution surface for
HA's add-on store; it's a copy of the monorepo subtree.

Pull requests welcome on either the monorepo or this repo (the
monorepo path is the one we sync from).

## License

Proprietary — © NeoHelio. Contact <hello@neohelio.io>.
