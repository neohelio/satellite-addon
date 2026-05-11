# NeoHelio Credentials (bootstrap)

A small bootstrap addon for Home Assistant. Its only job is to fetch short-lived Artifact Registry pull tokens from NeoHelio and register them with HA Supervisor so the **NeoHelio Satellite** addon's private image can be installed and updated.

## Why two addons

The Satellite addon's image is private (hosted on Google Artifact Registry, scoped per tenant). HA Supervisor needs Docker credentials to pull it. This addon provides those credentials.

We split it out because:

- This bootstrap addon is **built locally on the Pi** from this repo. No external image needed → solves the chicken-and-egg of "you need auth to pull the auth addon."
- The Satellite addon proper is built and signed in our CI, distributed as a pre-built image. Faster install, no Pi-side build overhead.
- This addon is a ~150-line credential refresh loop with no NeoHelio IP — happy for it to be visible source.

## Install order

1. **NeoHelio Credentials** (this addon) — paste your `site_token` + `gateway_serial`, start.
2. **NeoHelio Satellite** — paste the *same* `site_token` + `gateway_serial`, start.

Without step 1, step 2's install will fail with `denied: permission denied` on the docker pull.

## Configuration

| Key | Default | Description |
|---|---|---|
| `site_token` | _(required)_ | HMAC site token from NeoHelio onboarding email. Format `ngst_<sig>.<nonce>`. |
| `gateway_serial` | _(required)_ | Gateway serial shown on the SDS row in NeoHelio UI. |
| `neohelio_url` | `https://api.neohelio.io` | API base. Use the dev URL when testing. |
| `registry_host` | `africa-south1-docker.pkg.dev` | Artifact Registry host. Don't change unless NeoHelio support tells you to. |
| `refresh_buffer_sec` | `300` | How early before token expiry to refresh. 5 min default. |
| `log_level` | `info` | `debug` / `info` / `warning` / `error`. |

## What you should see in the logs

```
[INFO] neohelio-credentials starting up | serial=gw-… | registry=africa-south1-docker.pkg.dev | broker=https://api.neohelio.io
[INFO] minted registry token, expires_at=2026-05-11T20:32:18Z (token=ya29…X4ks)
[INFO] registered credentials with HA Supervisor for africa-south1-docker.pkg.dev
[INFO] sleeping 3300s (TTL 3600s, refresh_buffer 300s)
```

Repeat every ~55 min indefinitely.

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `auth rejected (HTTP 401)` | `site_token` is wrong or has been rotated | Refresh the token in NeoHelio UI → paste the new value → restart this addon |
| `broker returned 404` | `gateway_serial` doesn't match any provisioned site | Check Site Settings → Data Sources in NeoHelio — that gateway needs to exist |
| `denied: permission denied` (from the Satellite addon, not this one) | This addon isn't running, OR Supervisor hasn't picked up the credentials yet | Confirm this addon is running and showed `registered credentials with HA Supervisor`; restart Supervisor if it's been a while |
| `network error, backing off …` | Pi can't reach `neohelio_url` | Check DNS / firewall / HA's `Settings → System → Hardware → Network` |

## Licence

Proprietary — © 2026 NeoHelio Pty Ltd. Distributed for use only with a valid NeoHelio account.
