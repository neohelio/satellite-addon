"""
NeoHelio Credentials bootstrap daemon.

Polls NeoHelio for a short-lived Artifact Registry pull token, registers it
with HA Supervisor, sleeps until just before expiry, repeats. Without this
running, HA Supervisor can't pull the Satellite addon's private image from
africa-south1-docker.pkg.dev — every install/update would 401.

Stdlib-only by design. The whole thing is HTTP + JSON + sleep loops; no
need for aiohttp / requests / etc. Less to install, less to break, smaller
image, faster Pi-side build.

Environment (set by run.sh from addon options):
  NEOHELIO_SITE_TOKEN          HMAC site token, format ngst_<sig>.<nonce>
  NEOHELIO_GATEWAY_SERIAL      Matches the SiteDataSource.external_site_id
  NEOHELIO_URL                 Base URL, e.g. https://api.neohelio.io
  NEOHELIO_REGISTRY_HOST       e.g. africa-south1-docker.pkg.dev
  NEOHELIO_REFRESH_BUFFER_SEC  How many seconds before expiry to refresh
  NEOHELIO_LOG_LEVEL           debug/info/warning/error
  SUPERVISOR_TOKEN             Injected by HA Supervisor (hassio_api: true)
"""

import base64
import json
import logging
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from typing import Optional

# ── Config ──────────────────────────────────────────────────────────────────

SITE_TOKEN = os.environ["NEOHELIO_SITE_TOKEN"]
GATEWAY_SERIAL = os.environ["NEOHELIO_GATEWAY_SERIAL"]
NEOHELIO_URL = os.environ["NEOHELIO_URL"].rstrip("/")
REGISTRY_HOST = os.environ["NEOHELIO_REGISTRY_HOST"]
REFRESH_BUFFER_SEC = int(os.environ.get("NEOHELIO_REFRESH_BUFFER_SEC", "300"))
LOG_LEVEL = os.environ.get("NEOHELIO_LOG_LEVEL", "info").upper()
SUPERVISOR_TOKEN = os.environ["SUPERVISOR_TOKEN"]
SUPERVISOR_URL = "http://supervisor"

# Logging — match bashio's format conventions roughly so logs line up in
# HA's addon viewer.
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="[%(levelname)s] %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("neohelio-credentials")

# `neohelio_url` is the API host (`https://api.neohelio.io` in prod, the
# api-gateway Cloud Run URL in dev). api-gateway mounts the edge-gateways
# routes under `/api/v1/edge-gateways/*` and proxies them to core. Operators
# set `neohelio_url` to the host WITHOUT the `/api` suffix; we add it here.
REGISTRY_TOKEN_ENDPOINT = (
    f"{NEOHELIO_URL}/api/v1/edge-gateways/{GATEWAY_SERIAL}/registry-token"
)
SUPERVISOR_REGISTRIES_ENDPOINT = f"{SUPERVISOR_URL}/docker/registries"

# Refresh backoff. Network errors → exponential. Auth errors (401/404)
# → fixed long sleep (operator needs to fix the token / provisioning, no
# point hammering).
MIN_BACKOFF_SEC = 5
MAX_BACKOFF_SEC = 600
AUTH_FAIL_SLEEP_SEC = 600  # 10 min — gives operator time to rotate / fix


# ── HTTP helpers ────────────────────────────────────────────────────────────


class HttpError(Exception):
    """Raised on non-2xx HTTP responses. Carries the status code."""

    def __init__(self, status: int, message: str, body: str = "") -> None:
        super().__init__(f"HTTP {status}: {message}")
        self.status = status
        self.body = body


def http_request(
    method: str,
    url: str,
    *,
    headers: Optional[dict[str, str]] = None,
    body: Optional[dict[str, object]] = None,
    timeout: float = 30.0,
) -> dict[str, object]:
    """One-shot JSON HTTP request. Returns the parsed response body.
    Raises HttpError on non-2xx, OSError on network failure."""
    req_headers = {"Accept": "application/json"}
    if headers:
        req_headers.update(headers)
    data = None
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        req_headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url=url, data=data, method=method, headers=req_headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8")
            if not raw:
                return {}
            try:
                return json.loads(raw)
            except json.JSONDecodeError:
                # Supervisor's POST /docker/registries returns {"result":"ok"}
                # on success; everyone else returns JSON too. If something
                # returns a non-JSON 2xx, treat it as opaque success.
                return {"_raw": raw}
    except urllib.error.HTTPError as e:
        body_text = ""
        try:
            body_text = e.read().decode("utf-8", errors="replace")
        except Exception:
            pass
        raise HttpError(e.code, e.reason, body_text) from None


# ── Domain operations ───────────────────────────────────────────────────────


def fetch_registry_token() -> tuple[str, str]:
    """Mint a short-lived AR pull token via the NeoHelio broker.
    Returns (access_token, expires_at_iso)."""
    log.debug("requesting registry token from %s", REGISTRY_TOKEN_ENDPOINT)
    resp = http_request(
        "POST",
        REGISTRY_TOKEN_ENDPOINT,
        headers={"Authorization": f"Bearer {SITE_TOKEN}"},
        body={},
    )
    access_token = resp.get("access_token")
    expires_at = resp.get("expires_at")
    if not isinstance(access_token, str) or not access_token:
        raise RuntimeError(f"broker returned malformed access_token: {resp}")
    if not isinstance(expires_at, str) or not expires_at:
        raise RuntimeError(f"broker returned malformed expires_at: {resp}")
    return access_token, expires_at


def register_with_supervisor(access_token: str) -> None:
    """Register the AR pull token with HA Supervisor as a Docker registry
    credential. Subsequent `docker pull` of addon images from
    REGISTRY_HOST will use these credentials."""
    log.debug("registering credentials with HA Supervisor for %s", REGISTRY_HOST)
    # Supervisor's POST /docker/registries body is a FLAT host→creds map —
    # NOT wrapped in `{"registries": {...}}`. The schema (Supervisor source:
    # supervisor/api/docker.py `SCHEMA_DOCKER_REGISTRY`) is roughly
    #   vol.Schema({str: {ATTR_USERNAME: str, ATTR_PASSWORD: str}})
    # so any extra key (including "registries") gets rejected with
    # "extra keys not allowed".
    #
    # For OAuth tokens against Artifact Registry, the convention is
    # username=oauth2accesstoken, password=<the token itself>.
    http_request(
        "POST",
        SUPERVISOR_REGISTRIES_ENDPOINT,
        headers={"Authorization": f"Bearer {SUPERVISOR_TOKEN}"},
        body={
            REGISTRY_HOST: {
                "username": "oauth2accesstoken",
                "password": access_token,
            },
        },
    )


def seconds_until(expires_at_iso: str) -> int:
    """Parse the ISO-8601 expiry, return integer seconds from now until
    that point. Negative if already past."""
    # Python 3.11+ datetime.fromisoformat handles Z and offset. Be defensive
    # in case the server emits Z (Zulu) — replace with +00:00 for stdlib
    # compatibility on slightly older Python.
    normalised = expires_at_iso.replace("Z", "+00:00")
    expires_at = datetime.fromisoformat(normalised)
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)
    now = datetime.now(timezone.utc)
    return int((expires_at - now).total_seconds())


def mask_token(token: str) -> str:
    """For logs — show first/last 4 chars, hide the middle."""
    if len(token) <= 12:
        return "***"
    return f"{token[:4]}…{token[-4:]}"


# ── Main loop ───────────────────────────────────────────────────────────────


def refresh_once() -> int:
    """One refresh cycle. Returns seconds to sleep before the next refresh.
    Raises HttpError on auth failures (caller handles long-backoff)."""
    access_token, expires_at = fetch_registry_token()
    log.info(
        "minted registry token, expires_at=%s (token=%s)",
        expires_at,
        mask_token(access_token),
    )
    register_with_supervisor(access_token)
    log.info("registered credentials with HA Supervisor for %s", REGISTRY_HOST)

    ttl = seconds_until(expires_at)
    if ttl <= 0:
        # Server gave us a token that's already expired — odd, but treat as
        # immediate retry.
        log.warning("token TTL=%ds (≤0), refreshing immediately", ttl)
        return MIN_BACKOFF_SEC

    sleep_for = max(MIN_BACKOFF_SEC, ttl - REFRESH_BUFFER_SEC)
    log.info(
        "sleeping %ds (TTL %ds, refresh_buffer %ds)",
        sleep_for,
        ttl,
        REFRESH_BUFFER_SEC,
    )
    return sleep_for


def main() -> None:
    log.info(
        "neohelio-credentials starting up | serial=%s | registry=%s | broker=%s",
        GATEWAY_SERIAL,
        REGISTRY_HOST,
        NEOHELIO_URL,
    )

    backoff = MIN_BACKOFF_SEC
    while True:
        try:
            sleep_for = refresh_once()
            backoff = MIN_BACKOFF_SEC  # success → reset
            time.sleep(sleep_for)
        except HttpError as e:
            if e.status in (401, 403):
                log.error(
                    "auth rejected (HTTP %d) — site_token may have been rotated "
                    "or revoked. Sleeping %ds before retry. body=%s",
                    e.status,
                    AUTH_FAIL_SLEEP_SEC,
                    e.body[:200],
                )
                time.sleep(AUTH_FAIL_SLEEP_SEC)
            elif e.status == 404:
                log.error(
                    "broker returned 404 — gateway_serial=%s not provisioned in "
                    "NeoHelio. Sleeping %ds before retry. Check Site Settings → "
                    "Data Sources for this site.",
                    GATEWAY_SERIAL,
                    AUTH_FAIL_SLEEP_SEC,
                )
                time.sleep(AUTH_FAIL_SLEEP_SEC)
            elif e.status >= 500:
                log.warning(
                    "broker server error HTTP %d, backing off %ds: %s",
                    e.status,
                    backoff,
                    e.body[:200],
                )
                time.sleep(backoff)
                backoff = min(MAX_BACKOFF_SEC, backoff * 2)
            else:
                log.error(
                    "unexpected HTTP %d, backing off %ds: %s",
                    e.status,
                    backoff,
                    e.body[:200],
                )
                time.sleep(backoff)
                backoff = min(MAX_BACKOFF_SEC, backoff * 2)
        except (OSError, urllib.error.URLError) as e:
            log.warning("network error, backing off %ds: %s", backoff, e)
            time.sleep(backoff)
            backoff = min(MAX_BACKOFF_SEC, backoff * 2)
        except Exception as e:
            # Catch-all to keep the daemon alive. Configuration / parsing
            # issues land here and get the same exp backoff. If addon is
            # genuinely misconfigured, operator should see the repeated
            # error in HA addon logs and fix.
            log.exception("unexpected error, backing off %ds: %s", backoff, e)
            time.sleep(backoff)
            backoff = min(MAX_BACKOFF_SEC, backoff * 2)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.info("interrupted, shutting down")
        sys.exit(0)
