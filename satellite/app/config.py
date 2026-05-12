"""Runtime configuration loaded from environment (set by run.sh from add-on
options). Centralised so tests can monkey-patch a single object."""
from __future__ import annotations
import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    site_token: str
    gateway_serial: str
    neohelio_url: str
    poll_interval_sec: int
    log_level: str
    hass_url: str
    hass_token: str
    db_path: str = "/data/satellite.sqlite"
    blueprint_refresh_sec: int = 300
    # Optional override: in dev the ingest receiver is on a different port
    # than services/core. Set NEOHELIO_INGEST_URL to e.g. http://host:8108.
    # In prod it's empty and ingest_url derives from neohelio_url + /ingest.
    ingest_url_override: str = ""
    # Live tee — outbound WSS to services/realtime-relay. Falsy means "disabled";
    # the slow NDJSON path keeps working regardless. When set, every
    # HA state_changed event is forwarded sub-second alongside the aggregated
    # historical flush. See plan §"Why route live data through cloud at all?".
    realtime_relay_url_override: str = ""

    # In production both URLs sit behind a single api.neohelio.io host with the
    # API gateway doing the routing. For local dev they live on different ports
    # (services/core on 8080, the ingestion Cloud Function on 8108), so we
    # expose them as two settings. bashio returns the literal string "null"
    # for optional fields that aren't set — treat that as empty.
    @property
    def ingest_url(self) -> str:
        override = self.ingest_url_override
        if override and override.lower() != "null":
            base = override.rstrip('/')
        else:
            base = self.neohelio_url.rstrip('/')
        return f"{base}/ingest"

    @property
    def blueprint_url(self) -> str:
        return f"{self._api_base()}/edge-gateways/{self.gateway_serial}/blueprint"

    @property
    def register_url(self) -> str:
        return f"{self._api_base()}/edge-gateways/{self.gateway_serial}/register"

    @property
    def manifest_url(self) -> str:
        """POST endpoint where discovery.py uploads the HA entity manifest."""
        return f"{self._api_base()}/edge-gateways/{self.gateway_serial}/manifest"

    def _api_base(self) -> str:
        """Shared root for every /edge-gateways/* call.

        `neohelio_url` is the API host (`https://api.neohelio.io` in prod, the
        api-gateway Cloud Run URL in dev). Customer Pis always go through
        api-gateway — Cloud Run IAM blocks direct invocation of core, and the
        HMAC pass-through (services/api-gateway/src/index.ts:
        `app.use('/api/v1/edge-gateways', …)`) only exists at the gateway
        layer. Appending `/api/v1` here matches the gateway's route mount.

        Local-dev callers that hit core directly should set `neohelio_url` to
        a host + path that already produces a working URL (e.g. point at
        `http://localhost:8082/api` if running api-gateway locally too).
        """
        return f"{self.neohelio_url.rstrip('/')}/api/v1"

    @property
    def realtime_relay_url(self) -> str | None:
        """Resolved relay URL. None means the live tee is disabled (e.g. dev
        environments without the relay deployed yet, or operator-disabled).

        Resolution order:
          1. Explicit override NEOHELIO_REALTIME_RELAY_URL (full ws/wss URL).
          2. Derived from neohelio_url by swapping http→ws and substituting
             `/realtime/satellite/<serial>`. Production hosts both the REST
             API and the relay on the same domain via Cloud Run + load
             balancer.
        Falsy / 'null' / 'disabled' → returns None and the addon skips the
        live tee task entirely; slow NDJSON path is unaffected.
        """
        override = (self.realtime_relay_url_override or "").strip()
        if override.lower() in ("", "null", "disabled", "off"):
            # Fall back to derivation only when neohelio_url is HTTPS — never
            # silently introduce a default WSS URL in dev.
            base = self.neohelio_url.rstrip('/')
            if base.startswith('https://'):
                return f"wss://{base[len('https://'):]}/realtime/satellite/{self.gateway_serial}"
            if base.startswith('http://'):
                # Dev convenience: localhost:8095 is the realtime-relay default
                # port; honour it only when explicitly opted in via override.
                return None
            return None
        return override


def load() -> Settings:
    return Settings(
        site_token=os.environ["NEOHELIO_SITE_TOKEN"],
        gateway_serial=os.environ["NEOHELIO_GATEWAY_SERIAL"],
        neohelio_url=os.environ.get("NEOHELIO_URL", "https://api.neohelio.io"),
        ingest_url_override=os.environ.get("NEOHELIO_INGEST_URL", ""),
        realtime_relay_url_override=os.environ.get("NEOHELIO_REALTIME_RELAY_URL", ""),
        poll_interval_sec=int(os.environ.get("NEOHELIO_POLL_INTERVAL_SEC", "5")),
        log_level=os.environ.get("NEOHELIO_LOG_LEVEL", "info"),
        hass_url=os.environ.get("NEOHELIO_HASS_URL", "ws://supervisor/core/websocket"),
        hass_token=os.environ["NEOHELIO_HASS_TOKEN"],
    )
