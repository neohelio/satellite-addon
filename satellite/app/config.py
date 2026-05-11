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
        return f"{self.neohelio_url.rstrip('/')}/v1/edge-gateways/{self.gateway_serial}/blueprint"

    @property
    def register_url(self) -> str:
        return f"{self.neohelio_url.rstrip('/')}/v1/edge-gateways/{self.gateway_serial}/register"


def load() -> Settings:
    return Settings(
        site_token=os.environ["NEOHELIO_SITE_TOKEN"],
        gateway_serial=os.environ["NEOHELIO_GATEWAY_SERIAL"],
        neohelio_url=os.environ.get("NEOHELIO_URL", "https://api.neohelio.io"),
        ingest_url_override=os.environ.get("NEOHELIO_INGEST_URL", ""),
        poll_interval_sec=int(os.environ.get("NEOHELIO_POLL_INTERVAL_SEC", "5")),
        log_level=os.environ.get("NEOHELIO_LOG_LEVEL", "info"),
        hass_url=os.environ.get("NEOHELIO_HASS_URL", "ws://supervisor/core/websocket"),
        hass_token=os.environ["NEOHELIO_HASS_TOKEN"],
    )
