"""HA entity discovery — Phase 2.

Calls Home Assistant's REST API for the four sources we care about:

  • /api/states                            current value + attributes for every entity
  • /api/config/device_registry/list       physical device groupings
  • /api/config/entity_registry/list       entity → integration_of_origin mapping
  • /api/services                          available service domains (for Phase 3 controls)

Builds a "manifest" of every HA entity the gateway knows about — entity_id,
friendly_name, device_class, unit_of_measurement, integration_platform,
ha_device_id, ha_device_model, supported_services[] — and POSTs it to
NeoHelio cloud whenever the SHA-256 of the JSON changes.

The cloud-side (services/core /v1/edge-gateways/:serial/manifest) persists
the manifest on the gateway Device.metadata, then runs the rules-based
classifier (functions/adapters/edge-gateway/classifier.ts) to propose CTM
mappings the operator can accept in the Blueprint editor.

Triggers:
  • On addon startup (after the first blueprint fetch).
  • Periodically every refresh_sec (default 300s).
  • Reactively on HA `entity_registry_updated` events (wired in main.py).

The module is intentionally narrow: building the manifest is a pure side-
effect-free transformation that's easy to unit-test; the network calls are
isolated in the `fetch` helpers; the upload is isolated in `_push_manifest`.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from dataclasses import dataclass, field
from typing import Optional

import aiohttp

log = logging.getLogger("discovery")

DEFAULT_REFRESH_SEC = 300.0
PUSH_TIMEOUT_SEC = 30.0


@dataclass
class ManifestEntity:
    """One entry in the entity manifest. Stable, JSON-serialisable shape —
    the cloud-side Zod schema mirrors this exactly. Adding fields here means
    adding them in `services/core` and the classifier."""
    entity_id: str
    friendly_name: Optional[str]
    device_class: Optional[str]
    unit_of_measurement: Optional[str]
    integration_platform: Optional[str]
    ha_device_id: Optional[str]
    ha_device_manufacturer: Optional[str]
    ha_device_model: Optional[str]
    supported_services: list[str] = field(default_factory=list)
    # `state` is intentionally NOT included — it changes constantly and we
    # don't want to fingerprint into the manifest hash. The relay carries
    # current state; the manifest is structural metadata only.


def build_manifest(
    states: list[dict],
    entity_registry: list[dict],
    device_registry: list[dict],
    services: dict,
) -> list[ManifestEntity]:
    """Pure function. Combine the four HA REST responses into a flat list of
    ManifestEntity rows. Tested in isolation in test_discovery.py."""
    # Index device_registry by id for O(1) lookup of manufacturer / model.
    devices_by_id = {d["id"]: d for d in device_registry if isinstance(d, dict) and "id" in d}
    # Index entity_registry by entity_id so we can pull `platform` (the HA
    # integration that registered the entity) + linked device_id.
    er_by_entity = {e["entity_id"]: e for e in entity_registry if isinstance(e, dict) and "entity_id" in e}

    out: list[ManifestEntity] = []
    for state in states:
        if not isinstance(state, dict):
            continue
        entity_id = state.get("entity_id")
        if not entity_id or not isinstance(entity_id, str):
            continue
        attrs = state.get("attributes") if isinstance(state.get("attributes"), dict) else {}
        er = er_by_entity.get(entity_id) or {}
        ha_device_id = er.get("device_id") if isinstance(er, dict) else None
        device = devices_by_id.get(ha_device_id) if ha_device_id else None

        domain = entity_id.split(".", 1)[0] if "." in entity_id else None
        sup = _supported_services(domain, services)

        out.append(ManifestEntity(
            entity_id=entity_id,
            friendly_name=_str_or_none(attrs.get("friendly_name")),
            device_class=_str_or_none(attrs.get("device_class")),
            unit_of_measurement=_str_or_none(attrs.get("unit_of_measurement")),
            integration_platform=_str_or_none(er.get("platform") if isinstance(er, dict) else None),
            ha_device_id=_str_or_none(ha_device_id),
            ha_device_manufacturer=_str_or_none(device.get("manufacturer") if device else None),
            ha_device_model=_str_or_none(device.get("model") if device else None),
            supported_services=sup,
        ))
    # Stable sort so the manifest hash is deterministic across runs even when
    # HA returns states in different orders.
    out.sort(key=lambda e: e.entity_id)
    return out


def manifest_hash(entities: list[ManifestEntity]) -> str:
    """SHA-256 of the canonical JSON serialisation. Stable, deterministic."""
    blob = json.dumps([e.__dict__ for e in entities], sort_keys=True, separators=(',', ':'))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _str_or_none(value: object) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        s = value.strip()
        return s if s else None
    return str(value)


def _supported_services(domain: Optional[str], services: dict) -> list[str]:
    """Return the list of service names registered against this entity's
    domain (e.g. for `switch.foo`, returns `['turn_on', 'turn_off', 'toggle']`).
    Phase 3 uses this to expose controllable entities; Phase 2 just records it."""
    if not domain or not isinstance(services, dict):
        return []
    domain_block = services.get(domain)
    if not isinstance(domain_block, dict):
        # HA returns `services` as either {domain: {service: ...}} or a list of
        # {domain, services} pairs depending on the API version. Handle both.
        if isinstance(services, list):
            for entry in services:
                if isinstance(entry, dict) and entry.get("domain") == domain:
                    block = entry.get("services")
                    if isinstance(block, dict):
                        return sorted(block.keys())
        return []
    return sorted(domain_block.keys())


class DiscoveryLoop:
    """Periodic + event-triggered manifest builder + pusher.

    Cooperates with `live_uplink.LiveUplink` via `set_manifest_hash` — the
    relay echoes the hash back to browser subscribers, who use it to detect
    stale cached classifications and refresh the Blueprint editor."""

    def __init__(
        self,
        hass_url: str,
        hass_token: str,
        manifest_url: str,
        site_token: str,
        refresh_sec: float = DEFAULT_REFRESH_SEC,
    ):
        # `hass_url` from settings points at the HA WebSocket endpoint —
        # `ws://supervisor/core/websocket`. The REST API lives one level up at
        # `http://supervisor/core`. Strip the protocol + `/websocket` suffix
        # so subsequent `_get_json` calls land at `/core/api/...` not
        # `/core/websocket/api/...`.
        base = hass_url.rstrip("/").replace("ws://", "http://").replace("wss://", "https://")
        if base.endswith("/websocket"):
            base = base[: -len("/websocket")]
        self._hass_url = base
        self._hass_token = hass_token
        self._manifest_url = manifest_url
        self._site_token = site_token
        self._refresh_sec = refresh_sec
        self._last_hash: Optional[str] = None
        # Hook the LiveUplink so its `hello` frame carries the latest hash.
        self._manifest_hash_listeners: list[callable] = []

    def add_hash_listener(self, fn) -> None:
        self._manifest_hash_listeners.append(fn)

    async def run_forever(self) -> None:
        while True:
            try:
                await self.refresh_once()
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                log.warning("discovery refresh failed: %s", e)
            await asyncio.sleep(self._refresh_sec)

    async def refresh_once(self) -> None:
        states, entity_registry, device_registry, services = await self._fetch_all()
        entities = build_manifest(states, entity_registry, device_registry, services)
        h = manifest_hash(entities)
        if h == self._last_hash:
            log.debug("discovery: manifest hash unchanged (%s entities)", len(entities))
            return
        await self._push_manifest(entities, h)
        self._last_hash = h
        for fn in self._manifest_hash_listeners:
            try:
                fn(h)
            except Exception as e:  # noqa: BLE001
                log.debug("manifest hash listener raised (continuing): %s", e)
        log.info("discovery: manifest pushed (%s entities, hash=%s…)", len(entities), h[:12])

    async def _fetch_all(self):
        headers = {"Authorization": f"Bearer {self._hass_token}"}
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as s:
            states_task = self._get_json(s, "/api/states", headers)
            er_task = self._get_json(s, "/api/config/entity_registry/list", headers)
            dr_task = self._get_json(s, "/api/config/device_registry/list", headers)
            services_task = self._get_json(s, "/api/services", headers)
            states, er, dr, services = await asyncio.gather(
                states_task, er_task, dr_task, services_task,
            )
        return (
            states if isinstance(states, list) else [],
            er if isinstance(er, list) else [],
            dr if isinstance(dr, list) else [],
            services if (isinstance(services, dict) or isinstance(services, list)) else {},
        )

    async def _get_json(self, session: aiohttp.ClientSession, path: str, headers: dict):
        # `/api/config/{entity,device}_registry/list` are NOT REST endpoints in
        # current HA — they're WebSocket-only commands. Returning [] on 404
        # keeps the manifest viable (states + services still ship) while we
        # plan the WS-based fetcher. Without this, the entire discovery
        # refresh aborts and no manifest ever reaches the cloud.
        url = f"{self._hass_url}{path}"
        async with session.get(url, headers=headers) as r:
            if r.status == 404:
                log.warning(
                    "HA %s returned 404 — endpoint unavailable, treating as empty. "
                    "Manifest will lack registry-derived fields.",
                    path,
                )
                return []
            if r.status >= 400:
                raise RuntimeError(f"HA {path} returned HTTP {r.status}")
            return await r.json()

    async def _push_manifest(self, entities: list[ManifestEntity], h: str) -> None:
        payload = {
            "manifest_hash": h,
            "entities": [e.__dict__ for e in entities],
        }
        headers = {
            "Authorization": f"Bearer {self._site_token}",
            "Content-Type": "application/json",
        }
        timeout = aiohttp.ClientTimeout(total=PUSH_TIMEOUT_SEC)
        async with aiohttp.ClientSession(timeout=timeout) as s, s.post(
            self._manifest_url, headers=headers, json=payload,
        ) as r:
            if r.status >= 400:
                raise RuntimeError(f"manifest POST returned HTTP {r.status}: {await r.text()}")
