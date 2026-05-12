"""Cloud-pushed entity-to-Silver-column mapping.

The Satellite add-on is intentionally dumb about device semantics — it doesn't
know that `sensor.deye_battery_soc` means battery SOC. The mapping lives in
NeoHelio cloud per (gateway_serial), and the add-on fetches it on start +
periodically refreshes. Operators edit blueprints via the NeoHelio onboarding
wizard or admin UI; the add-on picks up changes within `blueprint_refresh_sec`.

Blueprint shape (returned by GET /v1/edge-gateways/<serial>/blueprint):
{
  "schema_version": 1,
  "site_id": "uuid",
  "tenant_id": "uuid",
  "device_id": "uuid",                 // gateway Device.id
  "subordinate_devices": {              // child Device rows by external_id
    "deye-2787155991": "<device-uuid>"
  },
  "entities": [
    {
      "entity_id": "sensor.deye_battery_soc",
      "device_external_id": "deye-2787155991",
      "device_type": "INVERTER",
      "field": "battery_soc_pct",       // → telemetry_readings column
      "transform": null,                // optional: { scale, offset, unit_in, unit_out }
      "aggregation": "last"             // 'last' | 'avg' | 'max' | 'min' | 'sum' (#185)
    },
    ...
  ]
}

Per-window aggregation rules (issue #185):
  - 'last' (default): preserve historical HA-path behaviour — snapshot returns
    the most recent value, the bucket is NOT cleared between flushes.
  - 'avg' / 'max' / 'min' / 'sum': accumulate over the poll window, snapshot
    returns the aggregate and resets the accumulator. Used for SunSpec mode
    where poll_interval is short (5s native) but emit cadence is coarse
    (300s default).
"""
from __future__ import annotations
import asyncio
import logging
from dataclasses import dataclass, field

import aiohttp

log = logging.getLogger("blueprint")


VALID_AGGREGATIONS = ('last', 'avg', 'max', 'min', 'sum')

# Cadence bounds — mirror the values the cloud-side resolvePolling() helper
# enforces. Out-of-band values from the cloud are clamped here so a malformed
# blueprint can never force the addon into a pathological flush rate.
MIN_FLUSH_INTERVAL_SEC = 1
MAX_FLUSH_INTERVAL_SEC = 300
DEFAULT_FLUSH_INTERVAL_SEC = 5


@dataclass
class PollingConfig:
    """Cloud-pushed polling cadence for the slow NDJSON path. Hot-reloaded on
    every blueprint refresh; no addon restart needed when an operator changes
    it from the NeoHelio UI."""
    flush_interval_sec: int = DEFAULT_FLUSH_INTERVAL_SEC


@dataclass
class EntitySpec:
    entity_id: str
    device_external_id: str
    device_type: str
    field: str
    scale: float = 1.0
    offset: float = 0.0
    # #185: per-field rolling aggregation over the poll window. 'last' is the
    # historical behaviour and stays the default for HA-path backwards
    # compatibility. SunSpec-mode blueprints set this per-field.
    aggregation: str = 'last'


@dataclass
class Blueprint:
    schema_version: int
    site_id: str
    tenant_id: str
    device_id: str
    subordinate_devices: dict[str, str] = field(default_factory=dict)
    entities: list[EntitySpec] = field(default_factory=list)
    polling: PollingConfig = field(default_factory=PollingConfig)

    def by_entity(self) -> dict[str, EntitySpec]:
        return {e.entity_id: e for e in self.entities}

    def watched_entities(self) -> list[str]:
        return [e.entity_id for e in self.entities]

    def signature(self) -> tuple:
        """Stable signature for change detection. Two blueprints with the same
        signature can be treated as equivalent — used by main.py to decide
        whether to trigger a fresh HA snapshot."""
        return (
            tuple(sorted(e.entity_id for e in self.entities)),
            tuple(sorted(self.subordinate_devices.keys())),
            self.polling.flush_interval_sec,
        )


def _parse_polling(raw: object) -> PollingConfig:
    """Parse the cloud's `polling` object, clamping out-of-bounds values to
    the safe default. The cloud-side resolver already enforces bounds; this
    is defensive so an addon never goes haywire on a malformed payload."""
    if not isinstance(raw, dict):
        return PollingConfig()
    v = raw.get("flush_interval_sec")
    if not isinstance(v, (int, float)):
        return PollingConfig()
    if v < MIN_FLUSH_INTERVAL_SEC or v > MAX_FLUSH_INTERVAL_SEC:
        log.warning(
            "blueprint polling.flush_interval_sec=%r out of bounds; falling back to default %ds",
            v, DEFAULT_FLUSH_INTERVAL_SEC,
        )
        return PollingConfig()
    return PollingConfig(flush_interval_sec=int(round(v)))


def _parse(data: dict) -> Blueprint:
    raw_entities = data.get("entities", []) or []
    entities = []
    for e in raw_entities:
        t = e.get("transform") or {}
        agg = e.get("aggregation", "last")
        if agg not in VALID_AGGREGATIONS:
            log.warning(
                "blueprint entity %s has unsupported aggregation %r; falling back to 'last'",
                e.get("entity_id", "<unknown>"), agg,
            )
            agg = "last"
        entities.append(EntitySpec(
            entity_id=e["entity_id"],
            device_external_id=e["device_external_id"],
            device_type=e.get("device_type", "INVERTER"),
            field=e["field"],
            scale=float(t.get("scale", 1.0)),
            offset=float(t.get("offset", 0.0)),
            aggregation=agg,
        ))
    return Blueprint(
        schema_version=int(data.get("schema_version", 1)),
        site_id=data["site_id"],
        tenant_id=data["tenant_id"],
        device_id=data["device_id"],
        subordinate_devices=data.get("subordinate_devices", {}) or {},
        entities=entities,
        polling=_parse_polling(data.get("polling")),
    )


ChangeListener = "Callable[[Blueprint, Blueprint | None], None]"  # documentation only


class BlueprintCache:
    """Keeps the latest blueprint in memory; refresh in the background.

    Listeners registered via `add_change_listener` are invoked whenever a
    fetch produces a blueprint whose signature differs from the previous one.
    main.py uses this to (a) re-snapshot HA so newly-mapped entities flow
    immediately and (b) hot-reload the flush cadence."""

    def __init__(self, url: str, token: str, refresh_sec: int):
        self._url = url
        self._token = token
        self._refresh = refresh_sec
        self._current: Blueprint | None = None
        self._cv = asyncio.Condition()
        self._listeners: list = []

    @property
    def current(self) -> Blueprint | None:
        return self._current

    def current_flush_interval_sec(self) -> int:
        """Live polling cadence read by the addon's flush_loop. Falls back to
        DEFAULT_FLUSH_INTERVAL_SEC before the first blueprint fetch lands."""
        if self._current is None:
            return DEFAULT_FLUSH_INTERVAL_SEC
        return self._current.polling.flush_interval_sec

    def add_change_listener(self, fn) -> None:
        """Register a sync callback `fn(new_bp, old_bp) -> None` invoked
        whenever the blueprint's signature changes. Exceptions are caught
        + logged so a broken listener never breaks the refresh loop."""
        self._listeners.append(fn)

    async def fetch_once(self) -> Blueprint:
        headers = {"Authorization": f"Bearer {self._token}"}
        async with aiohttp.ClientSession() as s, s.get(self._url, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as r:
            r.raise_for_status()
            payload = await r.json()
        bp = _parse(payload)
        prev = self._current
        async with self._cv:
            self._current = bp
            self._cv.notify_all()
        log.info(
            "blueprint loaded: %d entities, %d sub-devices, flush_interval_sec=%d",
            len(bp.entities), len(bp.subordinate_devices), bp.polling.flush_interval_sec,
        )
        if prev is None or prev.signature() != bp.signature():
            for fn in self._listeners:
                try:
                    fn(bp, prev)
                except Exception as e:  # noqa: BLE001
                    log.warning("blueprint change listener raised (continuing): %s", e)
        return bp

    async def refresh_loop(self) -> None:
        while True:
            try:
                await self.fetch_once()
            except Exception as e:  # noqa: BLE001
                log.warning("blueprint refresh failed (will retry): %s", e)
            await asyncio.sleep(self._refresh)

    async def wait_until_loaded(self, timeout: float = 30.0) -> Blueprint:
        async with self._cv:
            await asyncio.wait_for(self._cv.wait_for(lambda: self._current is not None), timeout=timeout)
            assert self._current is not None
            return self._current
