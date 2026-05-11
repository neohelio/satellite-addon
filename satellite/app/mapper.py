"""Entity-to-CommonTelemetryReading mapper.

State arrives from HA as `(entity_id, state_string, full_state_dict)`. The
blueprint says which Silver column each entity feeds and the device it
belongs to. We accumulate per (device_id, timestamp_5s_bucket) so a flurry
of state_changed events at near-identical timestamps coalesces into one
telemetry_readings row downstream.

The mapper is intentionally narrow: it doesn't compute summary fields like
PV total or grid-flow direction — those are derived in cloud-side
bronze-to-silver where the Silver schema is canonical. The receiver will
also fill in tenant_id / site_id / device_id (the UUID) from the gateway
serial, so we send `device_external_id` here and let the cloud resolve.
"""
from __future__ import annotations
import logging
import math
from collections.abc import Iterable

from blueprint import Blueprint, EntitySpec

log = logging.getLogger("mapper")


def _coerce_number(value: str | float | int | None) -> float | None:
    if value is None: return None
    if isinstance(value, (int, float)):
        return float(value) if math.isfinite(value) else None
    s = str(value).strip()
    # HA uses 'unavailable' / 'unknown' / 'on' / 'off' for non-numeric states.
    if not s or s.lower() in {"unavailable", "unknown", "none", "null"}:
        return None
    try:
        f = float(s)
        return f if math.isfinite(f) else None
    except (TypeError, ValueError):
        return None


def apply_transform(value: float, spec: EntitySpec) -> float:
    return value * spec.scale + spec.offset


class StateBucket:
    """Aggregates per-device readings. The reader pushes individual entity
    updates; on flush we emit one batch per device, each batch carrying all
    the current values for that device.

    The buffer is NOT cleared between flushes (so slow-changing fields like
    battery SOC and daily kWh totals remain populated even when they don't
    tick every poll). It is also NOT cleared when the cloud-pushed Blueprint
    refreshes — we update the entity→field lookup in place via
    `update_blueprint`. Earlier the addon rebuilt this entire bucket on every
    blueprint refresh because each fetch returns a fresh Blueprint object
    even if content is identical, which silently wiped accumulated values."""

    def __init__(self, blueprint: Blueprint):
        self._bp = blueprint
        self._lookup = blueprint.by_entity()
        # device_external_id → { field: value }
        self._buf: dict[str, dict[str, float]] = {}

    def update_blueprint(self, blueprint: Blueprint) -> None:
        """Swap in a new Blueprint without touching accumulated readings."""
        self._bp = blueprint
        self._lookup = blueprint.by_entity()

    def ingest(self, entity_id: str, state_value: str, full_state: dict) -> None:
        spec = self._lookup.get(entity_id)
        if spec is None:
            return
        v = _coerce_number(state_value)
        if v is None:
            return
        v = apply_transform(v, spec)
        dev_buf = self._buf.setdefault(spec.device_external_id, {})
        dev_buf[spec.field] = v

    def snapshot(self) -> dict[str, dict[str, float]]:
        # Return a shallow copy — caller frees the underlying dict via reset().
        return {dev: dict(fields) for dev, fields in self._buf.items()}

    def device_types(self) -> dict[str, str]:
        # Pick the first known device_type per device_external_id from the
        # blueprint's entity list. Devices the operator hasn't mapped any
        # entities for won't appear in the snapshot anyway.
        out: dict[str, str] = {}
        for e in self._bp.entities:
            out.setdefault(e.device_external_id, e.device_type)
        return out
