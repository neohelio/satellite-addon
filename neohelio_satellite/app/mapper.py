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

Per-field rolling aggregation (#185):
  Each entity declares an `aggregation` mode in the blueprint. Modes
  'avg' / 'max' / 'min' / 'sum' accumulate over the poll window and
  reset on snapshot — used for SunSpec mode where native poll cadence
  (5s) is much faster than emit cadence (300s default). Mode 'last'
  (the HA-path default) preserves the most recent value forever and
  is NOT cleared between flushes.
"""
from __future__ import annotations
import logging
import math
from dataclasses import dataclass

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


# ── Per-field accumulator ────────────────────────────────────────────────────


@dataclass
class _AccState:
    """Rolling per-field accumulator state for one (device, field) pair.

    Public via Accumulator.add() / value() / reset(); the dataclass is
    internal so tests can inspect raw counters when useful.
    """
    mode: str
    last: float | None = None
    sum: float = 0.0
    count: int = 0
    max: float | None = None
    min: float | None = None


class Accumulator:
    """Rolling aggregation over a poll window for one (device, field) pair.

    The five supported modes mirror the blueprint vocabulary in #185:

      'last'   — store the most recent value. NEVER reset. Existing HA-path
                 behaviour preserved. snapshot() always returns the last
                 value seen, even if no add() has happened in this window.
      'avg'    — sum / count over the window. reset() zeroes both.
      'max'    — running peak. reset() clears it.
      'min'    — running trough. reset() clears it.
      'sum'    — running total. Useful for cumulative-energy-delta payloads
                 where each tick reports an interval slice. reset() zeroes.

    For 'avg', 'max', 'min', 'sum': value() returns None until at least one
    add() has happened in the current window. After reset(), value() returns
    None until the next add().

    For 'last': value() returns None until the first add() ever, but then
    persists across reset() calls.
    """

    def __init__(self, mode: str = "last"):
        if mode not in ("last", "avg", "max", "min", "sum"):
            raise ValueError(f"unsupported accumulator mode: {mode!r}")
        self._state = _AccState(mode=mode)

    @property
    def mode(self) -> str:
        return self._state.mode

    def add(self, v: float) -> None:
        if not isinstance(v, (int, float)) or not math.isfinite(v):
            return
        v = float(v)
        s = self._state
        if s.mode == "last":
            s.last = v
        elif s.mode == "avg":
            s.sum += v
            s.count += 1
        elif s.mode == "sum":
            s.sum += v
            s.count += 1
        elif s.mode == "max":
            s.max = v if s.max is None else (v if v > s.max else s.max)
        elif s.mode == "min":
            s.min = v if s.min is None else (v if v < s.min else s.min)

    def value(self) -> float | None:
        s = self._state
        if s.mode == "last":
            return s.last
        if s.mode == "avg":
            return (s.sum / s.count) if s.count > 0 else None
        if s.mode == "sum":
            return s.sum if s.count > 0 else None
        if s.mode == "max":
            return s.max
        if s.mode == "min":
            return s.min
        return None  # unreachable

    def reset(self) -> None:
        """Clear window state. No-op for 'last' so the HA-path keeps its
        long-lived 'most recent value' semantics; aggregating modes reset
        their counters to start the next window fresh."""
        s = self._state
        if s.mode == "last":
            return
        s.sum = 0.0
        s.count = 0
        s.max = None
        s.min = None


class StateBucket:
    """Aggregates per-device readings. The reader pushes individual entity
    updates; on flush we emit one batch per device, each batch carrying all
    the current values for that device.

    Per-field aggregation (#185):
      Each entity declares its `aggregation` mode in the blueprint. The
      bucket maintains an Accumulator per (device, field). On snapshot()
      the bucket returns each accumulator's value() and then reset()s it
      — but reset() is a no-op for 'last' mode, so existing HA-path
      semantics ('keep last seen value forever, even when slow-changing
      fields go quiet') are preserved without conditional code at the
      call site. Aggregating modes ('avg' / 'max' / 'min' / 'sum') start
      each new poll window with a clean counter.

    The bucket is also NOT rebuilt when the cloud-pushed Blueprint refreshes
    — we update the entity→field lookup in place via `update_blueprint` so
    the same accumulators continue to receive ingests across blueprint
    refreshes (the previous version rebuilt the entire bucket on every
    refresh, silently wiping accumulated values)."""

    def __init__(self, blueprint: Blueprint):
        self._bp = blueprint
        self._lookup = blueprint.by_entity()
        # device_external_id → { field: Accumulator }
        self._buf: dict[str, dict[str, Accumulator]] = {}

    def update_blueprint(self, blueprint: Blueprint) -> None:
        """Swap in a new Blueprint without touching accumulator state.

        If a previously-mapped entity's aggregation mode changes in the new
        blueprint, the accumulator for that field is rebuilt with the new
        mode (any pending window data is discarded — the operator changing
        the mode mid-window is the rare case and the safest behaviour).
        """
        self._bp = blueprint
        new_lookup = blueprint.by_entity()
        # Rebuild accumulators whose mode changed.
        for spec in new_lookup.values():
            dev_buf = self._buf.get(spec.device_external_id)
            if dev_buf is None:
                continue
            existing = dev_buf.get(spec.field)
            if existing is not None and existing.mode != spec.aggregation:
                dev_buf[spec.field] = Accumulator(spec.aggregation)
        self._lookup = new_lookup

    def ingest(self, entity_id: str, state_value: str, full_state: dict) -> None:
        spec = self._lookup.get(entity_id)
        if spec is None:
            return
        v = _coerce_number(state_value)
        if v is None:
            return
        v = apply_transform(v, spec)
        dev_buf = self._buf.setdefault(spec.device_external_id, {})
        acc = dev_buf.get(spec.field)
        if acc is None or acc.mode != spec.aggregation:
            acc = Accumulator(spec.aggregation)
            dev_buf[spec.field] = acc
        acc.add(v)

    def snapshot(self) -> dict[str, dict[str, float]]:
        """Emit current values per (device, field), then reset window state.

        Fields whose accumulator has no value yet (aggregating modes that
        received no ingest in the current window AND have never received
        one before) are omitted from the snapshot — they would be NULL in
        Silver anyway. 'last'-mode fields with a stored value are always
        emitted because the HA-path Live Dashboard relies on slow-changing
        fields persisting across flushes.
        """
        out: dict[str, dict[str, float]] = {}
        for dev, fields in self._buf.items():
            row: dict[str, float] = {}
            for fname, acc in fields.items():
                v = acc.value()
                if v is not None:
                    row[fname] = v
                acc.reset()  # no-op for 'last'; clears window for aggregating modes
            if row:
                out[dev] = row
        return out

    def device_types(self) -> dict[str, str]:
        # Pick the first known device_type per device_external_id from the
        # blueprint's entity list. Devices the operator hasn't mapped any
        # entities for won't appear in the snapshot anyway.
        out: dict[str, str] = {}
        for e in self._bp.entities:
            out.setdefault(e.device_external_id, e.device_type)
        return out
