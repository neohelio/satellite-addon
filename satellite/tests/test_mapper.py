"""Unit tests for mapper.py — Accumulator + StateBucket (#185).

Covers the per-field rolling aggregation introduced for SunSpec mode where
native poll cadence (5s) is much faster than emit cadence (300s default).
The 'last' mode must preserve the historical HA-path behaviour: snapshot
returns the last-seen value forever and never resets between flushes.
"""
from __future__ import annotations

import math

import pytest

from blueprint import Blueprint, EntitySpec
from mapper import Accumulator, StateBucket


# ── Accumulator: per-mode add/value/reset semantics ──────────────────────────


class TestAccumulatorLast:
    def test_returns_None_before_first_add(self):
        acc = Accumulator("last")
        assert acc.value() is None

    def test_returns_most_recent_value(self):
        acc = Accumulator("last")
        acc.add(1.0)
        acc.add(2.0)
        acc.add(3.0)
        assert acc.value() == 3.0

    def test_reset_is_noop(self):
        # The HA-path Live Dashboard depends on slow-changing fields
        # (battery SOC, daily kWh totals) persisting across flushes.
        acc = Accumulator("last")
        acc.add(42.0)
        acc.reset()
        assert acc.value() == 42.0

    def test_value_is_stable_after_many_resets(self):
        acc = Accumulator("last")
        acc.add(7.0)
        for _ in range(100):
            acc.reset()
        assert acc.value() == 7.0


class TestAccumulatorAvg:
    def test_returns_None_before_first_add(self):
        acc = Accumulator("avg")
        assert acc.value() is None

    def test_simple_average(self):
        acc = Accumulator("avg")
        for v in (10.0, 20.0, 30.0):
            acc.add(v)
        assert acc.value() == 20.0

    def test_average_one_value(self):
        acc = Accumulator("avg")
        acc.add(7.5)
        assert acc.value() == 7.5

    def test_average_with_negatives(self):
        # Bidirectional power is signed: import positive, export negative.
        acc = Accumulator("avg")
        for v in (-50.0, 50.0, -50.0, 50.0):
            acc.add(v)
        assert acc.value() == 0.0

    def test_reset_clears_window(self):
        acc = Accumulator("avg")
        acc.add(100.0)
        acc.reset()
        assert acc.value() is None
        acc.add(200.0)
        assert acc.value() == 200.0

    def test_reset_then_avg_starts_fresh(self):
        acc = Accumulator("avg")
        for v in (1.0, 1.0, 1.0):
            acc.add(v)
        acc.reset()
        for v in (10.0, 20.0):
            acc.add(v)
        assert acc.value() == 15.0  # not influenced by previous window's 1.0s


class TestAccumulatorMax:
    def test_returns_None_before_first_add(self):
        acc = Accumulator("max")
        assert acc.value() is None

    def test_running_peak(self):
        acc = Accumulator("max")
        for v in (5.0, 12.0, 8.0, 15.0, 3.0):
            acc.add(v)
        assert acc.value() == 15.0

    def test_max_with_all_negatives(self):
        acc = Accumulator("max")
        for v in (-10.0, -5.0, -20.0):
            acc.add(v)
        assert acc.value() == -5.0

    def test_max_resets_on_reset(self):
        acc = Accumulator("max")
        acc.add(99.0)
        acc.reset()
        assert acc.value() is None
        acc.add(1.0)
        assert acc.value() == 1.0


class TestAccumulatorMin:
    def test_running_trough(self):
        acc = Accumulator("min")
        for v in (5.0, 12.0, 1.0, 15.0, 3.0):
            acc.add(v)
        assert acc.value() == 1.0

    def test_min_resets_on_reset(self):
        acc = Accumulator("min")
        acc.add(0.5)
        acc.reset()
        assert acc.value() is None


class TestAccumulatorSum:
    def test_running_total(self):
        acc = Accumulator("sum")
        for v in (1.5, 2.5, 3.0):
            acc.add(v)
        assert acc.value() == 7.0

    def test_sum_resets_on_reset(self):
        acc = Accumulator("sum")
        acc.add(100.0)
        acc.add(200.0)
        acc.reset()
        assert acc.value() is None
        acc.add(50.0)
        assert acc.value() == 50.0


class TestAccumulatorBadInput:
    @pytest.mark.parametrize("mode", ["last", "avg", "max", "min", "sum"])
    def test_NaN_is_rejected(self, mode):
        acc = Accumulator(mode)
        acc.add(float("nan"))
        assert acc.value() is None  # silently dropped

    @pytest.mark.parametrize("mode", ["last", "avg", "max", "min", "sum"])
    def test_inf_is_rejected(self, mode):
        acc = Accumulator(mode)
        acc.add(math.inf)
        acc.add(-math.inf)
        assert acc.value() is None

    @pytest.mark.parametrize("mode", ["last", "avg", "max", "min", "sum"])
    def test_None_is_rejected(self, mode):
        acc = Accumulator(mode)
        # Accumulator.add doesn't take None as a typed input but we should
        # still tolerate it without crashing if a caller sneaks one in.
        acc.add(None)  # type: ignore[arg-type]
        assert acc.value() is None

    @pytest.mark.parametrize("mode", ["last", "avg", "max", "min", "sum"])
    def test_string_value_is_rejected(self, mode):
        acc = Accumulator(mode)
        acc.add("3.14")  # type: ignore[arg-type]
        assert acc.value() is None

    def test_unsupported_mode_raises(self):
        with pytest.raises(ValueError, match="unsupported accumulator mode"):
            Accumulator("median")


# ── StateBucket: per-(device, field) accumulation through ingest+snapshot ────


def _bp(*entities: EntitySpec) -> Blueprint:
    return Blueprint(
        schema_version=1,
        site_id="site-1",
        tenant_id="tenant-1",
        device_id="gw-1",
        subordinate_devices={},
        entities=list(entities),
    )


class TestStateBucketLastMode:
    def test_default_mode_is_last(self):
        # Backwards compatibility: an HA-path blueprint with no aggregation
        # field falls through to 'last'.
        spec = EntitySpec(
            entity_id="sensor.battery_soc",
            device_external_id="dev-1",
            device_type="BATTERY",
            field="battery_soc_pct",
        )
        assert spec.aggregation == "last"

        bucket = StateBucket(_bp(spec))
        bucket.ingest("sensor.battery_soc", "73.4", {})
        snap = bucket.snapshot()
        assert snap == {"dev-1": {"battery_soc_pct": 73.4}}

    def test_last_value_persists_across_snapshots(self):
        spec = EntitySpec(
            entity_id="sensor.battery_soc",
            device_external_id="dev-1",
            device_type="BATTERY",
            field="battery_soc_pct",
            aggregation="last",
        )
        bucket = StateBucket(_bp(spec))
        bucket.ingest("sensor.battery_soc", "73.4", {})
        bucket.snapshot()  # discard

        # Second snapshot with no new ingest still sees the last value —
        # this is the HA-path behaviour the Live Dashboard depends on.
        snap = bucket.snapshot()
        assert snap == {"dev-1": {"battery_soc_pct": 73.4}}


class TestStateBucketAvgMode:
    def test_avg_over_window(self):
        spec = EntitySpec(
            entity_id="sensor.ac_power",
            device_external_id="inv-1",
            device_type="INVERTER",
            field="ac_power_kw",
            aggregation="avg",
        )
        bucket = StateBucket(_bp(spec))
        # Native 5s cadence in a 5-min (60-sample) window:
        for v in ("8.4", "8.5", "8.3", "8.6", "8.2"):
            bucket.ingest("sensor.ac_power", v, {})

        snap = bucket.snapshot()
        assert snap["inv-1"]["ac_power_kw"] == pytest.approx(8.4)

    def test_avg_resets_after_snapshot(self):
        spec = EntitySpec(
            entity_id="sensor.ac_power",
            device_external_id="inv-1",
            device_type="INVERTER",
            field="ac_power_kw",
            aggregation="avg",
        )
        bucket = StateBucket(_bp(spec))
        for v in ("100", "200"):
            bucket.ingest("sensor.ac_power", v, {})
        bucket.snapshot()

        # Window 2: completely new samples, no echo of window 1 values.
        for v in ("10", "20"):
            bucket.ingest("sensor.ac_power", v, {})
        snap = bucket.snapshot()
        assert snap["inv-1"]["ac_power_kw"] == pytest.approx(15.0)

    def test_avg_field_with_no_window_data_is_omitted(self):
        spec = EntitySpec(
            entity_id="sensor.ac_power",
            device_external_id="inv-1",
            device_type="INVERTER",
            field="ac_power_kw",
            aggregation="avg",
        )
        bucket = StateBucket(_bp(spec))
        bucket.ingest("sensor.ac_power", "10", {})
        bucket.snapshot()  # consume

        # No new ingest -> nothing to emit (in contrast to 'last' mode which
        # would persist).
        snap = bucket.snapshot()
        assert snap == {}


class TestStateBucketMaxMode:
    def test_max_over_window_for_demand(self):
        # Demand-relevant fields (apparent_load_power_kva) want the peak
        # within the window for NMD calculations, not the avg.
        spec = EntitySpec(
            entity_id="sensor.apparent_load",
            device_external_id="meter-1",
            device_type="METER",
            field="apparent_load_power_kva",
            aggregation="max",
        )
        bucket = StateBucket(_bp(spec))
        for v in ("80", "120", "95", "150", "100"):
            bucket.ingest("sensor.apparent_load", v, {})
        snap = bucket.snapshot()
        assert snap["meter-1"]["apparent_load_power_kva"] == 150.0


class TestStateBucketMixedModes:
    def test_two_fields_one_avg_one_last_on_same_device(self):
        spec_power = EntitySpec(
            entity_id="sensor.ac_power",
            device_external_id="inv-1",
            device_type="INVERTER",
            field="ac_power_kw",
            aggregation="avg",
        )
        spec_soc = EntitySpec(
            entity_id="sensor.battery_soc",
            device_external_id="inv-1",
            device_type="INVERTER",
            field="battery_soc_pct",
            aggregation="last",
        )
        bucket = StateBucket(_bp(spec_power, spec_soc))

        # Power ticks every 5s; SOC every 60s. Multiple power samples,
        # one SOC sample per window.
        for v in ("8", "10", "12"):
            bucket.ingest("sensor.ac_power", v, {})
        bucket.ingest("sensor.battery_soc", "73", {})

        snap = bucket.snapshot()
        assert snap["inv-1"]["ac_power_kw"] == pytest.approx(10.0)  # avg
        assert snap["inv-1"]["battery_soc_pct"] == 73.0             # last

        # Window 2: more power samples, no SOC update. SOC must persist.
        for v in ("4", "8"):
            bucket.ingest("sensor.ac_power", v, {})
        snap2 = bucket.snapshot()
        assert snap2["inv-1"]["ac_power_kw"] == pytest.approx(6.0)
        assert snap2["inv-1"]["battery_soc_pct"] == 73.0  # persisted


class TestStateBucketBlueprintRefresh:
    def test_aggregation_mode_change_rebuilds_accumulator(self):
        # If an operator switches a field from 'avg' to 'max' mid-flight,
        # we discard pending window data (the safe default). Otherwise
        # accumulators keep their state across blueprint refreshes — the
        # whole point of update_blueprint() instead of rebuild.
        spec_v1 = EntitySpec(
            entity_id="sensor.ac_power",
            device_external_id="inv-1",
            device_type="INVERTER",
            field="ac_power_kw",
            aggregation="avg",
        )
        bucket = StateBucket(_bp(spec_v1))
        for v in ("10", "20", "30"):
            bucket.ingest("sensor.ac_power", v, {})

        spec_v2 = EntitySpec(
            entity_id="sensor.ac_power",
            device_external_id="inv-1",
            device_type="INVERTER",
            field="ac_power_kw",
            aggregation="max",
        )
        bucket.update_blueprint(_bp(spec_v2))

        # After mode change, accumulator was rebuilt empty. Add one value
        # and snapshot — should be that value, not the old avg result.
        bucket.ingest("sensor.ac_power", "5", {})
        snap = bucket.snapshot()
        assert snap["inv-1"]["ac_power_kw"] == 5.0

    def test_blueprint_refresh_with_same_modes_preserves_accumulators(self):
        spec = EntitySpec(
            entity_id="sensor.ac_power",
            device_external_id="inv-1",
            device_type="INVERTER",
            field="ac_power_kw",
            aggregation="avg",
        )
        bucket = StateBucket(_bp(spec))
        for v in ("10", "20", "30"):
            bucket.ingest("sensor.ac_power", v, {})

        # Refresh with the same blueprint — accumulator state must survive.
        bucket.update_blueprint(_bp(spec))
        snap = bucket.snapshot()
        assert snap["inv-1"]["ac_power_kw"] == pytest.approx(20.0)


class TestStateBucketBackwardsCompatibility:
    def test_unmapped_entity_is_dropped(self):
        spec = EntitySpec(
            entity_id="sensor.known",
            device_external_id="dev-1",
            device_type="INVERTER",
            field="ac_power_kw",
        )
        bucket = StateBucket(_bp(spec))
        bucket.ingest("sensor.unknown", "42", {})
        assert bucket.snapshot() == {}

    def test_HA_unavailable_state_is_dropped(self):
        spec = EntitySpec(
            entity_id="sensor.battery",
            device_external_id="dev-1",
            device_type="BATTERY",
            field="battery_soc_pct",
        )
        bucket = StateBucket(_bp(spec))
        for s in ("unavailable", "unknown", "", "null", "none"):
            bucket.ingest("sensor.battery", s, {})
        assert bucket.snapshot() == {}

    def test_scale_and_offset_are_applied_before_aggregation(self):
        spec = EntitySpec(
            entity_id="sensor.power_w",  # HA exposes Watts
            device_external_id="inv-1",
            device_type="INVERTER",
            field="ac_power_kw",
            scale=0.001,                  # blueprint converts to kW
            aggregation="avg",
        )
        bucket = StateBucket(_bp(spec))
        for v in ("8000", "10000", "12000"):
            bucket.ingest("sensor.power_w", v, {})
        snap = bucket.snapshot()
        assert snap["inv-1"]["ac_power_kw"] == pytest.approx(10.0)  # 10000 W -> 10 kW


class TestStringFields:
    """field_type='string' entities bypass the numeric accumulator path.
    They always use 'last' semantics and are never cleared between flushes."""

    def _str_spec(self, entity_id: str, field: str, device: str = "inv-1") -> EntitySpec:
        return EntitySpec(
            entity_id=entity_id,
            device_external_id=device,
            device_type="INVERTER",
            field=field,
            field_type="string",
        )

    def test_string_value_appears_in_snapshot(self):
        spec = self._str_spec("sensor.alarm", "device_alarm")
        bucket = StateBucket(_bp(spec))
        bucket.ingest("sensor.alarm", "GROUND_FAULT", {})
        snap = bucket.snapshot()
        assert snap["inv-1"]["device_alarm"] == "GROUND_FAULT"

    def test_ha_unavailable_filtered_for_string(self):
        spec = self._str_spec("sensor.alarm", "device_alarm")
        bucket = StateBucket(_bp(spec))
        for sentinel in ("unavailable", "unknown", "", "none", "null"):
            bucket.ingest("sensor.alarm", sentinel, {})
        assert bucket.snapshot() == {}

    def test_string_persists_across_flushes(self):
        """String 'last' values must survive snapshot() the same way numeric
        'last' values do — the Live Dashboard relies on slow-changing fields
        (charge-status, fault codes) staying present across poll windows."""
        spec = self._str_spec("sensor.charge_status", "battery_charge_status")
        bucket = StateBucket(_bp(spec))
        bucket.ingest("sensor.charge_status", "Charging", {})
        bucket.snapshot()   # first flush — must not clear the value
        snap = bucket.snapshot()
        assert snap["inv-1"]["battery_charge_status"] == "Charging"

    def test_string_latest_value_wins(self):
        spec = self._str_spec("sensor.fault", "device_fault")
        bucket = StateBucket(_bp(spec))
        bucket.ingest("sensor.fault", "OVER_TEMP", {})
        bucket.ingest("sensor.fault", "GROUND_FAULT", {})
        snap = bucket.snapshot()
        assert snap["inv-1"]["device_fault"] == "GROUND_FAULT"

    def test_string_and_numeric_fields_coexist_on_same_device(self):
        numeric = EntitySpec(
            entity_id="sensor.soc", device_external_id="bat-1",
            device_type="BATTERY", field="battery_soc_pct",
        )
        string_spec = EntitySpec(
            entity_id="sensor.charge_status", device_external_id="bat-1",
            device_type="BATTERY", field="battery_charge_status",
            field_type="string",
        )
        bp = Blueprint(
            schema_version=1, site_id="s", tenant_id="t", device_id="g",
            entities=[numeric, string_spec],
        )
        bucket = StateBucket(bp)
        bucket.ingest("sensor.soc", "78.5", {})
        bucket.ingest("sensor.charge_status", "Discharging", {})
        snap = bucket.snapshot()
        assert snap["bat-1"]["battery_soc_pct"] == pytest.approx(78.5)
        assert snap["bat-1"]["battery_charge_status"] == "Discharging"
