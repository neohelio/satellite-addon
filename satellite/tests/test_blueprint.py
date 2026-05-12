"""Unit tests for blueprint.py — _parse() handling of the new aggregation
field added in #185.

The blueprint payload is operator-controlled (via the NeoHelio onboarding
wizard or admin UI), so the parser must defend against unknown / mistyped
aggregation values rather than crashing the addon.
"""
from __future__ import annotations

import logging

import pytest

from blueprint import (
    DEFAULT_FLUSH_INTERVAL_SEC,
    MAX_FLUSH_INTERVAL_SEC,
    MIN_FLUSH_INTERVAL_SEC,
    VALID_AGGREGATIONS,
    Blueprint,
    BlueprintCache,
    PollingConfig,
    _parse,
)


def _payload(*entities: dict) -> dict:
    return {
        "schema_version": 1,
        "site_id":  "site-1",
        "tenant_id": "tenant-1",
        "device_id": "gw-1",
        "subordinate_devices": {},
        "entities": list(entities),
    }


class TestAggregationParsing:
    def test_default_is_last_when_field_missing(self):
        # Backwards compatibility: blueprints emitted by the onboarding
        # wizard before #185 don't include an aggregation key. They must
        # parse and behave identically to the pre-#185 build.
        bp = _parse(_payload({
            "entity_id":          "sensor.battery",
            "device_external_id": "dev-1",
            "field":              "battery_soc_pct",
        }))
        assert bp.entities[0].aggregation == "last"

    @pytest.mark.parametrize("mode", VALID_AGGREGATIONS)
    def test_each_documented_mode_is_accepted(self, mode):
        bp = _parse(_payload({
            "entity_id":          "sensor.x",
            "device_external_id": "dev-1",
            "field":              "ac_power_kw",
            "aggregation":        mode,
        }))
        assert bp.entities[0].aggregation == mode

    def test_unknown_mode_falls_back_to_last_with_warning(self, caplog):
        with caplog.at_level(logging.WARNING):
            bp = _parse(_payload({
                "entity_id":          "sensor.x",
                "device_external_id": "dev-1",
                "field":              "ac_power_kw",
                "aggregation":        "median",  # not a documented mode
            }))
        assert bp.entities[0].aggregation == "last"
        assert any(
            "median" in record.getMessage() and "last" in record.getMessage()
            for record in caplog.records
        ), "expected warning naming the bad mode and the fallback"

    def test_None_aggregation_falls_back_to_last(self, caplog):
        # Some operators or older API versions may emit explicit null. We
        # treat that as 'use the default' rather than crashing — None is
        # not in VALID_AGGREGATIONS so the warning path is the right place
        # to land.
        with caplog.at_level(logging.WARNING):
            bp = _parse(_payload({
                "entity_id":          "sensor.x",
                "device_external_id": "dev-1",
                "field":              "ac_power_kw",
                "aggregation":        None,
            }))
        assert bp.entities[0].aggregation == "last"

    def test_mixed_modes_in_one_blueprint(self):
        bp = _parse(_payload(
            {
                "entity_id":          "sensor.power",
                "device_external_id": "inv-1",
                "field":              "ac_power_kw",
                "aggregation":        "avg",
            },
            {
                "entity_id":          "sensor.soc",
                "device_external_id": "bat-1",
                "field":              "battery_soc_pct",
                "aggregation":        "last",
            },
            {
                "entity_id":          "sensor.demand",
                "device_external_id": "meter-1",
                "field":              "apparent_load_power_kva",
                "aggregation":        "max",
            },
        ))
        modes = {e.entity_id: e.aggregation for e in bp.entities}
        assert modes == {
            "sensor.power":  "avg",
            "sensor.soc":    "last",
            "sensor.demand": "max",
        }

    def test_aggregation_coexists_with_transform(self):
        # The transform block (scale/offset for unit conversion) and the
        # aggregation field are independent. Both must round-trip.
        bp = _parse(_payload({
            "entity_id":          "sensor.power_w",
            "device_external_id": "inv-1",
            "field":              "ac_power_kw",
            "transform":          {"scale": 0.001, "offset": 0.0},
            "aggregation":        "avg",
        }))
        spec = bp.entities[0]
        assert spec.scale == 0.001
        assert spec.aggregation == "avg"


class TestPollingParsing:
    """Polling config is cloud-controlled; defend against missing / malformed
    payloads + out-of-bounds values so the addon never gets a flush rate
    that violates the option schema."""

    def test_missing_polling_uses_default(self):
        bp = _parse(_payload())
        assert bp.polling.flush_interval_sec == DEFAULT_FLUSH_INTERVAL_SEC

    def test_polling_field_extracted(self):
        payload = _payload()
        payload["polling"] = {"flush_interval_sec": 20}
        bp = _parse(payload)
        assert bp.polling.flush_interval_sec == 20

    def test_polling_fractional_rounded(self):
        payload = _payload()
        payload["polling"] = {"flush_interval_sec": 12.6}
        bp = _parse(payload)
        assert bp.polling.flush_interval_sec == 13

    def test_polling_below_min_falls_back_to_default(self, caplog):
        payload = _payload()
        payload["polling"] = {"flush_interval_sec": MIN_FLUSH_INTERVAL_SEC - 1}
        with caplog.at_level(logging.WARNING, logger="blueprint"):
            bp = _parse(payload)
        assert bp.polling.flush_interval_sec == DEFAULT_FLUSH_INTERVAL_SEC
        assert any("out of bounds" in r.message for r in caplog.records)

    def test_polling_above_max_falls_back_to_default(self, caplog):
        payload = _payload()
        payload["polling"] = {"flush_interval_sec": MAX_FLUSH_INTERVAL_SEC + 1}
        with caplog.at_level(logging.WARNING, logger="blueprint"):
            bp = _parse(payload)
        assert bp.polling.flush_interval_sec == DEFAULT_FLUSH_INTERVAL_SEC

    def test_polling_non_numeric_uses_default(self):
        payload = _payload()
        payload["polling"] = {"flush_interval_sec": "thirty"}
        bp = _parse(payload)
        assert bp.polling.flush_interval_sec == DEFAULT_FLUSH_INTERVAL_SEC

    def test_polling_non_object_uses_default(self):
        payload = _payload()
        payload["polling"] = 30
        bp = _parse(payload)
        assert bp.polling.flush_interval_sec == DEFAULT_FLUSH_INTERVAL_SEC


class TestBlueprintSignature:
    """signature() is used by main.py's change detection — equivalent
    blueprints must produce equal signatures so we don't re-snapshot HA
    unnecessarily; meaningful changes must change the signature so we do."""

    def _bp(self, entities=None, polling_sec=5, sub_devices=None) -> Blueprint:
        return Blueprint(
            schema_version=1, site_id="s", tenant_id="t", device_id="g",
            subordinate_devices=sub_devices or {},
            entities=entities or [],
            polling=PollingConfig(flush_interval_sec=polling_sec),
        )

    def test_equivalent_blueprints_match(self):
        a = self._bp()
        b = self._bp()
        assert a.signature() == b.signature()

    def test_polling_change_changes_signature(self):
        a = self._bp(polling_sec=5)
        b = self._bp(polling_sec=10)
        assert a.signature() != b.signature()

    def test_subordinate_devices_change_changes_signature(self):
        a = self._bp(sub_devices={"inv1": "uuid-1"})
        b = self._bp(sub_devices={"inv1": "uuid-1", "inv2": "uuid-2"})
        assert a.signature() != b.signature()


class TestBlueprintCacheChangeListeners:
    """Listeners fire on signature change; broken listeners must not break
    the refresh loop."""

    def test_current_flush_interval_before_load_returns_default(self):
        cache = BlueprintCache("http://example/blueprint", "tok", 300)
        assert cache.current_flush_interval_sec() == DEFAULT_FLUSH_INTERVAL_SEC

    def test_current_flush_interval_after_load_reads_polling(self):
        cache = BlueprintCache("http://example/blueprint", "tok", 300)
        bp = Blueprint(
            schema_version=1, site_id="s", tenant_id="t", device_id="g",
            polling=PollingConfig(flush_interval_sec=42),
        )
        # Bypass HTTP — wire current directly the way fetch_once would.
        cache._current = bp  # noqa: SLF001
        assert cache.current_flush_interval_sec() == 42
