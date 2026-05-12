"""Tests for the pure-function pieces of discovery.py.

Network calls (`refresh_once`, `_fetch_all`, `_push_manifest`) are excluded
— they'd need a stub HTTP server and would slow the suite. The hot path is
`build_manifest` (the actual data shaping) and `manifest_hash` (determinism)."""
from __future__ import annotations

import json

import pytest

from discovery import (
    DiscoveryLoop,
    ManifestEntity,
    build_manifest,
    manifest_hash,
)


# Sample HA REST responses — minimised to the fields the manifest uses.

SAMPLE_STATES = [
    {
        "entity_id": "sensor.deye_battery_soc",
        "state": "84.3",
        "attributes": {
            "friendly_name": "Battery SoC",
            "device_class": "battery",
            "unit_of_measurement": "%",
        },
    },
    {
        "entity_id": "sensor.deye_grid_power",
        "state": "1.23",
        "attributes": {
            "friendly_name": "Grid Power",
            "device_class": "power",
            "unit_of_measurement": "kW",
        },
    },
    {
        "entity_id": "switch.ems_force_charge",
        "state": "off",
        "attributes": {
            "friendly_name": "Force charge",
        },
    },
]

SAMPLE_ENTITY_REGISTRY = [
    {"entity_id": "sensor.deye_battery_soc", "platform": "deye_inverter", "device_id": "dev-deye-1"},
    {"entity_id": "sensor.deye_grid_power", "platform": "deye_inverter", "device_id": "dev-deye-1"},
    {"entity_id": "switch.ems_force_charge", "platform": "ess_controller", "device_id": "dev-ess-1"},
]

SAMPLE_DEVICE_REGISTRY = [
    {"id": "dev-deye-1", "manufacturer": "Deye", "model": "SUN-12K-SG04LP3"},
    {"id": "dev-ess-1", "manufacturer": "NeoHelio", "model": "EMS-Lite"},
]

SAMPLE_SERVICES = {
    "switch": {"turn_on": {}, "turn_off": {}, "toggle": {}},
    "sensor": {},  # no controllable services for sensor entities
}


class TestBuildManifest:
    def test_returns_one_entry_per_state(self):
        manifest = build_manifest(
            SAMPLE_STATES, SAMPLE_ENTITY_REGISTRY, SAMPLE_DEVICE_REGISTRY, SAMPLE_SERVICES,
        )
        assert len(manifest) == 3

    def test_resolves_friendly_name_and_device_class(self):
        manifest = build_manifest(
            SAMPLE_STATES, SAMPLE_ENTITY_REGISTRY, SAMPLE_DEVICE_REGISTRY, SAMPLE_SERVICES,
        )
        soc = next(e for e in manifest if e.entity_id == "sensor.deye_battery_soc")
        assert soc.friendly_name == "Battery SoC"
        assert soc.device_class == "battery"
        assert soc.unit_of_measurement == "%"

    def test_resolves_integration_platform_from_entity_registry(self):
        manifest = build_manifest(
            SAMPLE_STATES, SAMPLE_ENTITY_REGISTRY, SAMPLE_DEVICE_REGISTRY, SAMPLE_SERVICES,
        )
        soc = next(e for e in manifest if e.entity_id == "sensor.deye_battery_soc")
        assert soc.integration_platform == "deye_inverter"

    def test_resolves_device_manufacturer_model(self):
        manifest = build_manifest(
            SAMPLE_STATES, SAMPLE_ENTITY_REGISTRY, SAMPLE_DEVICE_REGISTRY, SAMPLE_SERVICES,
        )
        soc = next(e for e in manifest if e.entity_id == "sensor.deye_battery_soc")
        assert soc.ha_device_manufacturer == "Deye"
        assert soc.ha_device_model == "SUN-12K-SG04LP3"

    def test_supported_services_lists_domain_services(self):
        manifest = build_manifest(
            SAMPLE_STATES, SAMPLE_ENTITY_REGISTRY, SAMPLE_DEVICE_REGISTRY, SAMPLE_SERVICES,
        )
        switch = next(e for e in manifest if e.entity_id == "switch.ems_force_charge")
        assert switch.supported_services == ["toggle", "turn_off", "turn_on"]

    def test_supported_services_empty_for_sensors(self):
        manifest = build_manifest(
            SAMPLE_STATES, SAMPLE_ENTITY_REGISTRY, SAMPLE_DEVICE_REGISTRY, SAMPLE_SERVICES,
        )
        soc = next(e for e in manifest if e.entity_id == "sensor.deye_battery_soc")
        assert soc.supported_services == []

    def test_handles_missing_entity_registry_entry(self):
        # Entity exists in states but not in entity_registry.
        states = [{"entity_id": "sun.sun", "state": "above_horizon", "attributes": {}}]
        manifest = build_manifest(states, [], [], {})
        assert len(manifest) == 1
        assert manifest[0].entity_id == "sun.sun"
        assert manifest[0].integration_platform is None
        assert manifest[0].ha_device_id is None

    def test_handles_missing_device_in_registry(self):
        # entity_registry says device_id="unknown" but it's not in device_registry.
        er = [{"entity_id": "sensor.foo", "platform": "x", "device_id": "ghost"}]
        states = [{"entity_id": "sensor.foo", "state": "1", "attributes": {}}]
        manifest = build_manifest(states, er, [], {})
        assert manifest[0].ha_device_id == "ghost"
        assert manifest[0].ha_device_manufacturer is None
        assert manifest[0].ha_device_model is None

    def test_output_is_sorted_for_deterministic_hash(self):
        # Reverse the states order on input — output should still be alphabetical.
        manifest = build_manifest(
            list(reversed(SAMPLE_STATES)), SAMPLE_ENTITY_REGISTRY, SAMPLE_DEVICE_REGISTRY, SAMPLE_SERVICES,
        )
        ids = [e.entity_id for e in manifest]
        assert ids == sorted(ids)


class TestManifestHash:
    def test_is_deterministic(self):
        m1 = build_manifest(SAMPLE_STATES, SAMPLE_ENTITY_REGISTRY, SAMPLE_DEVICE_REGISTRY, SAMPLE_SERVICES)
        m2 = build_manifest(SAMPLE_STATES, SAMPLE_ENTITY_REGISTRY, SAMPLE_DEVICE_REGISTRY, SAMPLE_SERVICES)
        assert manifest_hash(m1) == manifest_hash(m2)

    def test_changes_when_entity_added(self):
        m1 = build_manifest(SAMPLE_STATES, SAMPLE_ENTITY_REGISTRY, SAMPLE_DEVICE_REGISTRY, SAMPLE_SERVICES)
        states_plus = SAMPLE_STATES + [{"entity_id": "sensor.new", "state": "0", "attributes": {}}]
        m2 = build_manifest(states_plus, SAMPLE_ENTITY_REGISTRY, SAMPLE_DEVICE_REGISTRY, SAMPLE_SERVICES)
        assert manifest_hash(m1) != manifest_hash(m2)

    def test_does_not_change_when_state_changes(self):
        # Manifest is structural metadata only; the entity's instantaneous
        # state must NOT influence the hash, otherwise we'd POST a new
        # manifest every second a value changes.
        m1 = build_manifest(SAMPLE_STATES, SAMPLE_ENTITY_REGISTRY, SAMPLE_DEVICE_REGISTRY, SAMPLE_SERVICES)
        # Same set of entities, different state values.
        states_v2 = [dict(s, state="99.9") for s in SAMPLE_STATES]
        m2 = build_manifest(states_v2, SAMPLE_ENTITY_REGISTRY, SAMPLE_DEVICE_REGISTRY, SAMPLE_SERVICES)
        assert manifest_hash(m1) == manifest_hash(m2)
