"""Unit tests for live_uplink.LiveUplink — focused on the synchronous,
side-effect-free pieces (enqueue, queue overflow, manifest_hash setter).

The async run_forever loop is integration-tested by spinning up a stub WS
server; that lives outside this file (see tests/test_live_uplink_integration.py
when added). Keep this file fast and dep-free."""

from __future__ import annotations

import asyncio
import json

import pytest

from live_uplink import LiveUplink, QUEUE_MAXSIZE


def _new_uplink() -> LiveUplink:
    return LiveUplink(
        relay_url="wss://relay.example/realtime/satellite/gw1",
        site_token="ngst_test.test",
        gateway_serial="gw1",
    )


class TestEnqueue:
    def test_enqueue_pushes_state_frame(self):
        up = _new_uplink()
        up.enqueue("sensor.soc", 84.3, {"unit_of_measurement": "%"}, "2026-05-11T10:00:00Z")
        frame = up._queue.get_nowait()  # noqa: SLF001
        assert frame["type"] == "state"
        assert frame["entity_id"] == "sensor.soc"
        assert frame["state"] == 84.3
        assert frame["attributes"]["unit_of_measurement"] == "%"
        assert frame["last_changed"] == "2026-05-11T10:00:00Z"

    def test_enqueue_handles_none_optionals(self):
        up = _new_uplink()
        up.enqueue("sensor.power", 1.23)
        frame = up._queue.get_nowait()  # noqa: SLF001
        assert frame["attributes"] is None
        assert frame["last_changed"] is None

    def test_enqueue_is_non_blocking_under_overflow(self):
        # Fill the queue to capacity, then enqueue one more — must not block,
        # must drop the oldest, must record the drop.
        up = _new_uplink()
        for i in range(QUEUE_MAXSIZE):
            up.enqueue(f"sensor.{i}", float(i))
        assert up._queue.qsize() == QUEUE_MAXSIZE  # noqa: SLF001
        up.enqueue("sensor.new", 999.0)
        assert up._queue.qsize() == QUEUE_MAXSIZE  # noqa: SLF001
        assert up._dropped_since_last_log == 1  # noqa: SLF001
        # The first item should have been evicted; the last item should be the new one.
        first = up._queue.get_nowait()  # noqa: SLF001
        assert first["entity_id"] != "sensor.0"  # 0 was dropped on overflow

    def test_state_frame_shape_is_json_serializable(self):
        # Every frame must round-trip through json.dumps without TypeError —
        # the async sender uses ws.send_str(json.dumps(frame)).
        up = _new_uplink()
        up.enqueue("sensor.foo", None, {"complex": {"nested": [1, 2, 3]}}, None)
        frame = up._queue.get_nowait()  # noqa: SLF001
        encoded = json.dumps(frame)
        decoded = json.loads(encoded)
        assert decoded["entity_id"] == "sensor.foo"


class TestManifestHash:
    def test_default_is_pending_phase_2_placeholder(self):
        up = _new_uplink()
        assert up._manifest_hash == "pending-phase-2"  # noqa: SLF001

    def test_set_manifest_hash_updates(self):
        up = _new_uplink()
        up.set_manifest_hash("abc123")
        assert up._manifest_hash == "abc123"  # noqa: SLF001
