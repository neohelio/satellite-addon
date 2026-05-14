"""Unit tests for commands.CommandExecutor — Phase C HA control caller.

Verifies:
  - Domain allowlist (defence-in-depth against a compromised cloud).
  - Frame validation (missing command_id / target_entity_id / etc.).
  - Successful HA call shape (POST /api/services/<domain>/<service>).
  - HA 4xx + 5xx responses surface as ok=False with a clear error.
  - Transport timeouts return ok=False without raising.

Tests are sync wrappers around asyncio.run() to match the existing satellite
test pattern (no pytest-asyncio dep in the addon).
"""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from unittest.mock import patch

from commands import CommandExecutor


def _run(coro):
    """Run an async test in a fresh event loop, leaving the previous loop
    untouched. Using `asyncio.run()` would close Python's default loop,
    which breaks test_live_uplink's queue creation when they run later in
    the same session (see 3.9's lazy-default-loop behaviour)."""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


# ── Helpers ─────────────────────────────────────────────────────────────────


class _FakeResponse:
    def __init__(self, status: int, body: str):
        self.status = status
        self._body = body

    async def text(self) -> str:
        return self._body

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakeSession:
    """Records the POST call and returns a configured response."""

    def __init__(self, response: _FakeResponse):
        self._response = response
        self.calls: list[tuple[str, dict, dict]] = []

    def post(self, url: str, headers=None, json=None):
        self.calls.append((url, headers or {}, json or {}))
        return self._response

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


@asynccontextmanager
async def _patch_session(response: _FakeResponse):
    """Patch `aiohttp.ClientSession(...)` to yield a fake session for one call."""
    fake = _FakeSession(response)

    def _factory(*_args, **_kwargs):
        return fake

    with patch("commands.aiohttp.ClientSession", side_effect=_factory):
        yield fake


# ── Tests ───────────────────────────────────────────────────────────────────


def test_executes_switch_turn_on() -> None:
    _run(_test_executes_switch_turn_on())


async def _test_executes_switch_turn_on() -> None:
    exe = CommandExecutor("ws://supervisor/core/websocket", "token-1")
    async with _patch_session(_FakeResponse(200, '[{"entity_id":"switch.x","state":"on"}]')) as session:
        result = await exe.execute({
            "command_id": "cmd-1",
            "target_entity_id": "switch.boardroom_lights",
            "ha_domain": "switch",
            "ha_service": "turn_on",
            "ha_service_data": {"entity_id": "switch.boardroom_lights"},
        })

    assert result["ok"] is True
    assert result["ha_response"] == [{"entity_id": "switch.x", "state": "on"}]
    assert len(session.calls) == 1
    url, headers, body = session.calls[0]
    assert url == "http://supervisor/core/api/services/switch/turn_on"
    assert headers["Authorization"] == "Bearer token-1"
    assert body == {"entity_id": "switch.boardroom_lights"}


def test_executes_climate_set_temperature() -> None:
    _run(_test_executes_climate_set_temperature())


async def _test_executes_climate_set_temperature() -> None:
    exe = CommandExecutor("ws://supervisor/core/websocket", "token-1")
    async with _patch_session(_FakeResponse(200, "[]")) as session:
        result = await exe.execute({
            "command_id": "cmd-2",
            "target_entity_id": "climate.floor1",
            "ha_domain": "climate",
            "ha_service": "set_temperature",
            "ha_service_data": {"entity_id": "climate.floor1", "temperature": 22.5},
        })

    assert result["ok"] is True
    _, _, body = session.calls[0]
    assert body["temperature"] == 22.5


def test_rejects_domain_outside_allowlist() -> None:
    _run(_test_rejects_domain_outside_allowlist())


async def _test_rejects_domain_outside_allowlist() -> None:
    exe = CommandExecutor("ws://supervisor/core/websocket", "token-1")
    # No HA call should be made for a forbidden domain.
    async with _patch_session(_FakeResponse(200, "[]")) as session:
        result = await exe.execute({
            "command_id": "cmd-3",
            "target_entity_id": "homeassistant.restart",
            "ha_domain": "homeassistant",
            "ha_service": "restart",
        })

    assert result["ok"] is False
    assert "not allowed" in (result.get("error") or "")
    assert len(session.calls) == 0


def test_rejects_missing_command_id() -> None:
    _run(_test_rejects_missing_command_id())


async def _test_rejects_missing_command_id() -> None:
    exe = CommandExecutor("ws://supervisor/core/websocket", "token-1")
    result = await exe.execute({
        "target_entity_id": "switch.x",
        "ha_domain": "switch",
        "ha_service": "turn_on",
    })
    assert result["ok"] is False
    assert "command_id" in (result.get("error") or "")


def test_surfaces_ha_4xx_as_error() -> None:
    _run(_test_surfaces_ha_4xx_as_error())


async def _test_surfaces_ha_4xx_as_error() -> None:
    exe = CommandExecutor("ws://supervisor/core/websocket", "token-1")
    async with _patch_session(_FakeResponse(400, '{"message":"unknown service"}')):
        result = await exe.execute({
            "command_id": "cmd-4",
            "target_entity_id": "switch.x",
            "ha_domain": "switch",
            "ha_service": "made_up",
        })
    assert result["ok"] is False
    assert "HTTP 400" in (result.get("error") or "")


def test_timeout_returns_ok_false() -> None:
    _run(_test_timeout_returns_ok_false())


async def _test_timeout_returns_ok_false() -> None:
    exe = CommandExecutor("ws://supervisor/core/websocket", "token-1")

    class _TimeoutSession:
        def post(self, *_args, **_kwargs):
            raise asyncio.TimeoutError()

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

    with patch("commands.aiohttp.ClientSession", side_effect=lambda *a, **k: _TimeoutSession()):
        result = await exe.execute({
            "command_id": "cmd-5",
            "target_entity_id": "switch.x",
            "ha_domain": "switch",
            "ha_service": "turn_on",
        })

    assert result["ok"] is False
    assert "timed out" in (result.get("error") or "")
