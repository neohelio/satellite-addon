"""Home Assistant WebSocket client.

Subscribes to state_changed events, filters to entity_ids in the active
blueprint, and forwards each new state to the mapper. Reconnects on close
with exponential backoff (HA Core restarts during updates / OS reboots).

Reference: https://developers.home-assistant.io/docs/api/websocket/
"""
from __future__ import annotations
import asyncio
import json
import logging
import random
from collections.abc import Callable, Awaitable

import websockets

log = logging.getLogger("ha")


class HassWebsocket:
    def __init__(self, url: str, token: str, on_state_change: Callable[[str, str, dict], Awaitable[None]]):
        self._url = url
        self._token = token
        self._on_state_change = on_state_change
        self._msg_id = 0

    async def run_forever(self) -> None:
        backoff = 1.0
        while True:
            try:
                await self._connect_and_listen()
                backoff = 1.0  # reset on clean exit
            except Exception as e:  # noqa: BLE001
                log.warning("ha ws disconnected (%s); reconnecting in %.1fs", e, backoff)
                await asyncio.sleep(backoff + random.uniform(0, 0.5))
                backoff = min(backoff * 2, 30.0)

    async def _connect_and_listen(self) -> None:
        async with websockets.connect(self._url, max_size=2 ** 20) as ws:
            # Auth handshake.
            hello = json.loads(await ws.recv())
            assert hello.get("type") == "auth_required", f"unexpected greeting: {hello}"
            await ws.send(json.dumps({"type": "auth", "access_token": self._token}))
            auth_ok = json.loads(await ws.recv())
            if auth_ok.get("type") != "auth_ok":
                raise RuntimeError(f"ha auth failed: {auth_ok}")
            log.info("ha ws connected: %s", auth_ok.get("ha_version"))

            # Subscribe to state_changed events.
            sub_id = self._next_id()
            await ws.send(json.dumps({
                "id": sub_id,
                "type": "subscribe_events",
                "event_type": "state_changed",
            }))
            sub_ack = json.loads(await ws.recv())
            assert sub_ack.get("success"), f"subscribe failed: {sub_ack}"

            # Snapshot all current states once so we don't have to wait for the
            # first state change to populate every entity.
            await self._fetch_initial_states(ws)

            async for raw in ws:
                msg = json.loads(raw)
                if msg.get("type") != "event": continue
                ev = msg.get("event", {})
                if ev.get("event_type") != "state_changed": continue
                data = ev.get("data", {})
                entity_id = data.get("entity_id")
                new_state = data.get("new_state")
                if not entity_id or not new_state: continue
                await self._on_state_change(entity_id, new_state.get("state"), new_state)

    async def _fetch_initial_states(self, ws) -> None:
        req_id = self._next_id()
        await ws.send(json.dumps({"id": req_id, "type": "get_states"}))
        # The response may interleave with events; loop until we see ours.
        while True:
            raw = await ws.recv()
            msg = json.loads(raw)
            if msg.get("id") == req_id and msg.get("type") == "result":
                if not msg.get("success"):
                    log.warning("get_states failed: %s", msg)
                    return
                states = msg.get("result", []) or []
                log.info("ha snapshot: %d entities", len(states))
                for st in states:
                    eid = st.get("entity_id")
                    val = st.get("state")
                    if eid and val is not None:
                        await self._on_state_change(eid, val, st)
                return

    def _next_id(self) -> int:
        self._msg_id += 1
        return self._msg_id
