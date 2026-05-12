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

import aiohttp
import websockets

log = logging.getLogger("ha")


class HassWebsocket:
    def __init__(self, url: str, token: str, on_state_change: Callable[[str, str, dict], Awaitable[None]]):
        self._url = url
        self._token = token
        self._on_state_change = on_state_change
        self._msg_id = 0

    async def request_state_snapshot(self) -> int:
        """Force a fresh full-state snapshot from HA outside the WS connection.
        Each state is fed through `on_state_change` exactly as the initial
        connect path does. Used after a blueprint refresh so newly-mapped
        entities flow through the bucket + live tee immediately, instead of
        waiting for each one's next `state_changed` event in HA — which for
        slow-moving entities (today_*kwh accumulators, temperatures,
        voltages) could be minutes or hours.

        Returns the count of states fed through. REST-only — does NOT touch
        the WS connection (which may be reconnecting). Uses the same
        Supervisor proxy + Long-Lived token as the WS path."""
        # Derive REST base from WS url: ws[s]://supervisor/core/websocket
        # → http[s]://supervisor/core. Same transform as discovery.py.
        base = self._url.rstrip("/").replace("ws://", "http://").replace("wss://", "https://")
        if base.endswith("/websocket"):
            base = base[: -len("/websocket")]
        headers = {"Authorization": f"Bearer {self._token}"}
        timeout = aiohttp.ClientTimeout(total=15)
        try:
            async with aiohttp.ClientSession(timeout=timeout) as s, s.get(
                f"{base}/api/states", headers=headers,
            ) as r:
                if r.status >= 400:
                    log.warning("re-snapshot: HA /api/states returned %d", r.status)
                    return 0
                states = await r.json()
        except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as e:
            log.warning("re-snapshot: HA /api/states failed: %s", e)
            return 0
        if not isinstance(states, list):
            return 0
        count = 0
        for st in states:
            if not isinstance(st, dict): continue
            eid = st.get("entity_id")
            val = st.get("state")
            if eid and val is not None:
                try:
                    await self._on_state_change(eid, val, st)
                    count += 1
                except Exception as e:  # noqa: BLE001
                    log.debug("re-snapshot: handler raised on %s: %s", eid, e)
        log.info("re-snapshot complete: %d entities fed", count)
        return count

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
