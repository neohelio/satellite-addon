"""Live tee: outbound WSS to NeoHelio's `services/realtime-relay`.

The aggregated NDJSON path (uplink.py) keeps emitting every poll_interval_sec
into the Bronze → Silver → Gold pipeline; that's unchanged. This module
forwards EVERY HA state_changed event in addition, so the operator's
dashboard drill-down sees sub-second updates without waiting for the
medallion.

Design:
    - One persistent WSS connection to wss://<host>/realtime/satellite/<serial>.
    - Authenticated by the same HMAC site_token used for ingestion (rotated
      atomically — when ops calls /rotate-token, both paths fail until the
      addon is restarted with the new token).
    - An asyncio.Queue buffers state events so the HA WS handler never blocks
      on the relay. Queue is bounded; on overflow we drop oldest (the live
      view is best-effort; the slow path is the system of record).
    - Heartbeats: send every HEARTBEAT_INTERVAL_S; if the relay misses 3 it
      closes us, and we reconnect with exponential backoff.
    - First frame after connect is `hello` with manifest_hash + agent_version.
      Phase 2 will populate manifest_hash from the discovery module; for v1
      we send a placeholder so the relay can still cache the value.

Failure modes:
    - Relay unreachable (DNS / network): exponential backoff 1s → 30s,
      reset on a successful frame. The slow NDJSON path is unaffected.
    - Auth rejected: log and back off the same way; rotation requires an
      addon restart, so spinning at 1s is wasteful — we cap at 60s for
      auth-failure retries specifically.
    - Queue overflow: oldest items dropped first; one structured log per
      overflow event with the drop count.

Out of scope (Phase 3): `command` frames from the relay. For now we ignore
them; the wire protocol leaves room.
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import time
from typing import Optional

import aiohttp

import config

log = logging.getLogger("live_uplink")

# Queue cap — at 10Hz max per entity × ~100 entities × ~5s of slack = 5000.
# Generous; the queue should rarely sit deeper than a handful of frames.
QUEUE_MAXSIZE = 5000

# Backoff schedule (seconds). Auth-rejected goes to AUTH_BACKOFF_MAX; other
# transport errors go to NETWORK_BACKOFF_MAX.
NETWORK_BACKOFF_INITIAL = 1.0
NETWORK_BACKOFF_MAX = 30.0
AUTH_BACKOFF_MAX = 60.0

# Cadence for the keepalive frame the addon sends to the relay. The relay
# default tolerates 3 misses; ~75s before disconnect.
HEARTBEAT_INTERVAL_S = 25.0

# Cap on how long the relay's snapshot_request reply can take before we
# treat the connection as dead and reconnect.
SNAPSHOT_REPLY_TIMEOUT_S = 10.0

AGENT_VERSION = "0.1.0"


class LiveUplink:
    """Background coroutine that maintains a WSS to the relay and drains the
    queue. Public API:

        - enqueue(entity_id, state_value, attributes, last_changed)
                                             ↳ non-blocking, drops on overflow
        - run_forever()                       ↳ loop until cancelled

    The addon's HA-state callback calls `enqueue`; `main.py` wires
    `run_forever` into its TaskGroup alongside the slow uplink + flush.
    """

    def __init__(self, relay_url: str, site_token: str, gateway_serial: str):
        self._relay_url = relay_url
        self._site_token = site_token
        self._serial = gateway_serial
        self._queue: asyncio.Queue[dict] = asyncio.Queue(maxsize=QUEUE_MAXSIZE)
        self._dropped_since_last_log = 0
        # Set from main.py once the discovery module computes it; before that
        # we ship a placeholder. Phase 2 wires this; Phase 1 ships placeholder.
        self._manifest_hash: str = "pending-phase-2"
        # Toggle so test harnesses can drain the queue synchronously.
        self._stop_requested = False

    # ── Public API ───────────────────────────────────────────────────────────

    def set_manifest_hash(self, manifest_hash: str) -> None:
        self._manifest_hash = manifest_hash

    def enqueue(
        self,
        entity_id: str,
        state_value: str | float | int | None,
        attributes: Optional[dict] = None,
        last_changed: Optional[str] = None,
    ) -> None:
        """Schedule a state frame for upstream send. Non-blocking. If the
        queue is at capacity, evicts the oldest frame and logs an aggregated
        drop count (one log per second of overflow, not per event)."""
        frame = {
            "type": "state",
            "entity_id": entity_id,
            "state": state_value,
            "attributes": attributes,
            "last_changed": last_changed,
        }
        try:
            self._queue.put_nowait(frame)
            return
        except asyncio.QueueFull:
            pass
        # Overflow path — drop the oldest, push the new one. The live view is
        # explicitly best-effort; the slow Bronze path is canonical.
        try:
            _ = self._queue.get_nowait()
        except asyncio.QueueEmpty:
            pass
        try:
            self._queue.put_nowait(frame)
        except asyncio.QueueFull:
            return
        self._dropped_since_last_log += 1
        if self._dropped_since_last_log % 100 == 1:
            log.warning(
                "live_uplink queue overflow — dropping oldest (count=%d)",
                self._dropped_since_last_log,
            )

    def stop(self) -> None:
        self._stop_requested = True

    # ── Main loop ────────────────────────────────────────────────────────────

    async def run_forever(self) -> None:
        backoff = NETWORK_BACKOFF_INITIAL
        while not self._stop_requested:
            try:
                await self._connect_and_serve()
                # Clean disconnect (relay closed cleanly) — short delay, then retry.
                backoff = NETWORK_BACKOFF_INITIAL
                await asyncio.sleep(NETWORK_BACKOFF_INITIAL)
            except _AuthRejected as e:
                log.warning("live_uplink auth rejected — %s; backing off %.1fs", e, AUTH_BACKOFF_MAX)
                await asyncio.sleep(AUTH_BACKOFF_MAX)
                # Keep at the max so we don't spin while the operator updates
                # the token on the addon side.
                backoff = AUTH_BACKOFF_MAX
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                log.warning("live_uplink connection error: %s; backoff %.1fs", e, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, NETWORK_BACKOFF_MAX)

    async def _connect_and_serve(self) -> None:
        headers = {"Authorization": f"Bearer {self._site_token}"}
        timeout = aiohttp.ClientTimeout(total=None, sock_read=None, sock_connect=15)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            try:
                async with session.ws_connect(
                    self._relay_url,
                    headers=headers,
                    heartbeat=HEARTBEAT_INTERVAL_S,
                    autoping=True,
                ) as ws:
                    log.info("live_uplink connected to %s", self._relay_url)
                    # Send `hello` first.
                    hello = {
                        "type": "hello",
                        "manifest_hash": self._manifest_hash,
                        "agent_version": AGENT_VERSION,
                    }
                    await ws.send_str(json.dumps(hello))

                    # Two concurrent tasks: drain queue → relay, and read
                    # relay → handle control frames. Whichever finishes first
                    # cancels the other.
                    sender = asyncio.create_task(self._drain_queue(ws))
                    receiver = asyncio.create_task(self._read_relay(ws))
                    done, pending = await asyncio.wait(
                        {sender, receiver},
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    for t in pending:
                        t.cancel()
                        with contextlib.suppress(asyncio.CancelledError):
                            await t
                    # Re-raise from the finished task so the outer loop can
                    # categorise the error.
                    for t in done:
                        exc = t.exception()
                        if exc:
                            raise exc
            except aiohttp.WSServerHandshakeError as e:
                if e.status in (401, 403):
                    raise _AuthRejected(f"HTTP {e.status}") from e
                raise

    async def _drain_queue(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        """Pull frames off the queue and send them. Sends a periodic
        heartbeat if the queue has been silent."""
        last_send_ms = time.monotonic() * 1000
        while True:
            try:
                # Wake at least at HEARTBEAT_INTERVAL_S to emit a heartbeat
                # even if no state changes arrive.
                frame = await asyncio.wait_for(self._queue.get(), timeout=HEARTBEAT_INTERVAL_S)
            except asyncio.TimeoutError:
                frame = {"type": "heartbeat", "ts": int(time.time() * 1000)}
            await ws.send_str(json.dumps(frame))
            last_send_ms = time.monotonic() * 1000

    async def _read_relay(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        """Read inbound frames. v1 mostly ignores them, but tracks heartbeats
        and snapshot_request (Phase 1+) and logs unexpected control frames."""
        async for msg in ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                try:
                    frame = json.loads(msg.data)
                except json.JSONDecodeError:
                    log.debug("live_uplink: non-JSON frame received, ignoring")
                    continue
                t = frame.get("type")
                if t == "heartbeat":
                    continue
                if t == "snapshot_request":
                    # Relay restarted and lost its cache; we don't currently
                    # cache state on the addon side either (the slow NDJSON
                    # path's StateBucket does, but it's aggregated). For now,
                    # log + skip; deltas will resume on next HA state_changed.
                    log.info("live_uplink: relay requested snapshot — Phase 1 ignores")
                    continue
                if t == "command":
                    # Phase 3.
                    log.debug("live_uplink: command frame received (Phase 3) — ignoring")
                    continue
                log.debug("live_uplink: unknown frame type=%s", t)
            elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSING):
                log.info("live_uplink: relay closed connection")
                return
            elif msg.type == aiohttp.WSMsgType.ERROR:
                raise RuntimeError(f"live_uplink WS error: {ws.exception()}")


class _AuthRejected(Exception):
    """Internal — signals auth failure so the outer loop applies the longer
    AUTH_BACKOFF_MAX rather than NETWORK_BACKOFF_MAX."""
