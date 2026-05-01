"""Drains the SQLite outbox to NeoHelio cloud as NDJSON over HTTPS.

Same contract as the Phase 0/1 spike's agent.py — Bearer token auth, NDJSON
body, idempotent on the receiver side. Failures leave rows queued; success
marks them sent. Bounded batch size keeps any single HTTP request small
enough to avoid timeouts on flaky links.
"""
from __future__ import annotations
import asyncio
import logging

import aiohttp

from outbox import Outbox

log = logging.getLogger("uplink")

MAX_BATCH = 200          # rows per HTTP request
DRAIN_INTERVAL_SEC = 10  # how often to attempt drain when there's queue
HTTP_TIMEOUT = aiohttp.ClientTimeout(total=20)


class Uplink:
    def __init__(self, ingest_url: str, site_token: str, outbox: Outbox):
        self._url = ingest_url
        self._token = site_token
        self._outbox = outbox

    async def drain_once(self, session: aiohttp.ClientSession) -> int:
        rows = self._outbox.peek_unsent(MAX_BATCH)
        if not rows: return 0
        body = "\n".join(j for _, j in rows)
        headers = {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/x-ndjson",
        }
        try:
            async with session.post(self._url, headers=headers, data=body, timeout=HTTP_TIMEOUT) as r:
                if r.status >= 200 and r.status < 300:
                    self._outbox.mark_sent(i for i, _ in rows)
                    return len(rows)
                # 4xx/5xx: leave rows queued, log error body for triage.
                err = await r.text()
                self._outbox.record_failure((i for i, _ in rows), f"HTTP {r.status}: {err[:200]}")
                log.warning("uplink HTTP %s: %s", r.status, err[:200])
                return -1
        except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as e:
            self._outbox.record_failure((i for i, _ in rows), str(e))
            log.warning("uplink failed (will retry): %s", e)
            return -1

    async def run_forever(self) -> None:
        async with aiohttp.ClientSession() as session:
            while True:
                try:
                    n = await self.drain_once(session)
                    if n > 0:
                        log.info("uplink: drained %d batches; outbox=%s", n, self._outbox.stats())
                except Exception as e:  # noqa: BLE001
                    log.exception("uplink loop error: %s", e)
                await asyncio.sleep(DRAIN_INTERVAL_SEC)
