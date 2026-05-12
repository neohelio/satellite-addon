"""Add-on entry point. Wires together: HA WebSocket → mapper → outbox → uplink,
plus a periodic flush that emits one batch per device per `poll_interval_sec`.

Lifecycle:
  1. Load settings from env (set by run.sh from add-on options).
  2. Fetch the cloud-pushed Blueprint (entity → field mapping).
  3. Subscribe to HA state_changed events; aggregate into a per-device buffer.
  4. Every poll_interval_sec, snapshot the buffer → enqueue NDJSON rows.
  5. Background uplink loop drains outbox to NeoHelio cloud.
  6. Background blueprint-refresh loop picks up cloud-side mapping changes.
"""
from __future__ import annotations
import asyncio
import json
import logging
import time
import uuid
from typing import Optional

import aiohttp

import config
from blueprint import Blueprint, BlueprintCache
from discovery import DiscoveryLoop
from ha_client import HassWebsocket
from live_uplink import LiveUplink
from mapper import StateBucket
from outbox import Outbox
from uplink import Uplink


def _setup_logging(level: str) -> None:
    lv = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(
        level=lv,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )


async def _register_with_cloud(settings: config.Settings) -> None:
    """First-boot announce to NeoHelio cloud. Idempotent — returns 200 on
    repeat calls. Cloud uses this to bump last_seen_at and refresh metadata."""
    payload = {
        "gateway_serial": settings.gateway_serial,
        "agent": "satellite-addon",
        "agent_version": "0.1.7",
        "registered_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    headers = {
        "Authorization": f"Bearer {settings.site_token}",
        "Content-Type": "application/json",
    }
    async with aiohttp.ClientSession() as s, s.post(
        settings.register_url, headers=headers, json=payload,
        timeout=aiohttp.ClientTimeout(total=15),
    ) as r:
        if r.status >= 400:
            raise RuntimeError(f"register failed: HTTP {r.status} — {await r.text()}")


def _build_batches(snapshot: dict[str, dict[str, float]],
                   types: dict[str, str],
                   captured_at: str) -> list[dict]:
    """Convert the per-device value buffer into one batch dict per device.
    Same shape the cloud-side `functions/ingestion/edge` Cloud Function
    expects (gateway_serial / batch_id / captured_at / device / readings)."""
    batches = []
    for dev_external_id, fields in snapshot.items():
        if not fields: continue
        batches.append({
            "batch_id": str(uuid.uuid4()),
            "captured_at": captured_at,
            "device": {
                "external_id": dev_external_id,
                "device_type": types.get(dev_external_id, "INVERTER"),
                "driver_slug": "satellite_ha",
            },
            "readings": fields,
            "filtered": [],
        })
    return batches


async def main_async() -> None:
    settings = config.load()
    _setup_logging(settings.log_level)
    log = logging.getLogger("main")

    log.info("NeoHelio Satellite v0.1.7 — gateway=%s url=%s",
             settings.gateway_serial, settings.neohelio_url)

    # Initial registration; non-fatal so we keep running even if cloud is down.
    try:
        await _register_with_cloud(settings)
        log.info("registered with NeoHelio cloud")
    except Exception as e:  # noqa: BLE001
        log.warning("register failed (will retry on next blueprint fetch): %s", e)

    outbox = Outbox(settings.db_path)
    uplink = Uplink(settings.ingest_url, settings.site_token, outbox)
    bp_cache = BlueprintCache(settings.blueprint_url, settings.site_token, settings.blueprint_refresh_sec)

    # Wait for the first blueprint before starting the HA subscription —
    # without it we'd discard every state change anyway.
    log.info("fetching initial blueprint…")
    try:
        await bp_cache.fetch_once()
    except Exception as e:  # noqa: BLE001
        log.error("initial blueprint fetch failed: %s — exiting", e)
        raise SystemExit(2)
    bp: Blueprint = bp_cache.current  # type: ignore[assignment]

    bucket = StateBucket(bp)

    # Live tee — wss to services/realtime-relay. Disabled silently when no
    # relay URL is configured (dev environments without the relay deployed).
    # The slow NDJSON path runs regardless; the live tee is additive.
    live: LiveUplink | None = None
    relay_url = settings.realtime_relay_url
    if relay_url:
        live = LiveUplink(relay_url, settings.site_token, settings.gateway_serial)
        log.info("live tee enabled → %s", relay_url)
    else:
        log.info("live tee disabled — no realtime_relay_url configured")

    async def on_state(entity_id: str, state_value: str, full_state: dict) -> None:
        # Pick up the latest blueprint without replacing the bucket — keeps
        # accumulated last-seen values intact across blueprint refreshes.
        if bp_cache.current is not None and bp_cache.current is not bucket._bp:  # noqa: SLF001
            bucket.update_blueprint(bp_cache.current)
        # Slow path: aggregate into the per-device buffer for the 5-min flush.
        bucket.ingest(entity_id, state_value, full_state)
        # Fast path (additive): tee the raw state event to the relay. Only
        # entities the blueprint touches — relay traffic shouldn't include
        # noisy HA internals like sun.sun or device_tracker.*. The blueprint
        # lookup is the same one bucket.ingest uses; if it ignored the event,
        # we ignore it for the live tee too.
        if live is not None and bucket.has_mapping(entity_id):
            attrs = full_state.get("attributes") if isinstance(full_state, dict) else None
            last_changed = full_state.get("last_changed") if isinstance(full_state, dict) else None
            live.enqueue(entity_id, state_value, attrs, last_changed)

    ha = HassWebsocket(settings.hass_url, settings.hass_token, on_state)

    async def flush_loop() -> None:
        # Bucket retains the last-seen value per (device, field) for the lifetime
        # of the addon — it is *not* reset between flushes. Each flush emits a
        # full snapshot of all currently-known values. This is the right shape
        # for downstream BQ rows: every row has every mapped field populated,
        # which keeps the Live Dashboard tiles + Power Curve consistent. The
        # cost is ~16 fields of JSON every flush_interval_sec — negligible.
        # Tradeoff: an entity that goes truly unavailable keeps emitting its
        # last good value. For Phase 2 polish we can add a freshness threshold
        # (drop fields older than N minutes); not blocking for v1.
        #
        # Cadence is read live from bp_cache on every iteration, so an operator
        # changing flush_interval_sec from the NeoHelio UI takes effect on the
        # next flush — no addon restart needed. Falls back to the addon option
        # if no blueprint has loaded yet.
        while True:
            interval = bp_cache.current_flush_interval_sec() or settings.poll_interval_sec
            await asyncio.sleep(interval)
            snap = bucket.snapshot()
            if not snap: continue
            captured = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            batches = _build_batches(
                snap, bucket.device_types(), captured,
            )
            for batch in batches:
                # Wrap with gateway_serial so receiver can resolve site/tenant.
                wrapped = {"gateway_serial": settings.gateway_serial, **batch}
                outbox.enqueue(wrapped)

    # Re-snapshot HA on blueprint change so newly-mapped entities flow through
    # the bucket + live tee immediately, instead of waiting for each one's
    # next HA `state_changed`. Slow-moving entities (today_*_kwh accumulators,
    # voltages, temps) would otherwise be invisible for hours after a mapping
    # edit. The listener is sync; it schedules the async snapshot.
    def _on_blueprint_change(new_bp: Blueprint, old_bp: Blueprint | None) -> None:
        # Skip the very first load — the WS connect path already snapshots
        # HA at startup. Listener fires on subsequent changes.
        if old_bp is None:
            return
        log.info(
            "blueprint changed (entities %d→%d, flush_interval %ds→%ds) — re-snapshotting HA",
            len(old_bp.entities), len(new_bp.entities),
            old_bp.polling.flush_interval_sec, new_bp.polling.flush_interval_sec,
        )
        asyncio.create_task(ha.request_state_snapshot())

    bp_cache.add_change_listener(_on_blueprint_change)

    # Phase 2: HA entity discovery → cloud-side classifier. Runs alongside the
    # existing slow / live paths. The discovery loop only POSTs when the
    # manifest hash changes, so periodic re-runs are cheap. Hooks into the
    # LiveUplink so its `hello` frame carries the latest hash — relay echoes
    # it to browser subscribers for stale-classification detection.
    discovery = DiscoveryLoop(
        hass_url=settings.hass_url,
        hass_token=settings.hass_token,
        manifest_url=settings.manifest_url,
        site_token=settings.site_token,
    )
    if live is not None:
        discovery.add_hash_listener(live.set_manifest_hash)

    tasks = ["ha-ws", "blueprint-refresh", "flush", "uplink", "discovery"]
    if live is not None:
        tasks.append("live-uplink")
    log.info("starting background tasks: %s", ", ".join(tasks))
    async with asyncio.TaskGroup() as tg:
        tg.create_task(ha.run_forever())
        tg.create_task(bp_cache.refresh_loop())
        tg.create_task(flush_loop())
        tg.create_task(uplink.run_forever())
        tg.create_task(discovery.run_forever())
        if live is not None:
            tg.create_task(live.run_forever())


def main() -> None:
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
