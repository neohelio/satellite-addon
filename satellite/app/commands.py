"""Phase C: HA control command executor.

The realtime-relay forwards `command` frames from NeoHelio cloud over the
existing live-uplink WebSocket. This module receives one such frame, calls
the corresponding `POST /api/services/{domain}/{service}` against the
addon's HA Supervisor proxy, and returns an ack payload that
`live_uplink.py` sends back on the same socket.

The wire shape (mirrors services/core/src/routes/device-control.ts):

    Inbound:  {
      "type": "command",
      "command_id": "<uuid>",
      "target_entity_id": "switch.boardroom_lights",
      "ha_domain": "switch",
      "ha_service": "turn_on",
      "ha_service_data": { "entity_id": "switch.boardroom_lights" }
    }

    Outbound: {
      "type": "command_ack",
      "command_id": "<uuid>",
      "ok": true | false,
      "error": "<message>" | null,
      "ha_response": <HA response body, optional>
    }

The executor is intentionally narrow: validate, call HA REST, normalise
the outcome. All the policy (opt-in gate, action → service mapping, tenant
isolation) is in NeoHelio cloud; the addon just executes what it's told.
"""
from __future__ import annotations
import asyncio
import json
import logging
from typing import Optional

import aiohttp

log = logging.getLogger("commands")

# HA Supervisor REST timeouts. Most service calls return < 1s; cap at 5s so
# the relay's 12s window stays well clear of compounding latency.
HA_SERVICE_TIMEOUT_S = 5.0

# Domains the addon will call. Anything outside this set is rejected so a
# misbehaving cloud (or a future bug) can't accidentally invoke
# `homeassistant.restart` or `persistent_notification.create`.
ALLOWED_DOMAINS = frozenset({
    "switch",
    "light",
    "climate",
    "fan",      # forward-compat — climate-adjacent loads sometimes register here
    "cover",    # ditto for blinds / awnings
})


def _derive_rest_base(hass_url: str) -> str:
    """Convert a HA WebSocket URL (`ws://supervisor/core/websocket`) to the
    REST base (`http://supervisor/core`). Same transform as `discovery.py`
    and `ha_client.py` use — duplicated to keep the executor importable on
    its own."""
    base = hass_url.rstrip("/").replace("ws://", "http://").replace("wss://", "https://")
    if base.endswith("/websocket"):
        base = base[: -len("/websocket")]
    return base


class CommandExecutor:
    """Stateless caller of HA's `/api/services` REST. Instantiated once per
    addon process and invoked by `live_uplink.py` on each inbound command."""

    def __init__(self, hass_url: str, hass_token: str):
        self._rest_base = _derive_rest_base(hass_url)
        self._token = hass_token

    async def execute(self, frame: dict) -> dict:
        """Run one command frame. Returns a dict suitable for inclusion in a
        `command_ack` frame: `{ok, error?, ha_response?}`."""
        command_id = frame.get("command_id")
        domain = frame.get("ha_domain")
        service = frame.get("ha_service")
        target = frame.get("target_entity_id")
        data = frame.get("ha_service_data") or {}

        if not isinstance(command_id, str) or not command_id:
            return {"ok": False, "error": "missing command_id"}
        if not isinstance(domain, str) or not domain:
            return {"ok": False, "error": "missing ha_domain"}
        if not isinstance(service, str) or not service:
            return {"ok": False, "error": "missing ha_service"}
        if not isinstance(target, str) or not target:
            return {"ok": False, "error": "missing target_entity_id"}

        # Cloud-side already enforces the action → service mapping and the
        # opt-in gate. We re-validate the domain whitelist as defence-in-depth
        # — a compromised cloud control plane should not be able to call
        # `homeassistant.restart` through the satellite.
        if domain not in ALLOWED_DOMAINS:
            log.warning("command rejected: domain %r not in allowlist", domain)
            return {"ok": False, "error": f"domain {domain!r} not allowed"}

        # The service name itself may be `turn_on`, `set_temperature`, etc —
        # we accept any service the domain advertises. HA returns 400 if the
        # service is unknown; we let it surface that error rather than
        # maintaining a per-domain service whitelist that would lag HA's
        # release cadence.

        url = f"{self._rest_base}/api/services/{domain}/{service}"
        # Ensure entity_id is in the body — HA accepts the target_entity_id
        # ONLY in the body, not the URL.
        body = dict(data)
        body.setdefault("entity_id", target)

        headers = {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json",
        }
        timeout = aiohttp.ClientTimeout(total=HA_SERVICE_TIMEOUT_S)

        log.info("command executing: command_id=%s service=%s.%s target=%s", command_id, domain, service, target)
        try:
            async with aiohttp.ClientSession(timeout=timeout) as s, s.post(
                url, headers=headers, json=body,
            ) as r:
                response_text = await r.text()
                if r.status >= 400:
                    log.warning("HA service rejected command_id=%s status=%d body=%s",
                                command_id, r.status, response_text[:200])
                    return {
                        "ok": False,
                        "error": f"HA returned HTTP {r.status}: {response_text[:160]}",
                    }
                # HA service calls return the changed entities as a JSON array.
                # We surface it back to NeoHelio so the cloud can confirm the
                # state actually flipped.
                try:
                    ha_response = json.loads(response_text) if response_text else None
                except json.JSONDecodeError:
                    ha_response = response_text
                log.info("command acked: command_id=%s status=%d", command_id, r.status)
                return {"ok": True, "ha_response": ha_response}
        except asyncio.TimeoutError:
            log.warning("HA service timeout: command_id=%s", command_id)
            return {"ok": False, "error": f"HA call timed out after {HA_SERVICE_TIMEOUT_S}s"}
        except aiohttp.ClientError as e:
            log.warning("HA service transport error: command_id=%s err=%s", command_id, e)
            return {"ok": False, "error": f"HA call failed: {e}"}
