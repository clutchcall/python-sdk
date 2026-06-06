"""Voice modality — telephony over QUIC (MoQT).

Two primitives baked in: :class:`Calls` (control plane — originate /
transfer / hangup over the BFF tRPC) and :class:`AudioBridge` (data plane
— bidirectional audio over MoQT with the ``voice/<sid>/uplink`` and
``voice/<sid>/downlink`` namespace convention enforced).

Mirrors the TypeScript ``@clutchcall/sdk/voice`` module. The legacy
:mod:`clutchcall.client` + :mod:`clutchcall.media` surfaces remain
available for backwards compatibility; the modality version below is the
recommended entry point for new integrations.

Usage::

    from clutchcall.voice import Voice

    v = Voice(
        base_url="https://app.clutchcall.dev",
        api_key=os.environ["CLUTCHCALL_API_KEY"],
        org_id="org_abc",
    )

    call = v.calls.originate(
        to="+15551234567", from_="+15558675309",
        trunk_id="trunk_main", agent="healthcare-assistant",
    )

    def on_uplink(frame, ts_us):
        asr.feed(frame)

    bridge = v.audio_bridge.attach(call.sid, codec="opus", on_uplink=on_uplink)
    tts.on_chunk(lambda opus: bridge.publish_downlink(opus))

    # … later
    call.hangup()
"""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, Callable, Literal, Optional


def _moqt_module():
    from clutchcall import moqt
    return moqt


# ─── errors ──────────────────────────────────────────────────────────────

class VoiceError(Exception):
    """Raised by every voice surface when an arg or response is off."""


# ─── transport ───────────────────────────────────────────────────────────

CallStatus = Literal["dialing", "ringing", "in_progress", "completed", "failed", "no_answer"]
AudioCodec = Literal["opus", "pcm16", "g711_ulaw", "g711_alaw"]


@dataclass
class _Transport:
    base_url: str
    api_key:  str
    org_id:   str

    def call(self, path: str, payload: Any, kind: Literal["query", "mutation"]) -> Any:
        url = f"{self.base_url.rstrip('/')}/api/trpc/{path}"
        body = None
        if kind == "query":
            qs = urllib.parse.urlencode({"input": json.dumps(payload)})
            url = f"{url}?{qs}"
        else:
            body = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            url, data=body,
            method="GET" if kind == "query" else "POST",
            headers={
                "content-type":  "application/json",
                "authorization": f"Bearer {self.api_key}",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                doc = json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            text = e.read().decode("utf-8", errors="replace")[:200]
            raise VoiceError(f"tRPC {path} {e.code}: {text}") from e
        except urllib.error.URLError as e:
            raise VoiceError(f"tRPC {path}: {e.reason}") from e
        if "error" in doc:
            raise VoiceError(doc["error"].get("message", "tRPC error"))
        try:
            return doc["result"]["data"]
        except KeyError as e:
            raise VoiceError(f"tRPC {path}: empty result") from e


# ─── data ────────────────────────────────────────────────────────────────

@dataclass
class CallData:
    sid:        str
    status:     CallStatus
    to:         str
    from_:      str
    started_at: str
    trunk_id:   Optional[str] = None
    agent:      Optional[str] = None


def _row_to_call_data(row: Any) -> CallData:
    return CallData(
        sid=row["sid"],
        status=row["status"],
        to=row["to"],
        from_=row.get("from", ""),
        started_at=row.get("startedAt", ""),
        trunk_id=row.get("trunkId"),
        agent=row.get("agent"),
    )


# ─── calls ───────────────────────────────────────────────────────────────

class Calls:
    def __init__(self, v: "Voice") -> None:
        self._v = v
        self._t = v._t  # type: ignore[attr-defined]

    def originate(
        self,
        *,
        to: str,
        from_: str,
        trunk_id: str,
        agent: Optional[str] = None,
        ring_timeout_sec: int = 30,
    ) -> "Call":
        row = self._t.call(
            "voice.calls.originate",
            {
                "orgId": self._t.org_id,
                "to": to, "from": from_,
                "trunkId": trunk_id,
                "agent": agent,
                "ringTimeoutSec": ring_timeout_sec,
            },
            "mutation",
        )
        return Call(self._v, _row_to_call_data(row))

    def get(self, *, sid: str) -> "Call":
        row = self._t.call(
            "voice.calls.get",
            {"orgId": self._t.org_id, "sid": sid},
            "query",
        )
        return Call(self._v, _row_to_call_data(row))


class Call:
    def __init__(self, v: "Voice", data: CallData) -> None:
        self._v = v
        self._d = data

    @property
    def sid(self) -> str:        return self._d.sid
    @property
    def status(self) -> CallStatus: return self._d.status
    @property
    def to(self) -> str:         return self._d.to
    @property
    def from_(self) -> str:      return self._d.from_
    @property
    def trunk_id(self) -> Optional[str]: return self._d.trunk_id
    @property
    def agent(self) -> Optional[str]:    return self._d.agent

    def transfer(self, *, to: Optional[str] = None, agent: Optional[str] = None) -> None:
        """Transfer to a PSTN number or re-attach to a different agent.

        Pass exactly one of ``to=<E.164>`` or ``agent=<id>``.
        """
        if not (bool(to) ^ bool(agent)):
            raise VoiceError("transfer: pass exactly one of to=<E.164> or agent=<id>")
        self._v._t.call(
            "voice.calls.transfer",
            {"orgId": self._v._t.org_id, "sid": self.sid, "to": to, "agent": agent},
            "mutation",
        )

    def hangup(self) -> None:
        self._v._t.call(
            "voice.calls.hangup",
            {"orgId": self._v._t.org_id, "sid": self.sid},
            "mutation",
        )


# ─── audio bridge ────────────────────────────────────────────────────────

OnUplink = Callable[[bytes, int], None]   # (frame_bytes, timestamp_us)


class AudioBridge:
    """Bidirectional audio bridge for one call. Hold the reference for the
    call's lifetime — releasing it closes the underlying MoQT session and
    drops both tracks."""

    def __init__(self, client: Any, pub: Any, sub: Any, call_sid: str) -> None:
        self._client = client
        self._pub = pub
        self._sub = sub
        self.call_sid = call_sid

    def publish_downlink(self, frame: bytes, *, timestamp_us: Optional[int] = None) -> None:
        ts = timestamp_us if timestamp_us is not None else time.monotonic_ns() // 1_000
        self._pub.write(ts, bytes(frame))

    def close(self) -> None:
        try: self._pub.close()
        except Exception: pass  # noqa: BLE001
        try: self._sub.close()
        except Exception: pass  # noqa: BLE001
        try: self._client.close()
        except Exception: pass  # noqa: BLE001


class AudioBridgeFactory:
    def __init__(self, v: "Voice") -> None:
        self._v = v

    def attach(
        self,
        call_sid: str,
        *,
        on_uplink: OnUplink,
        codec: AudioCodec = "opus",
        sample_rate: int = 48000,
        channels: int = 1,
        frame_ms: int = 20,
    ) -> AudioBridge:
        if not call_sid:
            raise VoiceError("AudioBridge.attach: call_sid required")
        if on_uplink is None:
            raise VoiceError("AudioBridge.attach: on_uplink required")
        moqt = _moqt_module()
        url = f"moq://{self._v.relay_host}/voice/{urllib.parse.quote(call_sid, safe='-_')}"
        client = moqt.MoqtClient.connect(url, self._v._t.api_key)
        sub = client.subscribe_audio(
            f"voice/{call_sid}/uplink", "audio",
            on_frame=lambda ts, frame: on_uplink(bytes(frame), int(ts)),
        )
        pub = client.publish_audio(
            f"voice/{call_sid}/downlink", "audio",
            capability=f"voice/{codec}",
            sample_rate=sample_rate, channels=channels, frame_ms=frame_ms,
        )
        return AudioBridge(client, pub, sub, call_sid)


# ─── agents ──────────────────────────────────────────────────────────────

class Agents:
    """Bind a running agent_runtime agent to a call. The engine wires the
    audio bridge end-to-end — you don't need to open AudioBridge yourself."""

    def __init__(self, v: "Voice") -> None:
        self._v = v

    def attach(self, call_sid: str, agent: str) -> None:
        if not call_sid:
            raise VoiceError("Agents.attach: call_sid required")
        if not agent:
            raise VoiceError("Agents.attach: agent required")
        self._v._t.call(
            "voice.agents.attach",
            {"orgId": self._v._t.org_id, "sid": call_sid, "agent": agent},
            "mutation",
        )


# ─── client ──────────────────────────────────────────────────────────────

class Voice:
    """Top-level Voice client. Hands out scoped helpers."""

    def __init__(
        self,
        *,
        base_url: str,
        api_key: str,
        org_id: str,
        relay_host: str = "relay.clutchcall.dev",
    ) -> None:
        if not base_url: raise VoiceError("Voice: base_url required")
        if not api_key:  raise VoiceError("Voice: api_key required")
        if not org_id:   raise VoiceError("Voice: org_id required")
        self.relay_host = relay_host
        self._t = _Transport(base_url=base_url, api_key=api_key, org_id=org_id)
        self.calls        = Calls(self)
        self.audio_bridge = AudioBridgeFactory(self)
        self.agents       = Agents(self)


__all__ = [
    "Voice",
    "VoiceError",
    "Calls",
    "Call",
    "CallData",
    "CallStatus",
    "AudioCodec",
    "AudioBridge",
    "AudioBridgeFactory",
    "Agents",
    "OnUplink",
]
