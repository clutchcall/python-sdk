"""Games modality — multiplayer rooms over QUIC (MoQT).

Three channels per room, mapped onto :mod:`clutchcall.moqt`'s
publish_frame/subscribe_frame substrate:

    state  (server → all)         priority 100, datagram lane (best-effort)
    input  (player → server)      priority 100, datagram lane (best-effort)
    event  (any → any subscriber) priority  50, stream lane   (reliable, ordered)

Namespaces baked in:

    game/<room>/state
    game/<room>/input                ← shared track; every player publishes here.
    game/<room>/event/<channel>      ← shared per channel.

Every input + event frame carries a 1-byte ``from`` header (``[u8 len][utf-8
player_id][payload]``) so the server can sort frames by player without
managing N subscriptions. State frames skip the header — the source is
always the room authority. Mirrors the TypeScript ``@clutchcall/sdk/games``
module so docs stay one document.

Usage::

    from clutchcall.games import Games

    # client
    me = Games(token=TOK, room_id="duel-42", player_id="alice")
    inp = me.publish_input()
    me.subscribe_state(lambda data: render(deserialize(data)))
    inp.write(serialize_input(local))

    # server
    auth = Games(token=TOK, room_id="duel-42")          # no player_id
    state = auth.publish_state(tick_hz=30)
    auth.subscribe_inputs(lambda pid, data: apply(pid, data))
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Callable, Optional
from urllib.parse import quote


def _moqt_module():
    from clutchcall import moqt
    return moqt


# ─── wire format ─────────────────────────────────────────────────────────

FROM_HEADER_BYTES = 1
MAX_FROM_LEN = 0xFF


class GamesError(Exception):
    """Raised by every games surface when the wire format or args are off."""


def encode_with_from(from_player_id: str, payload: bytes) -> bytes:
    """Encode ``[u8 from_len][from_player_id][payload]``."""
    from_bytes = from_player_id.encode("utf-8")
    if len(from_bytes) > MAX_FROM_LEN:
        raise GamesError(
            f"games: from_player_id longer than 255 bytes ({len(from_bytes)})"
        )
    return bytes([len(from_bytes)]) + from_bytes + bytes(payload)


@dataclass
class DecodedWithFrom:
    from_player_id: str
    payload: bytes


def decode_with_from(buf: bytes) -> DecodedWithFrom:
    if len(buf) < FROM_HEADER_BYTES:
        raise GamesError("games: frame too short for header")
    n = buf[0]
    end = FROM_HEADER_BYTES + n
    if len(buf) < end:
        raise GamesError(
            f"games: truncated frame (from_len={n}, available={len(buf) - FROM_HEADER_BYTES})"
        )
    return DecodedWithFrom(
        from_player_id=buf[FROM_HEADER_BYTES:end].decode("utf-8"),
        payload=bytes(buf[end:]),
    )


# ─── handles ─────────────────────────────────────────────────────────────

class StatePublisher:
    """Server-only state publisher — no ``from`` header."""

    def __init__(self, track: Any) -> None:
        self._track = track

    def write(self, state_bytes: bytes, *, priority: int = 100) -> None:
        self._track.write(time.monotonic_ns() // 1_000, state_bytes, priority=priority)

    def close(self) -> None:
        try:
            self._track.close()
        except Exception:  # noqa: BLE001
            pass


class FromPublisher:
    """Player-bound publisher — prefixes every frame with the caller's player_id."""

    def __init__(self, track: Any, from_player_id: str, default_priority: int) -> None:
        self._track = track
        self._from = from_player_id
        self._default_priority = default_priority

    def write(self, payload: bytes, *, priority: Optional[int] = None) -> None:
        self._track.write(
            time.monotonic_ns() // 1_000,
            encode_with_from(self._from, payload),
            priority=priority if priority is not None else self._default_priority,
        )

    def close(self) -> None:
        try:
            self._track.close()
        except Exception:  # noqa: BLE001
            pass


class GamesSubscription:
    def __init__(self, sub: Any, ns: str, name: str) -> None:
        self._sub = sub
        self.ns = ns
        self.name = name

    def close(self) -> None:
        try:
            self._sub.close()
        except Exception:  # noqa: BLE001
            pass


# ─── client ──────────────────────────────────────────────────────────────

class Games:
    """Games client — one per ``(room, player)`` or ``(room, server)``.

    Server / authority omits ``player_id``. Players must pass it; the SDK
    rejects ``publish_input`` / ``publish_event`` calls otherwise.
    """

    def __init__(
        self,
        *,
        token: str,
        room_id: str,
        player_id: Optional[str] = None,
        relay_host: str = "relay.clutchcall.dev",
        on_state: Optional[Callable[[int, Optional[str]], None]] = None,
    ) -> None:
        if not token:
            raise GamesError("Games: token required")
        if not room_id:
            raise GamesError("Games: room_id required")
        self._token = token
        self._room_id = room_id
        self._player_id = player_id
        self._relay_host = relay_host
        self._on_state = on_state
        self._client: Any = None

    @property
    def state_ns(self) -> str:
        return f"game/{self._room_id}/state"

    @property
    def input_ns(self) -> str:
        return f"game/{self._room_id}/input"

    def event_ns(self, channel: str) -> str:
        if not channel:
            raise GamesError("Games.event_ns: channel required")
        return f"game/{self._room_id}/event/{quote(channel, safe='._-')}"

    def _ensure_client(self) -> Any:
        if self._client is not None:
            return self._client
        moqt = _moqt_module()
        pid_seg = (
            f"/{quote(self._player_id, safe='-_')}"
            if self._player_id else "/_authority"
        )
        url = f"moq://{self._relay_host}/games/{quote(self._room_id, safe='-_')}{pid_seg}"
        self._client = moqt.MoqtClient.connect(url, self._token, on_state=self._on_state)
        return self._client

    def _require_player(self, op: str) -> str:
        if not self._player_id:
            raise GamesError(
                f"Games.{op}: player_id required on Games() — only players can publish on this channel"
            )
        return self._player_id

    # ── state: server → all ──────────────────────────────────────────────

    def publish_state(self, *, tick_hz: Optional[int] = None) -> StatePublisher:
        client = self._ensure_client()
        schema_tag = f"game/state;tickHz={tick_hz}" if tick_hz else "game/state"
        track = client.publish_frame(
            self.state_ns, "tick",
            capability="game.state",
            schema_tag=schema_tag,
        )
        return StatePublisher(track)

    def subscribe_state(self, on_state: Callable[[bytes], None]) -> GamesSubscription:
        client = self._ensure_client()

        def _frame(_ts: int, _prio: int, data: bytes) -> None:
            on_state(bytes(data))

        sub = client.subscribe_frame(self.state_ns, "tick", on_frame=_frame)
        return GamesSubscription(sub, self.state_ns, "tick")

    # ── input: each player → server ──────────────────────────────────────

    def publish_input(self) -> FromPublisher:
        pid = self._require_player("publish_input")
        client = self._ensure_client()
        track = client.publish_frame(
            self.input_ns, "frame",
            capability="game.input",
            schema_tag="game/input",
        )
        return FromPublisher(track, pid, 100)

    def subscribe_inputs(
        self, on_input: Callable[[str, bytes], None],
    ) -> GamesSubscription:
        client = self._ensure_client()

        def _frame(_ts: int, _prio: int, data: bytes) -> None:
            try:
                d = decode_with_from(bytes(data))
            except GamesError:
                return
            on_input(d.from_player_id, d.payload)

        sub = client.subscribe_frame(self.input_ns, "frame", on_frame=_frame)
        return GamesSubscription(sub, self.input_ns, "frame")

    # ── events: any peer → any subscriber ────────────────────────────────

    def publish_event(self, *, channel: str) -> FromPublisher:
        # Server-emitted events fall back to "_authority" so subscribers
        # always have a defined `from`.
        sender = self._player_id or "_authority"
        client = self._ensure_client()
        track = client.publish_frame(
            self.event_ns(channel), "msg",
            capability="game.event",
            schema_tag=f"game/event;channel={channel}",
        )
        return FromPublisher(track, sender, 50)

    def subscribe_events(
        self, *, channel: str,
        on_event: Callable[[str, bytes], None],
    ) -> GamesSubscription:
        client = self._ensure_client()
        ns = self.event_ns(channel)

        def _frame(_ts: int, _prio: int, data: bytes) -> None:
            try:
                d = decode_with_from(bytes(data))
            except GamesError:
                return
            on_event(d.from_player_id, d.payload)

        sub = client.subscribe_frame(ns, "msg", on_frame=_frame)
        return GamesSubscription(sub, ns, "msg")

    def close(self) -> None:
        if self._client is not None:
            try:
                self._client.close()
            except Exception:  # noqa: BLE001
                pass
            self._client = None


__all__ = [
    "Games",
    "GamesError",
    "StatePublisher",
    "FromPublisher",
    "GamesSubscription",
    "encode_with_from",
    "decode_with_from",
    "DecodedWithFrom",
    "FROM_HEADER_BYTES",
    "MAX_FROM_LEN",
]
