"""Robotics modality — typed pub/sub for a robot fleet over QUIC (MoQT).

Wraps :mod:`clutchcall.moqt` with the bidirectional teleop convention baked
in: telemetry on ``robot/<id>``, commands on ``robot/<id>/ctl``. The wire
format is a type-name-prefixed envelope so cross-language subscribers pick
the right deserializer with no out-of-band agreement. Mirrors the TypeScript
``@clutchcall/sdk/robotics`` module so docs stay one document.

Usage::

    from clutchcall.robotics import Robotics, QoSProfile

    r = Robotics(
        relay_host="relay.clutchcall.dev",
        token=os.environ["CLUTCHCALL_RELAY_TOKEN"],
        robot_id="turtlebot-7",
    )

    # robot side: publish odometry
    odom = r.publish_telemetry(
        topic="odom",
        type_name="nav_msgs/msg/Odometry",
        qos=QoSProfile(reliability="reliable", depth=10),
    )
    odom.write(cdr_bytes)

    # cloud side: receive odometry
    sub = r.subscribe_telemetry(
        topic="odom",
        type_name="nav_msgs/msg/Odometry",
        on_message=lambda tn, data: render(parse_odometry(data)),
    )

The CDR payload is opaque to the SDK — whatever your DDS / rmw_zenoh layer
emits goes on the wire unchanged. Non-ROS callers pick a ``type_name`` their
subscribers know (e.g. ``"json"``, ``"protobuf:my.pkg.Foo"``).
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Callable, Literal, Optional

# Lazy import — only the data plane needs the moqt module.
def _moqt_module():
    from clutchcall import moqt
    return moqt


# ─── wire format ─────────────────────────────────────────────────────────

HEADER_BYTES = 2
MAX_TYPE_NAME = 0xFFFF


class RoboticsError(Exception):
    """Raised by every robotics surface when the wire format or args are off."""


def encode_frame(type_name: str, payload: bytes) -> bytes:
    """Encode ``[u16 BE type_name_len][type_name][payload]``.

    Raises :class:`RoboticsError` on empty / over-long type names. Matches
    ``clutch::robot::wire::frame_with_type`` in the C++ engine.
    """
    if not type_name:
        raise RoboticsError("robotics: type_name required")
    type_bytes = type_name.encode("utf-8")
    if len(type_bytes) > MAX_TYPE_NAME:
        raise RoboticsError(
            f"robotics: type_name longer than 65535 bytes ({len(type_bytes)})"
        )
    return len(type_bytes).to_bytes(HEADER_BYTES, "big") + type_bytes + bytes(payload)


@dataclass
class DecodedFrame:
    type_name: str
    payload: bytes


def decode_frame(buf: bytes) -> DecodedFrame:
    """Parse ``[u16 BE type_name_len][type_name][payload]``.

    Raises :class:`RoboticsError` on truncated frames. Zero-length payloads
    are accepted (heartbeats, etc.).
    """
    if len(buf) < HEADER_BYTES:
        raise RoboticsError("robotics: frame too short for header")
    n = int.from_bytes(buf[:HEADER_BYTES], "big")
    end = HEADER_BYTES + n
    if len(buf) < end:
        raise RoboticsError(
            f"robotics: truncated frame (type_name_len={n}, available={len(buf) - HEADER_BYTES})"
        )
    return DecodedFrame(
        type_name=buf[HEADER_BYTES:end].decode("utf-8"),
        payload=bytes(buf[end:]),
    )


# ─── QoS ─────────────────────────────────────────────────────────────────

Reliability = Literal["best_effort", "reliable"]
Durability  = Literal["volatile", "transient_local"]


@dataclass
class QoSProfile:
    reliability: Reliability = "best_effort"
    durability:  Durability  = "volatile"
    depth:       int         = 10


def _capability(qos: QoSProfile) -> str:
    if qos.durability == "transient_local":
        return "ros.tl_reliable" if qos.reliability == "reliable" else "ros.tl_be"
    return "ros.reliable" if qos.reliability == "reliable" else "ros.best_effort"


def _default_priority(qos: QoSProfile) -> int:
    return 50 if qos.reliability == "reliable" else 100


# ─── handles ─────────────────────────────────────────────────────────────

class RoboticsPublication:
    """A typed publication on the relay. Hold the reference for the track's
    lifetime — releasing it closes the underlying MoQT publication."""

    def __init__(self, track: Any, type_name: str, default_priority: int) -> None:
        self._track = track
        self._type_name = type_name
        self._default_priority = default_priority

    def write(self, payload: bytes, *, priority: Optional[int] = None) -> None:
        """Push one typed message. ``payload`` is raw CDR bytes (or whatever
        matches ``type_name``). ``priority`` overrides the default for this
        single frame; 0 = highest, 255 = lowest."""
        ts_us = time.monotonic_ns() // 1_000
        self._track.write(
            ts_us,
            encode_frame(self._type_name, payload),
            priority=priority if priority is not None else self._default_priority,
        )

    def close(self) -> None:
        try:
            self._track.close()
        except Exception:  # noqa: BLE001 — best-effort teardown
            pass


class RoboticsSubscription:
    """A typed subscription on the relay. Hold the reference for the track's
    lifetime — releasing it segfaults the engine on the next inbound frame."""

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

class Robotics:
    """Robotics client — one per ``(tenant, robot)``.

    Bidirectional teleop namespace convention is enforced:
    :meth:`publish_telemetry` / :meth:`subscribe_telemetry` use
    ``robot/<id>``; :meth:`publish_command` / :meth:`subscribe_command` use
    ``robot/<id>/ctl``. The underlying MoQT session is opened lazily on the
    first call and reused; :meth:`close` tears it down.
    """

    def __init__(
        self,
        *,
        token: str,
        robot_id: str,
        relay_host: str = "relay.clutchcall.dev",
        on_state: Optional[Callable[[int, Optional[str]], None]] = None,
    ) -> None:
        if not token:
            raise RoboticsError("Robotics: token required")
        if not robot_id:
            raise RoboticsError("Robotics: robot_id required")
        self._token = token
        self._robot_id = robot_id
        self._relay_host = relay_host
        self._on_state = on_state
        self._client: Any = None

    @property
    def telemetry_ns(self) -> str:
        return f"robot/{self._robot_id}"

    @property
    def command_ns(self) -> str:
        return f"robot/{self._robot_id}/ctl"

    def _ensure_client(self) -> Any:
        if self._client is not None:
            return self._client
        moqt = _moqt_module()
        # urlencode the robot id so a slash in the id (rare but legal) doesn't
        # split the path. Keep ":" + "-" as-is — they're common in robot ids.
        from urllib.parse import quote
        url = f"moq://{self._relay_host}/robotics/{quote(self._robot_id, safe='-_:')}"
        self._client = moqt.MoqtClient.connect(url, self._token, on_state=self._on_state)
        return self._client

    # ── telemetry ────────────────────────────────────────────────────────

    def publish_telemetry(
        self, *, topic: str, type_name: str, qos: Optional[QoSProfile] = None,
    ) -> RoboticsPublication:
        q = qos or QoSProfile()
        client = self._ensure_client()
        track = client.publish_frame(
            self.telemetry_ns, topic,
            capability=_capability(q),
            schema_tag=f"ros2/cdr;type={type_name}",
        )
        return RoboticsPublication(track, type_name, _default_priority(q))

    def subscribe_telemetry(
        self, *, topic: str, type_name: str,
        on_message: Callable[[str, bytes], None],
    ) -> RoboticsSubscription:
        client = self._ensure_client()

        def _frame(_ts: int, _prio: int, data: bytes) -> None:
            try:
                f = decode_frame(bytes(data))
            except RoboticsError:
                return  # silently drop malformed frames; we won't crash the engine thread
            on_message(f.type_name, f.payload)

        sub = client.subscribe_frame(self.telemetry_ns, topic, on_frame=_frame)
        return RoboticsSubscription(sub, self.telemetry_ns, topic)

    # ── commands ─────────────────────────────────────────────────────────

    def publish_command(
        self, *, topic: str, type_name: str, qos: Optional[QoSProfile] = None,
    ) -> RoboticsPublication:
        q = qos or QoSProfile()
        client = self._ensure_client()
        track = client.publish_frame(
            self.command_ns, topic,
            capability=_capability(q),
            schema_tag=f"ros2/cdr;type={type_name}",
        )
        return RoboticsPublication(track, type_name, _default_priority(q))

    def subscribe_command(
        self, *, topic: str, type_name: str,
        on_message: Callable[[str, bytes], None],
    ) -> RoboticsSubscription:
        client = self._ensure_client()

        def _frame(_ts: int, _prio: int, data: bytes) -> None:
            try:
                f = decode_frame(bytes(data))
            except RoboticsError:
                return
            on_message(f.type_name, f.payload)

        sub = client.subscribe_frame(self.command_ns, topic, on_frame=_frame)
        return RoboticsSubscription(sub, self.command_ns, topic)

    def close(self) -> None:
        if self._client is not None:
            try:
                self._client.close()
            except Exception:  # noqa: BLE001
                pass
            self._client = None


__all__ = [
    "Robotics",
    "RoboticsError",
    "RoboticsPublication",
    "RoboticsSubscription",
    "QoSProfile",
    "Reliability",
    "Durability",
    "encode_frame",
    "decode_frame",
    "DecodedFrame",
    "HEADER_BYTES",
    "MAX_TYPE_NAME",
]
