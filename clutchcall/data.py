"""Data modality — MQTT-style typed pub/sub over QUIC (MoQT).

One MoQT track per top-level topic segment::

    sensors/room1/temp  →  data/sensors  (shared track for every "sensors/*")

The frame header carries the full topic and the publisher's ``client_id`` so
subscribers filter MQTT-style (``+`` / ``#``) and identify the source
without out-of-band lookup. Mirrors the TypeScript ``@clutchcall/sdk/data``
module so docs stay one document.

Usage::

    from clutchcall.data import Data

    data = Data(
        relay_host="relay.clutchcall.dev",
        token=os.environ["CLUTCHCALL_DATA_TOKEN"],
        client_id="device-7",
    )

    # publish
    data.publish(topic="sensors/room1/temp", payload=b"23.5")

    # subscribe with an MQTT-style filter
    sub = data.subscribe(
        topic_filter="sensors/+/temp",
        on_message=lambda m: print(m.topic, "←", m.from_client_id, m.payload),
    )
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional


def _moqt_module():
    from clutchcall import moqt
    return moqt


# ─── errors ──────────────────────────────────────────────────────────────

class DataError(Exception):
    """Raised by every data surface when the wire or args are off."""


# ─── wire format ─────────────────────────────────────────────────────────

FROM_LEN_BYTES = 1
TOPIC_LEN_BYTES = 1
MAX_FROM_LEN = 0xFF
MAX_TOPIC_LEN = 0xFF


def encode_data_frame(from_client_id: str, topic: str, payload: bytes) -> bytes:
    from_b  = from_client_id.encode("utf-8")
    topic_b = topic.encode("utf-8")
    if len(from_b)  > MAX_FROM_LEN:
        raise DataError(f"data: from_client_id > 255 bytes ({len(from_b)})")
    if len(topic_b) > MAX_TOPIC_LEN:
        raise DataError(f"data: topic > 255 bytes ({len(topic_b)})")
    return (bytes([len(from_b)])  + from_b
          + bytes([len(topic_b)]) + topic_b
          + bytes(payload))


@dataclass
class DecodedDataFrame:
    from_client_id: str
    topic: str
    payload: bytes


def decode_data_frame(buf: bytes) -> DecodedDataFrame:
    if len(buf) < FROM_LEN_BYTES:
        raise DataError("data: frame too short")
    n = buf[0]
    pos = FROM_LEN_BYTES
    if len(buf) < pos + n + TOPIC_LEN_BYTES:
        raise DataError("data: truncated frame (from + topic_len)")
    from_b = buf[pos:pos + n]; pos += n
    t = buf[pos]; pos += TOPIC_LEN_BYTES
    if len(buf) < pos + t:
        raise DataError("data: truncated frame (topic)")
    topic_b = buf[pos:pos + t]; pos += t
    return DecodedDataFrame(
        from_client_id=from_b.decode("utf-8"),
        topic=topic_b.decode("utf-8"),
        payload=bytes(buf[pos:]),
    )


# ─── topic filter matching ───────────────────────────────────────────────

def topic_matches(topic: str, topic_filter: str) -> bool:
    """MQTT-style match. ``+`` matches one segment; ``#`` matches the rest
    and must be the last segment."""
    if topic == topic_filter:
        return True
    t_parts = topic.split("/")
    f_parts = topic_filter.split("/")
    for i, fp in enumerate(f_parts):
        if fp == "#":
            return i == len(f_parts) - 1
        if i >= len(t_parts):
            return False
        if fp == "+":
            continue
        if fp != t_parts[i]:
            return False
    return len(t_parts) == len(f_parts)


def top_level_segment(filter_or_topic: str) -> str:
    """Return the concrete top-level path segment. Raise if it's a wildcard."""
    head = filter_or_topic.split("/", 1)[0]
    if head in ("+", "#"):
        raise DataError(
            f"data: top-level wildcard not supported (filter={filter_or_topic!r}) — "
            "pick a concrete top-level segment"
        )
    if not head:
        raise DataError("data: empty topic / filter")
    return head


# ─── handles ─────────────────────────────────────────────────────────────

class DataSubscription:
    def __init__(self, sub: Any, ns: str, topic_filter: str) -> None:
        self._sub = sub
        self.ns = ns
        self.topic_filter = topic_filter

    def close(self) -> None:
        try:
            self._sub.close()
        except Exception:  # noqa: BLE001
            pass


@dataclass
class DataMessage:
    topic: str
    from_client_id: str
    payload: bytes
    retained: bool


# ─── client ──────────────────────────────────────────────────────────────

class Data:
    """MQTT-style typed pub/sub client.

    One MoQT session per Data client; one ``publishFrame`` track per
    top-level topic segment, opened lazily on the first publish.
    """

    def __init__(
        self,
        *,
        token: str,
        client_id: str,
        relay_host: str = "relay.clutchcall.dev",
        on_state: Optional[Callable[[int, Optional[str]], None]] = None,
    ) -> None:
        if not token:
            raise DataError("Data: token required")
        if not client_id:
            raise DataError("Data: client_id required")
        self._token = token
        self._client_id = client_id
        self._relay_host = relay_host
        self._on_state = on_state
        self._client: Any = None
        self._pubs: Dict[str, Any] = {}

    def _ensure_client(self) -> Any:
        if self._client is not None:
            return self._client
        moqt = _moqt_module()
        from urllib.parse import quote
        url = f"moq://{self._relay_host}/data/{quote(self._client_id, safe='-_')}"
        self._client = moqt.MoqtClient.connect(url, self._token, on_state=self._on_state)
        return self._client

    def _get_pub(self, top: str) -> Any:
        pub = self._pubs.get(top)
        if pub is not None:
            return pub
        client = self._ensure_client()
        pub = client.publish_frame(
            f"data/{top}", "msg",
            capability="data.pubsub",
            schema_tag=f"data;top={top}",
        )
        self._pubs[top] = pub
        return pub

    def publish(
        self,
        *,
        topic: str,
        payload: bytes,
        reliable: bool = False,
        retained: bool = False,
    ) -> None:
        """Publish one message. ``retained=True`` asks the relay to cache
        the latest payload on this topic for late-joining subscribers."""
        top = top_level_segment(topic)
        pub = self._get_pub(top)
        frame = encode_data_frame(self._client_id, topic, payload)
        # reliable / retained get a higher priority (lower number); the
        # relay's retained cache keys on (namespace, topic).
        priority = 30 if (reliable or retained) else 100
        pub.write(time.monotonic_ns() // 1_000, frame, priority=priority)

    def subscribe(
        self,
        *,
        topic_filter: str,
        on_message: Callable[[DataMessage], None],
    ) -> DataSubscription:
        """Subscribe with an MQTT-style filter. The callback fires for every
        matching message (the SDK filters client-side after the substrate
        delivers everything on the top-level namespace)."""
        top = top_level_segment(topic_filter)
        ns  = f"data/{top}"
        client = self._ensure_client()

        def _frame(_ts: int, prio: int, data: bytes) -> None:
            try:
                f = decode_data_frame(bytes(data))
            except DataError:
                return
            if not topic_matches(f.topic, topic_filter):
                return
            on_message(DataMessage(
                topic=f.topic,
                from_client_id=f.from_client_id,
                payload=f.payload,
                retained=(prio <= 30),
            ))

        sub = client.subscribe_frame(ns, "msg", on_frame=_frame)
        return DataSubscription(sub, ns, topic_filter)

    def close(self) -> None:
        for p in self._pubs.values():
            try: p.close()
            except Exception: pass  # noqa: BLE001
        self._pubs.clear()
        if self._client is not None:
            try: self._client.close()
            except Exception: pass  # noqa: BLE001
            self._client = None


__all__ = [
    "Data",
    "DataError",
    "DataMessage",
    "DataSubscription",
    "encode_data_frame",
    "decode_data_frame",
    "topic_matches",
    "top_level_segment",
    "DecodedDataFrame",
    "FROM_LEN_BYTES",
    "TOPIC_LEN_BYTES",
    "MAX_FROM_LEN",
    "MAX_TOPIC_LEN",
]
