"""Streams modality — broadcast over QUIC (MoQT) with a signed playback URL.

One client for the control plane (live inputs, signing keys, mint playback
token), one helper for the data plane (``BroadcastViewer`` wraps
:class:`clutchcall.moqt.MoqtClient` so the integrator never has to deal with
relay path conventions). The shape mirrors the TypeScript ``@clutchcall/sdk/streams``
module so cross-language docs stay one document.

Usage::

    from clutchcall.streams import Streams, BroadcastViewer

    streams = Streams(base_url="https://app.clutchcall.dev",
                      api_key=os.environ["CLUTCHCALL_API_KEY"],
                      org_id="org_abc")

    inp = streams.live_inputs.get(id="li_xxx")
    ticket = inp.signed_playback_url(ttl_seconds=3600)

    viewer = BroadcastViewer.open(
        ticket.url,
        on_chunk=lambda is_init, chunk: pipe.write(chunk.data),
        on_close=lambda reason, detail: log("closed", reason, detail),
    )

The Streams client itself is plain HTTPS (``urllib.request``); the persistent
QUIC connection happens inside ``BroadcastViewer`` via :mod:`clutchcall.moqt`.
The relay enforces the playback JWT — bad / expired tokens close with
``auth_failed``, surfaced through ``on_close``.
"""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, Callable, Iterable, List, Literal, Optional

# Lazy import — only the viewer needs the moqt module, and the control
# plane should work even if the C engine FFI isn't on this host (e.g. for a
# pure-Python tool that only mints tokens).
def _moqt_module():
    from clutchcall import moqt  # local import to defer libclutchcall_moqt_ffi.so load
    return moqt


# ─── errors ─────────────────────────────────────────────────────────────

class StreamsError(Exception):
    """Raised by every Streams.* call when the BFF returns an error.

    The message is the server's tRPC error message verbatim when available;
    falls back to ``HTTP <status>`` for raw transport failures.
    """


# ─── control-plane client ───────────────────────────────────────────────

LiveInputStatus = Literal["idle", "live", "errored"]
IngestKind      = Literal["fmp4", "whip", "rtmp", "srt"]
SigningAlg      = Literal["Ed25519", "RS256"]
SigningUse      = Literal["playback"]


@dataclass
class _Transport:
    """Internal: bundles the auth + HTTP knobs every helper needs."""
    base_url: str
    api_key: str
    org_id: Optional[str]

    def call(self, path: str, payload: Any, kind: Literal["query", "mutation"]) -> Any:
        url = f"{self.base_url.rstrip('/')}/api/trpc/{path}"
        body = None
        if kind == "query":
            qs = urllib.parse.urlencode({"input": json.dumps(payload)})
            url = f"{url}?{qs}"
        else:
            body = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            url,
            data=body,
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
            raise StreamsError(f"tRPC {path} {e.code}: {text}") from e
        except urllib.error.URLError as e:
            raise StreamsError(f"tRPC {path}: {e.reason}") from e
        if "error" in doc:
            raise StreamsError(doc["error"].get("message", "tRPC error"))
        try:
            return doc["result"]["data"]
        except KeyError as e:
            raise StreamsError(f"tRPC {path}: empty result") from e


class Streams:
    """Top-level Streams client. Hands out scoped helpers."""

    def __init__(
        self,
        *,
        base_url: str,
        api_key: str,
        org_id: Optional[str] = None,
    ) -> None:
        if not base_url:
            raise ValueError("Streams: base_url required")
        if not api_key:
            raise ValueError("Streams: api_key required")
        self._t = _Transport(base_url=base_url, api_key=api_key, org_id=org_id)
        self.live_inputs  = LiveInputs(self._t)
        self.signing_keys = SigningKeys(self._t)


# ─── live inputs ────────────────────────────────────────────────────────

@dataclass
class LiveInputData:
    id: str
    external_input_id: str
    name: str
    status: LiveInputStatus
    ingest: IngestKind
    created_at: str


@dataclass
class LiveInputWithSecret:
    """Returned by ``create()`` and ``rotate_stream_key()``.

    The cleartext stream key is only available at creation/rotation — the
    BFF stores only a hash. Capture ``stream_key`` and persist it yourself.
    """
    input: "LiveInput"
    stream_key: str


@dataclass
class SignedPlaybackUrl:
    url: str           # moq://relay.../playback/<external_input_id>?tok=<jwt>
    kid: str
    alg: str
    expires_at: int    # unix seconds


class LiveInputs:
    def __init__(self, t: _Transport) -> None:
        self._t = t

    def create(self, *, name: str, ingest: IngestKind = "fmp4") -> LiveInputWithSecret:
        org = self._require_org("create")
        row = self._t.call(
            "streams.liveInputs.create",
            {"orgId": org, "name": name, "ingest": ingest},
            "mutation",
        )
        return LiveInputWithSecret(
            input=LiveInput(self._t, _row_to_data(row)),
            stream_key=row.get("stream_key_cleartext", ""),
        )

    def get(self, *, id: str) -> "LiveInput":
        org = self._require_org("get")
        row = self._t.call(
            "streams.liveInputs.get",
            {"orgId": org, "id": id},
            "query",
        )
        return LiveInput(self._t, _row_to_data(row))

    def list(self, *, page: int = 1, per_page: int = 50) -> List["LiveInput"]:
        org = self._require_org("list")
        rows: Iterable[Any] = self._t.call(
            "streams.liveInputs.list",
            {"orgId": org, "page": page, "perPage": per_page},
            "query",
        )
        return [LiveInput(self._t, _row_to_data(r)) for r in rows]

    def _require_org(self, op: str) -> str:
        if not self._t.org_id:
            raise StreamsError(f"Streams.live_inputs.{op}: org_id required (pass on Streams())")
        return self._t.org_id


def _row_to_data(row: Any) -> LiveInputData:
    return LiveInputData(
        id=row["id"],
        external_input_id=row["external_input_id"],
        name=row["name"],
        status=row["status"],
        ingest=row.get("ingest", "fmp4"),
        created_at=row.get("createdAt", ""),
    )


class LiveInput:
    """Handle to a ``stream_live_input``. Snapshot fields + bound methods.

    The snapshot is the row at fetch time; the methods always round-trip the
    BFF, so a stale instance is fine for re-minting or rotating keys.
    """

    def __init__(self, t: _Transport, data: LiveInputData) -> None:
        self._t = t
        self._d = data

    @property
    def id(self) -> str: return self._d.id

    @property
    def external_input_id(self) -> str: return self._d.external_input_id

    @property
    def name(self) -> str: return self._d.name

    @property
    def status(self) -> LiveInputStatus: return self._d.status

    @property
    def ingest(self) -> IngestKind: return self._d.ingest

    def signed_playback_url(self, *, ttl_seconds: int = 3600) -> SignedPlaybackUrl:
        """Mint a short-lived playback URL for this input.

        The relay verifies the JWT inside the SUBSCRIBE handler — bad or
        expired tokens close the session with ``auth_failed``. Re-mint and
        re-open before exp to keep a viewer alive across long sessions.
        """
        org = self._t.org_id
        if not org:
            raise StreamsError("LiveInput.signed_playback_url: org_id required")
        minted = self._t.call(
            "streams.liveInputs.mintPlaybackToken",
            {"orgId": org, "id": self.id, "ttlSeconds": ttl_seconds},
            "mutation",
        )
        return SignedPlaybackUrl(
            url=f"moq://relay.clutchcall.dev/playback/{minted['input']}?tok={minted['token']}",
            kid=minted["kid"],
            alg=minted["alg"],
            expires_at=int(minted["expires_at"]),
        )

    def rotate_stream_key(self) -> LiveInputWithSecret:
        org = self._t.org_id
        if not org:
            raise StreamsError("LiveInput.rotate_stream_key: org_id required")
        row = self._t.call(
            "streams.liveInputs.rotateStreamKey",
            {"orgId": org, "id": self.id},
            "mutation",
        )
        return LiveInputWithSecret(
            input=LiveInput(self._t, _row_to_data(row)),
            stream_key=row.get("stream_key_cleartext", ""),
        )


# ─── signing keys ───────────────────────────────────────────────────────

@dataclass
class SigningKeyData:
    id: str                # the JWT kid
    alg: SigningAlg
    use: SigningUse
    public_key_pem: str
    status: Literal["active", "inactive"]
    created_at: str


class SigningKeys:
    def __init__(self, t: _Transport) -> None:
        self._t = t

    def create(
        self,
        *,
        alg: SigningAlg = "Ed25519",
        use: SigningUse = "playback",
    ) -> "SigningKey":
        org = self._t.org_id
        if not org:
            raise StreamsError("Streams.signing_keys.create: org_id required")
        row = self._t.call(
            "streams.signingKeys.create",
            {"orgId": org, "alg": alg, "use": use},
            "mutation",
        )
        return SigningKey(self._t, _row_to_signing_key(row))

    def list(self) -> List["SigningKey"]:
        org = self._t.org_id
        if not org:
            raise StreamsError("Streams.signing_keys.list: org_id required")
        rows = self._t.call(
            "streams.signingKeys.list",
            {"orgId": org},
            "query",
        )
        return [SigningKey(self._t, _row_to_signing_key(r)) for r in rows]


def _row_to_signing_key(row: Any) -> SigningKeyData:
    return SigningKeyData(
        id=row["id"],
        alg=row["alg"],
        use=row["use"],
        public_key_pem=row.get("publicKeyPem", ""),
        status=row.get("status", "active"),
        created_at=row.get("createdAt", ""),
    )


class SigningKey:
    def __init__(self, t: _Transport, data: SigningKeyData) -> None:
        self._t = t
        self._d = data

    @property
    def id(self) -> str: return self._d.id

    @property
    def alg(self) -> SigningAlg: return self._d.alg

    @property
    def public_key_pem(self) -> str: return self._d.public_key_pem

    @property
    def status(self) -> str: return self._d.status

    def deactivate(self) -> None:
        org = self._t.org_id
        if not org:
            raise StreamsError("SigningKey.deactivate: org_id required")
        self._t.call(
            "streams.signingKeys.deactivate",
            {"orgId": org, "id": self.id},
            "mutation",
        )


# ─── broadcast viewer ───────────────────────────────────────────────────

CloseReason = Literal["complete", "auth_failed", "network", "closed_by_caller"]


@dataclass
class BroadcastChunk:
    data: bytes
    timestamp_us: int
    priority: int
    is_init: bool


class BroadcastViewer:
    """Connect to a signed playback URL and forward chunks to a callback.

    The viewer is single-use: ``open()``, consume chunks, ``close()`` (or
    rely on the relay to signal close). The first chunk is the CMAF init
    segment (``is_init=True``); subsequent chunks are media segments.
    """

    def __init__(
        self,
        client: Any,
        on_close: Callable[[CloseReason, Optional[str]], None],
    ) -> None:
        self._client = client
        self._on_close = on_close
        self._closed = False

    @classmethod
    def open(
        cls,
        url: str,
        *,
        on_chunk: Callable[[bool, BroadcastChunk], None],
        on_close: Optional[Callable[[CloseReason, Optional[str]], None]] = None,
    ) -> "BroadcastViewer":
        moqt = _moqt_module()
        parsed = _parse_playback_url(url)
        sentinel = {"saw_init": False, "closed": False}

        def _state(state_code: int, reason: Optional[str] = None) -> None:
            # ConnectionState: 3 = Closed, 4 = Failed. Map to our enum.
            if sentinel["closed"]:
                return
            if state_code in (3, 4):
                sentinel["closed"] = True
                if on_close is None:
                    return
                is_auth = bool(reason) and "auth" in (reason or "").lower()
                friendly: CloseReason = "auth_failed" if (state_code == 4 and is_auth) else "network"
                on_close(friendly, reason)

        client = moqt.MoqtClient.connect(parsed.wt_url, parsed.token, on_state=_state)

        def _frame(ts_us: int, prio: int, data: bytes) -> None:
            is_init = not sentinel["saw_init"]
            sentinel["saw_init"] = True
            on_chunk(is_init, BroadcastChunk(
                data=bytes(data), timestamp_us=int(ts_us),
                priority=int(prio), is_init=is_init,
            ))

        # Hold the sub handle on the viewer so the GC doesn't free the
        # underlying ctypes callback. See moqt module note: callbacks fire
        # from the engine thread.
        sub = client.subscribe_frame(parsed.namespace, "broadcast", on_frame=_frame)
        viewer = cls(client, on_close or (lambda r, d: None))
        viewer._sub = sub  # keep alive
        return viewer

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._client.close()
        finally:
            self._on_close("closed_by_caller", None)


PublisherCloseReason = Literal["closed_by_caller", "auth_failed", "network", "finished"]


@dataclass
class PublisherCodecs:
    """RFC 6381 codec hints. Optional but recommended."""
    video: Optional[str] = None       # e.g. "avc1.42E01F"
    audio: Optional[str] = None       # e.g. "opus" or "mp4a.40.2"


class BroadcastPublisher:
    """Push a broadcast into the relay.

    Auth model: the per-input STREAM KEY (sk_live_…) returned at
    :meth:`LiveInputs.create` / :meth:`LiveInput.rotate_stream_key` — never
    again. Save it once at issuance and pass it back here whenever you push.

    Usage::

        pub = BroadcastPublisher.open(
            input_id=input.external_input_id,
            stream_key=stream_key,
            codecs=PublisherCodecs(video="avc1.42E01F", audio="opus"),
        )
        pub.write(fmp4_init)            # CMAF init segment FIRST
        pub.write(fmp4_segment)         # media segments
        pub.close()

    The first chunk is the initialization segment (priority 0, starts a new
    group); subsequent chunks are media segments (priority 1).
    """

    def __init__(
        self,
        client: Any,
        track: Any,
        on_close: Callable[[PublisherCloseReason, Optional[str]], None],
    ) -> None:
        self._client = client
        self._track = track
        self._on_close = on_close
        self._wrote_init = False
        self._closed = False

    @classmethod
    def open(
        cls,
        *,
        input_id: str,
        stream_key: str,
        relay_host: str = "relay.clutchcall.dev",
        codecs: Optional[PublisherCodecs] = None,
        on_close: Optional[Callable[[PublisherCloseReason, Optional[str]], None]] = None,
    ) -> "BroadcastPublisher":
        if not input_id:
            raise StreamsError("BroadcastPublisher: input_id required")
        if not stream_key:
            raise StreamsError("BroadcastPublisher: stream_key required")

        moqt = _moqt_module()
        # Mirror the TS publisher's URL convention: query-param the stream
        # key (most WT polyfills can't set arbitrary CONNECT headers). The
        # relay's /streams/resolve hashes it and rejects bad keys.
        moq_url = (
            f"moq://{relay_host}/publish/{input_id}"
            f"?sk={urllib.parse.quote(stream_key, safe='')}"
        )
        sentinel = {"closed": False}
        cb = on_close or (lambda r, d: None)

        def _state(state_code: int, reason: Optional[str] = None) -> None:
            if sentinel["closed"]:
                return
            if state_code in (3, 4):
                sentinel["closed"] = True
                is_auth = bool(reason) and "auth" in (reason or "").lower()
                friendly: PublisherCloseReason = (
                    "auth_failed" if (state_code == 4 and is_auth) else "network"
                )
                cb(friendly, reason)

        client = moqt.MoqtClient.connect(moq_url, "", on_state=_state)

        # capability tag drives the per-vertical analytics row the relay
        # writes; schema_tag carries the codec hint so the BFF webhook
        # fan-out can attribute codec switches without parsing fMP4.
        c = codecs or PublisherCodecs()
        schema_tag = ",".join(v for v in (c.video, c.audio) if v)
        track = client.publish_frame(
            f"publish/{input_id}", "broadcast",
            capability="media.broadcast", schema_tag=schema_tag,
        )
        return cls(client, track, cb)

    def write(self, chunk: bytes, *, timestamp_us: Optional[int] = None) -> None:
        """Push one chunk. Pass the CMAF init segment first, then every media
        segment as it arrives. The publisher timestamps monotonically; pass
        ``timestamp_us`` to override (replay / tape sync)."""
        ts = timestamp_us if timestamp_us is not None else int(time.monotonic_ns() // 1_000)
        priority = 1 if self._wrote_init else 0
        self._wrote_init = True
        self._track.write(ts, chunk, priority=priority)

    def close(self, reason: PublisherCloseReason = "closed_by_caller") -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._track.close()
        except Exception:  # noqa: BLE001 — best-effort teardown
            pass
        try:
            self._client.close()
        finally:
            self._on_close(reason, None)


@dataclass
class _Parsed:
    wt_url: str
    token: str
    namespace: str


def _parse_playback_url(moq_url: str) -> _Parsed:
    if not moq_url.startswith("moq://"):
        raise StreamsError(
            f"BroadcastViewer: expected moq:// URL, got {moq_url[:32]!r}…"
        )
    no_scheme = moq_url[len("moq://"):]
    host_part, _, after = no_scheme.partition("/")
    if not host_part or not after:
        raise StreamsError(
            "BroadcastViewer: playback URL must look like "
            "moq://<host>/playback/<input_id>?tok=…"
        )
    path_part, _, query = after.partition("?")
    parts = [p for p in path_part.split("/") if p]
    if len(parts) < 2 or parts[0] != "playback":
        raise StreamsError(
            "BroadcastViewer: playback URL path must be /playback/<input_id>"
        )
    qs = dict(p.split("=", 1) for p in query.split("&") if "=" in p) if query else {}
    token = qs.get("tok", "")
    if not token:
        raise StreamsError(
            "BroadcastViewer: playback URL is missing ?tok=<jwt> — mint with "
            "LiveInput.signed_playback_url()"
        )
    return _Parsed(
        wt_url=moq_url,
        token=token,
        namespace=f"playback/{parts[1]}",
    )


__all__ = [
    "Streams",
    "StreamsError",
    "LiveInputs",
    "LiveInput",
    "LiveInputData",
    "LiveInputWithSecret",
    "SignedPlaybackUrl",
    "SigningKeys",
    "SigningKey",
    "SigningKeyData",
    "BroadcastViewer",
    "BroadcastChunk",
    "CloseReason",
    "BroadcastPublisher",
    "PublisherCloseReason",
    "PublisherCodecs",
]
