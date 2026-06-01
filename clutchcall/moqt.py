"""Capability-aware MoQT track pub/sub for Python.

Thin ctypes binding over the C++ engine (core/moqt_client.cc) exposed through
core/moqt_ffi.cc — the same engine the C++/TS SDKs use, so Python gets real
QUIC/MoQT tracks, not just the RPC serde. A published track carries a
`capability` (intent/routing key, e.g. "asr"/"tts"/"media.passthrough"); the
relay/gateway routes it to the module that registered that capability.

    client = MoqtClient.connect("quic://relay.acme.dev:4443", token)
    pub = client.publish_audio("voice/acme/call-1", "mic", capability="asr")
    pub.write(ts_us, pcm_bytes)

    sub = client.subscribe_audio("voice/acme/call-1", "agent",
                                 on_frame=lambda ts, data: play(data))

NOTE: callbacks fire from the engine's background io_thread; ctypes holds the
GIL across them, so your handler runs single-threaded w.r.t. the interpreter.
Keep handlers short.
"""

import ctypes
import os
from typing import Callable, Optional

# ── callback prototypes (must outlive the C side; we keep refs on the objects)
_STATE_CB = ctypes.CFUNCTYPE(None, ctypes.c_void_p, ctypes.c_int32)
_FRAME_CB = ctypes.CFUNCTYPE(None, ctypes.c_void_p, ctypes.c_uint64,
                             ctypes.POINTER(ctypes.c_uint8), ctypes.c_size_t)
# Frame-track object callback carries a per-frame priority (robot telemetry).
_FRAME_OBJ_CB = ctypes.CFUNCTYPE(None, ctypes.c_void_p, ctypes.c_uint64,
                                 ctypes.c_uint8, ctypes.POINTER(ctypes.c_uint8),
                                 ctypes.c_size_t)


def _load_lib() -> ctypes.CDLL:
    path = os.environ.get("CLUTCHCALL_MOQT_FFI")
    if not path:
        here = os.path.dirname(os.path.abspath(__file__))
        for cand in ("libclutchcall_moqt_ffi.so", "clutchcall_moqt_ffi.so"):
            p = os.path.join(here, cand)
            if os.path.exists(p):
                path = p
                break
    if not path:
        path = "clutchcall_moqt_ffi.so"  # rely on the loader path
    lib = ctypes.CDLL(path)
    v = ctypes.c_void_p
    lib.clutch_moqt_connect.restype = v
    lib.clutch_moqt_connect.argtypes = [ctypes.c_char_p, ctypes.c_char_p, _STATE_CB, v]
    lib.clutch_moqt_client_close.argtypes = [v]
    lib.clutch_moqt_publish_audio.restype = v
    lib.clutch_moqt_publish_audio.argtypes = [
        v, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p,
        ctypes.c_uint32, ctypes.c_uint8, ctypes.c_uint16]
    lib.clutch_moqt_pub_write.argtypes = [v, ctypes.c_uint64,
                                          ctypes.POINTER(ctypes.c_uint8), ctypes.c_size_t]
    lib.clutch_moqt_pub_subscriber_count.restype = ctypes.c_size_t
    lib.clutch_moqt_pub_subscriber_count.argtypes = [v]
    lib.clutch_moqt_pub_close.argtypes = [v]
    lib.clutch_moqt_subscribe_audio.restype = v
    lib.clutch_moqt_subscribe_audio.argtypes = [v, ctypes.c_char_p, ctypes.c_char_p, _FRAME_CB, v]
    lib.clutch_moqt_sub_close.argtypes = [v]
    # ── frame track (opaque binary, per-frame priority) — robot telemetry ──
    lib.clutch_moqt_publish_frame.restype = v
    lib.clutch_moqt_publish_frame.argtypes = [
        v, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p,
        ctypes.c_uint8]
    lib.clutch_moqt_frame_write.argtypes = [
        v, ctypes.c_uint64, ctypes.POINTER(ctypes.c_uint8), ctypes.c_size_t,
        ctypes.c_uint8]
    lib.clutch_moqt_frame_pub_subscriber_count.restype = ctypes.c_size_t
    lib.clutch_moqt_frame_pub_subscriber_count.argtypes = [v]
    lib.clutch_moqt_frame_pub_close.argtypes = [v]
    lib.clutch_moqt_subscribe_frame.restype = v
    lib.clutch_moqt_subscribe_frame.argtypes = [
        v, ctypes.c_char_p, ctypes.c_char_p, _FRAME_OBJ_CB, v]
    lib.clutch_moqt_frame_sub_close.argtypes = [v]
    return lib


_lib: Optional[ctypes.CDLL] = None


def _lib_once() -> ctypes.CDLL:
    global _lib
    if _lib is None:
        _lib = _load_lib()
    return _lib


def _b(s) -> bytes:
    return s.encode() if isinstance(s, str) else (s or b"")


class AudioPublication:
    """A live published audio track. `write` enqueues one frame (20 ms typ.)."""

    def __init__(self, lib, handle):
        self._lib, self._h = lib, handle

    def write(self, timestamp_us: int, data: bytes) -> None:
        if not self._h:
            return
        buf = (ctypes.c_uint8 * len(data)).from_buffer_copy(data) if data else None
        self._lib.clutch_moqt_pub_write(self._h, ctypes.c_uint64(timestamp_us),
                                        buf, len(data))

    def subscriber_count(self) -> int:
        return int(self._lib.clutch_moqt_pub_subscriber_count(self._h)) if self._h else 0

    def close(self) -> None:
        if self._h:
            self._lib.clutch_moqt_pub_close(self._h)
            self._h = None


class AudioSubscription:
    """A live subscription. Frames arrive on the on_frame callback."""

    def __init__(self, lib, handle, cb_ref):
        self._lib, self._h, self._cb = lib, handle, cb_ref  # keep cb alive

    def close(self) -> None:
        if self._h:
            self._lib.clutch_moqt_sub_close(self._h)
            self._h = None


class FramePublication:
    """A live published frame track (opaque binary, per-frame priority) — robot
    telemetry / game state. `write` enqueues one frame."""

    def __init__(self, lib, handle):
        self._lib, self._h = lib, handle

    def write(self, timestamp_us: int, data: bytes, priority: int = 128) -> None:
        if not self._h:
            return
        buf = (ctypes.c_uint8 * len(data)).from_buffer_copy(data) if data else None
        self._lib.clutch_moqt_frame_write(self._h, ctypes.c_uint64(timestamp_us),
                                          buf, len(data), ctypes.c_uint8(priority))

    def subscriber_count(self) -> int:
        return int(self._lib.clutch_moqt_frame_pub_subscriber_count(self._h)) if self._h else 0

    def close(self) -> None:
        if self._h:
            self._lib.clutch_moqt_frame_pub_close(self._h)
            self._h = None


class FrameSubscription:
    """A live frame subscription. Frames arrive on the on_frame(ts, priority,
    data) callback."""

    def __init__(self, lib, handle, cb_ref):
        self._lib, self._h, self._cb = lib, handle, cb_ref  # keep cb alive

    def close(self) -> None:
        if self._h:
            self._lib.clutch_moqt_frame_sub_close(self._h)
            self._h = None


class MoqtClient:
    """A MoQT session against the relay. Track publish/subscribe are
    capability-aware (publish stamps the track's routing intent)."""

    def __init__(self, lib, handle, state_cb_ref):
        self._lib, self._h, self._state_cb = lib, handle, state_cb_ref

    @classmethod
    def connect(cls, url: str, auth_token: str = "",
                on_state: Optional[Callable[[int], None]] = None) -> "MoqtClient":
        lib = _lib_once()
        cb = _STATE_CB((lambda u, st: on_state(int(st))) if on_state else (lambda u, st: None))
        h = lib.clutch_moqt_connect(_b(url), _b(auth_token), cb, None)
        if not h:
            raise RuntimeError("clutch_moqt_connect failed")
        return cls(lib, h, cb)

    def publish_audio(self, namespace: str, name: str, capability: str = "",
                      sample_rate: int = 48000, channels: int = 1,
                      frame_ms: int = 20) -> AudioPublication:
        h = self._lib.clutch_moqt_publish_audio(
            self._h, _b(namespace), _b(name), _b(capability),
            ctypes.c_uint32(sample_rate), ctypes.c_uint8(channels), ctypes.c_uint16(frame_ms))
        if not h:
            raise RuntimeError("publish_audio failed")
        return AudioPublication(self._lib, h)

    def subscribe_audio(self, namespace: str, name: str,
                        on_frame: Callable[[int, bytes], None]) -> AudioSubscription:
        def _trampoline(user, ts_us, data_ptr, length):
            buf = ctypes.string_at(data_ptr, length) if (data_ptr and length) else b""
            on_frame(int(ts_us), buf)
        cb = _FRAME_CB(_trampoline)
        h = self._lib.clutch_moqt_subscribe_audio(self._h, _b(namespace), _b(name), cb, None)
        if not h:
            raise RuntimeError("subscribe_audio failed")
        return AudioSubscription(self._lib, h, cb)

    def publish_frame(self, namespace: str, name: str, capability: str = "",
                      schema_tag: str = "", default_priority: int = 128) -> FramePublication:
        h = self._lib.clutch_moqt_publish_frame(
            self._h, _b(namespace), _b(name), _b(capability), _b(schema_tag),
            ctypes.c_uint8(default_priority))
        if not h:
            raise RuntimeError("publish_frame failed")
        return FramePublication(self._lib, h)

    def subscribe_frame(self, namespace: str, name: str,
                        on_frame: Callable[[int, int, bytes], None]) -> FrameSubscription:
        def _trampoline(user, ts_us, priority, data_ptr, length):
            buf = ctypes.string_at(data_ptr, length) if (data_ptr and length) else b""
            on_frame(int(ts_us), int(priority), buf)
        cb = _FRAME_OBJ_CB(_trampoline)
        h = self._lib.clutch_moqt_subscribe_frame(self._h, _b(namespace), _b(name), cb, None)
        if not h:
            raise RuntimeError("subscribe_frame failed")
        return FrameSubscription(self._lib, h, cb)

    def close(self) -> None:
        if self._h:
            self._lib.clutch_moqt_client_close(self._h)
            self._h = None
