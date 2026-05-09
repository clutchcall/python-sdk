import ctypes
import os
import struct
import asyncio
from dataclasses import dataclass
from typing import Optional, Tuple
from aioquic.asyncio.client import connect
import ctypes.util
from aioquic.quic.configuration import QuicConfiguration
from .auth import ServiceAccountAuthenticator
from .method_id import MethodID, DialplanAction
import ssl
class ClutchCallBuffer(ctypes.Structure):
    _fields_ = [("data", ctypes.c_void_p), ("length", ctypes.c_size_t)]

class C_AudioFrame(ctypes.Structure):
    _fields_ = [
        ("call_sid", ctypes.c_char_p),
        ("payload", ctypes.c_char_p),
        ("codec", ctypes.c_char_p),
        ("sequence_number", ctypes.c_uint64),
        ("end_of_stream", ctypes.c_bool)
    ]

class C_CallEvent(ctypes.Structure):
    # Layout MUST match clutchcall-sdk/core/ffi_bridge.cc::C_CallEvent.
    _fields_ = [
        ("call_sid",            ctypes.c_char_p),
        ("event_type",          ctypes.c_int32),
        ("status",              ctypes.c_char_p),
        ("start_timestamp_ms",  ctypes.c_int64),
        ("q850_cause",          ctypes.c_int32),
        ("recording_url",       ctypes.c_char_p),
        ("duration_seconds",    ctypes.c_int32),
        ("answer_timestamp_ms", ctypes.c_int64),
        ("end_timestamp_ms",    ctypes.c_int64),
        ("packets_sent",        ctypes.c_uint32),
        ("packets_received",    ctypes.c_uint32),
        ("packets_lost",        ctypes.c_uint32),
        ("bytes_sent",          ctypes.c_uint64),
        ("jitter_ms",           ctypes.c_double),
        ("estimated_mos",       ctypes.c_double),
        ("trunk_id",            ctypes.c_char_p),
        ("tenant_id",           ctypes.c_char_p),
        ("codec",               ctypes.c_char_p),
        ("timestamp_ms",        ctypes.c_int64),
        ("client_id",           ctypes.c_char_p),
    ]


@dataclass(frozen=True)
class CallEvent:
    """Decoded CallEvent envelope. Returned by `parse_call_event()`.

    Status / event_type sets:
      - CHANNEL_CREATE / CHANNEL_ANSWER / CHANNEL_HANGUP_COMPLETE
      - CHANNEL_HOLD / CHANNEL_RESUME
      - RECORDING_READY (carries recording_url + duration_seconds)
    """
    call_sid:            str
    event_type:          int
    status:              str
    start_timestamp_ms:  int
    q850_cause:          int       # populated on CHANNEL_HANGUP_COMPLETE
    recording_url:       str       # populated on RECORDING_READY
    duration_seconds:    int       # populated on RECORDING_READY
    answer_timestamp_ms: int
    end_timestamp_ms:    int
    packets_sent:        int
    packets_received:    int
    packets_lost:        int
    bytes_sent:          int
    jitter_ms:           float
    estimated_mos:       float     # ITU-T R-factor → MOS conversion
    trunk_id:            str
    tenant_id:           str
    codec:               str       # negotiated wire codec, e.g. "PCMU"
    timestamp_ms:        int       # event-emission time, distinct from leg start/end
    client_id:           str

from aioquic.asyncio.protocol import QuicConnectionProtocol

class ClutchCallProtocol(QuicConnectionProtocol):
    def quic_event_received(self, event):
        from aioquic.quic.events import StreamDataReceived
        if isinstance(event, StreamDataReceived):
            if event.stream_id % 4 == 3: # Server-initiated unidirectional stream
                if not hasattr(self, "_stream_buffers"):
                    self._stream_buffers = {}
                buf = self._stream_buffers.get(event.stream_id, bytearray())
                buf += event.data
                
                while len(buf) >= 8:
                    import struct
                    total_len = struct.unpack('<I', buf[:4])[0]
                    if len(buf) >= 4 + total_len:
                        dg_id = struct.unpack('<I', buf[4:8])[0]
                        payload_data = buf[8:4+total_len]
                        
                        if dg_id == MethodID.AUDIO_FRAME or dg_id == 9999:  # Audio Datagram
                            if hasattr(self, "client_ref") and getattr(self.client_ref, "on_audio_frame", None):
                                self.client_ref.on_audio_frame(bytes(payload_data))
                        elif dg_id == MethodID.STREAM_EVENTS or dg_id == 10000:  # Event Datagram
                            if hasattr(self, "client_ref") and getattr(self.client_ref, "on_call_event", None):
                                self.client_ref.on_call_event(bytes(payload_data))
                                
                        buf = buf[4+total_len:]
                    else:
                        break
                        
                self._stream_buffers[event.stream_id] = buf
                return # Intercept natively, do not delegate to asyncio.protocol
        
        super().quic_event_received(event)

class ClutchCallClient:
    def __init__(self, endpoint: str, service_account_path: str, lib_path: Optional[str] = None):
        self.endpoint = endpoint
        self.auth = ServiceAccountAuthenticator(service_account_path)
        self.tenant_id = self.auth._iss
        self.on_audio_frame = None
        self.on_call_event = None
        import uuid
        self.client_id = str(uuid.uuid4())

        if lib_path is None:
            lib_path = os.environ.get("CLUTCHCALL_LIB_PATH")
            if lib_path is None:
                lib_path = ctypes.util.find_library("clutchcall_core_ffi")
            if lib_path is None:
                packaged_path = os.path.join(os.path.dirname(__file__), "clutchcall_core_ffi.so")
                bazel_path = os.path.join(os.path.dirname(__file__), "..", "..", "bazel-bin", "core", "clutchcall_core_ffi.so")
                if os.path.exists(packaged_path):
                    lib_path = packaged_path
                elif os.path.exists(bazel_path):
                    lib_path = bazel_path
                else:
                    raise FileNotFoundError("Could not find ClutchCall FFI Core Library natively. Set CLUTCHCALL_LIB_PATH.")

        self.lib = ctypes.CDLL(lib_path)
        
        self.lib.clutchcall_free_buffer.argtypes = [ClutchCallBuffer]
        self.lib.clutchcall_rpc_originate_request.argtypes = [
            ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_int32, ctypes.c_char_p, ctypes.c_int32, ctypes.c_char_p, ctypes.c_bool, ctypes.c_int32, ctypes.c_char_p
        ]
        self.lib.clutchcall_rpc_originate_request.restype = ClutchCallBuffer
        
        self.lib.clutchcall_rpc_terminate_request.argtypes = [ctypes.c_char_p]
        self.lib.clutchcall_rpc_terminate_request.restype = ClutchCallBuffer
        
        self.lib.clutchcall_rpc_barge_request.argtypes = [ctypes.c_char_p]
        self.lib.clutchcall_rpc_barge_request.restype = ClutchCallBuffer
        
        self.lib.clutchcall_rpc_event_stream_request.argtypes = [ctypes.c_char_p]
        self.lib.clutchcall_rpc_event_stream_request.restype = ClutchCallBuffer
        
        self.lib.clutchcall_rpc_bulk_request.argtypes = [
            ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_int32, ctypes.c_int32, ctypes.c_char_p, ctypes.c_int32, ctypes.c_int32, ctypes.c_char_p, ctypes.c_bool, ctypes.c_int32, ctypes.c_char_p
        ]
        self.lib.clutchcall_rpc_bulk_request.restype = ClutchCallBuffer
        
        self.lib.clutchcall_deserialize_audio_frame.argtypes = [ctypes.c_char_p, ctypes.c_size_t]
        self.lib.clutchcall_deserialize_audio_frame.restype = C_AudioFrame
        
        self.lib.clutchcall_deserialize_call_event.argtypes = [ctypes.c_char_p, ctypes.c_size_t]
        self.lib.clutchcall_deserialize_call_event.restype = C_CallEvent
        
        self.lib.clutchcall_serialize_audio_frame.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_uint64, ctypes.c_bool]
        self.lib.clutchcall_serialize_audio_frame.restype = ClutchCallBuffer

        # Inbound-routing + admin RPCs. The C-FFI symbols are exported by
        # core/ffi_bridge.cc and already wired in the Go SDK; mirror them here
        # so Python and Go are on parity.
        self.lib.clutchcall_rpc_set_inbound_routing_request.argtypes = [
            ctypes.c_char_p,    # trunk_id
            ctypes.c_int32,     # rule (InboundRule enum: 1=AI, 2=WEBHOOK, 3=PLAYBACK)
            ctypes.c_char_p,    # audio_url
            ctypes.c_char_p,    # webhook_url
            ctypes.c_char_p,    # ai_websocket_url
            ctypes.c_char_p,    # ai_quic_url
        ]
        self.lib.clutchcall_rpc_set_inbound_routing_request.restype = ClutchCallBuffer

        self.lib.clutchcall_rpc_get_incoming_calls_request.argtypes = [ctypes.c_char_p]
        self.lib.clutchcall_rpc_get_incoming_calls_request.restype = ClutchCallBuffer

        self.lib.clutchcall_rpc_answer_incoming_call_request.argtypes = [
            ctypes.c_char_p,    # call_sid
            ctypes.c_char_p,    # ai_websocket_url
            ctypes.c_char_p,    # ai_quic_url
        ]
        self.lib.clutchcall_rpc_answer_incoming_call_request.restype = ClutchCallBuffer

        self.lib.clutchcall_rpc_abort_bulk_request.argtypes = [ctypes.c_char_p]
        self.lib.clutchcall_rpc_abort_bulk_request.restype = ClutchCallBuffer

        # Bucket admin (queue inspection + actions). Mirrors the Go SDK's
        # GetBucketCalls / ExecuteBucketAction.
        self.lib.clutchcall_rpc_bucket_request.argtypes = [ctypes.c_char_p]
        self.lib.clutchcall_rpc_bucket_request.restype = ClutchCallBuffer

        self.lib.clutchcall_rpc_bucket_action_request.argtypes = [
            ctypes.c_char_p,    # bucket_id
            ctypes.c_int32,     # action enum (BucketAction)
        ]
        self.lib.clutchcall_rpc_bucket_action_request.restype = ClutchCallBuffer

        # Mid-call dialplan execution. Lets you trigger a DialplanAction
        # against an active call_sid (e.g. PARK an active leg, switch a
        # call from PLAYBACK → AI_BIDIRECTIONAL_STREAM mid-flight). Once
        # MUTE / TRANSFER are added to the DialplanAction enum, those
        # become callable through this same RPC.
        self.lib.clutchcall_rpc_execute_dialplan_request.argtypes = [
            ctypes.c_char_p,    # call_sid
            ctypes.c_int32,     # action (DialplanAction enum)
            ctypes.c_char_p,    # app_args (e.g. agent_id for AI, wav path for PLAYBACK)
        ]
        self.lib.clutchcall_rpc_execute_dialplan_request.restype = ClutchCallBuffer

        self._quic_protocol = None

    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def connect_async(self):
        host, port = self.endpoint.replace("quic://", "").split(":")
        configuration = QuicConfiguration(is_client=True, alpn_protocols=["h3"])
        configuration.verify_mode = ssl.CERT_REQUIRED
        configuration.server_name = "dev.0.clutchcall.dev"
        configuration.max_datagram_frame_size = 65536
        
        def create_protocol(*args, **kwargs):
            proto = ClutchCallProtocol(*args, **kwargs)
            proto.client_ref = self
            return proto

        async with connect(host, int(port), configuration=configuration, create_protocol=create_protocol) as protocol:
            self._quic_protocol = protocol
            await protocol.wait_connected()
            
            buf = getattr(self.lib, 'clutchcall_rpc_event_stream_request')(self.client_id.encode('utf-8'))
            await self._send_rpc(buf)
            
            yield
            self._quic_protocol = None

    async def push_audio(self, call_sid: str, pcm_data: bytes):
        if self._quic_protocol is None:
            raise RuntimeError("Must connect first")
            
        import struct
        import json
        
        # We can write the audio dynamically by just packing the dg_id = 9999 and raw buffer... wait, quic_server checks for AudioFrame serde deserialization which uses raw c-struct dumps or msgpack?
        # Let's use the FFI if we have it!
        # Do we have clutchcall_serialize_audio_frame in FFI?
        buffer_struct = getattr(self.lib, "clutchcall_serialize_audio_frame", None)
        if buffer_struct:
            buf = self.lib.clutchcall_serialize_audio_frame(call_sid.encode('utf-8'), pcm_data, len(pcm_data), b"PCMU", 0, False)
            payload_bytes = ctypes.string_at(buf.data, buf.length)
            self.lib.clutchcall_free_buffer(buf)
            
            packet = struct.pack('<II', len(payload_bytes) + 4, MethodID.AUDIO_FRAME) + payload_bytes
            if not hasattr(self, "_audio_stream_writer"):
                reader, self._audio_stream_writer = await self._quic_protocol.create_stream(is_unidirectional=True)
            self._audio_stream_writer.write(packet)
            # No EOF to keep the multiplexed native stream piping

    async def _send_rpc(self, buffer_struct):
        payload_bytes = ctypes.string_at(buffer_struct.data, buffer_struct.length)
        self.lib.clutchcall_free_buffer(buffer_struct)
        
        packet = payload_bytes
        if self._quic_protocol is None:
            raise RuntimeError("Must execute dial() within 'async with client.connect_async()' context.")
            
        reader, writer = await self._quic_protocol.create_stream(is_unidirectional=False)
        writer.write(packet)
        writer.write_eof()
        await writer.drain()

    async def dial(self, to: str, trunk_id: str, call_from: str = "", max_duration_ms: int = 0, default_app: int = 1, default_app_args: str = "", ai_websocket_url: str = "", ai_quic_url: str = "", auto_barge_in: bool = False, barge_in_patience_ms: int = 250, client_id: str = None):
        token = "dummy_small_token_avoids_mtu_fragmentation"
        target_client_id = client_id if client_id else self.client_id
        buffer_struct = self.lib.clutchcall_rpc_originate_request(
            trunk_id.encode('utf-8'), to.encode('utf-8'), call_from.encode('utf-8'), ai_websocket_url.encode('utf-8'), ai_quic_url.encode('utf-8'), b"test_tenant", max_duration_ms, b"", default_app, default_app_args.encode('utf-8'), auto_barge_in, barge_in_patience_ms, target_client_id.encode('utf-8')
        )
        await self._send_rpc(buffer_struct)

    async def originate_bulk(self, csv_url: str, trunk_id: str, calls_per_second: int, max_concurrent: int, campaign_id: str, default_app: int = 1, default_app_args: str = "", ai_websocket_url: str = "", ai_quic_url: str = "", auto_barge_in: bool = False, barge_in_patience_ms: int = 250):
        token = "dummy_small_token_avoids_mtu_fragmentation"
        buffer_struct = self.lib.clutchcall_rpc_bulk_request(
            csv_url.encode('utf-8'), trunk_id.encode('utf-8'), b"", b"", ai_websocket_url.encode('utf-8'), ai_quic_url.encode('utf-8'), b"test_tenant", 0, default_app, default_app_args.encode('utf-8'), calls_per_second, max_concurrent, campaign_id.encode('utf-8'), auto_barge_in, barge_in_patience_ms, self.client_id.encode('utf-8')
        )
        await self._send_rpc(buffer_struct)

    async def terminate(self, call_sid: str):
        buffer_struct = getattr(self.lib, 'clutchcall_rpc_terminate_request')(call_sid.encode('utf-8'))
        await self._send_rpc(buffer_struct)

    async def barge(self, call_sid: str):
        buffer_struct = getattr(self.lib, 'clutchcall_rpc_barge_request')(call_sid.encode('utf-8'))
        await self._send_rpc(buffer_struct)

    async def stream_events(self, client_id: str):
        buffer_struct = getattr(self.lib, 'clutchcall_rpc_event_stream_request')(client_id.encode('utf-8'))
        await self._send_rpc(buffer_struct)

    # ─── Inbound routing ───────────────────────────────────────────────
    # Mirrors the Go SDK's SetInboundRouting / GetIncomingCalls /
    # AnswerIncomingCall. The C-FFI symbols are provided by ffi_bridge.cc.

    async def set_inbound_routing(
        self,
        trunk_id: str,
        rule: int,
        audio_url: str = "",
        webhook_url: str = "",
        ai_websocket_url: str = "",
        ai_quic_url: str = "",
    ):
        """Configure inbound routing for a trunk.

        `rule` is the C++ InboundRule enum:
          1 = AI            — auto-bridge to ai_websocket_url / ai_quic_url
          2 = WEBHOOK       — POST events to webhook_url; client polls
                              GetIncomingCalls and calls AnswerIncomingCall
          3 = PLAYBACK      — play audio_url then hang up
        """
        buf = self.lib.clutchcall_rpc_set_inbound_routing_request(
            trunk_id.encode('utf-8'),
            int(rule),
            audio_url.encode('utf-8'),
            webhook_url.encode('utf-8'),
            ai_websocket_url.encode('utf-8'),
            ai_quic_url.encode('utf-8'),
        )
        await self._send_rpc(buf)

    async def get_incoming_calls(self, trunk_id: str):
        """Pull queued inbound calls for a trunk (rule=WEBHOOK only).

        Results arrive on the event stream rather than as an RPC return
        value — match by trunk_id and react in your on_call_event handler.
        """
        buf = self.lib.clutchcall_rpc_get_incoming_calls_request(trunk_id.encode('utf-8'))
        await self._send_rpc(buf)

    async def answer_incoming_call(
        self,
        call_sid: str,
        ai_websocket_url: str = "",
        ai_quic_url: str = "",
    ):
        """Accept a queued inbound call and bridge it to an AI session.

        For non-AI handling (human queue, IVR), set both URLs empty;
        the gateway accepts the leg and emits CHANNEL_ANSWER. Subsequent
        bridge / playback is up to the calling app.
        """
        buf = self.lib.clutchcall_rpc_answer_incoming_call_request(
            call_sid.encode('utf-8'),
            ai_websocket_url.encode('utf-8'),
            ai_quic_url.encode('utf-8'),
        )
        await self._send_rpc(buf)

    async def abort_bulk(self, campaign_id: str):
        """Abort an in-flight bulk-dial campaign by id.

        In-flight calls finish naturally; only pending rows are dropped.
        """
        buf = self.lib.clutchcall_rpc_abort_bulk_request(campaign_id.encode('utf-8'))
        await self._send_rpc(buf)

    # ─── Bucket / queue ────────────────────────────────────────────────

    async def get_bucket_calls(self, bucket_id: str):
        """Inspect the calls currently queued in a bucket. Results arrive
        on the event stream; surface them in your on_call_event handler."""
        buf = self.lib.clutchcall_rpc_bucket_request(bucket_id.encode('utf-8'))
        await self._send_rpc(buf)

    async def execute_bucket_action(self, bucket_id: str, action: int):
        """Trigger a bucket action (TAKE_NEXT, SKIP, etc.) by enum value."""
        buf = self.lib.clutchcall_rpc_bucket_action_request(
            bucket_id.encode('utf-8'), int(action),
        )
        await self._send_rpc(buf)

    # ─── Mid-call dialplan execution ───────────────────────────────────

    async def execute_dialplan(self, call_sid: str, action: int, app_args: str = ""):
        """Apply a DialplanAction to a call already in flight.

        `action` accepts a `DialplanAction` member or the raw int. The
        convenience wrappers below cover the common verbs.
        """
        buf = self.lib.clutchcall_rpc_execute_dialplan_request(
            call_sid.encode('utf-8'), int(action), app_args.encode('utf-8'),
        )
        await self._send_rpc(buf)

    # ─── Call-control verbs (sugar over execute_dialplan) ──────────────

    async def transfer(self, call_sid: str, destination: str):
        """RFC 3515 blind transfer. `destination` may be a SIP URI
        (`sip:user@host`) or an E.164 number (`+15551234567`); the engine
        wraps the number into the trunk's outbound URI. Surfaces as a
        REFER on the active leg; carrier behavior varies (Twilio honors;
        most US PSTN carriers ignore mid-call REFER)."""
        await self.execute_dialplan(call_sid, DialplanAction.TRANSFER, destination)

    async def mute(self, call_sid: str, *, on_wire: bool = False):
        """Mute the gateway-side TX buffer for this call (caller hears
        silence). If `on_wire=True`, also send a SIP recvonly re-INVITE
        so the carrier sees the mute. Some carriers don't honor recvonly
        and continue billing as a connected leg."""
        await self.execute_dialplan(
            call_sid, DialplanAction.MUTE, "wire" if on_wire else "",
        )

    async def unmute(self, call_sid: str, *, on_wire: bool = False):
        """Lift a previous `mute()`. Pass `on_wire=True` if the original
        mute was on-wire."""
        await self.execute_dialplan(
            call_sid, DialplanAction.UNMUTE, "wire" if on_wire else "",
        )

    async def hold(self, call_sid: str):
        """Place the call on hold (silence + optional MOH on the
        opposite leg, depending on trunk config)."""
        await self.execute_dialplan(call_sid, DialplanAction.HOLD, "")

    async def unhold(self, call_sid: str):
        """Resume a held call."""
        await self.execute_dialplan(call_sid, DialplanAction.UNHOLD, "")

    async def send_dtmf(
        self,
        call_sid: str,
        digit: str,
        *,
        mode: str = "rfc2833",
        duration_ms: int = 200,
    ):
        """Send a DTMF digit on the active leg.

        `digit` is one of `0-9 * #`. `mode` selects the wire format:
        - "rfc2833" (default): in-band telephone-event payload type
        - "info":              SIP INFO method
        - "inband":            audible tone in the RTP stream
        """
        if len(digit) != 1 or digit not in "0123456789*#":
            raise ValueError(f"invalid DTMF digit: {digit!r}")
        if mode not in ("rfc2833", "info", "inband"):
            raise ValueError(f"invalid DTMF mode: {mode!r}")
        await self.execute_dialplan(
            call_sid,
            DialplanAction.SEND_DTMF,
            f"{digit}:{mode}:{duration_ms}",
        )

    def deserialize_audio_frame(self, payload_bytes: bytes) -> bytes:
        frame = self.lib.clutchcall_deserialize_audio_frame(payload_bytes, len(payload_bytes))
        return ctypes.string_at(frame.payload)

    def deserialize_call_event(self, payload_bytes: bytes) -> Tuple[str, str]:
        """Backward-compatible: return just (call_sid, status_string).

        Prefer parse_call_event() below for full event metadata.
        """
        evt = self.lib.clutchcall_deserialize_call_event(payload_bytes, len(payload_bytes))
        return ctypes.string_at(evt.call_sid).decode('utf-8'), ctypes.string_at(evt.status).decode('utf-8')

    def parse_call_event(self, payload_bytes: bytes) -> "CallEvent":
        """Decode a serialized CallEvent envelope into a rich CallEvent dataclass.

        Surfaces every field the C struct provides, including timing
        (answer/end timestamps), RTP stats (packets sent/received/lost,
        bytes), quality (jitter_ms, estimated_mos), and call context
        (trunk_id, tenant_id, codec, client_id).
        """
        evt = self.lib.clutchcall_deserialize_call_event(payload_bytes, len(payload_bytes))
        def _opt_str(p):
            return ctypes.string_at(p).decode('utf-8') if p else ""
        return CallEvent(
            call_sid            = _opt_str(evt.call_sid),
            event_type          = int(evt.event_type),
            status              = _opt_str(evt.status),
            start_timestamp_ms  = int(evt.start_timestamp_ms),
            q850_cause          = int(evt.q850_cause),
            recording_url       = _opt_str(evt.recording_url),
            duration_seconds    = int(evt.duration_seconds),
            answer_timestamp_ms = int(evt.answer_timestamp_ms),
            end_timestamp_ms    = int(evt.end_timestamp_ms),
            packets_sent        = int(evt.packets_sent),
            packets_received    = int(evt.packets_received),
            packets_lost        = int(evt.packets_lost),
            bytes_sent          = int(evt.bytes_sent),
            jitter_ms           = float(evt.jitter_ms),
            estimated_mos       = float(evt.estimated_mos),
            trunk_id            = _opt_str(evt.trunk_id),
            tenant_id           = _opt_str(evt.tenant_id),
            codec               = _opt_str(evt.codec),
            timestamp_ms        = int(evt.timestamp_ms),
            client_id           = _opt_str(evt.client_id),
        )

    def serialize_audio_frame(self, call_sid: str, pcm_raw: bytes, codec: str, sequence_number: int, end_of_stream: bool) -> bytes:
        import struct
        buf = self.lib.clutchcall_serialize_audio_frame(
            call_sid.encode('utf-8'), pcm_raw, codec.encode('utf-8'), sequence_number, end_of_stream
        )
        payload = ctypes.string_at(buf.data, buf.length)
        self.lib.clutchcall_free_buffer(buf)
        return struct.pack('<II', len(payload) + 4, MethodID.AUDIO_FRAME) + payload
