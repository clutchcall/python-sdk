import asyncio
import os
import sys
import ssl
import logging
from typing import Optional, cast
import struct
import socket

logging.basicConfig(level=logging.INFO)

from aioquic.asyncio.client import connect
from aioquic.asyncio.protocol import QuicConnectionProtocol
from aioquic.h3.connection import H3_ALPN, H3Connection
from aioquic.h3.events import DataReceived, HeadersReceived, WebTransportStreamDataReceived
from aioquic.quic.events import StreamDataReceived
from aioquic.quic.configuration import QuicConfiguration

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from clutchcall.client import ClutchCallClient
import ctypes

class WebTransportClient(QuicConnectionProtocol):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._http = H3Connection(self._quic, enable_webtransport=True)
        self._done = asyncio.Event()
        self._session_id: Optional[int] = None
        self.clutchcall_client: Optional[ClutchCallClient] = None
        self.f_out = open("captured_output_wt.alaw", "wb")
        self.events_out = open("events_wt.txt", "a")

    def connect_wt(self, host: str, path: str):
        stream_id = self._quic.get_next_available_stream_id()
        headers = [
            (b":method", b"CONNECT"),
            (b":protocol", b"webtransport"),
            (b":scheme", b"https"),
            (b":authority", host.encode()),
            (b":path", path.encode()),
            (b"origin", b"http://localhost"),
        ]
        self._http.send_headers(stream_id, headers, end_stream=False)
        self.transmit()
        self._session_id = stream_id
        return stream_id

    def send_rpc(self, buf_struct):
        payload_bytes = ctypes.string_at(buf_struct.data, buf_struct.length)
        self.clutchcall_client.lib.clutchcall_free_buffer(buf_struct)
        
        wt_stream_id = self._http.create_webtransport_stream(self._session_id, is_unidirectional=False)
        if not hasattr(self, "_wt_bidi_streams"): self._wt_bidi_streams = set()
        self._wt_bidi_streams.add(wt_stream_id)
        self._http._quic.send_stream_data(wt_stream_id, payload_bytes, end_stream=True)
        self.transmit()
        print(f"Sent RPC on WT Stream ID: {wt_stream_id}")

    def send_originate(self):
        print("Dispatching originate request over WT...")
        buf_struct = self.clutchcall_client.lib.clutchcall_rpc_originate_request(
            b"default", 
            b"sip:+1600258824@2x99i6f70f2.sip.livekit.cloud", 
            b"+18005551234", 
            b"", 
            b"", 
            b"test_tenant", 
            60000, 
            b"", 
            1, 
            b"", 
            False, 
            250, 
            self.clutchcall_client.client_id.encode('utf-8')
        )
        self.send_rpc(buf_struct)

    def send_stream_events_request(self):
        print("Dispatching event stream request over WT...")
        buf_struct = getattr(self.clutchcall_client.lib, 'clutchcall_rpc_event_stream_request')(
            self.clutchcall_client.client_id.encode('utf-8')
        )
        self.send_rpc(buf_struct)

    def quic_event_received(self, event):
        if isinstance(event, StreamDataReceived) and hasattr(self, "_wt_bidi_streams") and event.stream_id in self._wt_bidi_streams:
            self._handle_wt_data(event.stream_id, event.data)
            return

        for http_event in self._http.handle_event(event):
            if isinstance(http_event, HeadersReceived):
                print("Headers:", http_event.headers)
                for k, v in http_event.headers:
                    if k == b":status" and v == b"200":
                        print("WebTransport session established flawlessly")
                        self.send_stream_events_request()
                        self.send_originate()
                        
            elif isinstance(http_event, WebTransportStreamDataReceived):
                self._handle_wt_data(http_event.stream_id, http_event.data)
                
            elif isinstance(http_event, DataReceived):
                pass
        
    def _handle_wt_data(self, stream_id, data):
        if not hasattr(self, "_stream_buffers"):
            self._stream_buffers = {}
        buf = self._stream_buffers.get(stream_id, bytearray())
        buf += data
        
        while len(buf) >= 8:
            total_len = struct.unpack('<I', buf[:4])[0]
            if len(buf) >= 4 + total_len:
                dg_id = struct.unpack('<I', buf[4:8])[0]
                payload_data = buf[8:4+total_len]
                
                from clutchcall.method_id import MethodID
                if dg_id == MethodID.AUDIO_FRAME or dg_id == 2991054320:  # Audio Datagram
                    pcm = self.clutchcall_client.deserialize_audio_frame(bytes(payload_data))
                    if pcm:
                        self.f_out.write(pcm)
                        self.f_out.flush()
                elif dg_id == MethodID.STREAM_EVENTS or dg_id == 959835745:  # Event Datagram
                    call_sid, status = self.clutchcall_client.deserialize_call_event(bytes(payload_data))
                    print(f"--> [EVENT] call_sid: {call_sid} | status: {status}")
                    self.events_out.write(f"{call_sid},{status}\n")
                    self.events_out.flush()
                    if status in ["COMPLETED", "FAILED", "BUSY", "NO_ANSWER"]:
                        asyncio.get_event_loop().call_later(2, self._done.set)
                        
                buf = buf[4+total_len:]
            else:
                break
                
        self._stream_buffers[stream_id] = buf
        
        # Don't call super() which delegates to asyncio StreamWriter creation because WT streams aren't handled well by aioquic core
        # super().quic_event_received(event)

    async def wait_done(self):
        await self._done.wait()

async def main():
    with open("dummy_sa.json", "w") as f:
        f.write('{"client_email": "test_tenant", "private_key": "-----BEGIN PRIVATE KEY-----\\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQDKr7fNMuPW0Csi\\nIjXmOL9uAZ9utFIbneiQh6mIET8qb0bJ2Oe6VobLosqOUfC2POyNXUKczssdKBJn\\n6ouJSmddb2ykonhaSMh57B0m18iT85yhgxEjDCFQ1MXCJk0ovgsu3fXx8s4lg+3i\\neLBg7HjTufLIK8IzkxdySnUli/1B8d8sZn3FZ20uJ/GyqzscmB5drgxlRID9KAT7\\npIxiJ8Fv9F8u5v1FuseLLsEcBqt3dthEqJoV9DieQCX5cO4g603s34Z97kPaRG00\\n/HlcBJOu/gxk0Y2fsVGEjb4ewCN3sFHECxBHc1kHqlfrvsC41LrHdsC+QU0332xA\\n0ZtPTxjxAgMBAAECggEADbvyx5/4qgWIvrM0asln81N/KFhmA0K5JRNZl1jv1Pdz\\nA69ueFpvn4cu6Y4/h4xromKc5mo+1NbHydct+wYZ004pvaLYET9tp5FVIlzXKxYE\\nBDpmOV/o4VohnQ0iXQM6V8PTlsBTXAhrYKq5QkAT1JOnkg0hB4SilJcwsUl3Rkth\\n8BqA6kFYQW8S0XItZ99GWHDBlAb8DPMcQCMK0wg9B1e2M5t2vHf9eL7OARbK/eSb\\nMWwsWFgRIpkg2ZVm9U6GlFp6F7pazzfcddHldBb6wp2tCp3GUGX/wKv3+ytTPnPB\\nYpKNbc+Z/msf3dnKDjGPQyKMm1rL317bXNJ4nD4hSQKBgQD7MdXw3H0rg/2ZzXv8\\n1PCYtcW/LiPXEu2OQ/Sz9G5LAAAKpKDFtor7UEAtBw69zkZtPmW3UG4wRwmfSHDW\\nn9kWJII5pfF0yp5U3xZ3nvetSX50NFyi+C8pEp5sET/EhSrKzkys6MEtN6DHSZHl\\nq5FwSeqA/j9gCE3jqoRGqp7PHQKBgQDOkFMsuIPie7sqn0TPznXxYDVHPf5ZijLc\\nrjdHgAr9sPxDhBPLb9ygXdmZjgxSMiSMJogw0wv8XpOcDAUQ9XYHxRNI5IdcYM5D\\nvibEUQsVSWPAaPEmSiQUSaL1Xi4EjPyAHCFn+GZgdld3xRmnsvZJdfV9FQrM9hJe\\n0j7CbdLk5QKBgB07uIU2c867pqjelB5hfbqX9PKB4SPnjQPwfqruuGM8FcUnUZqQ\\n2u3SchWLa7jFJ8cQ6u+BicFOkx0ZZiBkK/R6vTkOSeJorjJ8X/X95x8gnXnSmjFR\\nJtPl2dAD1eL+CHPfvGanE8w6XBi1RChxZhSmVYc7j46SiNYFAy3iL2c1AoGAcHPF\\ntAznT38Ij9WRAohlUPiNSLGJLHm94sG9OmGMmjuluaPHmvLU60DsW1onfv/pQZsg\\nfWQHnGZoeYVZpLfcf7JcI0y2HCZfZCW6uRldrUL82RzIW431Qk4sNuQErVmLhLrL\\nvOxP36fNSli09MTKq4daE7RG4vn7Wj+fBv3+17kCgYBWGEVSdW9yGOsEvZsN+nTp\\nBQhdyvQjy6/lYMHeFrx1ga4V1E/XXmhyRRzk1lCg8AGRUfPyMqgo/tXOQSHoK6yp\\ntdIph/XuCmA8ZFGayFdGVrIL2nw9qs7cRGAUAPRnRNlgRzTzFc8WnB6vtItTfaDm\\nNhNocK9YfkydFdqQbB7Daw==\\n-----END PRIVATE KEY-----"}')

    client_sdk = ClutchCallClient("quic://dev.0.clutchcall.dev:443", "dummy_sa.json")

    configuration = QuicConfiguration(
        is_client=True,
        alpn_protocols=H3_ALPN,
        verify_mode=ssl.CERT_NONE,
        max_datagram_frame_size=65536,
        server_name="dev.0.clutchcall.dev"
    )

    host = "dev.0.clutchcall.dev"
    port = 443
    ip = socket.gethostbyname(host)
    print(f"Resolved {host} to {ip}")

    def create_protocol(*args, **kwargs):
        proto = WebTransportClient(*args, **kwargs)
        proto.clutchcall_client = client_sdk
        return proto

    async with connect(
        ip,
        port,
        configuration=configuration,
        create_protocol=create_protocol,
    ) as client_proto:
        client_proto = cast(WebTransportClient, client_proto)
        print(f"Connected QUIC to {host}:{port}")
        client_proto.connect_wt(host, "/")
        
        await client_proto.wait_done()

if __name__ == "__main__":
    asyncio.run(main())
