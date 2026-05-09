import asyncio
import os
import ssl
from typing import Optional, cast

from aioquic.asyncio.client import connect
from aioquic.asyncio.protocol import QuicConnectionProtocol
from aioquic.h3.connection import H3_ALPN, H3Connection
from aioquic.h3.events import DataReceived, HeadersReceived
from aioquic.quic.configuration import QuicConfiguration

class HttpClient(QuicConnectionProtocol):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._http = H3Connection(self._quic)
        self._done = asyncio.Event()

    def http_request(self, host: str, path: str):
        stream_id = self._quic.get_next_available_stream_id()
        headers = [
            (b":method", b"GET"),
            (b":scheme", b"https"),
            (b":authority", host.encode()),
            (b":path", path.encode()),
        ]
        self._http.send_headers(stream_id, headers, end_stream=True)
        self.transmit()
        return stream_id

    def quic_event_received(self, event):
        for http_event in self._http.handle_event(event):
            if isinstance(http_event, HeadersReceived):
                print("Headers:", http_event.headers)
            elif isinstance(http_event, DataReceived):
                print("Data:", http_event.data.decode())
                if http_event.stream_ended:
                    self._done.set()

    async def wait_done(self):
        await self._done.wait()

async def main():
    configuration = QuicConfiguration(
        is_client=True,
        alpn_protocols=H3_ALPN,
        verify_mode=ssl.CERT_NONE,
    )

    async with connect(
        "dev.0.clutchcall.dev",
        443,
        configuration=configuration,
        create_protocol=HttpClient,
    ) as client:
        client = cast(HttpClient, client)
        print("Connected HTTP/3!")
        client.http_request("dev.0.clutchcall.dev", "/ping")
        await asyncio.wait_for(client.wait_done(), timeout=5.0)

if __name__ == "__main__":
    asyncio.run(main())
