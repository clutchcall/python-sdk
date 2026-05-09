# ClutchCall Python SDK

The official Python wrapper for ClutchCall. Async-first; built on `aioquic` for
ALPN-QUIC transport and `PyJWT` for zero-trust auth, with a native FFI core for
audio processing.

## Installation

```bash
pip install .
```

## Quick start

Point `CLUTCHCALL_CREDENTIALS` at your service-account JSON, then:

```python
import asyncio
from clutchcall.client import ClutchCallClient
from clutchcall.media  import ClutchCallAudioStream

async def main():
    client = ClutchCallClient("pbx.clutchcall.com:443")

    # Originate an outbound call against an external trunk.
    response = await client.originate(
        to="+1234567890",
        ai_wss="wss://my-chatbot.com/media",
    )

    # Multiplex the raw audio stream.
    stream = ClutchCallAudioStream()
    await stream.connect("wss://pbx.clutchcall.com/media/session_789")
    async for pcm_chunk in stream.receive_audio_loop():
        # 16 kHz PCM, ready for your voice API.
        ...

if __name__ == "__main__":
    asyncio.run(main())
```

## Native core

The FFI core (`libclutchcall_core_ffi.{so,dylib,dll}`) is loaded at runtime.
Set `CLUTCHCALL_LIB_PATH` if it isn't on the default loader path; see the
[`core-sdk`](https://github.com/clutchcall/core-sdk) repo for build details.
