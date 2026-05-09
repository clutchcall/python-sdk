# ClutchCall Python SDK The official Python wrapper for ClutchCall, utilizing the standard `grpcio` and `websockets` libraries for lightning-fast voice AI integration. ## Installation Install via pip locally: ```bash
pip install .
``` ## Quick Start Ensure your credentials path is loaded:
`export CLUTCHCALL_CREDENTIALS=/path/to/credentials.json` ```python
import asyncio
from clutchcall.client import ClutchCallClient
from clutchcall.media import ClutchCallAudioStream async def main(): # Automatically manages PyJWT generation and gRPC Auth Metadata client = ClutchCallClient("pbx.clutchcall.com:443") # Initiate an external trunk call response = await client.originate( to="+1234567890", ai_wss="wss://my-chatbot.com/media" ) # Multiplex raw audio stream = ClutchCallAudioStream() await stream.connect("wss://pbx.clutchcall.com/media/session_789") async for pcm_chunk in stream.receive_audio_loop(): # Stream 16kHz audio out to your Voice API! pass if __name__ == "__main__": asyncio.run(main())
```
