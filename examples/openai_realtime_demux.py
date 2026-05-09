import asyncio
import base64
import json
import os
import websockets
from clutchcall.client import ClutchCallClient, MethodID

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "your-openai-api-key")

async def run_openai_demux_agent():
    credentials_path = os.environ.get("CLUTCHCALL_CREDENTIALS", "service_account.json")
    client = ClutchCallClient("quic://127.0.0.1:9090", credentials_path)
    await client.connect_async()

    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "OpenAI-Beta": "realtime=v1"
    }

    url = "wss://api.openai.com/v1/realtime?model=gpt-4o-realtime-preview-2024-10-01"
    async with websockets.connect(url, extra_headers=headers) as ai_ws:
        
        session_update = {
            "type": "session.update",
            "session": {
                "modalities": ["audio", "text"],
                "instructions": "You are a helpful telecom agent.",
                "voice": "alloy",
                "input_audio_format": "pcm16",
                "output_audio_format": "pcm16",
            }
        }
        await ai_ws.send(json.dumps(session_update))

        call_sid = "simulated_call_id"
        await client.dial(to="+15550000000", trunk_id="trunk_ai")

        async def route_clutchcall_to_openai():
            while True:
                # Simulated incoming frame
                method_id = MethodID.AUDIO_FRAME
                payload_bytes = b"mock"

                if method_id == MethodID.AUDIO_FRAME:
                    pcm_raw = client.deserialize_audio_frame(payload_bytes)
                    
                    b64_audio = base64.b64encode(pcm_raw).decode('utf-8')
                    await ai_ws.send(json.dumps({
                        "type": "input_audio_buffer.append",
                        "audio": b64_audio
                    }))
                elif method_id == MethodID.STREAM_EVENTS:
                    sid, status = client.deserialize_call_event(payload_bytes)
                    print(f"Call Event: {sid} {status}")
                await asyncio.sleep(0.1)

        async def route_openai_to_clutchcall():
            seq = 0
            async for message in ai_ws:
                event = json.loads(message)
                if event.get("type") == "response.audio.delta":
                    raw_pcm = base64.b64decode(event["delta"])
                    
                    packet = client.serialize_audio_frame(call_sid, raw_pcm, "PCMU", seq, False)
                    seq += 1

        await asyncio.gather(
            route_clutchcall_to_openai(),
            route_openai_to_clutchcall()
        )

if __name__ == "__main__":
    asyncio.run(run_openai_demux_agent())
