import asyncio
import os
import sys
import logging
import queue

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from clutchcall.client import ClutchCallClient

# Global configuration for telephony audio
CHUNK = 960  # Frame size (120ms at 8kHz, but 20ms is 160 frames. 960 bytes is common for some FFI buffers)
FORMAT = 8 # pyaudio.paInt16 value
CHANNELS = 1
RATE = 8000 # Typical telephony sample rate

class DummyStream:
    def __init__(self, mode='out'):
        self.mode = mode
        self.t = 0
    def read(self, frames, **kwargs):
        import math, struct
        wave = bytearray()
        for i in range(frames):
            sample = int(10000.0 * math.sin(2.0 * math.pi * 440.0 * self.t / 8000.0))
            wave.extend(struct.pack('<h', sample))
            self.t += 1
        return bytes(wave)
    def write(self, data):
        pass
    def stop_stream(self): pass
    def close(self): pass

def get_pyaudio_streams():
    try:
        import pyaudio
        p = pyaudio.PyAudio()
        in_stream = p.open(format=FORMAT, channels=CHANNELS, rate=RATE, input=True, frames_per_buffer=160)
        out_stream = p.open(format=FORMAT, channels=CHANNELS, rate=RATE, output=True, frames_per_buffer=160)
        return p, in_stream, out_stream
    except Exception as e:
        print(f"PyAudio hardware binding failed (running in WSL?): {e}. Falling back to Synthetic audio testing.")
        return None, DummyStream('in'), DummyStream('out')

async def main():
    print("Initiating ClutchCall bidirectional audio terminal...")
    
    with open("dummy_sa.json", "w") as f:
        f.write('{"client_email": "test_tenant", "private_key": "-----BEGIN PRIVATE KEY-----\\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQDKr7fNMuPW0Csi\\nIjXmOL9uAZ9utFIbneiQh6mIET8qb0bJ2Oe6VobLosqOUfC2POyNXUKczssdKBJn\\n6ouJSmddb2ykonhaSMh57B0m18iT85yhgxEjDCFQ1MXCJk0ovgsu3fXx8s4lg+3i\\neLBg7HjTufLIK8IzkxdySnUli/1B8d8sZn3FZ20uJ/GyqzscmB5drgxlRID9KAT7\\npIxiJ8Fv9F8u5v1FuseLLsEcBqt3dthEqJoV9DieQCX5cO4g603s34Z97kPaRG00\\n/HlcBJOu/gxk0Y2fsVGEjb4ewCN3sFHECxBHc1kHqlfrvsC41LrHdsC+QU0332xA\\n0ZtPTxjxAgMBAAECggEADbvyx5/4qgWIvrM0asln81N/KFhmA0K5JRNZl1jv1Pdz\\nA69ueFpvn4cu6Y4/h4xromKc5mo+1NbHydct+wYZ004pvaLYET9tp5FVIlzXKxYE\\nBDpmOV/o4VohnQ0iXQM6V8PTlsBTXAhrYKq5QkAT1JOnkg0hB4SilJcwsUl3Rkth\\n8BqA6kFYQW8S0XItZ99GWHDBlAb8DPMcQCMK0wg9B1e2M5t2vHf9eL7OARbK/eSb\\nMWwsWFgRIpkg2ZVm9U6GlFp6F7pazzfcddHldBb6wp2tCp3GUGX/wKv3+ytTPnPB\\nYpKNbc+Z/msf3dnKDjGPQyKMm1rL317bXNJ4nD4hSQKBgQD7MdXw3H0rg/2ZzXv8\\n1PCYtcW/LiPXEu2OQ/Sz9G5LAAAKpKDFtor7UEAtBw69zkZtPmW3UG4wRwmfSHDW\\nn9kWJII5pfF0yp5U3xZ3nvetSX50NFyi+C8pEp5sET/EhSrKzkys6MEtN6DHSZHl\\nq5FwSeqA/j9gCE3jqoRGqp7PHQKBgQDOkFMsuIPie7sqn0TPznXxYDVHPf5ZijLc\\nrjdHgAr9sPxDhBPLb9ygXdmZjgxSMiSMJogw0wv8XpOcDAUQ9XYHxRNI5IdcYM5D\\nvibEUQsVSWPAaPEmSiQUSaL1Xi4EjPyAHCFn+GZgdld3xRmnsvZJdfV9FQrM9hJe\\n0j7CbdLk5QKBgB07uIU2c867pqjelB5hfbqX9PKB4SPnjQPwfqruuGM8FcUnUZqQ\\n2u3SchWLa7jFJ8cQ6u+BicFOkx0ZZiBkK/R6vTkOSeJorjJ8X/X95x8gnXnSmjFR\\nJtPl2dAD1eL+CHPfvGanE8w6XBi1RChxZhSmVYc7j46SiNYFAy3iL2c1AoGAcHPF\\ntAznT38Ij9WRAohlUPiNSLGJLHm94sG9OmGMmjuluaPHmvLU60DsW1onfv/pQZsg\\nfWQHnGZoeYVZpLfcf7JcI0y2HCZfZCW6uRldrUL82RzIW431Qk4sNuQErVmLhLrL\\nvOxP36fNSli09MTKq4daE7RG4vn7Wj+fBv3+17kCgYBWGEVSdW9yGOsEvZsN+nTp\\nBQhdyvQjy6/lYMHeFrx1ga4V1E/XXmhyRRzk1lCg8AGRUfPyMqgo/tXOQSHoK6yp\\ntdIph/XuCmA8ZFGayFdGVrIL2nw9qs7cRGAUAPRnRNlgRzTzFc8WnB6vtItTfaDm\\nNhNocK9YfkydFdqQbB7Daw==\\n-----END PRIVATE KEY-----"}')
    
    client = ClutchCallClient(
        endpoint="quic://187.127.139.138:9090",
        service_account_path="dummy_sa.json"
    )

    p, in_stream, out_stream = get_pyaudio_streams()
    print("Mic and Speaker successfully bound natively.")

    active_calls = set()
    audio_queue = queue.Queue() # Thread-safe queue for UI/blocking writes
    
    f_out = open("captured_output_wsl.alaw", "wb")
    
    def audio_cb(datagram_data):
        try:
            payload = client.deserialize_audio_frame(datagram_data)
            if payload:
                # Assuming PCM is 16-bit. If the payload is G.711 PCMU/A-law natively encoded, we convert:
                try:
                    # In some FFI configurations, it returns raw G711 U-LAW
                    # To test playback, we try uncompressing. If it's already PCM this will generate static, but let's assume raw G711 per push_audio bounds:
                    import audioop
                    pcm = audioop.ulaw2lin(payload, 2)
                    out_stream.write(pcm)
                except Exception:
                    # Fallback to direct raw PCM writes
                    out_stream.write(payload)
                
                # Record to file simultaneously for WSL validation!
                f_out.write(payload)
                f_out.flush()
                    
        except Exception as e:
            pass

    def event_cb(datagram_data):
        try:
            call_sid, status = client.deserialize_call_event(datagram_data)
            print(f"--> [EVENT] call_sid: {call_sid} | status: {status}")
            if status not in ["COMPLETED", "FAILED", "BUSY", "NO_ANSWER"]:
                active_calls.add(call_sid)
            else:
                active_calls.discard(call_sid)
        except Exception:
            pass
            
    client.on_audio_frame = audio_cb
    client.on_call_event = event_cb

    try:
        async with client.connect_async():
            print("Successfully connected over QUIC!")
            
            await client.dial(
                to="sip:+1600258824@2x99i6f70f2.sip.livekit.cloud", 
                trunk_id="default",
                call_from="+18005551234",
                max_duration_ms=60000,
                client_id=client.client_id
            )
            
            print("Dialing... Starting mic thread.")
            
            # We run a tiny background worker to read mic to prevent starving the asyncio loop natively
            def mic_worker():
                print("Mic active: Send audio...")
                while True:
                    try:
                        # 160 frames at 8kHz is 20ms
                        pcm_frame = in_stream.read(160, exception_on_overflow=False)
                        # The FFI serialize_audio_frame implicitly wraps inside `client.push_audio`
                        # We compress to PCMU G.711 dynamically if needed
                        import audioop
                        ulaw_frame = audioop.lin2ulaw(pcm_frame, 2)
                        
                        # Just push to async loop safely
                        for sid in list(active_calls):
                            asyncio.run_coroutine_threadsafe(client.push_audio(sid, ulaw_frame), asyncio.get_running_loop())
                    except Exception as e:
                        pass
                        
            import threading
            threading.Thread(target=mic_worker, daemon=True).start()

            await asyncio.sleep(120) # Or until interrupted

            for sid in list(active_calls):
                await client.terminate(sid)
                
    except KeyboardInterrupt:
        print("Terminating via keyboard...")
    finally:
        in_stream.stop_stream()
        in_stream.close()
        out_stream.stop_stream()
        out_stream.close()
        if p:
            p.terminate()

if __name__ == "__main__":
    asyncio.run(main())
