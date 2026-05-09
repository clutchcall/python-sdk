import asyncio
import os
import sys
import logging

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s %(name)s %(message)s")

# Adjust path to import clutchcall sdk locally
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from clutchcall.client import ClutchCallClient

async def main():
    print("Initiating clutchcall dialing sequence via QUIC...")
    
    # We create a dummy service account since token protections are bypassed on the orchestrator backend
    with open("dummy_sa.json", "w") as f:
        f.write('{"client_email": "test_tenant", "private_key": "-----BEGIN PRIVATE KEY-----\\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQDKr7fNMuPW0Csi\\nIjXmOL9uAZ9utFIbneiQh6mIET8qb0bJ2Oe6VobLosqOUfC2POyNXUKczssdKBJn\\n6ouJSmddb2ykonhaSMh57B0m18iT85yhgxEjDCFQ1MXCJk0ovgsu3fXx8s4lg+3i\\neLBg7HjTufLIK8IzkxdySnUli/1B8d8sZn3FZ20uJ/GyqzscmB5drgxlRID9KAT7\\npIxiJ8Fv9F8u5v1FuseLLsEcBqt3dthEqJoV9DieQCX5cO4g603s34Z97kPaRG00\\n/HlcBJOu/gxk0Y2fsVGEjb4ewCN3sFHECxBHc1kHqlfrvsC41LrHdsC+QU0332xA\\n0ZtPTxjxAgMBAAECggEADbvyx5/4qgWIvrM0asln81N/KFhmA0K5JRNZl1jv1Pdz\\nA69ueFpvn4cu6Y4/h4xromKc5mo+1NbHydct+wYZ004pvaLYET9tp5FVIlzXKxYE\\nBDpmOV/o4VohnQ0iXQM6V8PTlsBTXAhrYKq5QkAT1JOnkg0hB4SilJcwsUl3Rkth\\n8BqA6kFYQW8S0XItZ99GWHDBlAb8DPMcQCMK0wg9B1e2M5t2vHf9eL7OARbK/eSb\\nMWwsWFgRIpkg2ZVm9U6GlFp6F7pazzfcddHldBb6wp2tCp3GUGX/wKv3+ytTPnPB\\nYpKNbc+Z/msf3dnKDjGPQyKMm1rL317bXNJ4nD4hSQKBgQD7MdXw3H0rg/2ZzXv8\\n1PCYtcW/LiPXEu2OQ/Sz9G5LAAAKpKDFtor7UEAtBw69zkZtPmW3UG4wRwmfSHDW\\nn9kWJII5pfF0yp5U3xZ3nvetSX50NFyi+C8pEp5sET/EhSrKzkys6MEtN6DHSZHl\\nq5FwSeqA/j9gCE3jqoRGqp7PHQKBgQDOkFMsuIPie7sqn0TPznXxYDVHPf5ZijLc\\nrjdHgAr9sPxDhBPLb9ygXdmZjgxSMiSMJogw0wv8XpOcDAUQ9XYHxRNI5IdcYM5D\\nvibEUQsVSWPAaPEmSiQUSaL1Xi4EjPyAHCFn+GZgdld3xRmnsvZJdfV9FQrM9hJe\\n0j7CbdLk5QKBgB07uIU2c867pqjelB5hfbqX9PKB4SPnjQPwfqruuGM8FcUnUZqQ\\n2u3SchWLa7jFJ8cQ6u+BicFOkx0ZZiBkK/R6vTkOSeJorjJ8X/X95x8gnXnSmjFR\\nJtPl2dAD1eL+CHPfvGanE8w6XBi1RChxZhSmVYc7j46SiNYFAy3iL2c1AoGAcHPF\\ntAznT38Ij9WRAohlUPiNSLGJLHm94sG9OmGMmjuluaPHmvLU60DsW1onfv/pQZsg\\nfWQHnGZoeYVZpLfcf7JcI0y2HCZfZCW6uRldrUL82RzIW431Qk4sNuQErVmLhLrL\\nvOxP36fNSli09MTKq4daE7RG4vn7Wj+fBv3+17kCgYBWGEVSdW9yGOsEvZsN+nTp\\nBQhdyvQjy6/lYMHeFrx1ga4V1E/XXmhyRRzk1lCg8AGRUfPyMqgo/tXOQSHoK6yp\\ntdIph/XuCmA8ZFGayFdGVrIL2nw9qs7cRGAUAPRnRNlgRzTzFc8WnB6vtItTfaDm\\nNhNocK9YfkydFdqQbB7Daw==\\n-----END PRIVATE KEY-----"}')
        
    client = ClutchCallClient(
        endpoint="quic://187.127.139.138:9090",
        service_account_path="dummy_sa.json",
        # FFI core lib needs to be compiled via Bazel and assigned in CLUTCHCALL_LIB_PATH
        # e.g., export CLUTCHCALL_LIB_PATH=../../bazel-bin/core/clutchcall_core_ffi.so
    )

    try:
        print("Connecting to QUIC Server at 187.127.139.138:9090...")
        async with client.connect_async():
            print("Successfully connected over QUIC!")

            f_out = open("captured_output.alaw", "wb")
            
            def audio_cb(datagram_data):
                try:
                    pcm = client.deserialize_audio_frame(datagram_data)
                    if pcm:
                        f_out.write(pcm)
                        f_out.flush()
                except Exception as e:
                    print(f"Error parsing audio datagram natively: {e}")
                    
            events_out = open("events.txt", "a")
            
            active_calls = set()
            
            def event_cb(datagram_data):
                try:
                    call_sid, status = client.deserialize_call_event(datagram_data)
                    print(f"--> [EVENT] call_sid: {call_sid} | status: {status}")
                    events_out.write(f"{call_sid},{status}\n")
                    events_out.flush()
                    if status not in ["COMPLETED", "FAILED", "BUSY", "NO_ANSWER"]:
                        active_calls.add(call_sid)
                    else:
                        active_calls.discard(call_sid)
                except Exception as e:
                    print(f"Error parsing call event datagram natively: {e}")
                    
            client.on_audio_frame = audio_cb
            client.on_call_event = event_cb

            # We do NOT manually call client.stream_events anymore because connect_async() naturally negotiates the UUID internally natively.

            print("Executing dialing RPC using trunk: default ...")
            await client.dial(
                to="sip:+1600258824@2x99i6f70f2.sip.livekit.cloud", 
                trunk_id="default",
                call_from="+18005551234", # Arbitrary caller ID
                max_duration_ms=60000,
                client_id=client.client_id
            )
            print("Dial RPC dispatched successfully. PBX is routing!")
            
            # Keep process alive to allow background events and audio routing
            await asyncio.sleep(20)
            
            for sid in list(active_calls):
                print(f"Terminating orphaned call automatically natively: {sid}")
                await client.terminate(sid)
            
            await asyncio.sleep(1)

    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"Dial Error: {e}")

if __name__ == "__main__":
    asyncio.run(main())
