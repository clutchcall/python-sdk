"""Voice — server-side outbound originate + agent attach + bridge tap.

Originate a call against a PSTN trunk, attach a running agent_runtime
agent that handles the ASR + LLM + TTS loop, and tap the bridge in
parallel so the same process can mirror the audio to a transcript file.

Pre-reqs
========

  pip install clutchcall
  bazel build //core:clutchcall_moqt_ffi.so
  export CLUTCHCALL_MOQT_FFI=/path/to/libclutchcall_moqt_ffi.so
  export CLUTCHCALL_API_KEY=tqs_…

Run
===

  python voice_agent_attach.py --org org_abc \\
      --to +15551234567 --from +15558675309 \\
      --trunk trunk_main --agent healthcare-assistant
"""

import argparse
import os
import signal
import struct
import sys
import threading
import time

from clutchcall.voice import Voice


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bff", default="https://app.clutchcall.dev")
    ap.add_argument("--org", required=True)
    ap.add_argument("--to",  required=True)
    ap.add_argument("--from", dest="from_", required=True)
    ap.add_argument("--trunk", required=True)
    ap.add_argument("--agent", required=True)
    ap.add_argument("--codec", default="opus", choices=("opus", "pcm16", "g711_ulaw", "g711_alaw"))
    ap.add_argument("--sample-rate", type=int, default=48000)
    ap.add_argument("--out", default="caller.opus", help="raw uplink frames written here")
    args = ap.parse_args()

    api_key = os.environ.get("CLUTCHCALL_API_KEY")
    if not api_key:
        print("error: CLUTCHCALL_API_KEY env var not set", file=sys.stderr)
        return 2

    v = Voice(base_url=args.bff, api_key=api_key, org_id=args.org)

    # 1. Originate. Pass `agent` so the engine wires the bridge end-to-end;
    #    we still attach our own bridge below so this process can tap the
    #    same audio (e.g. to write a transcript).
    call = v.calls.originate(
        to=args.to, from_=args.from_, trunk_id=args.trunk, agent=args.agent,
    )
    print(f"dialing {args.to}: sid={call.sid}")

    # 2. Bridge tap: write each uplink frame to disk with a tiny
    #    [u32 frame_bytes][u64 ts_us] header so a downstream tool can
    #    walk frames without per-frame timing inference. The engine
    #    handles the real ASR/TTS; we're just observing.
    f = open(args.out, "wb")
    frames = 0
    bytes_written = 0

    def on_uplink(frame: bytes, ts_us: int) -> None:
        nonlocal frames, bytes_written
        f.write(struct.pack("<IQ", len(frame), ts_us))
        f.write(frame)
        frames += 1
        bytes_written += len(frame)

    bridge = v.audio_bridge.attach(
        call.sid,
        codec=args.codec, sample_rate=args.sample_rate, frame_ms=20,
        on_uplink=on_uplink,
    )

    # 3. Hold until SIGTERM; print progress so a human can watch the soak.
    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    start = time.time()
    try:
        while not stop.is_set():
            stop.wait(timeout=2.0)
            elapsed = time.time() - start
            print(f"  {frames} frames, {bytes_written:,} B in {elapsed:.0f}s")
    except KeyboardInterrupt:
        pass
    finally:
        bridge.close()
        try:
            call.hangup()
        except Exception as e:  # noqa: BLE001 — call may already be torn down
            print(f"hangup: {e}", file=sys.stderr)
        f.close()

    print(f"wrote {bytes_written:,} B in {frames} frames to {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
