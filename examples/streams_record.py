"""Record a live broadcast to disk via the Streams Python SDK.

Pulls a live input, mints a short-lived playback URL, opens BroadcastViewer,
and writes the CMAF init segment + every media segment to ``out.fmp4`` in
arrival order. Drop into a server-side recorder, or adapt for an off-platform
re-encode pipeline. ``ffmpeg -i out.fmp4 -c copy out.mp4`` repackages to a
playable file.

Pre-reqs
========

  pip install clutchcall                       # this SDK
  bazel build //core:clutchcall_moqt_ffi.so    # the C engine the viewer needs
  export CLUTCHCALL_MOQT_FFI=/path/to/libclutchcall_moqt_ffi.so
  export CLUTCHCALL_API_KEY=tqs_…

Run
===

  python streams_record.py --org org_abc --input li_xyz --out out.fmp4
"""

import argparse
import os
import sys
import threading
import time

from clutchcall.streams import Streams, BroadcastViewer, BroadcastChunk


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bff", default="https://app.clutchcall.dev",
                    help="BFF origin (default: production)")
    ap.add_argument("--org", required=True, help="organization id")
    ap.add_argument("--input", required=True, help="live input id (li_…)")
    ap.add_argument("--out", default="out.fmp4", help="output file path")
    ap.add_argument("--ttl", type=int, default=3600,
                    help="playback token TTL in seconds (server-clamped 30..86400)")
    args = ap.parse_args()

    api_key = os.environ.get("CLUTCHCALL_API_KEY")
    if not api_key:
        print("error: CLUTCHCALL_API_KEY env var not set", file=sys.stderr)
        return 2

    # ── control plane ─────────────────────────────────────────────────
    streams = Streams(base_url=args.bff, api_key=api_key, org_id=args.org)
    inp = streams.live_inputs.get(id=args.input)
    ticket = inp.signed_playback_url(ttl_seconds=args.ttl)
    print(f"watching '{inp.name}' (kid={ticket.kid}), token good for {args.ttl}s")

    # ── data plane ────────────────────────────────────────────────────
    bytes_written = 0
    chunks_seen   = 0
    done = threading.Event()
    rc   = {"code": 0}

    f = open(args.out, "wb")

    def on_chunk(is_init: bool, chunk: BroadcastChunk) -> None:
        nonlocal bytes_written, chunks_seen
        f.write(chunk.data)
        bytes_written += len(chunk.data)
        chunks_seen   += 1
        if is_init:
            print(f"init segment received ({len(chunk.data)} B)")

    def on_close(reason: str, detail) -> None:
        if reason == "auth_failed":
            print(f"playback token rejected by relay: {detail}", file=sys.stderr)
            rc["code"] = 3
        elif reason == "network":
            print(f"network closed: {detail}", file=sys.stderr)
            rc["code"] = 4
        else:
            print(f"closed cleanly: {reason}")
        done.set()

    viewer = BroadcastViewer.open(ticket.url, on_chunk=on_chunk, on_close=on_close)

    # Wait until close, but report progress every few seconds so the operator
    # knows it's actually flowing.
    start = time.time()
    try:
        while not done.is_set():
            done.wait(timeout=5.0)
            elapsed = time.time() - start
            print(f"  {chunks_seen} chunks, {bytes_written:,} B in {elapsed:.0f}s")
    except KeyboardInterrupt:
        print("\ninterrupt — closing viewer")
        viewer.close()
        done.wait(timeout=2.0)
    finally:
        f.close()

    print(f"wrote {bytes_written:,} B to {args.out}")
    return rc["code"]


if __name__ == "__main__":
    sys.exit(main())
