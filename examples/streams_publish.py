"""Pipe a local fMP4 file into a live input via the Streams Python SDK.

Reads a fragmented MP4 from disk (or stdin) and pushes every box into
BroadcastPublisher in arrival order. The first chunk is the CMAF init
segment (priority 0); media segments follow (priority 1). Adapt the chunker
for raw H.264/Opus, an RTSP scrape, or an FFmpeg pipe.

Pre-reqs
========

  pip install clutchcall
  bazel build //core:clutchcall_moqt_ffi.so
  export CLUTCHCALL_MOQT_FFI=/path/to/libclutchcall_moqt_ffi.so
  export CLUTCHCALL_API_KEY=tqs_…

Run
===

  # New live input + push
  python streams_publish.py --org org_abc --new "my-show" --fmp4 input.mp4

  # Reuse an existing input — pass the stream key you saved at create()
  python streams_publish.py --org org_abc --input li_xyz \\
      --stream-key sk_live_… --fmp4 input.mp4
"""

import argparse
import os
import sys
import threading
import time

from clutchcall.streams import Streams, BroadcastPublisher, PublisherCodecs


# CMAF box header is a 32-bit big-endian length + 4-byte type. Walk the file
# box-by-box so we hand the relay one well-formed boundary at a time —
# scanning by fixed size would split moofs from their mdats and the viewer's
# MSE would refuse.
def iter_cmaf_boxes(path: str):
    with open(path, "rb") as f:
        while True:
            head = f.read(8)
            if len(head) < 8:
                return
            size = int.from_bytes(head[:4], "big")
            kind = head[4:8]
            if size < 8:
                # 64-bit large size (size==1 -> next 8 bytes), or 0 (to EOF).
                if size == 1:
                    big = f.read(8)
                    size = int.from_bytes(big, "big")
                    body = f.read(size - 16)
                    yield kind, head + big + body
                    continue
                if size == 0:
                    rest = f.read()
                    yield kind, head + rest
                    return
                raise SystemExit(f"malformed box size {size} at {f.tell()-8}")
            body = f.read(size - 8)
            yield kind, head + body


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bff", default="https://app.clutchcall.dev")
    ap.add_argument("--org", required=True)
    ap.add_argument("--new", metavar="NAME", help="provision a new live input with this name")
    ap.add_argument("--input", help="reuse an existing live input id")
    ap.add_argument("--stream-key", help="stream key for --input (required if --input is set)")
    ap.add_argument("--fmp4", required=True, help="path to a fragmented MP4 to push")
    ap.add_argument("--video-codec", default="avc1.42E01F")
    ap.add_argument("--audio-codec", default="opus")
    ap.add_argument("--relay", default="relay.clutchcall.dev")
    args = ap.parse_args()

    if not (args.new or args.input):
        print("error: pass --new <name> or --input <id>", file=sys.stderr)
        return 2
    if args.input and not args.stream_key:
        print("error: --input requires --stream-key (save the cleartext from create())",
              file=sys.stderr)
        return 2

    api_key = os.environ.get("CLUTCHCALL_API_KEY")
    if not api_key:
        print("error: CLUTCHCALL_API_KEY env var not set", file=sys.stderr)
        return 2

    streams = Streams(base_url=args.bff, api_key=api_key, org_id=args.org)

    if args.new:
        res = streams.live_inputs.create(name=args.new)
        input_id   = res.input.external_input_id
        stream_key = res.stream_key
        print(f"created {res.input.id} (external={input_id})")
        print(f"  stream_key: {stream_key}   <-- save this; it won't appear again")
    else:
        # Skip a control round-trip and just push.
        input_id   = args.input
        stream_key = args.stream_key

    done = threading.Event()
    rc   = {"code": 0}

    def on_close(reason, detail):
        print(f"publisher closed: {reason} {detail or ''}".rstrip())
        if reason == "auth_failed":
            rc["code"] = 3
        elif reason == "network":
            rc["code"] = 4
        done.set()

    pub = BroadcastPublisher.open(
        input_id=input_id,
        stream_key=stream_key,
        relay_host=args.relay,
        codecs=PublisherCodecs(video=args.video_codec, audio=args.audio_codec),
        on_close=on_close,
    )

    # Push at media-time pace. CMAF segments carry their own timing in moof
    # boxes; we just hand them over as fast as they were authored.
    chunks = 0
    bytes_pushed = 0
    start = time.time()
    try:
        # The init segment is the ftyp + moov pair at the head of the file.
        # We treat the first emitted box as init and every subsequent box
        # as a media segment — matches the BroadcastPublisher convention
        # (priority 0 then 1).
        for kind, box in iter_cmaf_boxes(args.fmp4):
            pub.write(box)
            chunks += 1
            bytes_pushed += len(box)
            if chunks % 30 == 0:
                elapsed = time.time() - start
                print(f"  {chunks} boxes, {bytes_pushed:,} B in {elapsed:.0f}s")
        pub.close("finished")
        done.wait(timeout=5.0)
    except KeyboardInterrupt:
        pub.close("closed_by_caller")
        done.wait(timeout=2.0)

    print(f"pushed {bytes_pushed:,} B in {chunks} boxes from {args.fmp4}")
    return rc["code"]


if __name__ == "__main__":
    sys.exit(main())
