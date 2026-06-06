"""IoT device publisher: retained device state at boot, periodic sensor
readings on the lossy lane, alerts on the reliable lane.

Pre-reqs
========

  pip install clutchcall
  bazel build //core:clutchcall_moqt_ffi.so
  export CLUTCHCALL_MOQT_FFI=/path/to/libclutchcall_moqt_ffi.so
  export CLUTCHCALL_DATA_TOKEN=tqs_…

Run
===

  python data_device.py device-7 --reading-hz 1
"""

import argparse
import json
import os
import random
import signal
import sys
import threading

from clutchcall.data import Data


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("device_id")
    ap.add_argument("--reading-hz", type=float, default=1.0)
    ap.add_argument("--relay", default="relay.clutchcall.dev")
    args = ap.parse_args()

    token = os.environ.get("CLUTCHCALL_DATA_TOKEN")
    if not token:
        print("error: CLUTCHCALL_DATA_TOKEN env var not set", file=sys.stderr)
        return 2

    data = Data(token=token, client_id=args.device_id, relay_host=args.relay)

    # 1. Retained boot-time state — any subscriber matching devices/+/state
    #    gets this on attach. Clear it with a zero-length payload at exit.
    data.publish(
        topic=f"devices/{args.device_id}/state",
        payload=json.dumps({"online": True, "version": "1.4.2"}).encode(),
        retained=True,
    )

    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())

    # 2. Periodic sensor reading on the lossy lane.
    period = 1.0 / args.reading_hz
    try:
        while not stop.is_set():
            reading = 20 + random.random() * 5
            data.publish(
                topic=f"sensors/{args.device_id}/temperature",
                payload=f"{reading:.2f}".encode(),
            )
            stop.wait(timeout=period)
    except KeyboardInterrupt:
        pass
    finally:
        # 3. Clean offline state so the dashboard's retained read flips
        #    to "offline" without a heartbeat timeout.
        try:
            data.publish(
                topic=f"devices/{args.device_id}/state",
                payload=b"",
                retained=True,
            )
        finally:
            data.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
