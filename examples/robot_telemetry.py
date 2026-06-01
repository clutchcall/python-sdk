#!/usr/bin/env python3
"""Realtime frame-track example: publish robot telemetry and subscribe to it
through the ClutchCall relay.

Run (with the native engine on the path and a relay reachable):

    CLUTCHCALL_MOQT_FFI=/path/to/libclutchcall_moqt_ffi.so \
    RELAY_URL=quic://relay.clutchcall.dev:4443 \
    python3 robot_telemetry.py
"""
import os
import time

from clutchcall.moqt import MoqtClient

URL = os.environ.get("RELAY_URL", "quic://127.0.0.1:4443")
NS, NAME = "robot/turtlebot4-001", "odom"


def main() -> None:
    received = []

    # The state callback reports Connecting/Connected/Reconnecting/Closed/Failed.
    sub_client = MoqtClient.connect(URL, on_state=lambda s: print("sub state", s))
    pub_client = MoqtClient.connect(URL, on_state=lambda s: print("pub state", s))

    # Subscribe FIRST: the relay holds the subscription until the publisher
    # announces, and the SDK queues it until the session is up.
    sub = sub_client.subscribe_frame(
        NS, NAME, lambda ts, priority, data: received.append((priority, len(data)))
    )

    # Publish odometry as opaque CDR bytes with a high per-frame priority.
    track = pub_client.publish_frame(NS, NAME, capability="ros.telemetry",
                                     schema_tag="ros2/cdr")
    for i in range(100):
        cdr = bytes([i & 0xFF]) * 48     # stand-in for a serialized message
        track.write(i * 1000, cdr, priority=200)
        time.sleep(0.1)                  # 10 Hz

    time.sleep(1.0)
    print(f"received {len(received)} frames; "
          f"all priority 200: {all(p == 200 for p, _ in received)}")

    track.close(); sub.close()
    pub_client.close(); sub_client.close()


if __name__ == "__main__":
    main()
