"""Subscribe to a fleet of robots' telemetry from a single Python process.

Demonstrates the cloud-dashboard pattern: open one Robotics client per robot
id, subscribe to the same topic on each, fan the messages into a single
queue. The MoQT substrate auto-reconnects each session independently — if a
robot drops, only its rows go quiet; the rest keep flowing.

Pre-reqs
========

  pip install clutchcall
  bazel build //core:clutchcall_moqt_ffi.so
  export CLUTCHCALL_MOQT_FFI=/path/to/libclutchcall_moqt_ffi.so
  export CLUTCHCALL_RELAY_TOKEN=tqs_…

Run
===

  python robotics_fleet_sub.py turtlebot-1 turtlebot-2 turtlebot-3
"""

import os
import queue
import signal
import sys
import threading
import time

from clutchcall.robotics import Robotics


def main(robot_ids: list[str]) -> int:
    token = os.environ.get("CLUTCHCALL_RELAY_TOKEN")
    if not token:
        print("error: CLUTCHCALL_RELAY_TOKEN env var not set", file=sys.stderr)
        return 2

    clients: list[Robotics] = []
    subs = []

    q: "queue.Queue[tuple[str, str, bytes]]" = queue.Queue(maxsize=1024)

    for rid in robot_ids:
        r = Robotics(token=token, robot_id=rid)
        clients.append(r)
        # closure over rid so the queue knows which robot the frame is from
        sub = r.subscribe_telemetry(
            topic="odom",
            type_name="nav_msgs/msg/Odometry",
            on_message=lambda tn, payload, rid=rid: q.put((rid, tn, payload)),
        )
        subs.append(sub)

    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())

    # Drain + print stats in 1-second windows.
    counts: dict[str, int] = {rid: 0 for rid in robot_ids}
    last_print = time.time()
    print(f"watching {len(robot_ids)} robot(s); Ctrl-C to stop")
    try:
        while not stop.is_set():
            try:
                rid, _tn, _payload = q.get(timeout=0.5)
                counts[rid] += 1
            except queue.Empty:
                pass
            now = time.time()
            if now - last_print >= 1.0:
                line = "  ".join(f"{rid}={counts[rid]}/s" for rid in robot_ids)
                print(line)
                counts = {rid: 0 for rid in robot_ids}
                last_print = now
    except KeyboardInterrupt:
        pass
    finally:
        for s in subs:
            s.close()
        for c in clients:
            c.close()
    return 0


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("usage: robotics_fleet_sub.py <robot_id> [robot_id ...]", file=sys.stderr)
        sys.exit(2)
    sys.exit(main(sys.argv[1:]))
