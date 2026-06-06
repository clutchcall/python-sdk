"""Server-authoritative game loop in Python.

Subscribes to every player's input, advances the world at a fixed tick rate,
broadcasts state. Drop in your own serializer / world impl. Useful for
tooling, AI-controlled NPCs, or a headless authority node that hosts a room
without a player client attached.

Pre-reqs
========

  pip install clutchcall
  bazel build //core:clutchcall_moqt_ffi.so
  export CLUTCHCALL_MOQT_FFI=/path/to/libclutchcall_moqt_ffi.so
  export CLUTCHCALL_RELAY_TOKEN=tqs_…

Run
===

  python games_server.py duel-42 --tick-hz 30
"""

import argparse
import json
import os
import signal
import sys
import threading
import time
from dataclasses import dataclass, field
from typing import Dict

from clutchcall.games import Games


# Stand-in for your real world / serializer. Drop in FlatBuffers / Protobuf /
# struct + msgpack — whatever your client expects.
@dataclass
class World:
    tick: int = 0
    players: Dict[str, dict] = field(default_factory=dict)


def apply_input(world: World, pid: str, payload: bytes) -> None:
    # Real code: deserialize `payload` (movement vec, buttons) and update
    # the player's intent. We just record the most recent payload size.
    world.players.setdefault(pid, {"in": 0})["in"] += 1


def tick_world(world: World, dt_ms: float) -> None:
    world.tick += 1
    # Real code: physics, AI, win-conditions, etc.


def serialize_state(world: World) -> bytes:
    # Real code: codec-of-choice. We use JSON for readability in the example.
    return json.dumps({
        "t": world.tick,
        "p": {pid: meta["in"] for pid, meta in world.players.items()},
    }).encode("utf-8")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("room")
    ap.add_argument("--tick-hz", type=int, default=30)
    ap.add_argument("--relay", default="relay.clutchcall.dev")
    args = ap.parse_args()

    token = os.environ.get("CLUTCHCALL_RELAY_TOKEN")
    if not token:
        print("error: CLUTCHCALL_RELAY_TOKEN env var not set", file=sys.stderr)
        return 2

    auth = Games(token=token, room_id=args.room, relay_host=args.relay)
    world = World()

    input_sub = auth.subscribe_inputs(
        lambda pid, payload: apply_input(world, pid, payload),
    )
    ready_sub = auth.subscribe_events(
        channel="ready",
        on_event=lambda pid, _payload: print(f"  player ready: {pid}"),
    )
    state    = auth.publish_state(tick_hz=args.tick_hz)
    match_end = auth.publish_event(channel="match_end")

    print(f"authority up: room={args.room} tick={args.tick_hz}Hz")

    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())

    tick_s = 1.0 / args.tick_hz
    last_t = time.monotonic()
    try:
        while not stop.is_set():
            now = time.monotonic()
            tick_world(world, (now - last_t) * 1000)
            last_t = now
            state.write(serialize_state(world))
            # Sleep until the next tick boundary.
            elapsed = time.monotonic() - now
            stop.wait(timeout=max(0.0, tick_s - elapsed))
    except KeyboardInterrupt:
        pass
    finally:
        try:
            match_end.write(json.dumps({"reason": "shutdown",
                                         "ticks": world.tick}).encode())
        finally:
            input_sub.close()
            ready_sub.close()
            state.close()
            match_end.close()
            auth.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
