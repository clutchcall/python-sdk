"""Headless game client in Python.

Useful for soak tests, AI-controlled players, or smoke-checking a room from
CI. Joins a room, publishes synthetic input, prints the most recent state
every second. Pair with ``games_server.py`` for a fully-headless smoke.

Pre-reqs same as games_server.py.

Run
===

  python games_client.py duel-42 alice
"""

import argparse
import json
import os
import signal
import sys
import threading
import time

from clutchcall.games import Games


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("room")
    ap.add_argument("player")
    ap.add_argument("--input-hz", type=int, default=60)
    ap.add_argument("--relay", default="relay.clutchcall.dev")
    args = ap.parse_args()

    token = os.environ.get("CLUTCHCALL_RELAY_TOKEN")
    if not token:
        print("error: CLUTCHCALL_RELAY_TOKEN env var not set", file=sys.stderr)
        return 2

    me = Games(
        token=token, room_id=args.room, player_id=args.player,
        relay_host=args.relay,
    )

    last_state = {"bytes": None}
    state_sub = me.subscribe_state(
        on_state=lambda data: last_state.update({"bytes": data}),
    )
    chat_sub = me.subscribe_events(
        channel="chat",
        on_event=lambda pid, payload: print(f"  chat ← {pid}: {payload.decode('utf-8', 'replace')}"),
    )
    inp  = me.publish_input()
    chat = me.publish_event(channel="chat")

    chat.write(b"hello, room")
    print(f"joined room={args.room} as player={args.player}")

    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())

    # Two background tasks: an input loop (synthetic), and a 1-Hz state
    # print so a human can watch the soak progress.
    def _input_loop() -> None:
        period = 1.0 / args.input_hz
        i = 0
        while not stop.is_set():
            inp.write(f"{i}".encode())
            i += 1
            stop.wait(timeout=period)
    threading.Thread(target=_input_loop, daemon=True).start()

    try:
        while not stop.is_set():
            stop.wait(timeout=1.0)
            if last_state["bytes"] is None:
                continue
            try:
                snap = json.loads(last_state["bytes"])
                print(f"  state tick={snap.get('t')} players={snap.get('p')}")
            except Exception:  # noqa: BLE001
                print(f"  state {len(last_state['bytes'])}B")
    except KeyboardInterrupt:
        pass
    finally:
        state_sub.close()
        chat_sub.close()
        inp.close()
        chat.close()
        me.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
