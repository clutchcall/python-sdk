"""Event-bus consumer with topic-prefix dispatch.

Subscribes to every events/<svc>/<kind> topic, routes each message to a
handler picked by the topic. The classical microservice integration shape:
your event producer publishes once; many consumers each subscribe to the
subset of topics they care about.

Pre-reqs same as data_device.py.

Run
===

  python data_consumer.py
"""

import json
import os
import signal
import sys
import threading
from typing import Callable, Dict, List

from clutchcall.data import Data, DataMessage, topic_matches


def main() -> int:
    token = os.environ.get("CLUTCHCALL_DATA_TOKEN")
    if not token:
        print("error: CLUTCHCALL_DATA_TOKEN env var not set", file=sys.stderr)
        return 2

    data = Data(token=token, client_id=f"consumer-{os.getpid()}")

    # Dispatch table: pattern → handler. Each consumer handles its slice
    # of the events/* tree; the SDK opens one subscription against the
    # `events` top-level namespace and we filter locally.
    handlers: List[tuple[str, Callable[[DataMessage], None]]] = []

    def on(pattern: str, fn: Callable[[DataMessage], None]) -> None:
        handlers.append((pattern, fn))

    def on_order_shipped(msg: DataMessage) -> None:
        body = json.loads(msg.payload)
        print(f"  shipped: order={body['orderId']} carrier={body.get('carrier','?')}")

    def on_user_signup(msg: DataMessage) -> None:
        body = json.loads(msg.payload)
        print(f"  signup: {body.get('userId')} via {body.get('source','?')}")

    on("events/orders/shipped", on_order_shipped)
    on("events/users/signup",   on_user_signup)

    # One subscription covers the whole events/* subtree. The SDK filters
    # client-side; we dispatch from the matched handlers list.
    def _route(msg: DataMessage) -> None:
        for pattern, fn in handlers:
            if topic_matches(msg.topic, pattern):
                try:
                    fn(msg)
                except Exception as e:  # noqa: BLE001
                    print(f"  handler {pattern} raised: {e}", file=sys.stderr)
                return
        # Unhandled — log for visibility; in prod this might be a dead-letter
        # publish to events/_unhandled.
        print(f"  unhandled topic: {msg.topic} from {msg.from_client_id}")

    sub = data.subscribe(topic_filter="events/#", on_message=_route)
    print(f"consumer up; routing {len(handlers)} pattern(s) under events/#")

    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    try:
        stop.wait()
    except KeyboardInterrupt:
        pass
    finally:
        sub.close()
        data.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
