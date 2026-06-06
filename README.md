# ClutchCall Python SDK

The official Python wrapper for ClutchCall. Async-first; built on `aioquic` for
ALPN-QUIC transport and `PyJWT` for zero-trust auth, with a native FFI core for
audio processing.

ClutchCall is **modality-oriented**: every modality is its own submodule, all
riding the same MoQT substrate underneath. Pick the entry point that matches
what you're building; mixing them in one process is fine.

| Module                 | Modality                                | Status |
| ---------------------- | --------------------------------------- | ------ |
| `clutchcall.streams`   | Live broadcasts + signed playback URLs  | **GA** |
| `clutchcall.robotics`  | Robotics topic pub/sub (Zenoh-over-QUIC, ROS 2 CDR) | **GA** |
| `clutchcall.games`     | Games (rooms, state/input/event channels; pairs with the Unity UPM drop-in) | **GA** |
| `clutchcall.data`      | MQTT-style typed pub/sub (topics + `+` / `#` filters, retained messages) | **GA** |
| `clutchcall.voice`     | Voice (calls + bidirectional audio bridge + agent attach) | **GA** |
| `clutchcall.moqt`      | Realtime tracks (audio/video/frame)     | GA     |
| `clutchcall.client`    | Legacy voice surface (`ClutchCallClient`) — kept for backwards compat | legacy |

## Installation

```bash
pip install .
```

## Streams — watch a live broadcast

The modality-first pattern. `Streams` is the stateless control plane;
`BroadcastViewer` opens a signed playback URL and forwards chunks.

```python
from clutchcall.streams import Streams, BroadcastViewer

streams = Streams(
    base_url="https://app.clutchcall.dev",
    api_key=os.environ["CLUTCHCALL_API_KEY"],
    org_id="org_abc",
)

inp = streams.live_inputs.get(id="li_xyz")
ticket = inp.signed_playback_url(ttl_seconds=3600)

viewer = BroadcastViewer.open(
    ticket.url,
    on_chunk=lambda is_init, c: pipe.write(c.data),
    on_close=lambda reason, _: print("closed:", reason),
)
```

A full record-to-disk example is in
[`examples/streams_record.py`](examples/streams_record.py). The matching skill
for code generation is
[`clutchcall-streams`](https://github.com/clutchcall/skills/tree/master/skills/clutchcall-streams).

## Streams — push a live broadcast

The publisher counterpart. The cleartext stream key is returned once at
`create()` / `rotate_stream_key()`; capture it.

```python
from clutchcall.streams import Streams, BroadcastPublisher, PublisherCodecs

streams = Streams(base_url=BFF, api_key=KEY, org_id=ORG)
res = streams.live_inputs.create(name="My Show")
# save res.stream_key — the BFF won't return it again

pub = BroadcastPublisher.open(
    input_id=res.input.external_input_id,
    stream_key=res.stream_key,
    codecs=PublisherCodecs(video="avc1.42E01F", audio="opus"),
)

pub.write(fmp4_init)             # CMAF init segment FIRST
pub.write(fmp4_segment)          # media segments
pub.close("finished")
```

A fragmented-MP4-file → live-input pusher is in
[`examples/streams_publish.py`](examples/streams_publish.py).

## Robotics — telemetry + commands across a fleet

Typed pub/sub for a robot fleet. Telemetry goes on `robot/<id>`; commands on
`robot/<id>/ctl`. Payload bytes are opaque CDR (whatever your DDS / rmw_zenoh
layer produces); the SDK prefixes the ROS 2 type name so cross-language
subscribers can decode without out-of-band agreement.

```python
from clutchcall.robotics import Robotics, QoSProfile

r = Robotics(
    token=os.environ["CLUTCHCALL_RELAY_TOKEN"],
    robot_id="turtlebot-7",
)

# robot side
odom = r.publish_telemetry(
    topic="odom",
    type_name="nav_msgs/msg/Odometry",
    qos=QoSProfile(reliability="reliable", depth=10),
)
odom.write(cdr_bytes)

# cloud side
cmd = r.publish_command(
    topic="cmd_vel",
    type_name="geometry_msgs/msg/Twist",
)
cmd.write(twist_cdr_bytes)
```

A full ROS 2 ↔ MoQT bridge (rclpy subscribe → MoQT publish, MoQT subscribe →
rclpy publish) is in
[`examples/robotics_ros_bridge.py`](examples/robotics_ros_bridge.py); a fleet
dashboard subscriber is in
[`examples/robotics_fleet_sub.py`](examples/robotics_fleet_sub.py). The
matching skill is
[`clutchcall-robotics`](https://github.com/clutchcall/skills/tree/master/skills/clutchcall-robotics).

## Games — multiplayer rooms over QUIC

One client per `(room, player)` or `(room, server)`. Three channels:
authoritative `state` (server → all, lossy datagram), per-player `input`
(player → server, lossy datagram), typed `event` (any peer → any subscriber,
reliable stream).

```python
from clutchcall.games import Games

# Authoritative server (no player_id) — run the tick loop here.
auth = Games(token=TOKEN, room_id="duel-42")
state    = auth.publish_state(tick_hz=30)
auth.subscribe_inputs(lambda pid, data: apply_input(pid, data))
# … 30 Hz: state.write(serialize_state(world))

# Player client
me = Games(token=TOKEN, room_id="duel-42", player_id="alice")
me.subscribe_state(lambda data: render(deserialize_state(data)))
inp = me.publish_input()
# … per frame: inp.write(serialize_input(local))
```

A complete server loop is in
[`examples/games_server.py`](examples/games_server.py); a headless client
soak is in [`examples/games_client.py`](examples/games_client.py). For
**Unity** games, install the matching UPM package
[`com.clutchcall.transport`](../../clutchcall-sdk/unity/com.clutchcall.transport/)
— a drop-in for `com.unity.transport` that speaks the same wire. The skill
is
[`clutchcall-games`](https://github.com/clutchcall/skills/tree/master/skills/clutchcall-games).

## Data — MQTT-style typed pub/sub

The MQTT-replacement modality. Hierarchical topics with `+` / `#` filters,
retained messages, lossy + reliable lanes — all over the QUIC relay mesh
(no broker).

```python
from clutchcall.data import Data

data = Data(
    token=os.environ["CLUTCHCALL_DATA_TOKEN"],
    client_id="device-7",
)

# publish — lossy by default
data.publish(topic="sensors/room1/temperature", payload=b"23.5")

# reliable application events
data.publish(topic="events/order-shipped",
             payload=json.dumps({"orderId": "o-42"}).encode(),
             reliable=True)

# subscribe with an MQTT-style filter
sub = data.subscribe(
    topic_filter="sensors/+/temperature",
    on_message=lambda m: print(m.topic, "←", m.from_client_id, m.payload),
)
```

A device-side publisher (with retained state + alerts) is in
[`examples/data_device.py`](examples/data_device.py); an event-bus consumer
with topic dispatch is in
[`examples/data_consumer.py`](examples/data_consumer.py). The matching
skill is
[`clutchcall-data`](https://github.com/clutchcall/skills/tree/master/skills/clutchcall-data).

## Voice — call control + audio bridge

The modality-shaped voice surface. Two primitives: `Calls` (control
plane) and `AudioBridge` (data plane).

```python
from clutchcall.voice import Voice

v    = Voice(base_url="https://app.clutchcall.dev",
             api_key=os.environ["CLUTCHCALL_API_KEY"],
             org_id="org_abc")
call = v.calls.originate(
    to="+15551234567", from_="+15558675309",
    trunk_id="trunk_main", agent="healthcare-assistant",
)

def on_uplink(frame, ts_us): asr.feed(frame)
bridge = v.audio_bridge.attach(call.sid, codec="opus", on_uplink=on_uplink)
tts.on_chunk(lambda opus: bridge.publish_downlink(opus))

call.hangup()
```

A full server-side outbound + bridge-tap example is in
[`examples/voice_agent_attach.py`](examples/voice_agent_attach.py). The
matching skill is
[`clutchcall-voice`](https://github.com/clutchcall/skills/tree/master/skills/clutchcall-voice).

### Legacy voice surface

The original `clutchcall.client.ClutchCallClient` + `clutchcall.media`
modules remain available for backwards compat. Point
`CLUTCHCALL_CREDENTIALS` at your service-account JSON, then:

```python
import asyncio
from clutchcall.client import ClutchCallClient
from clutchcall.media  import ClutchCallAudioStream

async def main():
    client = ClutchCallClient("pbx.clutchcall.com:443")

    # Originate an outbound call against an external trunk.
    response = await client.originate(
        to="+1234567890",
        ai_wss="wss://my-chatbot.com/media",
    )

    # Multiplex the raw audio stream.
    stream = ClutchCallAudioStream()
    await stream.connect("wss://pbx.clutchcall.com/media/session_789")
    async for pcm_chunk in stream.receive_audio_loop():
        # 16 kHz PCM, ready for your voice API.
        ...

if __name__ == "__main__":
    asyncio.run(main())
```

## Native core

The FFI core (`libclutchcall_core_ffi.{so,dylib,dll}`) is loaded at runtime.
Set `CLUTCHCALL_LIB_PATH` if it isn't on the default loader path; see the
[`core-sdk`](https://github.com/clutchcall/core-sdk) repo for build details.
