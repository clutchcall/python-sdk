"""Bridge a robot's ROS 2 graph to the ClutchCall relay.

Subscribes to the robot's local ROS 2 topics (odom, scan, battery_state),
forwards each message's raw CDR onto a MoQT telemetry track for cloud
consumers. In the other direction, subscribes to a MoQT command track and
re-publishes the cmd_vel onto the local DDS.

Pre-reqs
========

  pip install clutchcall
  bazel build //core:clutchcall_moqt_ffi.so
  export CLUTCHCALL_MOQT_FFI=/path/to/libclutchcall_moqt_ffi.so
  export CLUTCHCALL_RELAY_TOKEN=tqs_…

This file uses rclpy for the ROS 2 side; install via apt or pip. If your
robot speaks rmw_zenoh natively, drop the rclpy bits — the MoQT side is
identical.
"""

import os
import signal
import sys
import threading

from clutchcall.robotics import Robotics, QoSProfile

try:
    import rclpy
    from rclpy.node import Node
    from rclpy.qos import QoSProfile as RclQoS, ReliabilityPolicy
    HAVE_ROS = True
except ImportError:
    HAVE_ROS = False


# CDR (de)serialization is normally rosidl-generated. For the bridge we don't
# need to parse — pass the raw bytes through. rclpy gives us a deserialized
# Python object; convert with rclpy.serialize_message / deserialize_message.
def _serialize(msg) -> bytes:
    from rclpy.serialization import serialize_message
    return serialize_message(msg)


def _deserialize(cls, data: bytes):
    from rclpy.serialization import deserialize_message
    return deserialize_message(data, cls)


def main() -> int:
    if not HAVE_ROS:
        print("error: rclpy not installed. apt install python3-rclpy or use rmw_zenoh + the MoQT side directly.",
              file=sys.stderr)
        return 2

    token = os.environ.get("CLUTCHCALL_RELAY_TOKEN")
    if not token:
        print("error: CLUTCHCALL_RELAY_TOKEN env var not set", file=sys.stderr)
        return 2

    robot_id = os.environ.get("ROBOT_ID", "turtlebot-7")

    rclpy.init()
    ros = Node("clutchcall_bridge")

    r = Robotics(token=token, robot_id=robot_id)

    # ── telemetry: ROS → MoQT ────────────────────────────────────────────

    from nav_msgs.msg     import Odometry
    from sensor_msgs.msg  import BatteryState

    odom_pub = r.publish_telemetry(
        topic="odom",
        type_name="nav_msgs/msg/Odometry",
        qos=QoSProfile(reliability="reliable", depth=10),
    )
    batt_pub = r.publish_telemetry(
        topic="battery_state",
        type_name="sensor_msgs/msg/BatteryState",
        qos=QoSProfile(reliability="best_effort", depth=1),
    )

    rcl_reliable = RclQoS(depth=10, reliability=ReliabilityPolicy.RELIABLE)
    rcl_best     = RclQoS(depth=1,  reliability=ReliabilityPolicy.BEST_EFFORT)

    ros.create_subscription(Odometry,    "/odom",          lambda m: odom_pub.write(_serialize(m)), rcl_reliable)
    ros.create_subscription(BatteryState,"/battery_state", lambda m: batt_pub.write(_serialize(m)), rcl_best)

    # ── commands: MoQT → ROS ─────────────────────────────────────────────

    from geometry_msgs.msg import Twist
    cmd_vel_pub = ros.create_publisher(Twist, "/cmd_vel", rcl_best)

    def _on_cmd(type_name: str, payload: bytes) -> None:
        if type_name != "geometry_msgs/msg/Twist":
            return  # guard against schema drift; the relay will log the drop
        cmd_vel_pub.publish(_deserialize(Twist, payload))

    cmd_sub = r.subscribe_command(
        topic="cmd_vel",
        type_name="geometry_msgs/msg/Twist",
        on_message=_on_cmd,
    )

    print(f"bridge up: robot_id={robot_id}, "
          f"telemetry [odom, battery_state], commands [cmd_vel]")

    # rclpy spin in a background thread so we can wait on a SIGTERM event.
    stop = threading.Event()
    def _spin() -> None:
        while not stop.is_set() and rclpy.ok():
            rclpy.spin_once(ros, timeout_sec=0.1)
    th = threading.Thread(target=_spin, daemon=True)
    th.start()

    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    try:
        stop.wait()
    except KeyboardInterrupt:
        stop.set()
    finally:
        cmd_sub.close()
        odom_pub.close()
        batt_pub.close()
        r.close()
        ros.destroy_node()
        rclpy.shutdown()
    return 0


if __name__ == "__main__":
    sys.exit(main())
