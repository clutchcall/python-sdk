from enum import IntEnum

class MethodID(IntEnum):
    ORIGINATE = 1430677891
    ORIGINATE_BULK = 721069100
    ABORT_BULK = 3861915064
    TERMINATE = 3834253405
    STREAM_EVENTS = 959835745
    SET_INBOUND_ROUTING = 1933986897
    GET_INCOMING_CALLS = 1161946746
    ANSWER_INCOMING_CALL = 2990157256
    GET_ACTIVE_BUCKETS = 2624504207
    GET_BUCKET_CALLS = 1217351135
    EXECUTE_BUCKET_ACTION = 4030863293
    EXECUTE_DIALPLAN = 80147304
    BARGE = 3854301714
    AUDIO_FRAME = 2991054320


class DialplanAction(IntEnum):
    """Mirrors clutchcall/api/clutchcall_types.hh::DialplanAction.

    Values 0-6 are the original dialplan apps; usable as `default_app`
    on Dial. Values 7-12 are call-control verbs only valid through
    ExecuteDialplan against an active call_sid.
    """
    HANGUP = 0
    PARK = 1
    MUSIC_ON_HOLD = 2
    PLAYBACK = 3
    UNPARK_AND_BRIDGE = 4
    ANSWER = 5
    AI_BIDIRECTIONAL_STREAM = 6
    # Call-control verbs (route through ExecuteDialplan):
    TRANSFER = 7   # app_args: destination URI ("sip:user@host" or "+E.164")
    MUTE = 8       # app_args: "" (gateway-only) or "wire" (also send recvonly re-INVITE)
    UNMUTE = 9     # app_args: same shape as MUTE
    HOLD = 10      # app_args: ""
    UNHOLD = 11    # app_args: ""
    SEND_DTMF = 12 # app_args: "<digit>:<mode>:<duration_ms>" (mode = rfc2833 | info; inband not supported)