"""
ClutchCall SDK audio transcoders.

Linear16 PCM <-> G.711 PCMU/PCMA conversion using the stdlib ``audioop``
module (available through Python 3.12; removed in 3.13). When ``audioop``
is unavailable the transcoder functions raise ``RuntimeError``.
"""

from typing import Any

try:
    import audioop  # type: ignore[import-not-found]

    _HAS_AUDIOOP = True
except ImportError:
    _HAS_AUDIOOP = False


_RTP_HEADER_BYTES = 12
_SAMPLE_WIDTH = 2  # 16-bit linear PCM


def _require_audioop() -> None:
    if not _HAS_AUDIOOP:
        raise RuntimeError(
            "audioop is not available in this Python runtime "
            "(removed in 3.13). Install a compatible backend before "
            "calling ClutchCall transcoders."
        )


def pcmu_to_pcma(data: bytes) -> bytes:
    _require_audioop()
    pcm = audioop.ulaw2lin(data, _SAMPLE_WIDTH)
    return audioop.lin2alaw(pcm, _SAMPLE_WIDTH)


def pcma_to_pcmu(data: bytes) -> bytes:
    _require_audioop()
    pcm = audioop.alaw2lin(data, _SAMPLE_WIDTH)
    return audioop.lin2ulaw(pcm, _SAMPLE_WIDTH)


def pcmu_to_pcm(data: bytes) -> bytes:
    _require_audioop()
    return audioop.ulaw2lin(data, _SAMPLE_WIDTH)


def pcma_to_pcm(data: bytes) -> bytes:
    _require_audioop()
    return audioop.alaw2lin(data, _SAMPLE_WIDTH)


def pcm_to_pcmu(data: bytes) -> bytes:
    _require_audioop()
    return audioop.lin2ulaw(data, _SAMPLE_WIDTH)


def pcm_to_pcma(data: bytes) -> bytes:
    _require_audioop()
    return audioop.lin2alaw(data, _SAMPLE_WIDTH)


class ClutchCallAudioStream:
    """WebSocket wrapper that yields Linear16 PCM from inbound G.711 frames."""

    def __init__(self, websocket: Any, is_pcmu: bool = True) -> None:
        self._ws = websocket
        self._is_pcmu = is_pcmu

    async def receive_pcm(self) -> bytes:
        payload = await self._ws.receive_bytes()
        if len(payload) <= _RTP_HEADER_BYTES:
            return b""
        rtp_data = payload[_RTP_HEADER_BYTES:]
        return pcmu_to_pcm(rtp_data) if self._is_pcmu else pcma_to_pcm(rtp_data)

    async def send_pcm(self, pcm_bytes: bytes) -> None:
        encoded = pcm_to_pcmu(pcm_bytes) if self._is_pcmu else pcm_to_pcma(pcm_bytes)
        await self._ws.send_bytes(encoded)


class ClutchCallWebTransportStream:
    """QUIC datagram wrapper with the same PCM interface as the WS variant."""

    def __init__(self, quic_session: Any, is_pcmu: bool = True) -> None:
        self._quic = quic_session
        self._is_pcmu = is_pcmu

    async def receive_pcm(self) -> bytes:
        data = await self._quic.receive_datagram()
        if len(data) <= _RTP_HEADER_BYTES:
            return b""
        rtp_data = data[_RTP_HEADER_BYTES:]
        return pcmu_to_pcm(rtp_data) if self._is_pcmu else pcma_to_pcm(rtp_data)

    async def send_pcm(self, pcm_bytes: bytes) -> None:
        encoded = pcm_to_pcmu(pcm_bytes) if self._is_pcmu else pcm_to_pcma(pcm_bytes)
        self._quic.send_datagram(encoded)
