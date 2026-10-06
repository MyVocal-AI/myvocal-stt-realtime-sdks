"""Input encodings and WAV header reading. No FFmpeg or sound-card dependency."""
import struct
from dataclasses import dataclass
from typing import BinaryIO, Dict, Tuple, Union

from .errors import AudioFormatError

#: public inputEncoding -> (sample rate Hz, bytes per sample)
ENCODINGS: Dict[str, Tuple[int, int]] = {
    "pcm_s16le_8000": (8000, 2),
    "pcm_s16le_16000": (16000, 2),
    "pcm_s16le_22050": (22050, 2),
    "pcm_s16le_24000": (24000, 2),
    "pcm_s16le_44100": (44100, 2),
    "pcm_s16le_48000": (48000, 2),
    "mulaw_8000": (8000, 1),
}
DEFAULT_ENCODING = "pcm_s16le_16000"


@dataclass(frozen=True)
class AudioFormat:
    encoding: str
    sample_rate_hz: int
    bytes_per_sample: int

    @staticmethod
    def of(encoding: str) -> "AudioFormat":
        key = (encoding or DEFAULT_ENCODING).strip().lower()
        if key not in ENCODINGS:
            raise AudioFormatError("unknown inputEncoding %r" % encoding)
        rate, bps = ENCODINGS[key]
        return AudioFormat(key, rate, bps)


@dataclass(frozen=True)
class WavInfo:
    sample_rate_hz: int
    channels: int
    bits_per_sample: int
    audio_format: int
    data_offset: int
    data_size: int

    @property
    def encoding(self) -> str:
        """The matching MyVocal inputEncoding for PCM16 mono or 8 kHz mu-law mono WAV."""
        if self.channels != 1:
            raise AudioFormatError("WAV must be mono, found %d channels" % self.channels)
        if self.audio_format == 1 and self.bits_per_sample == 16:
            name = "pcm_s16le_%d" % self.sample_rate_hz
        elif self.audio_format == 7 and self.bits_per_sample == 8 and self.sample_rate_hz == 8000:
            name = "mulaw_8000"
        else:
            raise AudioFormatError("WAV format %d/%d-bit/%d Hz has no MyVocal inputEncoding"
                                   % (self.audio_format, self.bits_per_sample, self.sample_rate_hz))
        if name not in ENCODINGS:
            raise AudioFormatError("WAV sample rate %d Hz has no MyVocal inputEncoding" % self.sample_rate_hz)
        return name


def read_wav_header(source: Union[str, BinaryIO]) -> WavInfo:
    """Reads a RIFF/WAVE header and locates the ``data`` chunk."""
    fh = open(source, "rb") if isinstance(source, str) else source
    try:
        riff = fh.read(12)
        if len(riff) < 12 or riff[:4] != b"RIFF" or riff[8:12] != b"WAVE":
            raise AudioFormatError("not a RIFF/WAVE file")
        fmt = None
        pos = 12
        while True:
            head = fh.read(8)
            if len(head) < 8:
                raise AudioFormatError("WAV has no data chunk")
            cid, size = head[:4], struct.unpack("<I", head[4:])[0]
            pos += 8
            if cid == b"fmt ":
                body = fh.read(size)
                fmt = struct.unpack("<HHIIHH", body[:16])
                pos += size
            elif cid == b"data":
                if fmt is None:
                    raise AudioFormatError("WAV data chunk before fmt chunk")
                return WavInfo(sample_rate_hz=fmt[2], channels=fmt[1], bits_per_sample=fmt[5],
                               audio_format=fmt[0], data_offset=pos, data_size=size)
            else:
                fh.seek(size, 1)
                pos += size
            if size % 2:
                fh.seek(1, 1)
                pos += 1
    finally:
        if isinstance(source, str):
            fh.close()
