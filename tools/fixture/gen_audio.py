#!/usr/bin/env python3
"""Generates the deterministic synthetic audio fixtures (no external audio, no speech).

Each file is 1.25 s of a 440 Hz + 1000 Hz tone mix: 12 full 100 ms frames and a 50 ms short tail.
Run once; the outputs are committed under fixtures/audio.
"""
import math
import os
import struct

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.abspath(os.path.join(HERE, "..", "..", "fixtures", "audio"))
RATES = [8000, 16000, 22050, 24000, 44100, 48000]
SECONDS = 1.25


def pcm(rate):
    n = int(rate * SECONDS)
    out = bytearray()
    for i in range(n):
        t = i / rate
        v = int(6000 * math.sin(2 * math.pi * 440 * t) + 2000 * math.sin(2 * math.pi * 1000 * t))
        out += struct.pack("<h", v)
    return bytes(out)


def mulaw_encode(sample):
    bias, clip = 0x84, 32635
    sign = 0x80 if sample < 0 else 0
    if sample < 0:
        sample = -sample
    sample = min(sample, clip) + bias
    exponent = 7
    mask = 0x4000
    while exponent > 0 and not (sample & mask):
        exponent -= 1
        mask >>= 1
    mantissa = (sample >> (exponent + 3)) & 0x0F
    return (~(sign | (exponent << 4) | mantissa)) & 0xFF


def wav(rate, data):
    return (b"RIFF" + struct.pack("<I", 36 + len(data)) + b"WAVEfmt " + struct.pack("<IHHIIHH", 16, 1, 1, rate,
            rate * 2, 2, 16) + b"data" + struct.pack("<I", len(data)) + data)


def main():
    os.makedirs(OUT, exist_ok=True)
    for rate in RATES:
        with open(os.path.join(OUT, "tone_pcm_s16le_%d.raw" % rate), "wb") as fh:
            fh.write(pcm(rate))
    p8 = pcm(8000)
    samples = struct.unpack("<%dh" % (len(p8) // 2), p8)
    with open(os.path.join(OUT, "tone_mulaw_8000.raw"), "wb") as fh:
        fh.write(bytes(mulaw_encode(s) for s in samples))
    with open(os.path.join(OUT, "tone_pcm_s16le_16000.wav"), "wb") as fh:
        fh.write(wav(16000, pcm(16000)))


if __name__ == "__main__":
    main()
