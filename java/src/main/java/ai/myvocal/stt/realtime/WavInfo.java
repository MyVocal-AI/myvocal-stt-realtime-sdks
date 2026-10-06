package ai.myvocal.stt.realtime;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

/** A RIFF/WAVE header. {@link #read(InputStream)} leaves the stream at the data chunk. */
public final class WavInfo {
    private final int audioFormat;
    private final int channels;
    private final int sampleRateHz;
    private final int bitsPerSample;
    private final long dataOffset;
    private final long dataSize;

    private WavInfo(int audioFormat, int channels, int sampleRateHz, int bitsPerSample, long dataOffset, long dataSize) {
        this.audioFormat = audioFormat;
        this.channels = channels;
        this.sampleRateHz = sampleRateHz;
        this.bitsPerSample = bitsPerSample;
        this.dataOffset = dataOffset;
        this.dataSize = dataSize;
    }

    public int getAudioFormat() { return audioFormat; }
    public int getChannels() { return channels; }
    public int getSampleRateHz() { return sampleRateHz; }
    public int getBitsPerSample() { return bitsPerSample; }
    public long getDataOffset() { return dataOffset; }
    public long getDataSize() { return dataSize; }

    /** The matching inputEncoding for PCM16 mono or 8 kHz mu-law mono WAV. */
    public String getEncoding() {
        if (channels != 1) {
            throw new AudioFormatException("WAV must be mono, found " + channels + " channels");
        }
        String name;
        if (audioFormat == 1 && bitsPerSample == 16) {
            name = "pcm_s16le_" + sampleRateHz;
        } else if (audioFormat == 7 && bitsPerSample == 8 && sampleRateHz == 8000) {
            name = "mulaw_8000";
        } else {
            throw new AudioFormatException("WAV format " + audioFormat + "/" + bitsPerSample + "-bit/" + sampleRateHz
                    + " Hz has no MyVocal inputEncoding");
        }
        if (!AudioFormat.ENCODINGS.containsKey(name)) {
            throw new AudioFormatException("WAV sample rate " + sampleRateHz + " Hz has no MyVocal inputEncoding");
        }
        return name;
    }

    /** Reads the header and positions {@code in} at the first data byte. */
    public static WavInfo read(InputStream in) throws IOException {
        DataInputStream d = new DataInputStream(in);
        byte[] riff = new byte[12];
        try {
            d.readFully(riff);
        } catch (EOFException e) {
            throw new AudioFormatException("not a RIFF/WAVE file");
        }
        if (!"RIFF".equals(new String(riff, 0, 4, "US-ASCII")) || !"WAVE".equals(new String(riff, 8, 4, "US-ASCII"))) {
            throw new AudioFormatException("not a RIFF/WAVE file");
        }
        long pos = 12;
        int fmt = -1;
        int ch = 0;
        int rate = 0;
        int bits = 0;
        while (true) {
            byte[] head = new byte[8];
            try {
                d.readFully(head);
            } catch (EOFException e) {
                throw new AudioFormatException("WAV has no data chunk");
            }
            String id = new String(head, 0, 4, "US-ASCII");
            long size = le32(head, 4);
            pos += 8;
            if ("fmt ".equals(id)) {
                byte[] body = new byte[(int) size];
                d.readFully(body);
                if (size < 16) {
                    throw new AudioFormatException("WAV fmt chunk is truncated");
                }
                fmt = le16(body, 0);
                ch = le16(body, 2);
                rate = (int) le32(body, 4);
                bits = le16(body, 14);
                pos += size;
            } else if ("data".equals(id)) {
                if (fmt < 0) {
                    throw new AudioFormatException("WAV data chunk before fmt chunk");
                }
                return new WavInfo(fmt, ch, rate, bits, pos, size);
            } else {
                skip(d, size);
                pos += size;
            }
            if (size % 2 == 1) {
                skip(d, 1);
                pos++;
            }
        }
    }

    private static void skip(DataInputStream d, long n) throws IOException {
        while (n > 0) {
            long k = d.skip(n);
            if (k <= 0) {
                if (d.read() < 0) {
                    throw new AudioFormatException("WAV is truncated");
                }
                k = 1;
            }
            n -= k;
        }
    }

    private static int le16(byte[] b, int o) {
        return (b[o] & 0xff) | ((b[o + 1] & 0xff) << 8);
    }

    private static long le32(byte[] b, int o) {
        return (b[o] & 0xffL) | ((b[o + 1] & 0xffL) << 8) | ((b[o + 2] & 0xffL) << 16) | ((b[o + 3] & 0xffL) << 24);
    }
}
