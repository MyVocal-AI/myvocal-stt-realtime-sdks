package ai.myvocal.stt.realtime;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/** A public inputEncoding with its sample rate and sample width. */
public final class AudioFormat {
    /** Server default inputEncoding. */
    public static final String DEFAULT_ENCODING = "pcm_s16le_16000";

    /** The seven public input encodings. */
    public static final Map<String, AudioFormat> ENCODINGS;

    static {
        Map<String, AudioFormat> m = new LinkedHashMap<String, AudioFormat>();
        for (int rate : new int[] {8000, 16000, 22050, 24000, 44100, 48000}) {
            m.put("pcm_s16le_" + rate, new AudioFormat("pcm_s16le_" + rate, rate, 2));
        }
        m.put("mulaw_8000", new AudioFormat("mulaw_8000", 8000, 1));
        ENCODINGS = Collections.unmodifiableMap(m);
    }

    private final String encoding;
    private final int sampleRateHz;
    private final int bytesPerSample;

    private AudioFormat(String encoding, int sampleRateHz, int bytesPerSample) {
        this.encoding = encoding;
        this.sampleRateHz = sampleRateHz;
        this.bytesPerSample = bytesPerSample;
    }

    /** Resolves an inputEncoding (null/empty = default). */
    public static AudioFormat of(String encoding) {
        String key = encoding == null || encoding.trim().isEmpty() ? DEFAULT_ENCODING
                : encoding.trim().toLowerCase(Locale.ROOT);
        AudioFormat f = ENCODINGS.get(key);
        if (f == null) {
            throw new AudioFormatException("unknown inputEncoding " + encoding);
        }
        return f;
    }

    public String getEncoding() { return encoding; }
    public int getSampleRateHz() { return sampleRateHz; }
    public int getBytesPerSample() { return bytesPerSample; }

    @Override
    public String toString() {
        return "AudioFormat{" + encoding + "}";
    }
}
