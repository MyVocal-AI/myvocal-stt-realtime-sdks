package ai.myvocal.stt.realtime;

/** Audio does not fit the session encoding (half sample at the end, WAV mismatch, encoding mismatch). */
public class AudioFormatException extends MyVocalException {
    private static final long serialVersionUID = 1L;

    public AudioFormatException(String message) { super(message); }
}
