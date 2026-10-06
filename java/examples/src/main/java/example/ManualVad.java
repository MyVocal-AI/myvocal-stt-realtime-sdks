package example;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;

import ai.myvocal.stt.realtime.ConnectOptions;
import ai.myvocal.stt.realtime.FinishResult;
import ai.myvocal.stt.realtime.RealtimeClient;
import ai.myvocal.stt.realtime.RealtimeEvent;
import ai.myvocal.stt.realtime.RealtimeSession;
import ai.myvocal.stt.realtime.SessionOptions;
import ai.myvocal.stt.realtime.SessionRequest;
import ai.myvocal.stt.realtime.WavInfo;

/** Manual commit vs. VAD segmentation. manual: you decide line boundaries with commit() and keep
 * streaming afterwards. vad: the service commits on silence.
 * <pre>MYVOCAL_ACCESS_KEY=... java -cp ... example.ManualVad speech.wav manual|vad</pre> */
public final class ManualVad {
    public static void main(String[] args) throws Exception {
        String mode = args.length > 1 ? args[1] : "manual";
        String encoding;
        byte[] pcm;
        try (InputStream in = new BufferedInputStream(Files.newInputStream(Paths.get(args[0])))) {
            WavInfo info = WavInfo.read(in);
            encoding = info.getEncoding();
            pcm = readN(in, (int) info.getDataSize());
        }
        SessionOptions options = new SessionOptions().inputEncoding(encoding).segmentCommitMode(mode);
        if ("vad".equals(mode)) {
            options.commitSilenceMs(500.0).speechDetectionThreshold(0.5);
        }
        try (RealtimeClient client = Env.client();
             RealtimeSession session = client.connect(new ConnectOptions()
                     .request(new SessionRequest().title(mode + " example").options(options))
                     .on(RealtimeEvent.TRANSCRIPT_FINAL, e -> System.out.println("final: " + e.getText())))) {
            if ("manual".equals(mode)) {
                int half = pcm.length / 2 / 2 * 2;
                session.sendStream(new ByteArrayInputStream(Arrays.copyOf(pcm, half)), true);
                session.commit(); // line boundary; recording continues in the same epoch
                session.sendStream(new ByteArrayInputStream(Arrays.copyOfRange(pcm, half, pcm.length)), true);
            } else {
                session.sendStream(new ByteArrayInputStream(pcm), true);
            }
            FinishResult result = session.finish();
            System.out.println(mode + " -> " + result.getStatus() + " " + result.getLines().size() + " lines | "
                    + result.getText());
        }
    }

    static byte[] readN(InputStream in, int n) throws java.io.IOException {
        byte[] out = new byte[n];
        int off = 0;
        while (off < n) {
            int k = in.read(out, off, n - off);
            if (k < 0) {
                return Arrays.copyOf(out, off);
            }
            off += k;
        }
        return out;
    }
}
