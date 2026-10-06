package example;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import ai.myvocal.stt.realtime.ConnectOptions;
import ai.myvocal.stt.realtime.FinishResult;
import ai.myvocal.stt.realtime.RealtimeClient;
import ai.myvocal.stt.realtime.RealtimeEvent;
import ai.myvocal.stt.realtime.RealtimeSession;
import ai.myvocal.stt.realtime.SessionOptions;
import ai.myvocal.stt.realtime.SessionRequest;
import ai.myvocal.stt.realtime.WavInfo;

/** File stream (blocking API): stream a WAV file at its sample rate, print transcripts, finish.
 * <pre>MYVOCAL_ACCESS_KEY=... java -cp ... example.FileStream speech.wav</pre> */
public final class FileStream {
    public static void main(String[] args) throws Exception {
        Path wav = Paths.get(args[0]);
        String encoding;
        try (InputStream in = new BufferedInputStream(Files.newInputStream(wav))) {
            encoding = WavInfo.read(in).getEncoding(); // pcm_s16le_<rate> mono or mulaw_8000
        }
        try (RealtimeClient client = Env.client()) {
            SessionRequest request = new SessionRequest().title("file-stream example")
                    .options(new SessionOptions().inputEncoding(encoding).segmentCommitMode("vad"));
            RealtimeSession session = client.connect(new ConnectOptions().request(request)
                    .on(RealtimeEvent.TRANSCRIPT_DELTA, e -> System.out.println("  ... " + e.getText()))
                    .on(RealtimeEvent.TRANSCRIPT_FINAL, e -> System.out.println("final: " + e.getText())));
            try {
                session.sendFile(wav, true); // paced: one second of audio per second
                FinishResult result = session.finish();
                System.out.println("status: " + result.getStatus() + " | transcriptionId: " + result.getTranscriptionId());
                if (result.getUsage().getBillableCharacters() != null) {
                    System.out.println("billableCharacters: " + result.getUsage().getBillableCharacters());
                }
                System.out.println("text: " + result.getText());
            } finally {
                session.close(); // local resources only; finish() above is the business end
            }
        }
    }
}
