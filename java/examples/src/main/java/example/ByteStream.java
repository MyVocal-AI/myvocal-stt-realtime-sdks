package example;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import ai.myvocal.stt.realtime.ConnectOptions;
import ai.myvocal.stt.realtime.FinishResult;
import ai.myvocal.stt.realtime.Notice;
import ai.myvocal.stt.realtime.RealtimeClient;
import ai.myvocal.stt.realtime.RealtimeEvent;
import ai.myvocal.stt.realtime.RealtimeListener;
import ai.myvocal.stt.realtime.RealtimeSession;
import ai.myvocal.stt.realtime.SessionOptions;
import ai.myvocal.stt.realtime.SessionRequest;
import ai.myvocal.stt.realtime.TranscriptLine;

/** Byte stream (Future + Listener API): raw 16 kHz PCM arriving in arbitrary chunks, e.g. from
 * your own capture pipeline. Bytes already arriving at capture speed are not paced again.
 * <pre>MYVOCAL_ACCESS_KEY=... java -cp ... example.ByteStream audio_16k.raw</pre> */
public final class ByteStream {
    public static void main(String[] args) throws Exception {
        try (RealtimeClient client = Env.client()) {
            ConnectOptions options = new ConnectOptions()
                    .request(new SessionRequest().title("byte-stream example")
                            .options(new SessionOptions().inputEncoding("pcm_s16le_16000").segmentCommitMode("vad").includeDetectedLanguage(true)))
                    .listener(new RealtimeListener() {
                        @Override
                        public void onTranscriptFinal(TranscriptLine line, RealtimeEvent event) {
                            System.out.println("final: " + line.getText());
                        }

                        @Override
                        public void onNotice(Notice notice) {
                            System.out.println("notice: " + notice.getCode());
                        }
                    })
                    .onCallbackError((error, event) -> System.err.println("handler failed on " + event.getType()));
            CompletableFuture<RealtimeSession> connecting = client.connectAsync(options);
            RealtimeSession session = connecting.get(30, TimeUnit.SECONDS);
            try (InputStream in = new BufferedInputStream(Files.newInputStream(Paths.get(args[0])))) {
                byte[] chunk = new byte[1234]; // any size; the SDK frames ~100 ms and joins half samples
                int n;
                while ((n = in.read(chunk)) > 0) {
                    session.sendAudio(chunk, 0, n, null); // blocks only when the bounded buffer is full
                }
                FinishResult result = session.finishAsync().get(120, TimeUnit.SECONDS);
                System.out.println("status: " + result.getStatus() + " | transcriptionId: " + result.getTranscriptionId()
                        + " | text: " + result.getText());
            } finally {
                session.close();
            }
        }
    }
}
