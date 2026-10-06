package example;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;

import ai.myvocal.stt.realtime.ConnectOptions;
import ai.myvocal.stt.realtime.ConnectionLostException;
import ai.myvocal.stt.realtime.FinishResult;
import ai.myvocal.stt.realtime.FinishUnconfirmedException;
import ai.myvocal.stt.realtime.RealtimeClient;
import ai.myvocal.stt.realtime.RealtimeSession;
import ai.myvocal.stt.realtime.SessionOptions;
import ai.myvocal.stt.realtime.SessionRequest;
import ai.myvocal.stt.realtime.SessionView;
import ai.myvocal.stt.realtime.WavInfo;

/** Abnormal finish: the socket drops mid-stream. The SDK stops taking audio, queries the ORIGINAL
 * session and finishes it over REST (no reconnect, no resend, no new session). If the end cannot
 * be confirmed, keep the session id and finish it later explicitly.
 * <pre>MYVOCAL_ACCESS_KEY=... java -cp ... example.AbnormalFinish speech.wav</pre> */
public final class AbnormalFinish {
    public static void main(String[] args) throws Exception {
        String encoding;
        byte[] pcm;
        try (InputStream in = new BufferedInputStream(Files.newInputStream(Paths.get(args[0])))) {
            WavInfo info = WavInfo.read(in);
            encoding = info.getEncoding();
            pcm = ManualVad.readN(in, (int) info.getDataSize());
        }
        try (RealtimeClient client = Env.client()) {
            RealtimeSession session = client.connect(new ConnectOptions().request(new SessionRequest()
                    .title("abnormal-finish example").options(new SessionOptions().inputEncoding(encoding))));
            String sessionId = session.getSessionId();
            try {
                int half = pcm.length / 2 / 2 * 2;
                session.sendStream(new ByteArrayInputStream(Arrays.copyOf(pcm, half)), true);
                session.commit();
                session.abortConnection(); // simulates a network failure on this client only
                try {
                    session.sendAudio(Arrays.copyOfRange(pcm, half, pcm.length));
                } catch (ConnectionLostException e) {
                    System.out.println("socket lost; audio is no longer accepted");
                }
                try {
                    FinishResult result = session.finish(); // REST close-out of the same session
                    System.out.println("closed out: " + result.getStatus() + " via REST: " + result.isViaRest()
                            + " reason: " + result.getCloseoutReason());
                } catch (FinishUnconfirmedException e) {
                    System.out.println("end not confirmed yet for " + e.getSessionId() + " - finish it later:");
                    SessionView view = client.getSession(sessionId);
                    if (!view.isTerminal()) {
                        view = client.finishSession(sessionId, null);
                    }
                    System.out.println("explicit finish: " + view.getStatus());
                }
            } finally {
                session.close();
            }
        }
    }
}
