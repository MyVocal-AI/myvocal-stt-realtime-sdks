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
import ai.myvocal.stt.realtime.RealtimeSession;
import ai.myvocal.stt.realtime.SessionOptions;
import ai.myvocal.stt.realtime.SessionRequest;
import ai.myvocal.stt.realtime.SessionView;
import ai.myvocal.stt.realtime.Ticket;
import ai.myvocal.stt.realtime.WavInfo;

/** Lifecycle: create, issue a browser ticket (helper only), stream, pause, resume (new epoch),
 * finish twice (same result) and read the session back over REST.
 * <pre>MYVOCAL_ACCESS_KEY=... java -cp ... example.PauseResume speech.wav</pre> */
public final class PauseResume {
    public static void main(String[] args) throws Exception {
        String encoding;
        byte[] pcm;
        try (InputStream in = new BufferedInputStream(Files.newInputStream(Paths.get(args[0])))) {
            WavInfo info = WavInfo.read(in);
            encoding = info.getEncoding();
            pcm = ManualVad.readN(in, (int) info.getDataSize());
        }
        try (RealtimeClient client = Env.client();
             RealtimeSession session = client.connect(new ConnectOptions().request(new SessionRequest()
                     .title("pause/resume example").options(new SessionOptions().inputEncoding(encoding))))) {
            // Ticket helper: hand ticket.getTicket() to YOUR browser front end; never log it.
            Ticket ticket = client.createTicket(session.getSessionId());
            System.out.println("ticket issued: " + ticket.getTicketId() + " expires in " + ticket.getExpiresInSeconds() + " s");

            int third = pcm.length / 3 / 2 * 2;
            session.sendStream(new ByteArrayInputStream(Arrays.copyOf(pcm, third)), true);
            session.pause(); // flushes and confirms first; nothing is sent while paused
            System.out.println("paused at epoch " + session.getCursors().get("epoch"));
            Thread.sleep(1000); // paused time is never sent as audio
            session.resume(RealtimeSession.Via.WS); // or Via.REST
            System.out.println("resumed at epoch " + session.getCursors().get("epoch"));
            session.sendStream(new ByteArrayInputStream(Arrays.copyOfRange(pcm, third, pcm.length)), true);
            FinishResult result = session.finish();
            if (session.finish() != result) { // same result, no second settlement
                throw new IllegalStateException("repeated finish returned a different result");
            }
            SessionView view = client.getSession(result.getSessionId());
            System.out.println("status: " + result.getStatus() + " / GET: " + view.getStatus() + " | epochs: "
                    + view.getEpoch() + " | billable: " + result.getUsage().getBillableCharacters() + " | text: "
                    + result.getText());
        }
    }
}
