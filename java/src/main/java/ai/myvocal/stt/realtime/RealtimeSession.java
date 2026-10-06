package ai.myvocal.stt.realtime;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import okhttp3.Request;
import okhttp3.Response;
import okhttp3.WebSocket;
import okhttp3.WebSocketListener;

/** Handle of one realtime session (from {@link RealtimeClient#connect}). One state machine per
 * session: frames ~100 ms from arbitrary input chunks, keeps the session-wide capture count and the
 * per-epoch cursors, follows server rotation (resending only unconfirmed bytes), serializes
 * lifecycle operations, isolates callbacks from the network thread and closes out over REST after
 * an unexpected disconnect (no reconnect, no audio replay into a new task). Thread-safe. */
public final class RealtimeSession implements AutoCloseable {
    private static final Set<String> FATAL = new HashSet<String>(Arrays.asList("INPUT_INVALID", "PROJECT_NOT_FOUND",
            "INSUFFICIENT_CHARACTERS"));
    private static final Set<String> RETRYABLE = new HashSet<String>(Arrays.asList("TEMPORARILY_UNAVAILABLE",
            "PROVIDER_UNAVAILABLE"));
    private static final AtomicInteger THREADS = new AtomicInteger();

    enum State { OPEN, PAUSED, FINISHING, COMPLETED, LOST, CLOSED }

    /** Control path of pause/resume. */
    public enum Via { WS, REST }

    private final RealtimeClient client;
    private final String sessionId;
    private final String transcriptionId;
    private final AudioFormat format;
    private final int bps;
    private final long frameSamples;
    private final long maxBuffered;

    private final ReentrantLock op = new ReentrantLock(true);
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition changed = lock.newCondition();

    // ---- guarded by lock
    private byte[] buf = new byte[0];
    private int bufLen;
    private long bufBase;
    private int half = -1;
    private long captured;
    private long epoch;
    private long epochStart;
    private long sent;
    private long acked;
    private Long serverReceived;
    private boolean rotating;
    private int rotations;
    private boolean accepting = true;
    private boolean userPaused;
    private State state = State.OPEN;
    private RuntimeException fatal;
    private boolean flush;
    private boolean finishSent;
    private long lastProgress = System.nanoTime();
    private String preceding;
    private long framesSent;
    private long peakBuffered;
    private int frameRetries;
    private SessionView completedView;
    private SessionView view;
    private Usage latestUsage;
    private FinishResult result;
    private CompletableFuture<FinishResult> closeout;
    private boolean closingOut;
    private boolean closed;
    private boolean intentional;
    private final List<String> serverErrors = new ArrayList<String>();
    private int connection;
    private Long lastSequence;
    private int sequenceAnomalies;
    private WebSocket socket;
    private boolean ready;
    private Throwable openFailure;

    private final Transcript transcript = new Transcript();
    private final ConnectOptions options;
    private final BlockingQueue<Object> queue;
    private final Thread dispatcher;
    private volatile ExecutorService reporter;
    private final AtomicLong callbackErrors = new AtomicLong();
    private final AtomicLong droppedEvents = new AtomicLong();
    private static final Object STOP = new Object();
    private final CountDownLatch socketDone = new CountDownLatch(1);

    RealtimeSession(RealtimeClient client, SessionView v, AudioFormat format, ConnectOptions options) {
        this.client = client;
        this.sessionId = v.getSessionId();
        this.transcriptionId = v.getTranscriptionId();
        this.format = format;
        this.bps = format.getBytesPerSample();
        this.frameSamples = Math.max(1, (long) format.getSampleRateHz() * client.frameMs / 1000);
        this.maxBuffered = Math.max(frameSamples, (long) format.getSampleRateHz() * client.maxBufferedMs / 1000);
        this.epoch = v.getEpoch() > 0 ? v.getEpoch() : 1;
        this.view = v;
        this.latestUsage = v.getUsage();
        this.options = options;
        this.queue = new ArrayBlockingQueue<Object>(client.eventQueueSize + 1);
        this.dispatcher = new Thread(new Runnable() {
            @Override
            public void run() {
                dispatchLoop();
            }
        }, "myvocal-stt-callback-" + THREADS.incrementAndGet());
        this.dispatcher.setDaemon(true);
        this.dispatcher.start();
    }

    // ------------------------------------------------------------------ read-only state

    public String getSessionId() { return sessionId; }
    public String getTranscriptionId() { return transcriptionId; }
    public AudioFormat getFormat() { return format; }
    public Transcript getTranscript() { return transcript; }

    public String getState() {
        lock.lock();
        try {
            return state.name().toLowerCase(Locale.ROOT);
        } finally {
            lock.unlock();
        }
    }

    public SessionView getView() {
        lock.lock();
        try {
            return view;
        } finally {
            lock.unlock();
        }
    }

    public Usage getLatestUsage() {
        lock.lock();
        try {
            return latestUsage;
        } finally {
            lock.unlock();
        }
    }

    public List<String> getServerErrors() {
        lock.lock();
        try {
            return new ArrayList<String>(serverErrors);
        } finally {
            lock.unlock();
        }
    }

    public long getCallbackErrors() { return callbackErrors.get(); }
    public long getDroppedEvents() { return droppedEvents.get(); }

    /** Snapshot of the counters: epoch; sampleOffset (sent in the current epoch); sentSamples
     * (acknowledged in the current epoch); receivedSamples (server session-wide transport receipt,
     * null before known); capturedSamples (whole samples handed to the SDK); confirmedSamples. */
    public Map<String, Object> getCursors() {
        lock.lock();
        try {
            Map<String, Object> m = new LinkedHashMap<String, Object>();
            m.put("epoch", epoch);
            m.put("sampleOffset", sent);
            m.put("sentSamples", acked);
            m.put("receivedSamples", serverReceived);
            m.put("capturedSamples", captured);
            m.put("confirmedSamples", epochStart + acked);
            return m;
        } finally {
            lock.unlock();
        }
    }

    /** Local counters (buffer, frames, rotations, connections, callback errors...). */
    public Map<String, Object> getStats() {
        lock.lock();
        try {
            Map<String, Object> m = new LinkedHashMap<String, Object>();
            long conf = epochStart + acked;
            m.put("capturedSamples", captured);
            m.put("confirmedSamples", conf);
            m.put("bufferedSamples", captured - conf);
            m.put("peakBufferedSamples", peakBuffered);
            m.put("maxBufferedSamples", maxBuffered);
            m.put("frameSamples", frameSamples);
            m.put("framesSent", framesSent);
            m.put("rotations", (long) rotations);
            m.put("epoch", epoch);
            m.put("connections", (long) connection);
            m.put("sequenceAnomalies", (long) sequenceAnomalies);
            m.put("callbackErrors", callbackErrors.get());
            m.put("droppedEvents", droppedEvents.get());
            return m;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public String toString() {
        return "RealtimeSession{sessionId=" + sessionId + ", state=" + getState() + "}";
    }

    // ------------------------------------------------------------------ connection

    void open(String url, String accessKey) {
        Request req = new Request.Builder().url(url).header("accessKey", accessKey)
                .header("User-Agent", RealtimeClient.USER_AGENT).build();
        WebSocket w = client.ws.newWebSocket(req, new Listener());
        lock.lock();
        try {
            socket = w;
            connection++;
            lastSequence = null;
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(client.timeouts.getConnectMillis());
            while (!ready && openFailure == null && state != State.LOST) {
                long left = deadline - System.nanoTime();
                if (left <= 0) {
                    w.cancel();
                    throw new TransportException("socket open / session.ready timed out", 0, null);
                }
                changed.awaitNanos(left);
            }
            if (openFailure != null && !ready) {
                throw new TransportException("socket open failed (" + RealtimeClient.safe(openFailure) + ")",
                        openFailureStatus, null);
            }
            if (state == State.LOST) {
                throw new ConnectionLostException("socket closed before session.ready", sessionId);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            w.cancel();
            throw new CancellationException("interrupted while connecting");
        } finally {
            lock.unlock();
        }
    }

    private int openFailureStatus;

    private final class Listener extends WebSocketListener {
        @Override
        public void onMessage(WebSocket webSocket, String text) {
            Map<String, Object> msg;
            try {
                msg = Json.parseObject(text);
            } catch (RuntimeException e) {
                return;
            }
            onServerMessage(webSocket, msg);
        }

        @Override
        public void onClosing(WebSocket webSocket, int code, String reason) {
            webSocket.close(1000, null);
            onSocketEnd(webSocket, null, 0);
        }

        @Override
        public void onClosed(WebSocket webSocket, int code, String reason) {
            onSocketEnd(webSocket, null, 0);
        }

        @Override
        public void onFailure(WebSocket webSocket, Throwable t, Response response) {
            onSocketEnd(webSocket, t, response == null ? 0 : response.code());
        }
    }

    // ------------------------------------------------------------------ inbound

    private void onServerMessage(WebSocket w, Map<String, Object> msg) {
        String type = Json.asString(msg.get("eventType"));
        if (type == null) {
            type = "";
        }
        Map<String, Object> payload = Json.asMap(msg.get("payload"));
        Long seq = msg.get("sequence") instanceof Long ? (Long) msg.get("sequence") : null;
        int conn;
        lock.lock();
        try {
            if (w != socket) {
                return;
            }
            if (seq != null) {
                if (lastSequence != null && seq <= lastSequence) {
                    sequenceAnomalies++; // recorded; never used to drop another connection's events
                }
                lastSequence = seq;
            }
            try {
                if (RealtimeEvent.USAGE_UPDATED.equals(type)) {
                    onUsage(new Usage(payload));
                } else if (RealtimeEvent.SESSION_READY.equals(type)) {
                    onReady(new SessionView(payload));
                } else if (RealtimeEvent.SESSION_COMPLETED.equals(type)) {
                    SessionView v = new SessionView(payload);
                    completedView = v;
                    view = v;
                    latestUsage = v.getUsage();
                } else if (RealtimeEvent.SESSION_ERROR.equals(type)) {
                    String code = Json.asString(payload.get("errorCode"));
                    onServerError(code == null ? "UNKNOWN" : code);
                }
            } catch (RuntimeException malformed) {
                // a malformed payload never breaks the stream
            }
            conn = connection;
            changed.signalAll();
        } finally {
            lock.unlock();
        }
        // network state first, then the snapshot, then dispatch
        try {
            if (RealtimeEvent.SESSION_COMPLETED.equals(type)) {
                transcript.merge(new SessionView(payload));
            } else {
                transcript.apply(type, payload);
            }
        } catch (RuntimeException ignored) {
            // malformed supplement
        }
        enqueue(new RealtimeEvent(type, payload, seq, Json.asString(msg.get("eventId")),
                Json.asString(msg.get("sessionId")), conn));
    }

    private void onReady(SessionView v) {
        view = v;
        if (v.getEpoch() > 0) {
            epoch = v.getEpoch();
        }
        Long rec = v.getUsage().getReceivedSamples();
        Long snt = v.getUsage().getSentSamples();
        long received = rec == null ? 0 : rec;
        long s = snt == null ? 0 : snt;
        serverReceived = rec;
        if (captured == 0 && bufLen == 0) {
            epochStart = Math.max(0, received - s);
            sent = acked = s;
            captured = bufBase = received;
        }
        accepting = v.isAccepting();
        if (v.isTerminal()) {
            completedView = v;
            state = State.COMPLETED;
        } else if (!v.isAccepting() && "PAUSED".equals(v.getStatus())) {
            userPaused = true;
            state = State.PAUSED;
        }
        ready = true;
    }

    private void onUsage(Usage u) {
        Long ep = u.getEpoch();
        if (u.getReceivedSamples() != null && (serverReceived == null || ep == null || ep >= epoch)) {
            serverReceived = u.getReceivedSamples();
        }
        if (ep == null) {
            return;
        }
        if (ep >= epoch) {
            latestUsage = u;
        }
        if (ep == epoch) {
            if (u.getSentSamples() != null && u.getSentSamples() > acked) {
                acked = u.getSentSamples();
                if (acked > sent) {
                    sent = acked;
                }
                progress();
                frameRetries = 0;
            }
            if (u.getAccepting() != null) {
                accepting = u.getAccepting();
            }
            if (u.isRotate() && !rotating) {
                rotating = true;
                rotations++;
                // the triggering frame was not stored: close this epoch and open the next one
                sendControl("session.pause");
                sendControl("session.resume");
            }
            if (u.getErrorCode() != null) {
                setFatal(u.getErrorCode());
            }
        } else if (ep > epoch && Boolean.TRUE.equals(u.getAccepting())) {
            adoptEpoch(ep);
        }
        // ep < current epoch: a late answer for an older epoch never moves current cursors back
    }

    private void adoptEpoch(long ep) {
        epochStart += acked;
        epoch = ep;
        sent = acked = 0;
        rotating = false;
        accepting = true;
        progress();
        pump();
    }

    private void progress() {
        lastProgress = System.nanoTime();
        long confirmed = epochStart + acked;
        long drop = confirmed - bufBase;
        if (drop > 0) {
            int n = (int) Math.min((long) bufLen, drop * bps);
            System.arraycopy(buf, n, buf, 0, bufLen - n);
            bufLen -= n;
            bufBase = confirmed;
        }
    }

    private void onServerError(String code) {
        serverErrors.add(code);
        if (FATAL.contains(code)) {
            setFatal(code);
        } else if (RETRYABLE.contains(code) && sent > acked && !finishSent) {
            frameRetries++;
            if (frameRetries > client.maxFrameRetries) {
                if (fatal == null) {
                    fatal = new ServerEventException(code, sessionId);
                }
            } else {
                sent = acked; // replay unconfirmed frames at the confirmed sampleOffset
                pump();
            }
        }
    }

    private void setFatal(String code) {
        if (fatal != null) {
            return;
        }
        if ("INSUFFICIENT_CHARACTERS".equals(code)) {
            fatal = new InsufficientCharactersException(47210, code, "Characters balance exhausted during the stream",
                    false, null, null, null);
        } else {
            fatal = new ServerEventException(code, sessionId);
        }
    }

    private void onSocketEnd(WebSocket w, Throwable failure, int status) {
        lock.lock();
        try {
            if (w != socket) {
                return;
            }
            socketDone.countDown();
            changed.signalAll();
            if (!ready) {
                openFailure = failure != null ? failure : new IOException("closed before session.ready");
                openFailureStatus = status;
                state = State.LOST;
                return;
            }
            if (completedView != null || intentional || closingOut) {
                return;
            }
            state = State.LOST;
            if (client.closeoutOnDisconnect || finishSent) {
                startCloseout("DISCONNECTED");
            }
        } finally {
            lock.unlock();
        }
    }

    // ------------------------------------------------------------------ outbound (under lock; sends are non-blocking)

    private void sendControl(String type) {
        Map<String, Object> payload = new LinkedHashMap<String, Object>();
        payload.put("capturedSamples", String.valueOf(captured));
        send(type, payload);
    }

    private void send(String type, Map<String, Object> payload) {
        if (socket == null) {
            throw new ConnectionLostException("socket is not open", sessionId);
        }
        Map<String, Object> msg = new LinkedHashMap<String, Object>();
        msg.put("eventType", type);
        msg.put("epoch", epoch);
        msg.put("payload", payload);
        if (!socket.send(Json.write(msg))) {
            throw new ConnectionLostException("socket send failed", sessionId);
        }
    }

    private boolean canSend() {
        return socket != null && (state == State.OPEN || state == State.FINISHING) && !finishSent && accepting && !rotating
                && !userPaused && fatal == null && !closingOut;
    }

    private void pump() {
        while (canSend()) {
            long pos = epochStart + sent;
            long avail = captured - pos;
            if (avail <= 0) {
                return;
            }
            long n = Math.min(frameSamples, avail);
            if (n < frameSamples && !flush) {
                return;
            }
            int start = (int) ((pos - bufBase) * bps);
            byte[] chunk = Arrays.copyOfRange(buf, start, start + (int) (n * bps));
            if (sent == acked) {
                lastProgress = System.nanoTime(); // the no-ack clock starts with unconfirmed audio
            }
            Map<String, Object> payload = new LinkedHashMap<String, Object>();
            payload.put("audioBase64", Base64.getEncoder().encodeToString(chunk));
            payload.put("sampleOffset", String.valueOf(sent));
            payload.put("capturedSamples", String.valueOf(captured));
            if (preceding != null) {
                payload.put("precedingText", preceding);
                preceding = null;
            }
            send("audio.append", payload);
            sent += n;
            framesSent++;
        }
    }

    private RuntimeException broken() {
        if (fatal != null) {
            return fatal;
        }
        if (state == State.LOST) {
            return new ConnectionLostException("connection lost; no more audio is accepted", sessionId);
        }
        if (state == State.CLOSED || state == State.COMPLETED) {
            return new SessionStateException("session is " + state.name().toLowerCase(Locale.ROOT));
        }
        return null;
    }

    private void awaitChange(long millis) {
        try {
            changed.await(Math.max(1, millis), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CancellationException("interrupted");
        }
    }

    private void waitConfirmed() {
        while (true) {
            RuntimeException b = broken();
            if (b != null) {
                throw b;
            }
            if (epochStart + acked >= captured && !rotating) {
                return;
            }
            if (canSend() && epochStart + sent < captured) {
                pump();
                continue;
            }
            long idle = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - lastProgress);
            if (idle > client.timeouts.getAckMillis()) {
                throw new AckTimeoutException("no acknowledgement progress for " + client.timeouts.getAckMillis()
                        + " ms (sessionId=" + sessionId + ")");
            }
            awaitChange(Math.min(250, client.timeouts.getAckMillis() - idle + 10));
        }
    }

    private long waitRoom(long need) {
        long limit = client.timeouts.getBackpressureMillis();
        while (true) {
            RuntimeException b = broken();
            if (b != null) {
                throw b;
            }
            if (userPaused) {
                throw new SessionStateException("session is paused; resume() before sending audio");
            }
            long room = maxBuffered - (captured - (epochStart + acked));
            if (room >= Math.min(need, frameSamples) && room > 0) {
                return room;
            }
            long idle = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - lastProgress);
            if (idle > limit) {
                throw new BackpressureTimeoutException("send buffer full for " + limit
                        + " ms without acknowledgement progress (sessionId=" + sessionId + ")");
            }
            awaitChange(Math.min(250, limit - idle + 10));
        }
    }

    private void checkInput() {
        if (state == State.PAUSED || userPaused) {
            throw new SessionStateException("session is paused; resume() before sending audio");
        }
        if (state != State.OPEN) {
            RuntimeException b = broken();
            if (b != null) {
                throw b;
            }
            throw new SessionStateException("session is " + state.name().toLowerCase(Locale.ROOT)
                    + "; no more audio is accepted");
        }
        RuntimeException b = broken();
        if (b != null) {
            throw b;
        }
    }

    private void lockOp() {
        try {
            op.lockInterruptibly();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CancellationException("interrupted");
        }
    }

    // ------------------------------------------------------------------ public operations

    /** Queues audio in the session encoding. Any chunk size is accepted; complete ~100 ms frames are
     * sent as soon as they exist. Blocks while the bounded buffer is full (never drops audio). A
     * trailing half PCM sample is kept for the next call. */
    public void sendAudio(byte[] data) {
        sendAudio(data, 0, data.length, null);
    }

    /** {@link #sendAudio(byte[])} with optional {@code precedingText} on the next frame. */
    public void sendAudio(byte[] data, String precedingText) {
        sendAudio(data, 0, data.length, precedingText);
    }

    public void sendAudio(byte[] data, int offset, int length, String precedingText) {
        lockOp();
        try {
            lock.lock();
            try {
                checkInput();
                byte[] in;
                int len = length;
                if (half >= 0) {
                    in = new byte[length + 1];
                    in[0] = (byte) half;
                    System.arraycopy(data, offset, in, 1, length);
                    len = length + 1;
                    half = -1;
                } else {
                    in = Arrays.copyOfRange(data, offset, offset + length);
                }
                if (bps == 2 && len % 2 == 1) {
                    half = in[len - 1] & 0xff;
                    len--;
                }
                if (precedingText != null) {
                    preceding = precedingText;
                }
                int off = 0;
                while (off < len) {
                    long room = waitRoom((len - off) / bps);
                    int take = (int) Math.min(room * bps, (long) (len - off));
                    ensure(bufLen + take);
                    System.arraycopy(in, off, buf, bufLen, take);
                    bufLen += take;
                    captured += take / bps;
                    off += take;
                    peakBuffered = Math.max(peakBuffered, captured - (epochStart + acked));
                    pump();
                }
            } finally {
                lock.unlock();
            }
        } finally {
            op.unlock();
        }
    }

    private void ensure(int cap) {
        if (cap > buf.length) {
            buf = Arrays.copyOf(buf, Math.max(cap, buf.length * 2));
        }
    }

    /** Streams {@code in} (raw audio in the session encoding) frame by frame until EOF. {@code paced}
     * sends by sample time (files); use false for a live source that already arrives at capture speed. */
    public void sendStream(InputStream in, boolean paced) throws IOException {
        int frameBytes = (int) (frameSamples * bps);
        byte[] chunk = new byte[frameBytes];
        long t0 = System.nanoTime();
        long pos = 0;
        while (true) {
            int n = 0;
            while (n < frameBytes) {
                int k = in.read(chunk, n, frameBytes - n);
                if (k < 0) {
                    break;
                }
                n += k;
            }
            if (n > 0) {
                if (paced) {
                    pos += n;
                    long dueNanos = (long) ((pos / bps) * 1e9 / format.getSampleRateHz());
                    long wait = TimeUnit.NANOSECONDS.toMillis(dueNanos - (System.nanoTime() - t0));
                    RealtimeClient.sleep(wait);
                }
                sendAudio(chunk, 0, n, null);
            }
            if (n < frameBytes) {
                return;
            }
        }
    }

    /** Streams a raw file in the session encoding, or a WAV file ({@code .wav}) whose header matches it.
     * {@code paced=true} sends one second of audio per second. */
    public void sendFile(Path path, boolean paced) throws IOException {
        try (InputStream raw = new BufferedInputStream(Files.newInputStream(path))) {
            InputStream in = raw;
            if (path.getFileName().toString().toLowerCase(Locale.ROOT).endsWith(".wav")) {
                WavInfo info = WavInfo.read(raw);
                String enc = info.getEncoding();
                if (!enc.equals(format.getEncoding())) {
                    throw new AudioFormatException("WAV is " + enc + " but the session encoding is " + format.getEncoding());
                }
                in = new LimitedInputStream(raw, info.getDataSize());
            }
            sendStream(in, paced);
        }
    }

    private void flushAndConfirm() {
        flush = true;
        try {
            pump();
            waitConfirmed();
        } finally {
            flush = false;
        }
    }

    /** {@code segment.commit}: the audio so far becomes a line; recording continues in the same epoch. */
    public void commit() {
        lockOp();
        try {
            lock.lock();
            try {
                checkInput();
                flushAndConfirm();
                sendControl("segment.commit");
            } finally {
                lock.unlock();
            }
        } finally {
            op.unlock();
        }
    }

    /** Pause over the socket. */
    public void pause() { pause(Via.WS); }

    /** Flushes and confirms buffered audio, then closes the current epoch. */
    public void pause(Via via) {
        lockOp();
        try {
            lock.lock();
            try {
                checkInput();
                flushAndConfirm();
                if (via == Via.REST) {
                    SessionView v = restControl("pause");
                    applyRestView(v);
                } else {
                    sendControl("session.pause");
                    waitUntil(new Condition0() {
                        @Override
                        public boolean test() {
                            return !accepting;
                        }
                    }, "pause");
                }
                userPaused = true;
                state = State.PAUSED;
                changed.signalAll();
            } finally {
                lock.unlock();
            }
        } finally {
            op.unlock();
        }
    }

    /** Resume over the socket. */
    public void resume() { resume(Via.WS); }

    /** Opens the next epoch. Sample offsets restart at 0; the captured count continues. */
    public void resume(Via via) {
        lockOp();
        try {
            lock.lock();
            try {
                if (state != State.PAUSED) {
                    RuntimeException b = broken();
                    throw b != null ? b : new SessionStateException("resume() needs a paused session");
                }
                final long old = epoch;
                if (via == Via.REST) {
                    SessionView v = restControl("resume");
                    applyRestView(v);
                    if (v.getEpoch() > old && v.isAccepting()) {
                        adoptEpoch(v.getEpoch());
                    }
                } else {
                    sendControl("session.resume");
                    waitUntil(new Condition0() {
                        @Override
                        public boolean test() {
                            return epoch > old;
                        }
                    }, "resume");
                }
                if (epoch <= old) {
                    throw new SessionStateException("server did not open a new epoch");
                }
                userPaused = false;
                state = State.OPEN;
                changed.signalAll();
            } finally {
                lock.unlock();
            }
        } finally {
            op.unlock();
        }
    }

    private interface Condition0 {
        boolean test();
    }

    private void waitUntil(Condition0 c, String what) {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(client.timeouts.getAckMillis());
        while (!c.test()) {
            RuntimeException b = broken();
            if (b != null) {
                throw b;
            }
            long left = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
            if (left <= 0) {
                throw new AckTimeoutException(what + " not acknowledged in " + client.timeouts.getAckMillis()
                        + " ms (sessionId=" + sessionId + ")");
            }
            awaitChange(Math.min(250, left));
        }
    }

    /** REST pause/resume (called under lock; the REST call itself does not need it). An unknown
     * outcome is resolved by reading the session first; the call is repeated only when the read
     * shows it did not take effect. */
    private SessionView restControl(String action) {
        long cap = captured;
        long old = epoch;
        lock.unlock();
        try {
            try {
                return "pause".equals(action) ? client.pauseSession(sessionId, cap) : client.resumeSession(sessionId, cap);
            } catch (TransportException e) {
                SessionView got = client.getSession(sessionId);
                boolean done = "pause".equals(action) ? !got.isAccepting() : got.getEpoch() > old && got.isAccepting();
                if (done) {
                    return got;
                }
                return "pause".equals(action) ? client.pauseSession(sessionId, cap) : client.resumeSession(sessionId, cap);
            }
        } finally {
            lock.lock();
        }
    }

    private void applyRestView(SessionView v) {
        view = v;
        latestUsage = v.getUsage();
        accepting = v.isAccepting();
        if (v.getUsage().getReceivedSamples() != null) {
            serverReceived = v.getUsage().getReceivedSamples();
        }
        changed.signalAll();
    }

    /** Flushes the tail, waits until every sample is confirmed (handling rotation), sends
     * {@code session.finish} and returns the server's terminal result. Repeated calls return the
     * same result. On disconnect/timeout it closes out over REST ({@link FinishResult#getCloseoutReason()})
     * or throws {@link FinishUnconfirmedException}. Interrupting the calling thread cancels the wait
     * ({@link CancellationException}). */
    public FinishResult finish() {
        CompletableFuture<FinishResult> pending;
        lock.lock();
        try {
            if (result != null) {
                return result;
            }
            pending = closeout;
        } finally {
            lock.unlock();
        }
        if (pending != null) {
            return await(pending);
        }
        lockOp();
        try {
            lock.lock();
            try {
                if (result != null) {
                    return result;
                }
                if (closeout != null) {
                    pending = closeout;
                } else {
                    return finishLocked();
                }
            } finally {
                lock.unlock();
            }
            return await(pending);
        } finally {
            op.unlock();
        }
    }

    private FinishResult finishLocked() {
        if (half >= 0) {
            throw new AudioFormatException("one byte of an incomplete PCM sample remains; send the missing byte or call "
                    + "discardPartialSample() before finish()");
        }
        if (state == State.COMPLETED && completedView != null) {
            return finalizeResult(completedView, false, null);
        }
        if (state == State.CLOSED) {
            throw new SessionStateException("session handle is closed; use client.finishSession(sessionId)");
        }
        if (state == State.LOST) {
            return closeoutAndWait("DISCONNECTED");
        }
        state = State.FINISHING;
        if (!userPaused && fatal == null) {
            try {
                flushAndConfirm();
            } catch (AckTimeoutException e) {
                return closeoutAndWait("ACK_TIMEOUT");
            } catch (ConnectionLostException e) {
                return closeoutAndWait("DISCONNECTED");
            } catch (ServerEventException | InsufficientCharactersException e) {
                // stream stopped by the server; settle what it confirmed
            } catch (CancellationException e) {
                if (state == State.FINISHING) {
                    state = State.OPEN;
                }
                throw e;
            }
        }
        finishSent = true;
        try {
            sendControl("session.finish");
        } catch (ConnectionLostException e) {
            return closeoutAndWait("DISCONNECTED");
        }
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(client.timeouts.getFinishMillis());
        while (completedView == null) {
            if (closeout != null) {
                return awaitUnlocked(closeout);
            }
            if (state == State.LOST) {
                return closeoutAndWait("DISCONNECTED");
            }
            long left = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
            if (left <= 0) {
                return closeoutAndWait("FINISH_TIMEOUT");
            }
            awaitChange(Math.min(250, left));
        }
        FinishResult r = finalizeResult(completedView, false, null);
        intentional = true;
        final WebSocket w = socket;
        if (w != null) {
            w.close(1000, null);
        }
        return r;
    }

    private FinishResult closeoutAndWait(String reason) {
        return awaitUnlocked(startCloseout(reason));
    }

    private FinishResult awaitUnlocked(CompletableFuture<FinishResult> f) {
        lock.unlock();
        try {
            return await(f);
        } finally {
            lock.lock();
        }
    }

    private static FinishResult await(CompletableFuture<FinishResult> f) {
        try {
            return f.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CancellationException("interrupted");
        } catch (java.util.concurrent.ExecutionException e) {
            Throwable c = e.getCause();
            if (c instanceof RuntimeException) {
                throw (RuntimeException) c;
            }
            throw new MyVocalException("close-out failed", c);
        }
    }

    /** {@link #finish()} on a background thread; {@code cancel(true)} interrupts the wait. */
    public CompletableFuture<FinishResult> finishAsync() {
        return async("myvocal-stt-finish", new Callable<FinishResult>() {
            @Override
            public FinishResult call() {
                return finish();
            }
        });
    }

    /** Drops a pending half PCM sample (returns whether one was pending). */
    public boolean discardPartialSample() {
        lock.lock();
        try {
            boolean had = half >= 0;
            half = -1;
            return had;
        } finally {
            lock.unlock();
        }
    }

    /** Drops this client's socket without a close handshake or finish, like a network failure
     * (test/diagnostic helper). The disconnect path then runs. */
    public void abortConnection() {
        WebSocket w;
        lock.lock();
        try {
            w = socket;
            if (ready && completedView == null && !closingOut && !intentional) {
                state = State.LOST; // synchronous: no audio is accepted from here on
                changed.signalAll();
                if (client.closeoutOnDisconnect || finishSent) {
                    startCloseout("DISCONNECTED");
                }
            }
        } finally {
            lock.unlock();
        }
        if (w != null) {
            w.cancel();
        }
    }

    /** Releases local resources only (socket, callback thread). It is not a business finish; an
     * unfinished session stays open server-side (paused on disconnect). Idempotent. */
    @Override
    public void close() {
        WebSocket w;
        lock.lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            intentional = true;
            if (state != State.COMPLETED && state != State.LOST) {
                state = State.CLOSED;
            }
            changed.signalAll();
            w = socket;
        } finally {
            lock.unlock();
        }
        if (w != null) {
            w.close(1000, null);
            try {
                if (!socketDone.await(client.timeouts.getCloseMillis(), TimeUnit.MILLISECONDS)) {
                    w.cancel();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                w.cancel();
            }
        }
        queue.offer(STOP);
        try {
            dispatcher.join(client.timeouts.getCloseMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (dispatcher.isAlive()) {
            dispatcher.interrupt();
        }
        ExecutorService r = reporter;
        if (r != null) {
            r.shutdown();
        }
        client.forget(this);
    }

    // ------------------------------------------------------------------ close-out

    private CompletableFuture<FinishResult> startCloseout(final String reason) {
        if (closeout == null) {
            closeout = new CompletableFuture<FinishResult>();
            closingOut = true;
            if (state != State.CLOSED) {
                state = State.LOST;
            }
            final long cap = captured;
            final WebSocket w = socket;
            final CompletableFuture<FinishResult> f = closeout;
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        if (w != null) {
                            w.cancel();
                        }
                        f.complete(runCloseout(reason, cap));
                    } catch (Throwable e) {
                        f.completeExceptionally(e);
                    } finally {
                        lock.lock();
                        try {
                            changed.signalAll();
                        } finally {
                            lock.unlock();
                        }
                    }
                }
            }, "myvocal-stt-closeout-" + THREADS.incrementAndGet());
            t.setDaemon(true);
            t.start();
        }
        return closeout;
    }

    /** One budget for the whole close-out: every GET/finish is bounded by the time that is left
     * (OkHttp call timeout = whole call), and nothing is started once it is used up. */
    private FinishResult runCloseout(String reason, long cap) {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(client.timeouts.getCloseoutMillis());
        RuntimeException last = null;
        int attempt = 0;
        while (leftMillis(deadline) > 0) {
            attempt++;
            try {
                SessionView v = client.getSession(sessionId, Math.min(client.timeouts.getRequestMillis(), leftMillis(deadline)));
                if (!v.isTerminal()) {
                    long left = leftMillis(deadline);
                    if (left <= 0) {
                        break;
                    }
                    v = client.finishSession(sessionId, cap, left);
                }
                if (v.isTerminal()) {
                    lock.lock();
                    try {
                        return finalizeResult(v, true, reason);
                    } finally {
                        lock.unlock();
                    }
                }
                last = null;
            } catch (ApiException e) {
                if (!e.isRetryable()) {
                    throw new FinishUnconfirmedException("close-out was refused (" + e.getErrorCode() + ")", sessionId, e);
                }
                last = e;
            } catch (TransportException e) {
                last = e;
            }
            long back = Math.min(250L << Math.min(attempt, 4), Math.max(0, leftMillis(deadline)));
            try {
                Thread.sleep(back);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        throw new FinishUnconfirmedException("the end of the session is not confirmed", sessionId, last);
    }

    private static long leftMillis(long deadlineNanos) {
        return TimeUnit.NANOSECONDS.toMillis(deadlineNanos - System.nanoTime());
    }

        private FinishResult finalizeResult(SessionView v, boolean viaRest, String reason) {
        view = v;
        latestUsage = v.getUsage();
        transcript.merge(v);
        String sid = v.getSessionId() != null ? v.getSessionId() : sessionId;
        String tid = v.getTranscriptionId() != null ? v.getTranscriptionId() : transcriptionId;
        result = new FinishResult(v, sid, tid, transcript.getLines(), viaRest, reason, new ArrayList<String>(serverErrors),
                callbackErrors.get(), droppedEvents.get());
        if (state != State.CLOSED) {
            state = State.COMPLETED;
        }
        changed.signalAll();
        return result;
    }

    // ------------------------------------------------------------------ dispatch

    private void enqueue(RealtimeEvent ev) {
        if (!options.hasConsumers()) {
            return;
        }
        if (!queue.offer(ev)) {
            droppedEvents.incrementAndGet();
            report(new IllegalStateException("event handler queue full; event not delivered (state and transcript "
                    + "snapshot are still updated)"), ev);
        }
    }

    private void report(final Throwable error, final RealtimeEvent ev) {
        if (options.onCallbackError == null && options.listeners.isEmpty()) {
            return;
        }
        ExecutorService r = reporter;
        if (r == null) {
            synchronized (this) {
                if (reporter == null) {
                    reporter = Executors.newSingleThreadExecutor(new ThreadFactory() {
                        @Override
                        public Thread newThread(Runnable task) {
                            Thread t = new Thread(task, "myvocal-stt-report-" + THREADS.incrementAndGet());
                            t.setDaemon(true);
                            return t;
                        }
                    });
                }
                r = reporter;
            }
        }
        try {
            r.execute(new Runnable() {
                @Override
                public void run() {
                    notifyError(error, ev);
                }
            });
        } catch (java.util.concurrent.RejectedExecutionException ignored) {
            // closing
        }
    }

    private void notifyError(Throwable error, RealtimeEvent ev) {
        if (options.onCallbackError != null) {
            try {
                options.onCallbackError.accept(error, ev);
            } catch (Throwable ignored) {
                // never propagate
            }
        }
        for (RealtimeListener l : options.listeners) {
            try {
                l.onCallbackError(error, ev);
            } catch (Throwable ignored) {
                // never propagate
            }
        }
    }

    private void dispatchLoop() {
        while (true) {
            Object o;
            try {
                o = queue.take();
            } catch (InterruptedException e) {
                return;
            }
            if (o == STOP) {
                return;
            }
            deliver((RealtimeEvent) o);
        }
    }

    private void deliver(RealtimeEvent ev) {
        List<Consumer<RealtimeEvent>> targets = new ArrayList<Consumer<RealtimeEvent>>();
        List<Consumer<RealtimeEvent>> byType = options.handlers.get(ev.getType());
        if (byType != null) {
            targets.addAll(byType);
        }
        List<Consumer<RealtimeEvent>> all = options.handlers.get("*");
        if (all != null) {
            targets.addAll(all);
        }
        for (Consumer<RealtimeEvent> h : targets) {
            try {
                h.accept(ev);
            } catch (Throwable e) {
                callbackErrors.incrementAndGet();
                notifyError(e, ev);
            }
        }
        for (RealtimeListener l : options.listeners) {
            try {
                dispatchTo(l, ev);
            } catch (Throwable e) {
                callbackErrors.incrementAndGet();
                notifyError(e, ev);
            }
        }
    }

    private static void dispatchTo(RealtimeListener l, RealtimeEvent ev) {
        l.onEvent(ev);
        String t = ev.getType();
        if (RealtimeEvent.SESSION_READY.equals(t)) {
            l.onReady(ev.getView());
        } else if (RealtimeEvent.TRANSCRIPT_DELTA.equals(t)) {
            l.onTranscriptDelta(ev.getText(), ev);
        } else if (RealtimeEvent.TRANSCRIPT_FINAL.equals(t)) {
            l.onTranscriptFinal(ev.getLine(), ev);
        } else if (RealtimeEvent.TRANSCRIPT_ALIGNMENT.equals(t) || RealtimeEvent.TRANSCRIPT_REVISION.equals(t)
                || RealtimeEvent.TRANSCRIPT_ENTITIES.equals(t)) {
            l.onSupplement(ev);
        } else if (RealtimeEvent.USAGE_UPDATED.equals(t)) {
            l.onUsage(ev.getUsage());
        } else if (RealtimeEvent.SESSION_NOTICE.equals(t)) {
            l.onNotice(ev.getNotice());
        } else if (RealtimeEvent.SESSION_COMPLETED.equals(t)) {
            l.onCompleted(ev.getView());
        } else if (RealtimeEvent.SESSION_ERROR.equals(t)) {
            l.onServerError(ev.getErrorCode());
        }
    }

    // ------------------------------------------------------------------ helpers

    static <T> CompletableFuture<T> async(String name, final Callable<T> work) {
        final InterruptingFuture<T> f = new InterruptingFuture<T>();
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    f.complete(work.call());
                } catch (Throwable e) {
                    f.completeExceptionally(e);
                }
            }
        }, name + "-" + THREADS.incrementAndGet());
        t.setDaemon(true);
        f.worker = t;
        t.start();
        return f;
    }

    private static final class InterruptingFuture<T> extends CompletableFuture<T> {
        volatile Thread worker;

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            boolean done = super.cancel(mayInterruptIfRunning);
            Thread w = worker;
            if (mayInterruptIfRunning && w != null) {
                w.interrupt();
            }
            return done;
        }
    }

    private static final class LimitedInputStream extends InputStream {
        private final InputStream in;
        private long left;

        LimitedInputStream(InputStream in, long limit) {
            this.in = in;
            this.left = limit;
        }

        @Override
        public int read() throws IOException {
            if (left <= 0) {
                return -1;
            }
            int b = in.read();
            if (b >= 0) {
                left--;
            }
            return b;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (left <= 0) {
                return -1;
            }
            int n = in.read(b, off, (int) Math.min(len, left));
            if (n > 0) {
                left -= n;
            }
            return n;
        }
    }
}
