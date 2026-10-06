package ai.myvocal.stt.realtime;

import java.io.IOException;
import java.net.Proxy;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.X509TrustManager;

import okhttp3.Call;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;

/** MyVocal STT Realtime client: REST helpers plus per-session connections. Thread-safe; create
 * one per application and {@link #close()} it at shutdown. */
public final class RealtimeClient implements AutoCloseable {
    /** SDK version. */
    public static final String VERSION = "1.0.0";
    static final String USER_AGENT = "myvocal-stt-realtime-java/" + VERSION;
    static final String REALTIME_PATH = "/sound_clone/api/v1/stt/realtime";
    private static final MediaType JSON = MediaType.get("application/json; charset=utf-8");
    private static final Pattern URL_PATTERN = Pattern.compile("(?i)\\b(?:https?|wss?)://\\S+");

    private final String accessKey;
    final String baseUrl;
    final Timeouts timeouts;
    final long pingIntervalMillis;
    final int frameMs;
    final int maxBufferedMs;
    final int eventQueueSize;
    final int maxCreateRetries;
    final int maxFrameRetries;
    final boolean closeoutOnDisconnect;
    private final OkHttpClient base;
    private final boolean ownsHttp;
    final OkHttpClient rest;
    final OkHttpClient ws;
    private final Set<RealtimeSession> sessions = Collections.newSetFromMap(new ConcurrentHashMap<RealtimeSession, Boolean>());
    private volatile boolean closed;

    private RealtimeClient(Builder b) {
        this.accessKey = b.accessKey;
        this.baseUrl = b.baseUrl;
        this.timeouts = b.timeouts;
        this.pingIntervalMillis = b.pingIntervalMillis;
        this.frameMs = b.frameMs;
        this.maxBufferedMs = b.maxBufferedMs;
        this.eventQueueSize = b.eventQueueSize;
        this.maxCreateRetries = b.maxCreateRetries;
        this.maxFrameRetries = b.maxFrameRetries;
        this.closeoutOnDisconnect = b.closeoutOnDisconnect;
        this.ownsHttp = b.httpClient == null;
        OkHttpClient.Builder ob = ownsHttp ? new OkHttpClient.Builder() : b.httpClient.newBuilder();
        if (ownsHttp) {
            ob.connectTimeout(timeouts.getConnectMillis(), TimeUnit.MILLISECONDS);
        }
        if (b.proxy != null) {
            ob.proxy(b.proxy);
        }
        if (b.sslSocketFactory != null) {
            ob.sslSocketFactory(b.sslSocketFactory, b.trustManager);
        }
        this.base = ob.build();
        this.rest = base.newBuilder().readTimeout(timeouts.getRequestMillis(), TimeUnit.MILLISECONDS).build();
        this.ws = base.newBuilder().readTimeout(0, TimeUnit.MILLISECONDS)
                .pingInterval(Math.max(0, pingIntervalMillis), TimeUnit.MILLISECONDS).build();
    }

    /** Starts a builder. Only the access key is required. */
    public static Builder builder(String accessKey) {
        return new Builder(accessKey);
    }

    @Override
    public String toString() {
        return "RealtimeClient{baseUrl=" + baseUrl + "}";
    }

    // ------------------------------------------------------------------ REST helpers

    /** Creates a session with a generated Idempotency-Key. */
    public SessionView createSession(SessionRequest request) {
        return createSession(request, null);
    }

    /** Creates a session. Retries (transport errors, 5xx/429, retryable envelopes) reuse the same
     * Idempotency-Key and body, so a lost response never creates a second session. */
    public SessionView createSession(SessionRequest request, String idempotencyKey) {
        String key = idempotencyKey != null ? idempotencyKey : UUID.randomUUID().toString();
        String body = Json.write((request != null ? request : new SessionRequest()).toWire());
        long deadline = System.currentTimeMillis() + timeouts.getCreateMillis();
        int attempts = 1 + Math.max(0, maxCreateRetries);
        MyVocalException last = null;
        for (int attempt = 0; attempt < attempts; attempt++) {
            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) {
                break;
            }
            try {
                return new SessionView(call("POST", "/sessions", body, key, Math.min(remaining, timeouts.getRequestMillis())));
            } catch (ApiException e) {
                if (!e.isRetryable()) {
                    throw e;
                }
                last = e;
            } catch (TransportException e) {
                if (e.getStatus() != 0 && e.getStatus() < 500 && e.getStatus() != 429) {
                    throw e;
                }
                last = e;
            }
            if (attempt + 1 < attempts) {
                sleep(Math.min(Math.min(250L << attempt, 2000L), Math.max(0, deadline - System.currentTimeMillis())));
            }
        }
        throw last != null ? last : new TransportException("create session timed out", 0, null);
    }

    public SessionView getSession(String sessionId) {
        return getSession(sessionId, timeouts.getRequestMillis());
    }

    /** GET with an explicit whole-call timeout (used by close-out to pass on its remaining budget). */
    SessionView getSession(String sessionId, long timeoutMillis) {
        return new SessionView(call("GET", "/sessions/" + id(sessionId), null, null, timeoutMillis));
    }

    /** Closes the current epoch over REST. {@code capturedSamples} null = not declared. */
    public SessionView pauseSession(String sessionId, Long capturedSamples) {
        return new SessionView(call("POST", "/sessions/" + id(sessionId) + "/pause", cap(capturedSamples), null,
                timeouts.getRequestMillis()));
    }

    /** Opens the next epoch over REST. */
    public SessionView resumeSession(String sessionId, Long capturedSamples) {
        return new SessionView(call("POST", "/sessions/" + id(sessionId) + "/resume", cap(capturedSamples), null,
                timeouts.getRequestMillis()));
    }

    /** Idempotent REST finish; a repeated finish returns the same task without a second charge. */
    public SessionView finishSession(String sessionId, Long capturedSamples) {
        return new SessionView(call("POST", "/sessions/" + id(sessionId) + "/finish", cap(capturedSamples), null,
                Math.max(timeouts.getRequestMillis(), 70_000L)));
    }

    SessionView finishSession(String sessionId, Long capturedSamples, long timeoutMillis) {
        return new SessionView(call("POST", "/sessions/" + id(sessionId) + "/finish", cap(capturedSamples), null,
                timeoutMillis));
    }

    /** Issues a single-use browser ticket for your own front end. This SDK never connects with a
     * ticket. Never log {@link Ticket#getTicket()}. */
    public Ticket createTicket(String sessionId) {
        return new Ticket(call("POST", "/sessions/" + id(sessionId) + "/tickets", "{}", null, timeouts.getRequestMillis()));
    }

    private static String cap(Long captured) {
        return captured == null ? "{}" : "{\"capturedSamples\":\"" + captured + "\"}";
    }

    private static String id(String sessionId) {
        if (sessionId == null || sessionId.isEmpty() || sessionId.indexOf('/') >= 0 || sessionId.indexOf('?') >= 0
                || sessionId.indexOf('#') >= 0) {
            throw new IllegalArgumentException("invalid session id");
        }
        return sessionId;
    }

    Map<String, Object> call(String method, String path, String body, String idempotencyKey, long timeoutMillis) {
        String op = method + " " + REALTIME_PATH + path;
        Request.Builder rb = new Request.Builder().url(baseUrl + REALTIME_PATH + path)
                .header("accessKey", accessKey)
                .header("Accept", "application/json")
                .header("User-Agent", USER_AGENT);
        if (idempotencyKey != null) {
            rb.header("Idempotency-Key", idempotencyKey);
        }
        rb.method(method, body == null ? null : RequestBody.create(body, JSON));
        Call call = rest.newCall(rb.build());
        call.timeout().timeout(Math.max(1, timeoutMillis), TimeUnit.MILLISECONDS);
        Map<String, Object> env;
        int status;
        try (Response resp = call.execute()) {
            status = resp.code();
            ResponseBody rbody = resp.body();
            String text = rbody == null ? "" : rbody.string();
            try {
                env = Json.parseObject(text);
            } catch (RuntimeException e) {
                throw new TransportException(op + ": HTTP " + status + " without a MyVocal envelope", status, null);
            }
        } catch (IOException e) {
            throw new TransportException(op + " failed (" + safe(e) + ")", 0, null);
        }
        if (!env.containsKey("code")) {
            throw new TransportException(op + ": HTTP " + status + " without a MyVocal envelope", status, null);
        }
        if (Json.asLong(env.get("code"), -1L) != 1L) {
            throw ApiException.fromEnvelope(env);
        }
        if (status >= 400) {
            throw new TransportException(op + ": HTTP " + status, status, null);
        }
        return Json.asMap(env.get("data"));
    }

    static String safe(Throwable e) {
        String m = e.getClass().getSimpleName() + (e.getMessage() == null ? "" : ": " + e.getMessage());
        return URL_PATTERN.matcher(m).replaceAll("<url>");
    }

    // ------------------------------------------------------------------ sessions

    /** Creates a session from {@code request} and connects. */
    public RealtimeSession connect(SessionRequest request) {
        return connect(new ConnectOptions().request(request));
    }

    /** Creates a session (or attaches to {@code sessionId}), opens its socket with the accessKey
     * header and waits for {@code session.ready}. */
    public RealtimeSession connect(ConnectOptions options) {
        if (closed) {
            throw new SessionStateException("client is closed");
        }
        boolean created = options.sessionId == null;
        SessionView view = created ? createSession(options.request, options.idempotencyKey) : getSession(options.sessionId);
        AudioFormat format;
        try {
            format = resolveFormat(options.request, view);
        } catch (AudioFormatException e) {
            if (created) {
                quietFinish(view.getSessionId());
            }
            throw e;
        }
        RealtimeSession s = new RealtimeSession(this, view, format, options);
        try {
            s.open(streamUrl(view), accessKey);
        } catch (MyVocalException e) {
            s.close();
            if (created) {
                quietFinish(view.getSessionId());
            }
            if (e instanceof TransportException) {
                ((TransportException) e).setSessionId(view.getSessionId());
            }
            throw e;
        }
        sessions.add(s);
        return s;
    }

    /** {@link #connect(ConnectOptions)} on a background thread. */
    public CompletableFuture<RealtimeSession> connectAsync(final ConnectOptions options) {
        return RealtimeSession.async("myvocal-stt-connect", new java.util.concurrent.Callable<RealtimeSession>() {
            @Override
            public RealtimeSession call() {
                return connect(options);
            }
        });
    }

    void forget(RealtimeSession s) {
        sessions.remove(s);
    }

    private void quietFinish(String sessionId) {
        if (sessionId == null) {
            return;
        }
        try {
            finishSession(sessionId, null, timeouts.getRequestMillis());
        } catch (RuntimeException ignored) {
            // best effort: a session without audio settles as FAILED without charge
        }
    }

    String streamUrl(SessionView v) {
        String path = v.getStreamUrl();
        if (path == null || path.isEmpty()) {
            path = REALTIME_PATH + "/sessions/" + v.getSessionId() + "/stream";
        }
        String full = path.matches("(?i)^(https?|wss?)://.*") ? path : baseUrl + (path.startsWith("/") ? path : "/" + path);
        // OkHttp takes http(s) URLs for web sockets; ws(s) are mapped to http(s) and back on the wire.
        if (full.regionMatches(true, 0, "wss://", 0, 6)) {
            full = "https://" + full.substring(6);
        } else if (full.regionMatches(true, 0, "ws://", 0, 5)) {
            full = "http://" + full.substring(5);
        }
        HttpUrl url = HttpUrl.parse(full);
        if (url == null) {
            throw new TransportException("invalid stream URL", 0, null);
        }
        return url.newBuilder().fragment(null).build().toString();
    }

    static AudioFormat resolveFormat(SessionRequest request, SessionView view) {
        Object eff = view.getEffectiveOptions().get("inputEncoding");
        String effective = eff instanceof String ? (String) eff : null;
        String requested = request == null ? null : request.requestedEncoding();
        if (requested != null && effective != null && !requested.trim().equalsIgnoreCase(effective)) {
            throw new AudioFormatException("requested inputEncoding " + requested + " but the session uses " + effective);
        }
        AudioFormat f = AudioFormat.of(effective != null ? effective : requested);
        Long rate = Json.optLong(view.getEffectiveOptions().get("sampleRateHz"));
        if (rate != null && rate != f.getSampleRateHz()) {
            throw new AudioFormatException("session sampleRateHz " + rate + " does not match " + f.getEncoding());
        }
        return f;
    }

    /** Closes this client's sessions (local resources only) and the HTTP resources it owns. A
     * borrowed {@link OkHttpClient} is never shut down. Idempotent. */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        for (RealtimeSession s : sessions.toArray(new RealtimeSession[0])) {
            s.close();
        }
        if (ownsHttp) {
            base.dispatcher().executorService().shutdown();
            base.connectionPool().evictAll();
        }
    }

    static void sleep(long millis) {
        if (millis <= 0) {
            return;
        }
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new java.util.concurrent.CancellationException("interrupted");
        }
    }

    /** Builder of {@link RealtimeClient}. */
    public static final class Builder {
        private final String accessKey;
        private String baseUrl = "https://api.myvocal.ai";
        private Timeouts timeouts = new Timeouts();
        private long pingIntervalMillis = 20_000;
        private int frameMs = 100;
        private int maxBufferedMs = 30_000;
        private int eventQueueSize = 10_000;
        private int maxCreateRetries = 2;
        private int maxFrameRetries = 3;
        private boolean closeoutOnDisconnect = true;
        private OkHttpClient httpClient;
        private Proxy proxy;
        private SSLSocketFactory sslSocketFactory;
        private X509TrustManager trustManager;

        private Builder(String accessKey) {
            if (accessKey == null || accessKey.trim().isEmpty()) {
                throw new IllegalArgumentException("accessKey is required");
            }
            this.accessKey = accessKey;
        }

        /** Default {@code https://api.myvocal.ai}; http maps to ws, https to wss. */
        public Builder baseUrl(String v) {
            String b = v == null ? "" : v.replaceAll("/+$", "");
            if (!b.startsWith("http://") && !b.startsWith("https://")) {
                throw new IllegalArgumentException("baseUrl must be http:// or https://");
            }
            this.baseUrl = b;
            return this;
        }

        public Builder timeouts(Timeouts v) { this.timeouts = v; return this; }
        /** Client WebSocket ping interval (default 20 s; 0 disables). Unrelated to heartbeatIntervalMs. A
         * missing pong within the interval fails the socket. */
        public Builder pingIntervalMillis(long v) { this.pingIntervalMillis = v; return this; }
        /** Approximate audio frame duration (default 100 ms). */
        public Builder frameMs(int v) { this.frameMs = v; return this; }
        /** Bound of audio held by the SDK and not yet confirmed (default 30000 ms). */
        public Builder maxBufferedMs(int v) { this.maxBufferedMs = v; return this; }
        /** Bound of events waiting for callbacks (default 10000). */
        public Builder eventQueueSize(int v) { this.eventQueueSize = v; return this; }
        public Builder maxCreateRetries(int v) { this.maxCreateRetries = v; return this; }
        public Builder maxFrameRetries(int v) { this.maxFrameRetries = v; return this; }
        /** Automatic REST close-out after an unexpected disconnect (default true). */
        public Builder closeoutOnDisconnect(boolean v) { this.closeoutOnDisconnect = v; return this; }
        /** Borrowed client (pool, dispatcher, interceptors); never shut down by the SDK. */
        public Builder httpClient(OkHttpClient v) { this.httpClient = v; return this; }
        /** Explicit proxy for REST and WebSocket ({@link Proxy#NO_PROXY} disables). Default: the JVM
         * ProxySelector (system properties). */
        public Builder proxy(Proxy v) { this.proxy = v; return this; }
        /** Custom trust (private CA). TLS verification stays on. */
        public Builder sslSocketFactory(SSLSocketFactory factory, X509TrustManager trustManager) {
            this.sslSocketFactory = factory;
            this.trustManager = trustManager;
            return this;
        }

        public RealtimeClient build() {
            if (frameMs <= 0 || maxBufferedMs < frameMs) {
                throw new IllegalArgumentException("frameMs must be > 0 and maxBufferedMs >= frameMs");
            }
            if (eventQueueSize <= 0) {
                throw new IllegalArgumentException("eventQueueSize must be > 0");
            }
            return new RealtimeClient(this);
        }
    }
}
