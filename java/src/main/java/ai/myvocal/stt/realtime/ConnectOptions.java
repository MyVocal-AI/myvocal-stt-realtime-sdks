package ai.myvocal.stt.realtime;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/** Options of {@link RealtimeClient#connect(ConnectOptions)}. Handlers are registered before the
 * socket opens. */
public final class ConnectOptions {
    SessionRequest request;
    String sessionId;
    String idempotencyKey;
    final List<RealtimeListener> listeners = new ArrayList<RealtimeListener>();
    final Map<String, List<Consumer<RealtimeEvent>>> handlers = new LinkedHashMap<String, List<Consumer<RealtimeEvent>>>();
    BiConsumer<Throwable, RealtimeEvent> onCallbackError;

    /** Creates a new session with this request. */
    public ConnectOptions request(SessionRequest v) { this.request = v; return this; }
    /** Attaches to an existing session instead of creating one. */
    public ConnectOptions sessionId(String v) { this.sessionId = v; return this; }
    /** Idempotency-Key of the create request (generated when null). */
    public ConnectOptions idempotencyKey(String v) { this.idempotencyKey = v; return this; }
    public ConnectOptions listener(RealtimeListener v) { this.listeners.add(v); return this; }

    /** Handler for one event type, or {@code "*"} for every event. */
    public ConnectOptions on(String eventType, Consumer<RealtimeEvent> handler) {
        List<Consumer<RealtimeEvent>> l = handlers.get(eventType);
        if (l == null) {
            l = new ArrayList<Consumer<RealtimeEvent>>();
            handlers.put(eventType, l);
        }
        l.add(handler);
        return this;
    }

    public ConnectOptions onEvent(Consumer<RealtimeEvent> handler) { return on("*", handler); }
    public ConnectOptions onCallbackError(BiConsumer<Throwable, RealtimeEvent> v) { this.onCallbackError = v; return this; }

    boolean hasConsumers() {
        return !listeners.isEmpty() || !handlers.isEmpty();
    }
}
