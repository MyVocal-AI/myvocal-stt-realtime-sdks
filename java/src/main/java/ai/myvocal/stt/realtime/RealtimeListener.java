package ai.myvocal.stt.realtime;

/** Typed event callbacks. Override what you need; every method has an empty default. Callbacks run
 * on the session's callback thread, never on the network thread, so a slow listener does not delay
 * acknowledgements. An exception thrown here goes to {@link #onCallbackError} and never counts as a
 * server failure. */
public interface RealtimeListener {
    default void onEvent(RealtimeEvent event) { }
    default void onReady(SessionView view) { }
    default void onTranscriptDelta(String text, RealtimeEvent event) { }
    default void onTranscriptFinal(TranscriptLine line, RealtimeEvent event) { }
    /** alignment, revision and entities supplements, merged into {@link RealtimeSession#getTranscript()}. */
    default void onSupplement(RealtimeEvent event) { }
    default void onUsage(Usage usage) { }
    default void onNotice(Notice notice) { }
    default void onCompleted(SessionView view) { }
    default void onServerError(String errorCode) { }
    /** A listener threw, or an event could not be delivered (queue overflow). */
    default void onCallbackError(Throwable error, RealtimeEvent event) { }
}
