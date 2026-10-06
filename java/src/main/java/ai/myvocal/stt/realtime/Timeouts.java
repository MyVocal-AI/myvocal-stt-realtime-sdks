package ai.myvocal.stt.realtime;

/** Configurable deadlines in milliseconds. Immutable; {@code with*} returns a copy. */
public final class Timeouts {
    private final long requestMillis;
    private final long createMillis;
    private final long connectMillis;
    private final long ackMillis;
    private final long finishMillis;
    private final long closeoutMillis;
    private final long closeMillis;
    private final long backpressureMillis;

    /** Defaults: request 30 s, create 30 s, connect 15 s, ack 10 s, finish 60 s, close-out 70 s, close 5 s,
     * backpressure = ack. */
    public Timeouts() {
        this(30_000, 30_000, 15_000, 10_000, 60_000, 70_000, 5_000, 0);
    }

    private Timeouts(long request, long create, long connect, long ack, long finish, long closeout, long close,
                     long backpressure) {
        this.requestMillis = request;
        this.createMillis = create;
        this.connectMillis = connect;
        this.ackMillis = ack;
        this.finishMillis = finish;
        this.closeoutMillis = closeout;
        this.closeMillis = close;
        this.backpressureMillis = backpressure;
    }

    /** One REST request. */
    public long getRequestMillis() { return requestMillis; }
    /** The whole create call including idempotent retries. */
    public long getCreateMillis() { return createMillis; }
    /** Socket open plus {@code session.ready}. */
    public long getConnectMillis() { return connectMillis; }
    /** Unacknowledged audio without acknowledgement progress; idle time never counts. */
    public long getAckMillis() { return ackMillis; }
    /** After {@code session.finish}, waiting for {@code session.completed}. */
    public long getFinishMillis() { return finishMillis; }
    /** REST close-out (query + idempotent finish, with retries). */
    public long getCloseoutMillis() { return closeoutMillis; }
    /** Local resource release in {@code close()}. */
    public long getCloseMillis() { return closeMillis; }
    /** How long {@code sendAudio} may wait for buffer room (defaults to ack). */
    public long getBackpressureMillis() { return backpressureMillis > 0 ? backpressureMillis : ackMillis; }

    public Timeouts withRequestMillis(long v) { return new Timeouts(v, createMillis, connectMillis, ackMillis, finishMillis, closeoutMillis, closeMillis, backpressureMillis); }
    public Timeouts withCreateMillis(long v) { return new Timeouts(requestMillis, v, connectMillis, ackMillis, finishMillis, closeoutMillis, closeMillis, backpressureMillis); }
    public Timeouts withConnectMillis(long v) { return new Timeouts(requestMillis, createMillis, v, ackMillis, finishMillis, closeoutMillis, closeMillis, backpressureMillis); }
    public Timeouts withAckMillis(long v) { return new Timeouts(requestMillis, createMillis, connectMillis, v, finishMillis, closeoutMillis, closeMillis, backpressureMillis); }
    public Timeouts withFinishMillis(long v) { return new Timeouts(requestMillis, createMillis, connectMillis, ackMillis, v, closeoutMillis, closeMillis, backpressureMillis); }
    public Timeouts withCloseoutMillis(long v) { return new Timeouts(requestMillis, createMillis, connectMillis, ackMillis, finishMillis, v, closeMillis, backpressureMillis); }
    public Timeouts withCloseMillis(long v) { return new Timeouts(requestMillis, createMillis, connectMillis, ackMillis, finishMillis, closeoutMillis, v, backpressureMillis); }
    public Timeouts withBackpressureMillis(long v) { return new Timeouts(requestMillis, createMillis, connectMillis, ackMillis, finishMillis, closeoutMillis, closeMillis, v); }
}
