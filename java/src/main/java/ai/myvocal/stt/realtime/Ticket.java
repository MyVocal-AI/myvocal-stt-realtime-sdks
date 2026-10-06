package ai.myvocal.stt.realtime;

import java.util.Map;

/** A single-use browser ticket for your own front end. This SDK never connects with it.
 * {@link #getTicket()} is a secret: never log it. {@link #toString()} hides it. */
public final class Ticket {
    private final String ticketId;
    private final String sessionId;
    private final String streamUrl;
    private final long expiresInSeconds;
    private final String ticket;

    Ticket(Map<String, Object> p) {
        ticketId = Json.asString(p.get("ticketId"));
        sessionId = Json.asString(p.get("sessionId"));
        streamUrl = Json.asString(p.get("streamUrl"));
        expiresInSeconds = Json.asLong(p.get("expiresInSeconds"), 0L);
        ticket = Json.asString(p.get("ticket"));
    }

    public String getTicketId() { return ticketId; }
    public String getSessionId() { return sessionId; }
    public String getStreamUrl() { return streamUrl; }
    public long getExpiresInSeconds() { return expiresInSeconds; }
    public String getTicket() { return ticket; }

    @Override
    public String toString() {
        return "Ticket{ticketId=" + ticketId + ", sessionId=" + sessionId + ", expiresInSeconds=" + expiresInSeconds + "}";
    }
}
