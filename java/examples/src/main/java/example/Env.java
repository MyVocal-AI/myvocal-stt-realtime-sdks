package example;

import ai.myvocal.stt.realtime.RealtimeClient;

/** Builds a client from MYVOCAL_ACCESS_KEY (required) and MYVOCAL_API_BASE (optional). The key is
 * read from the environment and never printed. */
final class Env {
    private Env() {
    }

    static RealtimeClient client() {
        String key = System.getenv("MYVOCAL_ACCESS_KEY");
        if (key == null || key.isEmpty()) {
            System.err.println("set MYVOCAL_ACCESS_KEY");
            System.exit(2);
        }
        RealtimeClient.Builder b = RealtimeClient.builder(key);
        String base = System.getenv("MYVOCAL_API_BASE");
        if (base != null && !base.isEmpty()) {
            b.baseUrl(base);
        }
        return b.build();
    }
}
