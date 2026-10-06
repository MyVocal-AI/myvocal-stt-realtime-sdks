package ai.myvocal.stt.realtime;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.Proxy;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

/** Test support: starts the shared Python protocol fixture (tools/fixture/server.py) and reads its
 * control endpoints. FIXTURE_PYTHON selects the interpreter; STT_REPO_ROOT the repository root. */
final class Fx {
    static final Path ROOT = root();
    static final Map<String, Object> SCENARIOS = Json.parseObject(read(ROOT.resolve("fixtures/scenarios.json")));
    static final String KEY = (String) SCENARIOS.get("accessKey");
    private static Fx plain;
    private static Fx tls;

    final Process process;
    final String base;
    final int proxyPort;
    final boolean isTls;

    private static Path root() {
        String env = System.getenv("STT_REPO_ROOT");
        if (env != null && !env.isEmpty()) {
            return Paths.get(env).toAbsolutePath().normalize();
        }
        Path p = Paths.get("").toAbsolutePath();
        while (p != null && !Files.exists(p.resolve("fixtures/scenarios.json"))) {
            p = p.getParent();
        }
        if (p == null) {
            throw new IllegalStateException("set STT_REPO_ROOT");
        }
        return p;
    }

    static String read(Path p) {
        try {
            return new String(Files.readAllBytes(p), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Fx(String... flags) throws IOException {
        String py = System.getenv("FIXTURE_PYTHON");
        if (py == null || py.isEmpty()) {
            py = System.getProperty("os.name").toLowerCase().contains("win") ? "python" : "python3";
        }
        List<String> cmd = new ArrayList<String>(Arrays.asList(py, ROOT.resolve("tools/fixture/server.py").toString()));
        cmd.addAll(Arrays.asList(flags));
        ProcessBuilder pb = new ProcessBuilder(cmd).redirectError(ProcessBuilder.Redirect.INHERIT);
        process = pb.start();
        BufferedReader r = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8));
        String line = r.readLine();
        if (line == null) {
            throw new IOException("fixture did not start");
        }
        Map<String, Object> info = Json.parseObject(line);
        isTls = Boolean.TRUE.equals(info.get("tls"));
        base = (isTls ? "https" : "http") + "://127.0.0.1:" + info.get("http");
        proxyPort = info.get("proxy") instanceof Long ? ((Long) info.get("proxy")).intValue() : 0;
        Runtime.getRuntime().addShutdownHook(new Thread(process::destroyForcibly));
    }

    static synchronized Fx plain() {
        if (plain == null) {
            try {
                plain = new Fx();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return plain;
    }

    static synchronized Fx tls() {
        if (tls == null) {
            try {
                tls = new Fx("--tls", "--proxy");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return tls;
    }

    // ------------------------------------------------------------ control endpoints

    Object get(String path) {
        return Json.parse(http("GET", path));
    }

    void post(String path) {
        http("POST", path);
    }

    private String http(String method, String path) {
        try {
            HttpURLConnection c = (HttpURLConnection) new URL(base + "/__fixture" + path).openConnection(Proxy.NO_PROXY);
            if (c instanceof HttpsURLConnection) {
                ((HttpsURLConnection) c).setSSLSocketFactory(sslContext().getSocketFactory());
            }
            c.setRequestMethod(method);
            if ("POST".equals(method)) {
                c.setDoOutput(true);
                c.getOutputStream().close();
            }
            try (InputStream in = c.getInputStream()) {
                return new String(readAll(in), StandardCharsets.UTF_8);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    Map<String, Object> record(String sessionId) {
        return (Map<String, Object>) get("/sessions/" + sessionId);
    }

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> byTitle(String title) {
        return (List<Map<String, Object>>) get("/by-title/" + title);
    }

    // ------------------------------------------------------------ clients and data

    RealtimeClient client(Consumer<RealtimeClient.Builder> mod) {
        RealtimeClient.Builder b = RealtimeClient.builder(KEY).baseUrl(base).proxy(Proxy.NO_PROXY);
        if (isTls) {
            b.sslSocketFactory(sslContext().getSocketFactory(), trustManager());
        }
        if (mod != null) {
            mod.accept(b);
        }
        return b.build();
    }

    RealtimeClient client() {
        return client(null);
    }

    static X509TrustManager trustManager() {
        try {
            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            Certificate ca;
            try (InputStream in = Files.newInputStream(ROOT.resolve("fixtures/tls/ca.crt"))) {
                ca = cf.generateCertificate(in);
            }
            KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
            ks.load(null, null);
            ks.setCertificateEntry("fixture-ca", ca);
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(ks);
            return (X509TrustManager) tmf.getTrustManagers()[0];
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static SSLContext sslContext() {
        try {
            SSLContext ctx = SSLContext.getInstance("TLS");
            ctx.init(null, new javax.net.ssl.TrustManager[] {trustManager()}, null);
            return ctx;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static byte[] audio(String enc, double seconds) {
        try {
            byte[] data = Files.readAllBytes(ROOT.resolve("fixtures/audio/tone_" + enc + ".raw"));
            if (seconds > 0) {
                AudioFormat f = AudioFormat.of(enc);
                int n = (int) (f.getSampleRateHz() * seconds) * f.getBytesPerSample();
                data = Arrays.copyOf(data, n);
            }
            return data;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static byte[] readAll(InputStream in) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] b = new byte[8192];
        int n;
        while ((n = in.read(b)) > 0) {
            out.write(b, 0, n);
        }
        return out.toByteArray();
    }

    static String sha(byte[] b) {
        try {
            StringBuilder s = new StringBuilder();
            for (byte x : MessageDigest.getInstance("SHA-256").digest(b)) {
                s.append(String.format("%02x", x));
            }
            return s.toString();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static SessionRequest req(String title, String enc) {
        return new SessionRequest().title(title).options(new SessionOptions().inputEncoding(enc == null ? "pcm_s16le_16000" : enc));
    }

    static SessionRequest req(String title) {
        return req(title, null);
    }

    static ConnectOptions opts(String title) {
        return new ConnectOptions().request(req(title));
    }

    static void sendAll(RealtimeSession s, byte[] data, int chunk) {
        for (int i = 0; i < data.length; i += chunk) {
            s.sendAudio(data, i, Math.min(chunk, data.length - i), null);
        }
    }

    @SuppressWarnings("unchecked")
    static Map<String, Object> scenario(String id) {
        for (Object o : (List<Object>) SCENARIOS.get("scenarios")) {
            Map<String, Object> m = (Map<String, Object>) o;
            if (id.equals(m.get("id"))) {
                return m;
            }
        }
        throw new IllegalArgumentException(id);
    }

    static long num(Object v) {
        if (v == null) {
            return -1;
        }
        if (v instanceof Number) {
            return ((Number) v).longValue();
        }
        return Long.parseLong(v.toString());
    }

    @SuppressWarnings("unchecked")
    static long maxClaim(Map<String, Object> rec) {
        long m = 0;
        for (Object c : (List<Object>) rec.get("capturedClaims")) {
            m = Math.max(m, num(c));
        }
        return m;
    }

    @SuppressWarnings("unchecked")
    static List<Map<String, Object>> list(Map<String, Object> rec, String key) {
        return (List<Map<String, Object>>) rec.get(key);
    }

    static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    static void check(boolean ok, Object... detail) {
        assertTrue(ok, Arrays.deepToString(detail));
    }
}
