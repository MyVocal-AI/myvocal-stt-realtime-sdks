package ai.myvocal.stt.realtime;

import static ai.myvocal.stt.realtime.Fx.check;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/** A14: every shipped Java example compiles and runs unchanged against the protocol fixture. The
 * examples are compiled against the code sources of the SDK classes actually loaded by this test
 * (the source build or the installed JAR) and their runtime dependencies. java.class.path alone
 * is not enough: launchers such as the JUnit ConsoleLauncher load them through their own class
 * loader. */
class A14ExamplesTest {
    @Test
    void a14Examples() throws Exception {
        Fx f = Fx.plain();
        JavaCompiler javac = ToolProvider.getSystemJavaCompiler();
        Assumptions.assumeTrue(javac != null, "needs a JDK");
        Path src = Fx.ROOT.resolve("java/examples/src/main/java/example");
        Path out = Files.createTempDirectory("stt-examples");
        String cp = sdkClassPath();
        List<String> args = new ArrayList<String>(Arrays.asList("-d", out.toString(), "-cp", cp, "-encoding", "UTF-8"));
        try (Stream<Path> files = Files.list(src)) {
            args.addAll(files.filter(p -> p.toString().endsWith(".java")).map(Path::toString).collect(Collectors.toList()));
        }
        check(javac.run(null, null, null, args.toArray(new String[0])) == 0, "javac failed");
        String wav = Fx.ROOT.resolve("fixtures/audio/tone_pcm_s16le_16000.wav").toString();
        String raw = Fx.ROOT.resolve("fixtures/audio/tone_pcm_s16le_16000.raw").toString();
        String[][] cases = {
            {"FileStream", wav, "", "status: COMPLETED"},
            {"ByteStream", raw, "", "status: COMPLETED"},
            {"ManualVad", wav, "manual", "manual -> COMPLETED"},
            {"ManualVad", wav, "vad", "vad -> COMPLETED"},
            {"PauseResume", wav, "", "status: COMPLETED / GET: COMPLETED"},
            {"AbnormalFinish", wav, "", "closed out: COMPLETED via REST: true reason: DISCONNECTED"},
        };
        String java = System.getProperty("java.home") + File.separator + "bin" + File.separator + "java";
        for (String[] tc : cases) {
            List<String> cmd = new ArrayList<String>(Arrays.asList(java, "-cp", out + File.pathSeparator + cp, "example." + tc[0], tc[1]));
            if (!tc[2].isEmpty()) {
                cmd.add(tc[2]);
            }
            ProcessBuilder pb = new ProcessBuilder(cmd).redirectErrorStream(true);
            pb.environment().put("MYVOCAL_ACCESS_KEY", Fx.KEY);
            pb.environment().put("MYVOCAL_API_BASE", f.base);
            Process p = pb.start();
            String text = new String(Fx.readAll(p.getInputStream()), StandardCharsets.UTF_8);
            int code = p.waitFor();
            check(code == 0 && text.contains(tc[3]), tc[0], tc[2], code, text);
            check(!text.contains(Fx.KEY), tc[0], "printed the key");
        }
    }

    private static String sdkClassPath() throws Exception {
        java.util.LinkedHashSet<String> parts = new java.util.LinkedHashSet<String>();
        for (Class<?> c : new Class<?>[] {RealtimeClient.class, okhttp3.OkHttpClient.class, okio.Buffer.class, kotlin.Unit.class}) {
            java.security.CodeSource cs = c.getProtectionDomain().getCodeSource();
            if (cs != null && cs.getLocation() != null) {
                parts.add(java.nio.file.Paths.get(cs.getLocation().toURI()).toString());
            }
        }
        for (String e : System.getProperty("java.class.path").split(java.util.regex.Pattern.quote(File.pathSeparator))) {
            if (!e.isEmpty()) {
                parts.add(e);
            }
        }
        return String.join(File.pathSeparator, parts);
    }
}
