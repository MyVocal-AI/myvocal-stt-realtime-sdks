# Java runtime dependencies

`ai.myvocal:myvocal-stt-realtime:1.0.0` (Java 8+ bytecode, no Spring):

| Artifact | Version | Why | Licence |
|---|---|---|---|
| `com.squareup.okhttp3:okhttp-jvm` | 5.5.0 | HTTP + WebSocket (REST, stream, ping, proxy, TLS) | Apache-2.0 |
| `com.squareup.okio:okio-jvm` | 3.18.1 | transitive (OkHttp I/O) | Apache-2.0 |
| `org.jetbrains.kotlin:kotlin-stdlib` | 2.1.21 | transitive (OkHttp runtime) | Apache-2.0 |
| `org.jetbrains:annotations` | 13.0 | transitive (compile-time annotations) | Apache-2.0 |

All are Java 8 class files (major 52) and are checked by the runtime matrix on JDK 8, 11, 13,
17 and 21. JSON is handled by a small internal parser (no JSON library). Test scope only:
`org.junit.jupiter:junit-jupiter` 5.14.4.

A borrowed `OkHttpClient` (`RealtimeClient.Builder.httpClient`) must be OkHttp 5.x.
