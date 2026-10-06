package ai.myvocal.stt.realtime;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Minimal JSON reader/writer (no external dependency). Integers become {@link Long} (or
 * {@link BigInteger} beyond 64 bits), other numbers {@link Double}; objects keep key order. */
final class Json {
    private final String s;
    private int i;

    private Json(String s) {
        this.s = s;
    }

    static Object parse(String text) {
        Json p = new Json(text);
        p.ws();
        Object v = p.value();
        p.ws();
        if (p.i != p.s.length()) {
            throw new IllegalArgumentException("trailing data in JSON");
        }
        return v;
    }

    @SuppressWarnings("unchecked")
    static Map<String, Object> parseObject(String text) {
        Object v = parse(text);
        if (!(v instanceof Map)) {
            throw new IllegalArgumentException("JSON object expected");
        }
        return (Map<String, Object>) v;
    }

    private void ws() {
        while (i < s.length()) {
            char c = s.charAt(i);
            if (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
                i++;
            } else {
                break;
            }
        }
    }

    private Object value() {
        if (i >= s.length()) {
            throw new IllegalArgumentException("unexpected end of JSON");
        }
        char c = s.charAt(i);
        switch (c) {
            case '{':
                return object();
            case '[':
                return array();
            case '"':
                return string();
            case 't':
                expect("true");
                return Boolean.TRUE;
            case 'f':
                expect("false");
                return Boolean.FALSE;
            case 'n':
                expect("null");
                return null;
            default:
                return number();
        }
    }

    private void expect(String word) {
        if (!s.startsWith(word, i)) {
            throw new IllegalArgumentException("invalid JSON literal");
        }
        i += word.length();
    }

    private Map<String, Object> object() {
        Map<String, Object> m = new LinkedHashMap<String, Object>();
        i++;
        ws();
        if (i < s.length() && s.charAt(i) == '}') {
            i++;
            return m;
        }
        while (true) {
            ws();
            String k = string();
            ws();
            if (i >= s.length() || s.charAt(i) != ':') {
                throw new IllegalArgumentException("':' expected");
            }
            i++;
            ws();
            m.put(k, value());
            ws();
            if (i >= s.length()) {
                throw new IllegalArgumentException("unterminated object");
            }
            char c = s.charAt(i++);
            if (c == '}') {
                return m;
            }
            if (c != ',') {
                throw new IllegalArgumentException("',' expected");
            }
        }
    }

    private List<Object> array() {
        List<Object> l = new ArrayList<Object>();
        i++;
        ws();
        if (i < s.length() && s.charAt(i) == ']') {
            i++;
            return l;
        }
        while (true) {
            ws();
            l.add(value());
            ws();
            if (i >= s.length()) {
                throw new IllegalArgumentException("unterminated array");
            }
            char c = s.charAt(i++);
            if (c == ']') {
                return l;
            }
            if (c != ',') {
                throw new IllegalArgumentException("',' expected");
            }
        }
    }

    private String string() {
        if (s.charAt(i) != '"') {
            throw new IllegalArgumentException("string expected");
        }
        i++;
        StringBuilder b = new StringBuilder();
        while (true) {
            if (i >= s.length()) {
                throw new IllegalArgumentException("unterminated string");
            }
            char c = s.charAt(i++);
            if (c == '"') {
                return b.toString();
            }
            if (c != '\\') {
                b.append(c);
                continue;
            }
            char e = s.charAt(i++);
            switch (e) {
                case '"': b.append('"'); break;
                case '\\': b.append('\\'); break;
                case '/': b.append('/'); break;
                case 'b': b.append('\b'); break;
                case 'f': b.append('\f'); break;
                case 'n': b.append('\n'); break;
                case 'r': b.append('\r'); break;
                case 't': b.append('\t'); break;
                case 'u':
                    b.append((char) Integer.parseInt(s.substring(i, i + 4), 16));
                    i += 4;
                    break;
                default:
                    throw new IllegalArgumentException("invalid escape");
            }
        }
    }

    private Object number() {
        int start = i;
        boolean integral = true;
        while (i < s.length()) {
            char c = s.charAt(i);
            if ((c >= '0' && c <= '9') || c == '-' || c == '+') {
                i++;
            } else if (c == '.' || c == 'e' || c == 'E') {
                integral = false;
                i++;
            } else {
                break;
            }
        }
        String t = s.substring(start, i);
        if (t.isEmpty()) {
            throw new IllegalArgumentException("invalid JSON value");
        }
        if (integral) {
            BigInteger bi = new BigInteger(t);
            return bi.bitLength() < 64 ? (Object) Long.valueOf(bi.longValue()) : bi;
        }
        return Double.valueOf(new BigDecimal(t).doubleValue());
    }

    // ------------------------------------------------------------------ writer

    static String write(Object v) {
        StringBuilder b = new StringBuilder();
        write(b, v);
        return b.toString();
    }

    @SuppressWarnings("unchecked")
    private static void write(StringBuilder b, Object v) {
        if (v == null) {
            b.append("null");
        } else if (v instanceof String) {
            quote(b, (String) v);
        } else if (v instanceof Boolean || v instanceof Integer || v instanceof Long || v instanceof BigInteger) {
            b.append(v.toString());
        } else if (v instanceof Double || v instanceof Float) {
            double d = ((Number) v).doubleValue();
            if (Double.isNaN(d) || Double.isInfinite(d)) {
                throw new IllegalArgumentException("non-finite number");
            }
            if (d == Math.rint(d) && Math.abs(d) < 1e15) {
                b.append((long) d);
            } else {
                b.append(BigDecimal.valueOf(d).stripTrailingZeros().toPlainString());
            }
        } else if (v instanceof Number) {
            b.append(v.toString());
        } else if (v instanceof Map) {
            b.append('{');
            boolean first = true;
            for (Map.Entry<String, Object> e : ((Map<String, Object>) v).entrySet()) {
                if (!first) {
                    b.append(',');
                }
                first = false;
                quote(b, e.getKey());
                b.append(':');
                write(b, e.getValue());
            }
            b.append('}');
        } else if (v instanceof Iterable) {
            b.append('[');
            boolean first = true;
            for (Object x : (Iterable<Object>) v) {
                if (!first) {
                    b.append(',');
                }
                first = false;
                write(b, x);
            }
            b.append(']');
        } else {
            quote(b, v.toString());
        }
    }

    private static void quote(StringBuilder b, String s) {
        b.append('"');
        for (int k = 0; k < s.length(); k++) {
            char c = s.charAt(k);
            switch (c) {
                case '"': b.append("\\\""); break;
                case '\\': b.append("\\\\"); break;
                case '\n': b.append("\\n"); break;
                case '\r': b.append("\\r"); break;
                case '\t': b.append("\\t"); break;
                default:
                    if (c < 0x20) {
                        b.append(String.format("\\u%04x", (int) c));
                    } else {
                        b.append(c);
                    }
            }
        }
        b.append('"');
    }

    // ------------------------------------------------------------------ typed access

    /** Decimal string or JSON integer to Long; null stays null. Rejects fractions. */
    static Long asLongOrNull(Object v) {
        if (v == null) {
            return null;
        }
        if (v instanceof Long || v instanceof Integer) {
            return ((Number) v).longValue();
        }
        if (v instanceof String) {
            String t = ((String) v).trim();
            try {
                return Long.valueOf(t);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("not a decimal integer");
            }
        }
        if (v instanceof BigInteger) {
            return ((BigInteger) v).longValueExact();
        }
        throw new IllegalArgumentException("not an integer field");
    }

    static long asLong(Object v, long dflt) {
        try {
            Long l = asLongOrNull(v);
            return l == null ? dflt : l;
        } catch (RuntimeException e) {
            return dflt;
        }
    }

    static Long optLong(Object v) {
        try {
            return asLongOrNull(v);
        } catch (RuntimeException e) {
            return null;
        }
    }

    static Double asDouble(Object v) {
        return v instanceof Number && !(v instanceof BigInteger) ? ((Number) v).doubleValue() : null;
    }

    static String asString(Object v) {
        return v instanceof String ? (String) v : null;
    }

    static Boolean asBool(Object v) {
        return v instanceof Boolean ? (Boolean) v : null;
    }

    @SuppressWarnings("unchecked")
    static Map<String, Object> asMap(Object v) {
        return v instanceof Map ? (Map<String, Object>) v : new LinkedHashMap<String, Object>();
    }

    @SuppressWarnings("unchecked")
    static List<Object> asList(Object v) {
        return v instanceof List ? (List<Object>) v : new ArrayList<Object>();
    }
}
