package xyz.fz.omni.core;

import io.modelcontextprotocol.spec.McpSchema.CallToolResult;

import org.springframework.core.env.Environment;

import java.nio.ByteBuffer;
import java.nio.charset.*;
import java.time.Instant;
import java.util.*;

/** Bounded message presentation and sanitized structured failures shared by connectors. */
public final class MessageSupport {
    private static final ThreadLocal<Long> DEADLINE = new ThreadLocal<>();

    private MessageSupport() {}

    @FunctionalInterface
    public interface Operation {
        Map<String, Object> run() throws Exception;
    }

    public static final class Failure extends RuntimeException {
        public final String code;
        public final Map<String, Object> details;

        public Failure(String code, String message) {
            this(code, message, Map.of());
        }

        public Failure(String code, String message, Map<String, Object> details) {
            super(message);
            this.code = code;
            this.details = details;
        }
    }

    public static CallToolResult call(Operation operation) {
        Long previous = DEADLINE.get();
        if (previous == null)
            DEADLINE.set(System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(30));
        try {
            var body = new LinkedHashMap<String, Object>(operation.run());
            body.put("success", true);
            body.put("observedAt", Instant.now().toString());
            return ConnectorRegistry.success(body);
        } catch (Failure e) {
            return error(e.code, e.getMessage(), e.details);
        } catch (IllegalArgumentException e) {
            return error("INVALID_ARGUMENT", "Invalid operation parameters", Map.of());
        } catch (Exception e) {
            if (e instanceof InterruptedException) Thread.currentThread().interrupt();
            Throwable cause = e;
            while ((cause instanceof java.util.concurrent.ExecutionException
                            || cause instanceof java.util.concurrent.CompletionException)
                    && cause.getCause() != null) cause = cause.getCause();
            String name = cause.getClass().getSimpleName();
            String code =
                    name.contains("Authentication")
                            ? "AUTHENTICATION_FAILED"
                            : name.contains("Authorization")
                                    ? "PERMISSION_DENIED"
                                    : name.contains("UnknownTopic")
                                            ? "RESOURCE_NOT_FOUND"
                                            : name.contains("Timeout")
                                                    ? "TIMEOUT"
                                                    : "BROKER_UNAVAILABLE";
            return error(
                    code,
                    "MQ operation failed; check permissions, resources and connectivity",
                    Map.of());
        } finally {
            if (previous == null) DEADLINE.remove();
        }
    }

    public static int remainingMs(int maximum) {
        Long deadline = DEADLINE.get();
        if (deadline == null) return Math.min(maximum, 30000);
        long left =
                java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
        if (left <= 0) throw new Failure("TIMEOUT", "Operation deadline exceeded");
        return (int) Math.min(maximum, left);
    }

    public static CallToolResult error(String code, String message, Map<String, Object> details) {
        var body = new LinkedHashMap<String, Object>();
        body.put("success", false);
        body.put("code", code);
        body.put("message", message);
        body.put("details", details);
        return CallToolResult.builder()
                .isError(true)
                .structuredContent(body)
                .addTextContent(
                        new tools.jackson.databind.json.JsonMapper().writeValueAsString(body))
                .build();
    }

    public static int setting(Environment env, String prefix, String name, int fallback) {
        return Math.max(1, env.getProperty(prefix + "." + name, Integer.class, fallback));
    }

    public static int bound(Integer value, int fallback, int max) {
        int n = value == null ? Math.min(fallback, max) : value;
        if (n < 1 || n > max) throw new IllegalArgumentException("Out of range");
        return n;
    }

    public static String required(String value) {
        if (value == null || value.isBlank())
            throw new IllegalArgumentException("Missing parameter");
        return value;
    }

    public static byte[] payload(String value, String encoding, int max) {
        if (value == null) throw new IllegalArgumentException("Missing payload");
        if (value.length() > max * 2L) throw new IllegalArgumentException("Payload too large");
        byte[] bytes;
        if (encoding == null || encoding.equals("utf8"))
            bytes = value.getBytes(StandardCharsets.UTF_8);
        else if (encoding.equals("base64")) bytes = Base64.getDecoder().decode(value);
        else throw new IllegalArgumentException("Unknown encoding");
        if (bytes.length > max) throw new IllegalArgumentException("Payload too large");
        return bytes;
    }

    public static Map<String, Object> preview(byte[] bytes, int max) {
        if (bytes == null) return Map.of("isNull", true);
        byte[] shown = Arrays.copyOf(bytes, Math.min(bytes.length, max));
        String encoding = "utf8", text;
        try {
            text =
                    StandardCharsets.UTF_8
                            .newDecoder()
                            .onMalformedInput(CodingErrorAction.REPORT)
                            .decode(ByteBuffer.wrap(shown))
                            .toString();
        } catch (CharacterCodingException e) {
            encoding = "base64";
            text = Base64.getEncoder().encodeToString(shown);
        }
        return Map.of(
                "payload",
                text,
                "encoding",
                encoding,
                "sizeBytes",
                bytes.length,
                "truncated",
                bytes.length > max);
    }

    public static void headers(Map<String, String> headers) {
        if (headers == null) return;
        if (headers.size() > 64) throw new IllegalArgumentException("Too many headers");
        long size = 0;
        for (var e : headers.entrySet()) {
            required(e.getKey());
            if (e.getValue() == null) throw new IllegalArgumentException();
            size +=
                    e.getKey().getBytes(StandardCharsets.UTF_8).length
                            + e.getValue().getBytes(StandardCharsets.UTF_8).length;
        }
        if (size > 16384) throw new IllegalArgumentException("Headers too large");
    }

    public static int jsonSize(Object value) {
        return new tools.jackson.databind.json.JsonMapper().writeValueAsBytes(value).length;
    }
}
