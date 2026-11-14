package xyz.fz.omni.core;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

class MessageSupportTest {
    @Test
    void encodingSizeLimitsAndNulls() {
        assertEquals(
                "hello",
                MessageSupport.preview("hello".getBytes(StandardCharsets.UTF_8), 20)
                        .get("payload"));
        assertEquals("base64", MessageSupport.preview(new byte[] {(byte) 255}, 20).get("encoding"));
        assertEquals(true, MessageSupport.preview(new byte[5], 2).get("truncated"));
        assertEquals(true, MessageSupport.preview(null, 2).get("isNull"));
        assertArrayEquals(new byte[] {1, 2}, MessageSupport.payload("AQI=", "base64", 2));
        assertThrows(
                IllegalArgumentException.class, () -> MessageSupport.payload("AQI=", "base64", 1));
        assertThrows(
                IllegalArgumentException.class, () -> MessageSupport.payload("a", "unknown", 1));
        assertThrows(
                IllegalArgumentException.class,
                () -> MessageSupport.headers(Map.of("x", "a".repeat(20000))));
    }

    @Test
    void errorsAreSanitizedAndDetailsRetained() {
        var result =
                MessageSupport.call(
                        () -> {
                            throw new ExecutionException(new TimeoutException("private-marker"));
                        });
        assertTrue(result.isError());
        assertTrue(result.toString().contains("TIMEOUT"));
        assertFalse(result.toString().contains("private-marker"));
        var partial =
                MessageSupport.call(
                        () -> {
                            throw new MessageSupport.Failure(
                                    "COMMIT_RESULT_UNKNOWN",
                                    "Commit unknown",
                                    Map.of("offset", 10));
                        });
        assertTrue(partial.toString().contains("offset"));
    }
}
