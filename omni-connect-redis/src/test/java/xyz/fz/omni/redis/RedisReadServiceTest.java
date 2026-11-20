package xyz.fz.omni.redis;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.*;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

class RedisReadServiceTest {
    private final RedisDatabaseService databases = mock(RedisDatabaseService.class);
    private final StringRedisTemplate redis = mock(StringRedisTemplate.class);
    private final RedisReadService service = new RedisReadService(databases, 2, 2, 4);

    @BeforeEach
    void useSelectedDatabase() {
        when(databases.currentTemplate()).thenReturn(redis);
    }

    @Test
    void scanSendsTheCursorAndBoundedCountAndReturnsTheEntirePage() {
        RedisConnection connection = useScanConnection();
        String cursor = "18446744073709551615";
        byte[][] arguments = {
                bytes(cursor), bytes("MATCH"), bytes("订单:*"), bytes("COUNT"), bytes("2")
        };
        when(connection.execute("SCAN", arguments))
                .thenReturn(List.of(bytes("26"), List.of(bytes("订单:1"), bytes("订单:2"), bytes("订单:3"))));

        assertEquals(Map.of("keys", List.of("订单:1", "订单:2", "订单:3"), "nextCursor", "26", "count", 3),
                service.scanKeys("订单:*", cursor, 100));
        verify(databases).currentTemplate();
        verify(connection).execute("SCAN", arguments);
        verifyNoMoreInteractions(connection);
    }

    @Test
    void scanUsesDefaultPositionAndPatternAndHandlesACompletedEmptyPage() {
        RedisConnection connection = useScanConnection();
        when(connection.execute("SCAN", bytes("0"), bytes("MATCH"), bytes("*"),
                bytes("COUNT"), bytes("2"))).thenReturn(List.of(0L, List.of()));

        assertEquals(Map.of("keys", List.of(), "nextCursor", "0", "count", 0),
                service.scanKeys(null, null, null));
    }

    @Test
    void rejectsInvalidScanInputsBeforeIssuingRedisCommands() {
        assertThrows(IllegalArgumentException.class, () -> service.scanKeys("x".repeat(257), null, null));
        assertThrows(IllegalArgumentException.class, () -> service.scanKeys("*", "-1", 1));
        verifyNoInteractions(redis);
    }

    @Test
    void rejectsAnInvalidScanReply() {
        RedisConnection connection = useScanConnection();
        when(connection.execute("SCAN", bytes("0"), bytes("MATCH"), bytes("*"),
                bytes("COUNT"), bytes("1"))).thenReturn(List.of("0", "invalid keys"));

        var error = assertThrows(IllegalStateException.class, () -> service.scanKeys("*", "0", 1));
        assertTrue(error.getMessage().contains("unrecognized"));
    }

    @Test
    void inspectReturnsAbsenceOrTypeSpecificMetadataWithoutFetchingTheValue() {
        when(redis.type("missing")).thenReturn(DataType.NONE);
        when(redis.type("queue")).thenReturn(DataType.LIST);
        when(redis.getExpire("queue", TimeUnit.SECONDS)).thenReturn(-1L);
        ListOperations<String, String> lists = mock();
        when(redis.opsForList()).thenReturn(lists);
        when(lists.size("queue")).thenReturn(7L);

        assertEquals(Map.of("key", "missing", "exists", false), service.inspectKey("missing"));
        assertEquals(Map.of("key", "queue", "exists", true, "type", "list", "ttlSeconds", -1L, "size", 7L),
                service.inspectKey("queue"));
        verify(redis, never()).getExpire("missing", TimeUnit.SECONDS);
        verify(lists).size("queue");
        verifyNoMoreInteractions(lists);
    }

    @Test
    void readReturnsAbsenceWithoutFetchingContent() {
        when(redis.type("missing")).thenReturn(DataType.NONE);

        assertEquals(Map.of("key", "missing", "exists", false), service.readKey("missing", null));
        verify(redis).type("missing");
        verifyNoMoreInteractions(redis);
    }

    @Test
    void readsOnlyTheConfiguredStringRangeAndReportsTruncation() {
        ValueOperations<String, String> values = mock();
        when(redis.type("text")).thenReturn(DataType.STRING);
        when(redis.opsForValue()).thenReturn(values);
        when(values.size("text")).thenReturn(8L);
        when(values.get("text", 0, 3)).thenReturn("abcd");

        assertEquals(Map.of("key", "text", "type", "string", "value", "abcd", "truncated", true),
                service.readKey("text", null));
        verify(values).get("text", 0, 3);
        verify(values, never()).get("text");
        verify(databases).currentTemplate();
    }

    @Test
    void boundsListRangeAndTrimsLongElements() {
        ListOperations<String, String> lists = mock();
        when(redis.type("queue")).thenReturn(DataType.LIST);
        when(redis.opsForList()).thenReturn(lists);
        when(lists.size("queue")).thenReturn(3L);
        when(lists.range("queue", 0, 1)).thenReturn(List.of("long text", "ok"));

        assertEquals(Map.of("key", "queue", "type", "list", "items", List.of("long", "ok"), "truncated", true),
                service.readKey("queue", 30));
    }

    @Test
    void scansHashFieldsWithABoundedCursorAndClosesIt() {
        HashOperations<String, Object, Object> hashes = mock();
        when(redis.type("hash")).thenReturn(DataType.HASH);
        when(redis.opsForHash()).thenReturn(hashes);
        Cursor<Map.Entry<Object, Object>> cursor = cursor(List.of(Map.entry("field", "value long")));
        when(hashes.scan(eq("hash"), any(ScanOptions.class))).thenReturn(cursor);
        when(hashes.size("hash")).thenReturn(5L);

        assertEquals(List.of(Map.of("field", "fiel", "value", "valu")), service.readKey("hash", 1).get("items"));
        verify(hashes).scan(eq("hash"), argThat(options -> options.getCount() == 1L));
        verify(cursor).close();
    }

    @Test
    void readsSetMembersAndReportsWhenAllFitInTheLimit() {
        SetOperations<String, String> sets = mock();
        when(redis.type("members")).thenReturn(DataType.SET);
        when(redis.opsForSet()).thenReturn(sets);
        Cursor<String> cursor = cursor(List.of("member"));
        when(sets.scan(eq("members"), any(ScanOptions.class))).thenReturn(cursor);
        when(sets.size("members")).thenReturn(1L);

        assertEquals(Map.of("key", "members", "type", "set", "items", List.of("memb"), "truncated", false),
                service.readKey("members", 2));
        verify(cursor).close();
    }

    @Test
    void readsSortedSetScoresAndStopsAtTheItemLimit() {
        ZSetOperations<String, String> sets = mock();
        when(redis.type("rank")).thenReturn(DataType.ZSET);
        when(redis.opsForZSet()).thenReturn(sets);
        Cursor<ZSetOperations.TypedTuple<String>> cursor = cursor(List.of(
                new DefaultTypedTuple<>("first", 1.25), new DefaultTypedTuple<>("second", 2.5)));
        when(sets.scan(eq("rank"), any(ScanOptions.class))).thenReturn(cursor);
        when(sets.size("rank")).thenReturn(2L);

        var result = service.readKey("rank", 1);
        assertEquals(List.of(Map.of("value", "firs", "score", 1.25)), result.get("items"));
        assertEquals(true, result.get("truncated"));
        verify(cursor).next();
        verify(cursor).close();
    }

    @Test
    void closesSetCursorAndSuppressesBackendDetailsOnReadFailure() {
        SetOperations<String, String> sets = mock();
        Cursor<String> cursor = mock();
        when(redis.type("members")).thenReturn(DataType.SET);
        when(redis.opsForSet()).thenReturn(sets);
        when(sets.scan(eq("members"), any(ScanOptions.class))).thenReturn(cursor);
        when(cursor.hasNext()).thenReturn(true);
        when(cursor.next()).thenThrow(new DataAccessResourceFailureException("private detail"));

        var error = assertThrows(IllegalStateException.class, () -> service.readKey("members", 2));
        assertEquals("Redis read failed; check the service status and permissions.", error.getMessage());
        verify(cursor).close();
    }

    private RedisConnection useScanConnection() {
        RedisConnection connection = mock(RedisConnection.class);
        when(redis.execute(any(RedisCallback.class))).thenAnswer(invocation -> {
            RedisCallback<?> callback = invocation.getArgument(0);
            return callback.doInRedis(connection);
        });
        return connection;
    }

    private static <T> Cursor<T> cursor(List<T> values) {
        Cursor<T> cursor = mock();
        var iterator = values.iterator();
        when(cursor.hasNext()).thenAnswer(ignored -> iterator.hasNext());
        when(cursor.next()).thenAnswer(ignored -> iterator.next());
        return cursor;
    }

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }
}
