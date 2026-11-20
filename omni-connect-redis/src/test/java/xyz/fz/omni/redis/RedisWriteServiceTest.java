package xyz.fz.omni.redis;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.data.redis.core.*;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;

class RedisWriteServiceTest {
    private final RedisDatabaseService databases = mock(RedisDatabaseService.class);
    private final StringRedisTemplate redis = mock(StringRedisTemplate.class);
    private final RedisWriteService service = new RedisWriteService(databases, 2, 5);

    @BeforeEach
    void useSelectedDatabase() {
        when(databases.currentTemplate()).thenReturn(redis);
    }

    @Test
    void writesAnEmptyStringAndAppliesExpiryUsingTheSameTemplate() {
        ValueOperations<String, String> values = mock();
        when(redis.opsForValue()).thenReturn(values);
        var ttl = Duration.ofSeconds(15);
        when(redis.expire("text", ttl)).thenReturn(true);

        assertEquals(Map.of("key", "text", "type", "string", "written", 1, "ttlSeconds", 15),
                service.writeKey("text", " STRING ", "", null, null, null, 15));
        var order = inOrder(values, redis);
        order.verify(values).set("text", "");
        order.verify(redis).expire("text", ttl);
        verify(databases).currentTemplate();
    }

    @Test
    void appendsListElementsAndReturnsTheReportedListSize() {
        ListOperations<String, String> lists = mock();
        var items = List.of("first", "next");
        when(redis.opsForList()).thenReturn(lists);
        when(lists.rightPushAll("queue", items)).thenReturn(7L);

        assertEquals(7L, service.writeKey("queue", "list", null, items, null, null, null).get("pushed"));
        verify(lists).rightPushAll("queue", items);
        verify(redis, never()).expire(anyString(), any(Duration.class));
    }

    @Test
    void returnsTheBackendCountForDuplicateSetMembers() {
        SetOperations<String, String> sets = mock();
        when(redis.opsForSet()).thenReturn(sets);
        when(sets.add("tags", "same", "same")).thenReturn(1L);

        assertEquals(1L, service.writeKey("tags", "set", null, List.of("same", "same"), null,
                null, null).get("added"));
        verify(sets).add("tags", "same", "same");
    }

    @Test
    void mergesHashFieldsAndPreservesEmptyFieldValues() {
        HashOperations<String, Object, Object> hashes = mock();
        when(redis.opsForHash()).thenReturn(hashes);
        var fields = Map.of("name", "test", "note", "");

        assertEquals(2, service.writeKey("hash", "hash", null, null, fields, null, null).get("written"));
        verify(hashes).putAll("hash", fields);
        verifyNoMoreInteractions(hashes);
    }

    @Test
    void writesSortedSetScoresWithoutChangingSignOrPrecision() {
        ZSetOperations<String, String> sets = mock();
        when(redis.opsForZSet()).thenReturn(sets);
        Set<ZSetOperations.TypedTuple<String>> tuples =
                Set.of(new DefaultTypedTuple<>("one", -1.5), new DefaultTypedTuple<>("two", 2.25));
        when(sets.add("rank", tuples)).thenReturn(1L);

        assertEquals(1L, service.writeKey("rank", "zset", null, null, null,
                Map.of("one", -1.5, "two", 2.25), null).get("written"));
        verify(sets).add("rank", tuples);
    }

    @Test
    void rejectsOversizedOrMalformedPayloadsWithoutSendingCommands() {
        assertThrows(IllegalArgumentException.class,
                () -> service.writeKey("key", "string", "123456", null, null, null, null));
        assertThrows(IllegalArgumentException.class,
                () -> service.writeKey("key", "list", null, List.of("a", "b", "c"), null, null, null));
        assertThrows(IllegalArgumentException.class,
                () -> service.writeKey("key", "hash", null, null, Map.of("", "value"), null, null));
        assertThrows(IllegalArgumentException.class,
                () -> service.writeKey("key", "stream", null, null, null, null, null));
        verifyNoInteractions(redis);
    }

    @ParameterizedTest
    @ValueSource(doubles = {Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY})
    void rejectsNonFiniteSortedSetScores(double score) {
        var error = assertThrows(IllegalArgumentException.class,
                () -> service.writeKey("rank", "zset", null, null, null, Map.of("one", score), null));
        assertTrue(error.getMessage().contains("finite"));
        verifyNoInteractions(redis);
    }

    @Test
    void deduplicatesDeleteTargetsAndReportsTheOriginalRequestCount() {
        when(redis.delete(Set.of("key"))).thenReturn(1L);

        assertEquals(Map.of("requested", 2, "deleted", 1L), service.deleteKeys(List.of("key", "key")));
        verify(redis).delete(Set.of("key"));
    }

    @Test
    void rejectsAnInvalidDeleteBatchBeforeSendingAnyCommand() {
        assertThrows(IllegalArgumentException.class, () -> service.deleteKeys(List.of("key", " ")));
        assertThrows(IllegalArgumentException.class, () -> service.deleteKeys(List.of("a", "b", "c")));
        verifyNoInteractions(redis);
    }

    @Test
    void appliesPositiveExpiryAndUsesPersistForZero() {
        when(redis.expire("key", Duration.ofSeconds(3))).thenReturn(true);
        when(redis.persist("key")).thenReturn(false);

        assertEquals(Map.of("key", "key", "ttlSeconds", 3, "applied", true), service.expireKey("key", 3));
        assertEquals(Map.of("key", "key", "persisted", false), service.expireKey("key", 0));
        verify(redis).expire("key", Duration.ofSeconds(3));
        verify(redis).persist("key");
        verifyNoMoreInteractions(redis);
    }

    @Test
    void rejectsMissingOrNegativeExpiryBeforeUsingRedis() {
        assertThrows(IllegalArgumentException.class, () -> service.expireKey("key", null));
        assertThrows(IllegalArgumentException.class, () -> service.expireKey("key", -1));
        verifyNoInteractions(redis);
    }
}
