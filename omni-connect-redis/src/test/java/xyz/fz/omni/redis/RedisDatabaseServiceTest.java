package xyz.fz.omni.redis;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

class RedisDatabaseServiceTest {
    @ParameterizedTest
    @ValueSource(ints = {-1, 4})
    void rejectsAConfiguredDefaultOutsideTheDatabaseRange(int database) {
        assertThrows(IllegalArgumentException.class,
                () -> new RedisDatabaseService(database, "localhost", 6379, null, null, 4));
    }

    @Test
    void rejectsInvalidSelectionsBeforeCreatingAConnectionFactory() {
        try (var factories = mockConstruction(LettuceConnectionFactory.class)) {
            var service = new RedisDatabaseService(1, "localhost", 6379, null, null, 4);
            assertThrows(IllegalArgumentException.class, () -> service.selectDatabase(null));
            assertThrows(IllegalArgumentException.class, () -> service.selectDatabase(-1));
            assertThrows(IllegalArgumentException.class, () -> service.selectDatabase(4));
            assertEquals(1, service.currentDatabase());
            assertTrue(factories.constructed().isEmpty());
        }
    }

    @Test
    void validatesBeforeSelectingCachesTemplatesAndClosesEveryCreatedFactory() {
        var service = new RedisDatabaseService(0, "cache.example.test", 6380, "test-user", "test-password", 4);
        RedisConnection available = mock(RedisConnection.class);
        RedisConnection unavailable = mock(RedisConnection.class);
        when(available.ping()).thenAnswer(ignored -> {
            assertEquals(0, service.currentDatabase());
            return "PONG";
        });
        when(unavailable.ping()).thenThrow(new RedisConnectionFailureException("test connection failure"));
        var configs = new ArrayList<RedisStandaloneConfiguration>();

        try (var factories = mockConstruction(LettuceConnectionFactory.class, (factory, context) -> {
            var config = (RedisStandaloneConfiguration) context.arguments().getFirst();
            configs.add(config);
            when(factory.getConnection()).thenReturn(config.getDatabase() == 2 ? unavailable : available);
        })) {
            try {
                assertTrue(factories.constructed().isEmpty());
                var defaultTemplate = service.currentTemplate();
                assertSame(defaultTemplate, service.currentTemplate());

                assertEquals(Map.of("database", 1), service.selectDatabase(1));
                assertEquals(1, service.currentDatabase());
                var selectedTemplate = service.currentTemplate();
                assertNotSame(defaultTemplate, selectedTemplate);

                var error = assertThrows(IllegalStateException.class, () -> service.selectDatabase(2));
                assertTrue(error.getMessage().contains("Failed to switch to database 2"));
                assertEquals(1, service.currentDatabase());
                assertSame(selectedTemplate, service.currentTemplate());
                verify(available).ping();
                verify(unavailable).ping();
                assertEquals(List.of(0, 1, 2),
                        configs.stream().map(RedisStandaloneConfiguration::getDatabase).toList());
                for (var config : configs) {
                    assertEquals("cache.example.test", config.getHostName());
                    assertEquals(6380, config.getPort());
                    assertEquals("test-user", config.getUsername());
                    assertArrayEquals("test-password".toCharArray(), config.getPassword().get());
                }
            } finally {
                service.destroy();
            }
            for (var factory : factories.constructed()) {
                verify(factory).afterPropertiesSet();
                verify(factory).destroy();
            }
        }
    }

    @Test
    void leavesAuthenticationUnsetWhenCredentialsAreBlank() {
        var service = new RedisDatabaseService(0, "localhost", 6379, " ", "", 4);
        try (var factories = mockConstruction(LettuceConnectionFactory.class, (factory, context) -> {
            var config = (RedisStandaloneConfiguration) context.arguments().getFirst();
            assertNull(config.getUsername());
            assertFalse(config.getPassword().isPresent());
        })) {
            try {
                service.currentTemplate();
                assertEquals(1, factories.constructed().size());
            } finally {
                service.destroy();
            }
        }
    }
}
