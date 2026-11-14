package xyz.fz.omni.app;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.sun.net.httpserver.HttpServer;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.*;

import xyz.fz.omni.kafka.KafkaClients;

import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

class MqMcpOfflineTest {
    @TestConfiguration(proxyBeanMethods = false)
    static class Clients {
        @Bean
        @Primary
        KafkaClients offlineKafkaClients() {
            var clients = mock(KafkaClients.class);
            var admin = mock(Admin.class);
            var result = mock(DescribeClusterResult.class);
            when(result.clusterId()).thenReturn(KafkaFuture.completedFuture("offline"));
            when(admin.describeCluster()).thenReturn(result);
            when(clients.admin(any())).thenReturn(admin);
            return clients;
        }
    }

    @Test
    void bothConnectorsRegisterExactlySeventeenToolsAcrossSessions() throws Exception {
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext(
                "/",
                e -> {
                    byte[] b =
                            (e.getRequestURI().getPath().endsWith("whoami")
                                            ? "{\"name\":\"debug\"}"
                                            : "{\"items\":[]}")
                                    .getBytes(StandardCharsets.UTF_8);
                    e.sendResponseHeaders(200, b.length);
                    e.getResponseBody().write(b);
                    e.close();
                });
        server.start();
        try (var peers = new xyz.fz.omni.config.support.UpstreamFixture();
                var redis = new RedisFixture();
                var context =
                        new SpringApplicationBuilder(
                                        OmniConnectApplication.class,
                                        Clients.class,
                                        TestDatabase.class)
                                .run(
                                        "--server.port=0",
                                        "--omni.auth.enabled=false",
                                        "--spring.ai.mcp.server.stdio=false",
                                        "--spring.ai.mcp.server.annotation-scanner.enabled=false",
                                        "--spring.config.location=file:../application.yml");
                var first = RuntimeConnectionsTest.client(context);
                var second = RuntimeConnectionsTest.client(context)) {
            assertEquals(8, RuntimeConnectionsTest.names(first).size());
            RuntimeConnectionsTest.call(
                    first,
                    "rabbitmq_connect",
                    Map.of(
                            "managementUrl",
                            "http://127.0.0.1:" + server.getAddress().getPort(),
                            "username",
                            "debug",
                            "password",
                            "secret"));
            RuntimeConnectionsTest.call(
                    first, "kafka_connect", Map.of("bootstrapServers", "unused:9092"));
            var tools = second.listTools().tools();
            assertEquals(23, tools.size());
            assertEquals(
                    17,
                    tools.stream()
                            .filter(
                                    t ->
                                            t.name().startsWith("rabbitmq_")
                                                    || t.name().startsWith("kafka_"))
                            .count());
            for (var tool : tools) {
                if (tool.name().equals("kafka_read_records"))
                    assertTrue(tool.annotations().readOnlyHint());
                if (tool.name().equals("rabbitmq_sample_messages")
                        || tool.name().equals("kafka_consume_records"))
                    assertFalse(tool.annotations().readOnlyHint());
            }
            var result =
                    second.callTool(
                            new io.modelcontextprotocol.spec.McpSchema.CallToolRequest(
                                    "rabbitmq_publish_message",
                                    Map.of("exchange", "", "routingKey", "q", "payload", "x")));
            assertTrue(result.isError());
            assertTrue(result.toString().contains("CAPABILITY_NOT_CONFIGURED"));
            assertFalse(result.toString().contains("secret"));
            RuntimeConnectionsTest.call(
                    first, "apollo_connect", Map.of("meta", peers.apollo(), "appId", "source-app"));
            RuntimeConnectionsTest.call(
                    first,
                    "nacos_connect",
                    Map.of("serverAddr", peers.base(), "username", "user", "password", "password"));
            RuntimeConnectionsTest.call(
                    first,
                    "db_switch_datasource",
                    Map.of(
                            "jdbcUrl",
                            "jdbc:h2:mem:" + UUID.randomUUID(),
                            "username",
                            "sa",
                            "password",
                            ""));
            RuntimeConnectionsTest.call(
                    first, "redis_connect", Map.of("host", "127.0.0.1", "port", redis.port()));
            assertEquals(43, RuntimeConnectionsTest.names(second).size());

        } finally {
            server.stop(0);
        }
    }
}
