package xyz.fz.omni.rabbitmq;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.rabbitmq.client.*;
import com.sun.net.httpserver.HttpServer;

import io.modelcontextprotocol.server.McpSyncServer;

import org.junit.jupiter.api.*;
import org.springframework.mock.env.MockEnvironment;

import xyz.fz.omni.core.*;

import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.*;

class RabbitMqOfflineTest {
    HttpServer server;
    String base;
    final AtomicInteger status = new AtomicInteger(200);
    final AtomicReference<String> lastPath = new AtomicReference<>();
    final AtomicReference<String> lastBody = new AtomicReference<>();

    @BeforeEach
    void start() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext(
                "/",
                e -> {
                    lastPath.set(e.getRequestURI().toString());
                    lastBody.set(
                            new String(e.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
                    String path = e.getRequestURI().getPath();
                    String body =
                            path.endsWith("/get")
                                    ? "[]"
                                    : path.contains("whoami")
                                            ? "{\"name\":\"debug\"}"
                                            : path.contains("/queues/")
                                                            && e.getRequestURI().getQuery() != null
                                                    ? "{\"items\":[],\"page\":1}"
                                                    : "{\"name\":\"q\",\"messages_ready\":2}";
                    byte[] b = body.getBytes(StandardCharsets.UTF_8);
                    e.sendResponseHeaders(status.get(), b.length);
                    e.getResponseBody().write(b);
                    e.close();
                });
        server.start();
        base = "http://127.0.0.1:" + server.getAddress().getPort();
    }

    @AfterEach
    void stop() {
        server.stop(0);
    }

    RabbitMqRuntime runtime(Connection c, int max) {
        return new RabbitMqRuntime(base, "debug", "secret", "/", c, 200, 4096, 65536, max);
    }

    @Test
    void pathsMissingMetricsAndSamplingMode() throws Exception {
        try (var r = runtime(null, 1048576)) {
            var result = r.inspectQueue("a/b");
            assertTrue(lastPath.get().contains("%2F/a%2Fb"));
            assertNull(result.get("messages_unacknowledged"));
            r.sample("q", 2);
            assertTrue(lastBody.get().contains("ack_requeue_true"));
            var tools = new RabbitMqTools(r, new MockEnvironment());
            assertTrue(
                    tools.rabbitmq_publish_message("", "q", "x", null, null, null, null, null, null)
                            .isError());
            status.set(403);
            assertTrue(tools.rabbitmq_inspect_queue("q").toString().contains("PERMISSION_DENIED"));
        }
    }

    @Test
    void failedReplacementRetainsTargetAndOnlyConnectIsManagement() throws Exception {
        var sdk = mock(McpSyncServer.class);
        var clients = mock(RabbitMqClients.class);
        try (var registry = new ConnectorRegistry()) {
            registry.attach(sdk, List.of("rabbitmq"));
            var module = new RabbitMqConnectorModule(registry, new MockEnvironment(), clients);
            assertEquals(1, ConnectorRegistry.specifications(module.managementTools()).size());
            assertFalse(module.connect(base, "debug", "secret", null, null, null, null).isError());
            var before = registry.status();
            status.set(401);
            assertTrue(
                    module.connect(base, "debug", "private-marker", null, null, null, null)
                            .isError());
            assertEquals(before, registry.status());
            verify(sdk, times(8)).addTool(any());
            verifyNoInteractions(clients);
        }
    }

    @Test
    void confirmReturnAndUnknownOutcomes() throws Exception {
        var c = mock(Connection.class);
        var channel = mock(Channel.class);
        when(c.createChannel()).thenReturn(channel);
        when(channel.waitForConfirms(anyLong())).thenReturn(true);
        try (var r = runtime(c, 1048576)) {
            assertEquals(
                    "CONFIRMED",
                    r.publish("", "q", "hi", null, null, null, null, null, null).get("outcome"));
            doAnswer(
                            i -> {
                                ReturnCallback listener = i.getArgument(0);
                                listener.handle(
                                        new com.rabbitmq.client.Return(
                                                312, "NO_ROUTE", "", "q", null, new byte[0]));
                                return null;
                            })
                    .when(channel)
                    .addReturnListener(any(ReturnCallback.class));
            assertEquals(
                    "UNROUTABLE",
                    assertThrows(
                                    MessageSupport.Failure.class,
                                    () ->
                                            r.publish(
                                                    "", "q", "hi", null, null, null, null, null,
                                                    null))
                            .code);
        }
    }

    GetResponse message(long tag) {
        return new GetResponse(
                new Envelope(tag, false, "", "q"),
                new AMQP.BasicProperties.Builder()
                        .headers(
                                Map.of(
                                        "traceId",
                                        com.rabbitmq.client.impl.LongStringHelper.asLongString(
                                                "trace-123")))
                        .build(),
                "hello".getBytes(StandardCharsets.UTF_8),
                0);
    }

    @Test
    void consumeAcksOnlyReturnedMessagesAndRetainsFailureProgress() throws Exception {
        var c = mock(Connection.class);
        var channel = mock(Channel.class);
        when(c.createChannel()).thenReturn(channel);
        when(channel.basicGet("q", false)).thenReturn(message(1), message(2));
        doThrow(new java.io.IOException("secret")).when(channel).basicAck(2, false);
        try (var r = runtime(c, 1048576)) {
            var error = assertThrows(MessageSupport.Failure.class, () -> r.consume("q", 2, 200));
            assertEquals(1, error.details.get("ackSent"));
            assertEquals(2, ((List<?>) error.details.get("messages")).size());
        }
        reset(channel);
        when(c.createChannel()).thenReturn(channel);
        when(channel.basicGet("q", false)).thenReturn(message(3));
        try (var r = runtime(c, 1)) {
            assertEquals(0, r.consume("q", 1, 200).get("ackSent"));
            verify(channel, never()).basicAck(anyLong(), anyBoolean());
            verify(channel).basicNack(3, false, true);
        }
    }
}
