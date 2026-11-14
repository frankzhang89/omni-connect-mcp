package xyz.fz.omni.app;

import static org.junit.jupiter.api.Assertions.*;

import com.rabbitmq.client.*;
import io.modelcontextprotocol.client.McpSyncClient;
import io.modelcontextprotocol.spec.McpSchema.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.boot.builder.SpringApplicationBuilder;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.net.*;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

/** Explicit opt-in only: creates and deletes its own vhost, never uses business queues. */
@EnabledIfEnvironmentVariable(named = "OMNI_RABBITMQ_ACCEPTANCE", matches = "true")
class RabbitMqBrokerTest {
    private static final JsonMapper JSON = new JsonMapper();
    private final String base = required("RABBITMQ_MANAGEMENT_URL");
    private final String addresses = required("RABBITMQ_AMQP_ADDRESSES");
    private final String user = required("RABBITMQ_USERNAME");
    private final String password = required("RABBITMQ_PASSWORD");
    private final String vhost = "omni-mcp-acceptance-" + UUID.randomUUID();
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

    static String required(String key) {
        return Objects.requireNonNull(System.getenv(key), key + " is required");
    }

    static String enc(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
    }

    JsonNode api(String method, String path, Object body) throws Exception {
        var response = http.send(HttpRequest.newBuilder(URI.create(base + "/api/" + path))
                .timeout(Duration.ofSeconds(10))
                .header("Authorization", "Basic " + Base64.getEncoder().encodeToString((user + ":" + password).getBytes(StandardCharsets.UTF_8)))
                .header("Content-Type", "application/json")
                .method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofByteArray(JSON.writeValueAsBytes(body)))
                .build(), HttpResponse.BodyHandlers.ofString());
        assertTrue(response.statusCode() >= 200 && response.statusCode() < 300, method + " " + path + " status=" + response.statusCode() + " " + response.body());
        return response.body().isBlank() ? JSON.readTree("{}") : JSON.readTree(response.body());
    }

    Map<String, Object> connectArgs(boolean amqp) {
        var args = new HashMap<String, Object>(Map.of("managementUrl", base, "username", user, "password", password, "virtualHost", vhost));
        if (amqp) args.put("amqpAddresses", addresses);
        return args;
    }

    JsonNode call(McpSyncClient client, String name, Map<String, Object> args) {
        return RuntimeConnectionsTest.call(client, name, args);
    }

    void error(McpSyncClient client, String name, Map<String, Object> args, String code) {
        var result = client.callTool(new CallToolRequest(name, args));
        assertTrue(Boolean.TRUE.equals(result.isError()), result.toString());
        assertEquals(code, JSON.readTree(((TextContent) result.content().getFirst()).text()).path("code").asString());
    }

    void passed(String label) { System.out.println("RABBITMQ_ACCEPTANCE PASS " + label); }

    @Test
    void realBrokerThroughHttpMcp() throws Exception {
        System.out.println("RABBITMQ_ACCEPTANCE version=" + api("GET", "overview", null).path("rabbitmq_version").asString() + " vhost=" + vhost);
        boolean created = false;
        try {
            api("PUT", "vhosts/" + enc(vhost), Map.of());
            created = true;
            api("PUT", "permissions/" + enc(vhost) + "/" + enc(user), Map.of("configure", ".*", "write", ".*", "read", ".*"));
            var factory = new ConnectionFactory();
            factory.setUsername(user); factory.setPassword(password); factory.setVirtualHost(vhost);
            factory.setConnectionTimeout(5000); factory.setHandshakeTimeout(5000); factory.setAutomaticRecoveryEnabled(false);
            try (var fixture = factory.newConnection(Address.parseAddresses(addresses), "omni-mcp-acceptance-fixture");
                    var channel = fixture.createChannel();
                    var context = new SpringApplicationBuilder(OmniConnectApplication.class).run(
                            "--server.address=127.0.0.1", "--server.port=0", "--omni.auth.enabled=false",
                            "--spring.config.location=file:../application.yml",
                            "--omni.rabbitmq.limits.preview-bytes=64", "--omni.rabbitmq.limits.max-response-bytes=1000");
                    var first = RuntimeConnectionsTest.client(context);
                    var second = RuntimeConnectionsTest.client(context)) {
                String queue = "acceptance / encoded";
                String exchange = "acceptance.exchange";
                channel.queueDeclare(queue, false, false, false, Map.of());
                channel.exchangeDeclare(exchange, "direct", false);
                channel.queueBind(queue, exchange, "acceptance");
                assertEquals(8, RuntimeConnectionsTest.names(first).size());
                call(first, "rabbitmq_connect", connectArgs(false));
                assertEquals(16, RuntimeConnectionsTest.names(second).size());
                error(second, "rabbitmq_publish_message", Map.of("exchange", "", "routingKey", queue, "payload", "x"), "CAPABILITY_NOT_CONFIGURED");
                call(first, "rabbitmq_connect", connectArgs(true));
                var bad = connectArgs(false); bad.put("password", UUID.randomUUID().toString());
                error(first, "rabbitmq_connect", bad, "AUTHENTICATION_FAILED");
                call(second, "rabbitmq_inspect_queue", Map.of("queue", queue));
                passed("management-only, AMQP auth, shared tool registration, failed replacement retains target");

                var publish = Map.<String, Object>of("exchange", "", "routingKey", queue, "payload", "hello real broker", "messageId", "acceptance-id", "correlationId", "acceptance-correlation", "headers", Map.of("traceId", "acceptance-trace"));
                assertEquals("CONFIRMED", call(first, "rabbitmq_publish_message", publish).path("outcome").asString());
                var sample = call(second, "rabbitmq_sample_messages", Map.of("queue", queue, "count", 1));
                assertTrue(sample.path("requeued").asBoolean());
                assertEquals("hello real broker", sample.path("messages").get(0).path("payload").asString());
                assertEquals(1, channel.queueDeclarePassive(queue).getMessageCount());
                var received = call(first, "rabbitmq_consume_messages", Map.of("queue", queue, "count", 1, "maxWaitMs", 1000));
                assertEquals(1, received.path("ackSent").asInt());
                assertEquals("SENT", received.path("ackStatus").asString());
                assertEquals("acceptance-id", received.path("messages").get(0).path("messageId").asString());
                assertEquals("acceptance-correlation", received.path("messages").get(0).path("correlationId").asString());
                assertEquals("acceptance-trace", received.path("messages").get(0).path("headers").path("traceId").asString());
                assertNull(channel.basicGet(queue, true));
                passed("default-exchange confirm, sample requeue, identifiers/headers, ACK removes message");

                assertEquals("CONFIRMED", call(first, "rabbitmq_publish_message", Map.of("exchange", exchange, "routingKey", "acceptance", "payload", "routed")).path("outcome").asString());
                error(first, "rabbitmq_publish_message", Map.of("exchange", exchange, "routingKey", "missing", "payload", "x"), "UNROUTABLE");
                error(first, "rabbitmq_publish_message", Map.of("exchange", "missing.exchange", "routingKey", "missing", "payload", "x"), "RESOURCE_NOT_FOUND");
                error(first, "rabbitmq_consume_messages", Map.of("queue", "missing.queue", "maxWaitMs", 100), "RESOURCE_NOT_FOUND");
                call(first, "rabbitmq_list_bindings", Map.of("queue", queue));
                call(first, "rabbitmq_list_bindings", Map.of("exchange", exchange));
                call(first, "rabbitmq_list_bindings", Map.of("exchange", ""));
                var queues = call(first, "rabbitmq_list_queues", Map.of("nameContains", "encoded", "pageSize", 1));
                for (int attempt = 0; queues.path("items").size() == 0 && attempt < 30; attempt++) {
                    Thread.sleep(500);
                    queues = call(first, "rabbitmq_list_queues", Map.of("nameContains", "encoded", "pageSize", 1));
                }
                assertEquals(1, queues.path("items").size(), queues.toString());
                passed("exchange routing, unroutable return, missing resources, bindings, queue filter/pagination, encoded name");

                var delivery = new ArrayBlockingQueue<Delivery>(1);
                try (var consumerChannel = fixture.createChannel()) {
                    consumerChannel.basicQos(1);
                    consumerChannel.basicConsume(queue, false, (t, d) -> delivery.offer(d), t -> {});
                    var held = delivery.poll(5, TimeUnit.SECONDS); assertNotNull(held);
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
                    JsonNode consumers;
                    do {
                        consumers = call(first, "rabbitmq_list_consumers", Map.of("queue", queue));
                        if (consumers.path("items").size() > 0 && call(first, "rabbitmq_inspect_queue", Map.of("queue", queue)).path("messages_unacknowledged").asInt() == 1) break;
                        Thread.sleep(500);
                    } while (System.nanoTime() < deadline);
                    assertEquals(1, consumers.path("items").size());
                    assertEquals(1, call(first, "rabbitmq_inspect_queue", Map.of("queue", queue)).path("messages_unacknowledged").asInt());
                    var details = call(first, "rabbitmq_inspect_channel", Map.of("channelName", consumers.path("items").get(0).path("channel_details").path("name").asString()));
                    assertEquals(1, details.path("messages_unacknowledged").asInt());
                    assertEquals("omni-mcp-acceptance-fixture", details.path("connection").path("clientProvidedName").asString());
                    assertEquals(0, call(first, "rabbitmq_sample_messages", Map.of("queue", queue)).path("messages").size());
                }
                assertEquals(1, call(first, "rabbitmq_consume_messages", Map.of("queue", queue, "count", 1, "maxWaitMs", 2000)).path("ackSent").asInt());
                passed("Ready/Unack, consumer/channel/connection correlation, Unack excluded from sample, close requeues");

                var largeHeaders = new HashMap<String, Object>();
                for (int i = 0; i < 64; i++) largeHeaders.put("header" + i, "h".repeat(64));
                channel.basicPublish("", queue, new AMQP.BasicProperties.Builder().headers(largeHeaders).build(), new byte[1]);
                var limited = call(first, "rabbitmq_consume_messages", Map.of("queue", queue, "count", 1, "maxWaitMs", 1000));
                assertEquals(0, limited.path("ackSent").asInt());
                assertNotNull(channel.basicGet(queue, true), "Hidden oversized result must remain available");
                channel.basicPublish("", queue, null, "b".repeat(512).getBytes(StandardCharsets.UTF_8));
                var truncated = call(first, "rabbitmq_sample_messages", Map.of("queue", queue, "count", 1)).path("messages").get(0);
                assertTrue(truncated.path("truncated").asBoolean());
                assertEquals(512, truncated.path("sizeBytes").asInt());
                assertEquals(1, call(first, "rabbitmq_consume_messages", Map.of("queue", queue, "count", 1)).path("ackSent").asInt());
                passed("response limit leaves hidden message unacked, sample body truncation");
            }
            passed("HTTP MCP sessions and resource closure");
            try (var context = new SpringApplicationBuilder(OmniConnectApplication.class).run(
                    "--server.address=127.0.0.1", "--server.port=0", "--omni.auth.enabled=false",
                    "--spring.config.location=file:../application.yml", "--omni.rabbitmq.enabled=true",
                    "--omni.rabbitmq.connection.virtual-host=" + vhost);
                    var client = RuntimeConnectionsTest.client(context)) {
                assertEquals(16, RuntimeConnectionsTest.names(client).size());
                call(client, "rabbitmq_inspect_queue", Map.of("queue", "acceptance / encoded"));
                passed("YAML startup activation");
            }
        } finally {
            if (created) { api("DELETE", "vhosts/" + enc(vhost), null); passed("isolated vhost deleted"); }
            http.close();
        }
    }
}
