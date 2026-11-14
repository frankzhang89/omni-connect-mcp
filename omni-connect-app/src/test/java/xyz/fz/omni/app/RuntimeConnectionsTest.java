package xyz.fz.omni.app;

import static org.junit.jupiter.api.Assertions.*;

import io.modelcontextprotocol.client.*;
import io.modelcontextprotocol.client.transport.HttpClientStreamableHttpTransport;
import io.modelcontextprotocol.spec.McpSchema.*;

import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.server.context.WebServerApplicationContext;

import tools.jackson.databind.json.JsonMapper;

import xyz.fz.omni.config.support.UpstreamFixture;

import java.time.Duration;
import java.util.*;

class RuntimeConnectionsTest {
    private static final JsonMapper JSON = new JsonMapper();

    @Test
    void nacosVersionIsSharedAndReplacedAtomicallyAcrossHttpSessions() throws Exception {
        try (var old = new UpstreamFixture();
                var newer = new UpstreamFixture();
                var context =
                        start(
                                "--omni.config.nacos.enabled=false",
                                "--omni.config.nacos.server-addr=" + old.base());
                var first = client(context);
                var second = client(context)) {
            newer.nacosVersion = "3.2.0";
            assertEquals(0, old.stateV3Reads.get());
            assertEquals(0, old.stateV1Reads.get());
            assertFalse(call(first, "connector_status", Map.of()).path("nacos").has("version"));
            var connected =
                    call(
                            first,
                            "nacos_connect",
                            Map.of(
                                    "serverAddr",
                                    old.base(),
                                    "username",
                                    "user",
                                    "password",
                                    "password"));
            assertEquals("2.4.3", connected.path("version").asString());
            call(first, "nacos_publish_config", Map.of("dataId", "a", "content", "key: old"));
            assertEquals(
                    "key: old",
                    call(second, "nacos_read_config", Map.of("dataId", "a"))
                            .path("content")
                            .asString());
            assertEquals(
                    "2.4.3",
                    call(second, "connector_status", Map.of())
                            .path("nacos")
                            .path("version")
                            .asString());
            // A failed version probe cannot replace either the old connection or its metadata.
            newer.stateBody = "{\"version\":\"4.0.0\"}";
            var failed =
                    first.callTool(
                            new CallToolRequest(
                                    "nacos_connect", Map.of("serverAddr", newer.base())));
            assertTrue(failed.isError());
            assertEquals(
                    "2.4.3",
                    call(second, "connector_status", Map.of())
                            .path("nacos")
                            .path("version")
                            .asString());
            call(second, "nacos_read_config", Map.of("dataId", "a"));
            newer.stateBody = null;
            // Namespace validation failure also leaves the old generation intact.
            newer.namespaceV3Status = 403;
            failed =
                    first.callTool(
                            new CallToolRequest(
                                    "nacos_connect",
                                    Map.of(
                                            "serverAddr",
                                            newer.base(),
                                            "username",
                                            "user",
                                            "password",
                                            "password")));
            assertTrue(failed.isError());
            assertEquals(
                    "2.4.3",
                    call(second, "connector_status", Map.of())
                            .path("nacos")
                            .path("version")
                            .asString());
            newer.namespaceV3Status = 200;
            assertEquals(
                    "3.2.0",
                    call(
                                    first,
                                    "nacos_connect",
                                    Map.of(
                                            "serverAddr",
                                            newer.base(),
                                            "username",
                                            "user",
                                            "password",
                                            "password"))
                            .path("version")
                            .asString());
            assertEquals(
                    "3.2.0",
                    call(second, "connector_status", Map.of())
                            .path("nacos")
                            .path("version")
                            .asString());
            call(second, "nacos_publish_config", Map.of("dataId", "a", "content", "key: new"));
            assertEquals(
                    "key: new",
                    call(first, "nacos_read_config", Map.of("dataId", "a"))
                            .path("content")
                            .asString());
            try (var third = client(context)) {
                assertEquals(
                        "3.2.0",
                        call(third, "connector_status", Map.of())
                                .path("nacos")
                                .path("version")
                                .asString());
                call(third, "nacos_read_config", Map.of("dataId", "a"));
            }
            assertEquals(1, old.stateV3Reads.get());
            assertEquals(1, old.stateV1Reads.get());
            assertEquals(3, newer.stateV3Reads.get());
            assertEquals(0, newer.stateV1Reads.get());
        }
    }

    @Test
    void yamlDetectsVersionAtStartupAndFailsOnInvalidStateOrMissingAddress() throws Exception {
        try (var peer = new UpstreamFixture()) {
            peer.nacosVersion = "3.0.0";
            try (var context =
                            start(
                                    "--omni.config.nacos.enabled=true",
                                    "--omni.config.nacos.server-addr=" + peer.base(),
                                    "--omni.config.nacos.username=user",
                                    "--omni.config.nacos.password=password");
                    var client = client(context)) {
                assertEquals(1, peer.stateV3Reads.get());
                assertEquals(0, peer.namespaceV3Reads.get());
                assertEquals(
                        "3.0.0",
                        call(client, "connector_status", Map.of())
                                .path("nacos")
                                .path("version")
                                .asString());
                call(client, "nacos_list_namespaces", Map.of());
                assertEquals(1, peer.stateV3Reads.get());
            }
            peer.stateBody = "{}";
            assertThrows(
                    RuntimeException.class,
                    () ->
                            start(
                                    "--omni.config.nacos.enabled=true",
                                    "--omni.config.nacos.server-addr=" + peer.base()));
            assertThrows(RuntimeException.class, () -> start("--omni.config.nacos.enabled=true"));
            peer.stateBody = null;
            try (var context =
                            start(
                                    "--omni.config.nacos.enabled=true",
                                    "--omni.config.nacos.server-addr=" + peer.base());
                    var client = client(context)) {
                assertEquals(
                        "3.0.0",
                        call(client, "connector_status", Map.of())
                                .path("nacos")
                                .path("version")
                                .asString());
                assertEquals(3, peer.stateV3Reads.get());
            }
        }
    }

    static org.springframework.context.ConfigurableApplicationContext start(String... extra) {
        var args =
                new ArrayList<>(
                        List.of(
                                "--spring.config.location=optional:classpath:/missing-test.yml",
                                "--server.port=0",
                                "--server.address=127.0.0.1",
                                "--spring.ai.mcp.server.stdio=false",
                                "--spring.ai.mcp.server.protocol=STREAMABLE",
                                "--spring.ai.mcp.server.type=SYNC",
                                "--omni.auth.enabled=false"));
        args.addAll(List.of(extra));
        return new SpringApplicationBuilder(OmniConnectApplication.class, TestDatabase.class)
                .run(args.toArray(String[]::new));
    }

    static McpSyncClient client(
            org.springframework.context.ConfigurableApplicationContext context) {
        int port = ((WebServerApplicationContext) context).getWebServer().getPort();
        var transport =
                HttpClientStreamableHttpTransport.builder("http://127.0.0.1:" + port)
                        .endpoint("/mcp")
                        .build();
        var client = McpClient.sync(transport).requestTimeout(Duration.ofSeconds(20)).build();
        client.initialize();
        return client;
    }

    static Set<String> names(McpSyncClient client) {
        return new TreeSet<>(client.listTools().tools().stream().map(Tool::name).toList());
    }

    static tools.jackson.databind.JsonNode call(
            McpSyncClient client, String name, Map<String, Object> args) {
        var result = client.callTool(new CallToolRequest(name, args));
        assertFalse(Boolean.TRUE.equals(result.isError()), result.toString());
        var content = (TextContent) result.content().getFirst();
        return JSON.readTree(content.text());
    }

    @Test
    void activatesOnTheSameSessionSharesAcrossClientsAndRetainsConnectionsAfterFailure()
            throws Exception {
        try (var peers = new UpstreamFixture();
                var context = start();
                var first = client(context);
                var second = client(context)) {
            assertEquals(8, names(first).size());
            assertFalse(names(first).contains("db_query"));
            call(first, "convert_config", Map.of("content", "answer=42"));
            call(first, "apollo_connect", Map.of("meta", peers.apollo(), "appId", "source-app"));
            assertTrue(names(second).contains("apollo_read_config"));
            call(second, "apollo_read_config", Map.of());
            call(
                    first,
                    "nacos_connect",
                    Map.of("serverAddr", peers.base(), "username", "user", "password", "password"));
            call(
                    first,
                    "nacos_publish_config",
                    Map.of("dataId", "merged.yaml", "content", "value: test\n"));
            assertEquals(
                    "value: test\n",
                    call(second, "nacos_read_config", Map.of("dataId", "merged.yaml"))
                            .path("content")
                            .asString());
            var invalid =
                    first.callTool(
                            new CallToolRequest(
                                    "nacos_connect", Map.of("serverAddr", "http://127.0.0.1:1")));
            assertTrue(invalid.isError());
            call(second, "nacos_read_config", Map.of("dataId", "merged.yaml"));
            String url = "jdbc:h2:mem:" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1";
            call(
                    first,
                    "db_switch_datasource",
                    Map.of("jdbcUrl", url, "username", "sa", "password", ""));
            assertEquals(21, names(second).size());
            call(first, "db_execute_ddl", Map.of("sql", "CREATE TABLE omni_sample(id INT)"));
            var begin = call(first, "db_execute", Map.of("sql", "BEGIN"));
            String tx = begin.path("transactionId").asString();
            assertFalse(tx.isBlank());
            call(
                    first,
                    "db_execute",
                    Map.of("sql", "INSERT INTO omni_sample VALUES (42)", "transactionId", tx));
            call(
                    second,
                    "db_switch_datasource",
                    Map.of(
                            "jdbcUrl",
                            "jdbc:h2:mem:" + UUID.randomUUID(),
                            "username",
                            "sa",
                            "password",
                            ""));
            call(first, "db_execute", Map.of("sql", "COMMIT", "transactionId", tx));
            try (var connection = java.sql.DriverManager.getConnection(url, "sa", "");
                    var statement = connection.createStatement();
                    var rs = statement.executeQuery("SELECT COUNT(*) FROM omni_sample")) {
                rs.next();
                assertEquals(1, rs.getInt(1));
            }
            assertEquals(21, names(first).size());
            assertTrue(
                    call(first, "connector_status", Map.of())
                            .path("jdbc")
                            .path("enabled")
                            .asBoolean());
        }
        try (var context = start();
                var fresh = client(context)) {
            assertEquals(8, names(fresh).size());
        }
    }

    @Test
    void yamlCanEnableOnlyOneProviderWithoutOtherSettings() throws Exception {
        try (var peers = new UpstreamFixture();
                var context =
                        start(
                                "--omni.config.nacos.enabled=true",
                                "--omni.config.nacos.server-addr=" + peers.base(),
                                "--omni.config.nacos.username=user",
                                "--omni.config.nacos.password=password");
                var client = client(context)) {
            assertEquals(12, names(client).size());
            assertFalse(names(client).contains("apollo_read_config"));
            call(client, "nacos_list_namespaces", Map.of());
        }
    }

    @Test
    void allCapabilitiesExposeTwentySixToolsAndRedisReconfigurationRetainsDataOnFailure()
            throws Exception {
        try (var peers = new UpstreamFixture();
                var redis = new RedisFixture();
                var context = start();
                var client = client(context)) {
            call(client, "apollo_connect", Map.of("meta", peers.apollo(), "appId", "source-app"));
            call(
                    client,
                    "nacos_connect",
                    Map.of("serverAddr", peers.base(), "username", "user", "password", "password"));
            call(
                    client,
                    "db_switch_datasource",
                    Map.of(
                            "jdbcUrl",
                            "jdbc:h2:mem:" + UUID.randomUUID(),
                            "username",
                            "sa",
                            "password",
                            ""));
            call(client, "redis_connect", Map.of("host", "127.0.0.1", "port", redis.port()));
            assertEquals(28, names(client).size());
            call(
                    client,
                    "redis_write_key",
                    Map.of("key", "greeting", "type", "string", "value", "hello"));
            assertTrue(
                    call(client, "redis_read_key", Map.of("key", "greeting"))
                            .toString()
                            .contains("hello"));
            var invalid =
                    client.callTool(
                            new CallToolRequest(
                                    "redis_connect",
                                    Map.of("host", "", "password", "private-marker")));
            assertTrue(invalid.isError());
            assertFalse(invalid.toString().contains("private-marker"));
            assertTrue(
                    call(client, "redis_read_key", Map.of("key", "greeting"))
                            .toString()
                            .contains("hello"));
            call(
                    client,
                    "redis_connect",
                    Map.of("host", "127.0.0.1", "port", redis.port(), "database", 1));
            assertEquals(28, names(client).size());
            assertFalse(
                    call(client, "redis_inspect_key", Map.of("key", "greeting"))
                            .path("exists")
                            .asBoolean());
        }
    }
}
