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
    void listsApolloAppIdsWithPortalLoginAndPrefersTheTokenWhenConfigured() throws Exception {
        try (var peers = new UpstreamFixture();
                var context = start(
                        "--omni.config.apollo.enabled=true",
                        "--omni.config.apollo.meta=" + peers.apollo(),
                        "--omni.config.apollo.app-id=source-app",
                        "--omni.config.apollo.portal-url=" + peers.apollo(),
                        "--omni.config.apollo.portal-username=fixture-user",
                        "--omni.config.apollo.portal-password=" + UpstreamFixture.PORTAL_PASSWORD);
                var client = client(context)) {
            var expected = JSON.readTree("{\"appIds\":[\"sq-order\",\"other-app\",\"sq-payment\"]}");
            assertEquals(expected, call(client, "apollo_list_app_ids", Map.of()));
            assertEquals(expected, call(client, "apollo_list_app_ids", Map.of()));
            assertEquals(2, peers.portalLoginAttempts.get());
            assertEquals(2, peers.portalSessionAppReads.get());
            assertNull(peers.lastPortalPath);

            peers.portalSessionExpired = true;
            assertApolloListError(client, "AUTHENTICATION_FAILED");
            peers.portalSessionExpired = false;
            peers.portalLoginRedirect = "https://example.invalid/signin";
            assertApolloListError(client, "UPSTREAM_ERROR");
            assertEquals(3, peers.portalSessionAppReads.get());
            peers.portalLoginRedirect = "/apollo/";

            var settings = new HashMap<String, Object>(Map.of(
                    "meta", peers.apollo(), "appId", "source-app", "portalUrl", peers.apollo(),
                    "portalUsername", "fixture-user"));
            call(client, "apollo_connect", settings);
            assertApolloListError(client, "INVALID_ARGUMENT");
            settings.put("portalPassword", "incorrect");
            call(client, "apollo_connect", settings);
            assertApolloListError(client, "AUTHENTICATION_FAILED");
            assertEquals(5, peers.portalLoginAttempts.get());

            settings.put("portalPassword", UpstreamFixture.PORTAL_PASSWORD);
            settings.put("openapiToken", "invalid-token");
            call(client, "apollo_connect", settings);
            assertApolloListError(client, "AUTHENTICATION_FAILED");
            settings.put("openapiToken", "test-openapi-token");
            call(client, "apollo_connect", settings);
            assertEquals(expected, call(client, "apollo_list_app_ids", Map.of()));
            assertEquals(5, peers.portalLoginAttempts.get());
            assertEquals(3, peers.portalSessionAppReads.get());
        }
    }

    private static void assertApolloListError(McpSyncClient client, String code) {
        assertApolloError(client, "apollo_list_app_ids", Map.of(), code);
    }

    private static tools.jackson.databind.JsonNode assertApolloError(
            McpSyncClient client, String tool, Map<String, Object> args, String code) {
        var response = client.callTool(new CallToolRequest(tool, args));
        assertTrue(response.isError());
        var error = JSON.valueToTree(response.structuredContent());
        assertEquals(code, error.path("code").asString());
        assertFalse(response.toString().contains(UpstreamFixture.PORTAL_PASSWORD));
        assertFalse(response.toString().contains("SESSION="));
        return error;
    }

    @Test
    void editsApolloItemsUsingOnePortalSessionAndVerifiesPublication() throws Exception {
        try (var peers = new UpstreamFixture();
                var context = start();
                var client = client(context)) {
            var settings = new HashMap<String, Object>(Map.of(
                    "meta", peers.apollo(), "appId", "source-app", "portalUrl", peers.apollo(),
                    "portalUsername", "fixture-user", "portalPassword", UpstreamFixture.PORTAL_PASSWORD,
                    "env", "DEV", "namespace", "web-config"));
            call(client, "apollo_connect", settings);
            assertTrue(names(client).containsAll(
                    Set.of("apollo_create_config", "apollo_delete_config", "apollo_read_item")));
            var target = Map.<String, Object>of("key", "session.new");
            var create = Map.<String, Object>of(
                    "key", "session.new", "value", "new value", "comment", "first remark");
            var created = call(client, "apollo_create_config", create);
            assertTrue(created.path("created").asBoolean());
            assertFalse(created.path("published").asBoolean());
            assertEquals(1, peers.portalLoginAttempts.get());
            assertEquals(0, peers.releaseWrites.get());
            assertEquals("first remark", call(client, "apollo_read_item", target).path("comment").asString());

            var update = Map.<String, Object>of("key", "session.new", "value", "changed value");
            var updated = call(client, "apollo_update_config", update);
            assertTrue(updated.path("updated").asBoolean());
            assertTrue(updated.path("published").asBoolean());
            assertTrue(updated.path("verified").asBoolean());
            assertEquals("first remark", peers.lastItemWrite.comment());
            assertFalse(peers.lastReleaseWrite.containsKey("releasedBy"));

            var deleted = call(client, "apollo_delete_config", target);
            assertTrue(deleted.path("deleted").asBoolean());
            assertFalse(deleted.path("published").asBoolean());
            assertTrue(peers.lastDeleteIdentifier.matches("[1-9][0-9]*"));
            assertNull(peers.lastDeleteOperator);
            assertApolloError(client, "apollo_read_item", target, "NOT_FOUND");
            assertTrue(call(client, "apollo_read_config", Map.of()).path("content").asString()
                    .contains("session.new=changed value"));
            var publishDelete = call(client, "apollo_delete_config", Map.of("key", "session.new", "publish", true));
            assertFalse(publishDelete.path("deleted").asBoolean());
            assertTrue(publishDelete.path("published").asBoolean());
            assertFalse(call(client, "apollo_read_config", Map.of()).path("content").asString()
                    .contains("session.new="));

            int before = peers.itemWrites.get();
            assertApolloError(client, "apollo_delete_config",
                    Map.of("key", "service.url", "operator", "another-user"), "INVALID_ARGUMENT");
            assertApolloError(client, "apollo_create_config",
                    Map.of("key", "x", "value", "y", "publish", true, "releasedBy", "another-user"),
                    "INVALID_ARGUMENT");
            peers.portalConfigHidden = true;
            assertApolloError(client, "apollo_create_config", create, "AUTHENTICATION_FAILED");
            peers.portalConfigHidden = false;
            assertEquals(before, peers.itemWrites.get());

            var draft = peers.draft("DEV", "source-app", "default", "web-config");
            peers.afterItemWrite = () -> draft.put("other.key", "raced");
            var blocked = assertApolloError(client, "apollo_delete_config",
                    Map.of("key", "service.url", "publish", true), "CONCURRENT_MODIFICATION");
            assertEquals("PREPUBLISH", blocked.path("stage").asString());
            assertTrue(blocked.path("deleted").asBoolean());
            assertFalse(blocked.path("published").asBoolean());
            draft.put("other.key", "unchanged");
            peers.afterItemWrite = () -> {};
            peers.releaseWriteStatus = 403;
            var denied = assertApolloError(client, "apollo_delete_config",
                    Map.of("key", "service.url", "publish", true), "AUTHENTICATION_FAILED");
            assertEquals("PUBLISH", denied.path("stage").asString());
            assertFalse(denied.path("deleted").asBoolean());
            assertFalse(denied.path("published").asBoolean());

            settings.put("openapiToken", "invalid-token");
            call(client, "apollo_connect", settings);
            int logins = peers.portalLoginAttempts.get();
            assertApolloError(client, "apollo_read_item", Map.of("key", "other.key"), "AUTHENTICATION_FAILED");
            assertApolloError(client, "apollo_delete_config",
                    Map.of("key", "other.key", "operator", "fixture-user"), "AUTHENTICATION_FAILED");
            assertEquals(logins, peers.portalLoginAttempts.get());
        }
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
            assertFalse(names(first).contains("apollo_list_app_ids"));
            call(first, "convert_config", Map.of("content", "answer=42"));
            call(first, "apollo_connect", Map.of("meta", peers.apollo(), "appId", "source-app"));
            assertTrue(names(second).contains("apollo_read_config"));
            call(second, "apollo_read_config", Map.of());
            assertTrue(names(second).contains("apollo_list_app_ids"));
            var missingPortal = second.callTool(new CallToolRequest("apollo_list_app_ids", Map.of()));
            assertTrue(missingPortal.isError());
            assertEquals(
                    "INVALID_ARGUMENT",
                    JSON.valueToTree(missingPortal.structuredContent()).path("code").asString());
            assertNull(peers.lastPortalPath);
            call(
                    first,
                    "apollo_connect",
                    Map.of(
                            "meta", peers.apollo(),
                            "appId", "source-app",
                            "portalUrl", peers.apollo() + "/",
                            "openapiToken", "test-openapi-token"));
            assertEquals(
                    JSON.readTree("{\"appIds\":[\"sq-order\",\"other-app\",\"sq-payment\"]}"),
                    call(second, "apollo_list_app_ids", Map.of()));
            assertEquals("/apollo/openapi/v1/apps", peers.lastPortalPath);
            assertEquals("test-openapi-token", peers.portalAuthorization);
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
            assertEquals(24, names(second).size());
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
            assertEquals(24, names(first).size());
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
    void allCapabilitiesExposeToolsAndRedisReconfigurationRetainsDataOnFailure()
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
            assertEquals(31, names(client).size());
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
            assertEquals(31, names(client).size());
            assertFalse(
                    call(client, "redis_inspect_key", Map.of("key", "greeting"))
                            .path("exists")
                            .asBoolean());
        }
    }
}
