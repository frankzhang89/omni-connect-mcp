package xyz.fz.omni.config.mcp;

import static org.assertj.core.api.Assertions.*;

import io.modelcontextprotocol.client.McpClient;
import io.modelcontextprotocol.client.McpSyncClient;
import io.modelcontextprotocol.client.transport.HttpClientStreamableHttpTransport;
import io.modelcontextprotocol.spec.McpSchema.*;

import org.junit.jupiter.api.*;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import xyz.fz.omni.app.OmniConnectApplication;
import xyz.fz.omni.config.support.UpstreamFixture;

import java.time.Duration;
import java.util.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class McpIntegrationTest {
    private UpstreamFixture upstream;
    private ConfigurableApplicationContext application;
    private McpSyncClient client;
    private final JsonMapper json = new JsonMapper();

    /** Starts a real MCP server against local HTTP peers or the optional Postman simulation. */
    @BeforeAll
    void start() throws Exception {
        // Set -Dmigrate.simulation-url=http://127.0.0.1:14900 to use the checked-in Postman
        // simulation.
        String simulation = System.getProperty("migrate.simulation-url");
        String apollo;
        String nacos;
        if (simulation == null) {
            upstream = new UpstreamFixture();
            apollo = upstream.apollo();
            nacos = upstream.base();
        } else {
            apollo = simulation + "/apollo";
            nacos = simulation;
        }
        application =
                SpringApplication.run(
                        OmniConnectApplication.class,
                        "--spring.config.location=file:"
                                + java.nio.file.Path.of("../application.yml").toAbsolutePath(),
                        "--omni.auth.enabled=false",
                        "--omni.config.apollo.enabled=true",
                        "--omni.config.nacos.enabled=true",
                        "--server.port=0",
                        "--spring.main.banner-mode=off",
                        // Isolate the fixture endpoint from user-edited deployment configuration.
                        "--spring.ai.mcp.server.stdio=false",
                        "--spring.ai.mcp.server.streamable-http.mcp-endpoint=/mcp",
                        "--omni.config.apollo.file-type=properties",
                        "--omni.config.nacos.file-type=yaml",
                        "--omni.config.apollo.meta=" + apollo,
                        "--omni.config.apollo.app-id=source-app",
                        "--omni.config.apollo.access-key=",
                        "--omni.config.apollo.portal-url=" + apollo,
                        "--omni.config.apollo.openapi-token=test-openapi-token",
                        "--omni.config.apollo.env=DEV",
                        "--omni.config.apollo.operator=editor",
                        "--omni.config.apollo.released-by=publisher",
                        "--omni.config.nacos.server-addr=" + nacos,
                        "--omni.config.nacos.context-path=/nacos",
                        "--omni.config.nacos.username=user",
                        "--omni.config.nacos.password=password");
        int port = ((WebServerApplicationContext) application).getWebServer().getPort();
        var transport =
                HttpClientStreamableHttpTransport.builder("http://127.0.0.1:" + port)
                        .endpoint("/mcp")
                        .build();
        client = McpClient.sync(transport).requestTimeout(Duration.ofSeconds(10)).build();
        assertThat(client.initialize().serverInfo().name()).isEqualTo("omni-connect-mcp");
    }

    /** Closes the MCP client, application server, and any locally owned HTTP peers. */
    @AfterAll
    void stop() {
        if (client != null) client.closeGracefully();
        if (application != null) application.close();
        if (upstream != null) upstream.close();
    }

    /** Checks tool discovery, required arguments, and the publication mutation hint over MCP. */
    @Test
    void listsExactlyEightToolsWithRequiredInputsAndMutationHints() {
        var tools =
                client.listTools().tools().stream()
                        .filter(
                                t ->
                                        !Set.of(
                                                        "connector_status",
                                                        "apollo_connect",
                                                        "nacos_connect",
                                                        "redis_connect",
                                                        "db_switch_datasource")
                                                .contains(t.name()))
                        .toList();
        assertThat(tools)
                .extracting(Tool::name)
                .containsExactlyInAnyOrder(
                        "apollo_read_config",
                        "apollo_read_item",
                        "apollo_update_config",
                        "nacos_read_config",
                        "nacos_publish_config",
                        "nacos_list_namespaces",
                        "nacos_list_and_count_data_ids",
                        "convert_config");
        assertThat(
                        tools.stream()
                                .filter(
                                        t ->
                                                Set.of(
                                                                "nacos_list_namespaces",
                                                                "nacos_list_and_count_data_ids")
                                                        .contains(t.name())))
                .allSatisfy(tool -> assertThat(tool.annotations().readOnlyHint()).isTrue());
        Tool item =
                tools.stream()
                        .filter(t -> t.name().equals("apollo_read_item"))
                        .findFirst()
                        .orElseThrow();
        assertThat(
                        ((List<?>) item.inputSchema().get("required"))
                                .stream().map(Object::toString).toList())
                .containsExactly("key");
        assertThat(item.annotations().readOnlyHint()).isTrue();
        Tool update =
                tools.stream()
                        .filter(t -> t.name().equals("apollo_update_config"))
                        .findFirst()
                        .orElseThrow();
        assertThat(
                        ((List<?>) update.inputSchema().get("required"))
                                .stream().map(Object::toString).toList())
                .containsExactlyInAnyOrder("key", "value");
        assertThat(update.annotations().destructiveHint()).isTrue();
        assertThat(update.annotations().idempotentHint()).isFalse();
        Tool publish =
                tools.stream()
                        .filter(t -> t.name().equals("nacos_publish_config"))
                        .findFirst()
                        .orElseThrow();
        assertThat(
                        ((List<?>) publish.inputSchema().get("required"))
                                .stream().map(Object::toString).toList())
                .containsExactlyInAnyOrder("dataId", "content");
        assertThat(publish.annotations().destructiveHint()).isTrue();
        assertThat(publish.description()).contains("never converts");
        assertThat(
                        ((Map<?, ?>) publish.inputSchema().get("properties"))
                                .keySet().stream().map(Object::toString).toList())
                .contains("fileType", "group", "namespace");
    }

    @Test
    void listsNacosNamespacesAndCountsOnlyTheRequestedGroup() {
        String group = "GROUP_" + UUID.randomUUID();
        var identity = Map.<String, Object>of("namespace", "public", "group", group);
        var empty = call("nacos_list_and_count_data_ids", identity);
        assertThat(empty.path("totalCount").asInt()).isZero();
        assertThat(empty.path("dataIds").isArray()).isTrue();
        assertThat(empty.path("dataIds")).isEmpty();
        for (String dataId : List.of("one.yaml", "two.yaml")) {
            call(
                    "nacos_publish_config",
                    Map.of(
                            "namespace",
                            "",
                            "group",
                            group,
                            "dataId",
                            dataId,
                            "content",
                            "key: value"));
        }
        call(
                "nacos_publish_config",
                Map.of(
                        "namespace",
                        "",
                        "group",
                        group + "_OTHER",
                        "dataId",
                        "one.yaml",
                        "content",
                        "key: value"));
        var count = call("nacos_list_and_count_data_ids", identity);
        assertThat(count.path("namespace").asString()).isEmpty();
        assertThat(count.path("group").asString()).isEqualTo(group);
        assertThat(count.path("totalCount").asInt()).isEqualTo(2);
        assertThat(count.path("dataIds"))
                .extracting(JsonNode::asString)
                .containsExactlyInAnyOrder("one.yaml", "two.yaml");
        assertThat(
                        call(
                                        "nacos_list_and_count_data_ids",
                                        Map.of("namespace", "empty-ns", "group", group))
                                .path("totalCount")
                                .asInt())
                .isZero();
        assertThat(call("nacos_list_namespaces", Map.of()).path("namespaces"))
                .anySatisfy(
                        namespace -> {
                            assertThat(namespace.path("namespaceShowName").asString())
                                    .isEqualTo("public");
                            assertThat(namespace.path("configCount").asInt())
                                    .isGreaterThanOrEqualTo(3);
                        });
        assertError("nacos_list_and_count_data_ids", Map.of("group", " "), "INVALID_ARGUMENT");
    }

    /** Exercises the standalone read and update's shared item reads through real MCP and HTTP. */
    @Test
    void readsEditsAndPublishesOneApolloItem() {
        String namespace = "items-" + UUID.randomUUID();
        Map<String, Object> identity = Map.of("key", "service.url", "namespace", namespace);
        var before = call("apollo_read_item", identity);
        assertThat(before.path("valueSource").asString()).isEqualTo("EDITING");
        assertThat(before.path("value").asString())
                .isEqualTo("https://api.example.com:8080/auth?enabled=true");
        String replacement = "https://api.example.com:9090/auth?enabled=true";
        var saved =
                call(
                        "apollo_update_config",
                        Map.of(
                                "key",
                                "service.url",
                                "namespace",
                                namespace,
                                "value",
                                replacement,
                                "publish",
                                false));
        assertThat(saved.path("published").asBoolean()).isFalse();
        assertThat(call("apollo_read_item", identity).path("value").asString())
                .isEqualTo(replacement);
        var released =
                call(
                        "apollo_update_config",
                        Map.of("key", "service.url", "namespace", namespace, "value", replacement));
        assertThat(released.path("updated").asBoolean()).isFalse();
        assertThat(released.path("published").asBoolean()).isTrue();
        assertThat(released.path("verified").asBoolean()).isTrue();
        assertThat(
                        call("apollo_read_config", Map.of("namespace", namespace))
                                .path("content")
                                .asString())
                .contains("service.url=" + replacement);
        assertError(
                "apollo_read_item",
                Map.of("key", "missing-key", "namespace", namespace),
                "NOT_FOUND");
        assertError(
                "apollo_update_config",
                Map.of("key", "service.url", "value", "x", "fileType", "yaml"),
                "INVALID_FORMAT");
    }

    /** Runs the complete read-convert-publish-verify workflow through the MCP protocol. */
    @Test
    void readsConvertsPublishesAndReadsBackThroughMcp() {
        JsonNode source = call("apollo_read_config", Map.of());
        assertThat(source.path("releaseKey").asString()).isEqualTo("release-1");
        JsonNode converted =
                call("convert_config", Map.of("content", source.path("content").asString()));
        assertThat(converted.path("converted").asBoolean()).isTrue();
        String dataId = "integration-" + UUID.randomUUID() + ".yaml";
        JsonNode published =
                call(
                        "nacos_publish_config",
                        Map.of("dataId", dataId, "content", converted.path("content").asString()));
        JsonNode read = call("nacos_read_config", Map.of("dataId", dataId));
        assertThat(read.path("sha256").asString()).isEqualTo(published.path("sha256").asString());
        assertThat(read.path("content").asString()).isEqualTo(converted.path("content").asString());
    }

    /**
     * Checks direct publication, overwrite, and same-format preservation without double conversion.
     */
    @Test
    void publishesAgentConvertedContentWithoutConvertingAndSupportsOverwrite() {
        String id = "agent-" + UUID.randomUUID() + ".yaml";
        call("nacos_publish_config", Map.of("dataId", id, "content", UpstreamFixture.YAML));
        assertThat(call("nacos_read_config", Map.of("dataId", id)).path("content").asString())
                .isEqualTo(UpstreamFixture.YAML);
        String changed = "# exact content\napp:\n  name: 'second'\n";
        call("nacos_publish_config", Map.of("dataId", id, "content", changed));
        assertThat(call("nacos_read_config", Map.of("dataId", id)).path("content").asString())
                .isEqualTo(changed);
        var same =
                call(
                        "convert_config",
                        Map.of("content", changed, "sourceType", "yml", "targetType", "yaml"));
        assertThat(same.path("converted").asBoolean()).isFalse();
        assertThat(same.path("content").asString()).isEqualTo(changed);
    }

    /** Checks MCP error envelopes, sensitive-message suppression, and valid empty documents. */
    @Test
    void reportsStableMcpToolErrorsAndSupportsEmptyDocuments() {
        assertError(
                "nacos_publish_config",
                Map.of("dataId", "invalid.yaml", "content", "app.name=value"),
                "INVALID_FORMAT");
        assertError(
                "nacos_read_config", Map.of("dataId", "missing-" + UUID.randomUUID()), "NOT_FOUND");
        assertError("convert_config", Map.of("content", "a=x\na.b=y"), "CONVERSION_CONFLICT");
        assertError(
                "convert_config",
                Map.of("content", "password: [PRIVATE_SECRET", "sourceType", "yaml"),
                "INVALID_FORMAT");
        String id = "empty-" + UUID.randomUUID();
        call("nacos_publish_config", Map.of("dataId", id, "content", ""));
        assertThat(call("nacos_read_config", Map.of("dataId", id)).path("content").asString())
                .isEmpty();
    }

    /**
     * Invokes a tool, requires success, and returns its structured payload for scenario assertions.
     */
    private JsonNode call(String name, Map<String, Object> arguments) {
        var result = client.callTool(new CallToolRequest(name, arguments));
        assertThat(result.isError()).as(name + " error: " + result.content()).isFalse();
        return json.valueToTree(result.structuredContent());
    }

    /** Requires an MCP tool error with the expected stable code and no sensitive parser excerpt. */
    private void assertError(String name, Map<String, Object> arguments, String code) {
        var result = client.callTool(new CallToolRequest(name, arguments));
        assertThat(result.isError()).isTrue();
        JsonNode error = json.valueToTree(result.structuredContent());
        assertThat(error.path("code").asString()).isEqualTo(code);
        assertThat(error.toString()).doesNotContain("PRIVATE_SECRET");
    }
}
