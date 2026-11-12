package xyz.fz.omni.app;

import static org.junit.jupiter.api.Assertions.*;

import io.modelcontextprotocol.spec.McpSchema.CallToolRequest;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.net.URI;
import java.net.http.*;
import java.util.Map;

/** Runs the real connector and MCP transport against the checked-in Postman simulation. */
@EnabledIfSystemProperty(named = "migrate.simulation-url", matches = "http://.*")
class NacosSimulationTest {
    private static final JsonMapper JSON = new JsonMapper();

    @Test
    void detectsOncePerActivationAndRoutesEachGenerationWithoutBusinessFallback() throws Exception {
        String base = System.getProperty("migrate.simulation-url");
        try (var controls = HttpClient.newHttpClient();
                var context = RuntimeConnectionsTest.start();
                var client = RuntimeConnectionsTest.client(context)) {
            try {
                for (String version :
                        new String[] {
                            "1.4.8", "2.0.0", "2.1.2", "2.2.0", "2.4.3", "3.0.0", "3.2.0"
                        }) {
                    configure(controls, base, "version=" + version);
                    var result =
                            RuntimeConnectionsTest.call(
                                    client,
                                    "nacos_connect",
                                    Map.of(
                                            "serverAddr",
                                            base,
                                            "username",
                                            "user",
                                            "password",
                                            "password"));
                    assertEquals(version, result.path("version").asString());
                    RuntimeConnectionsTest.call(
                            client,
                            "nacos_publish_config",
                            Map.of("dataId", "simulation.yaml", "content", "key: '中文 & +'"));
                    assertEquals(
                            "key: '中文 & +'",
                            RuntimeConnectionsTest.call(
                                            client,
                                            "nacos_read_config",
                                            Map.of("dataId", "simulation.yaml"))
                                    .path("content")
                                    .asString());
                    RuntimeConnectionsTest.call(
                            client, "nacos_read_config", Map.of("dataId", "simulation.yaml"));
                    RuntimeConnectionsTest.call(client, "nacos_list_and_count_data_ids", Map.of());
                    RuntimeConnectionsTest.call(client, "nacos_list_namespaces", Map.of());
                    assertEquals(
                            version,
                            RuntimeConnectionsTest.call(client, "connector_status", Map.of())
                                    .path("nacos")
                                    .path("version")
                                    .asString());
                    JsonNode requests = requests(controls, base);
                    assertEquals(1, count(requests, "/v3/admin/core/state"));
                    assertEquals(
                            version.startsWith("3.") ? 0 : 1,
                            count(requests, "/v1/console/server/state"));
                    String namespace =
                            version.startsWith("3.")
                                    ? "/v3/admin/core/namespace/list"
                                    : version.startsWith("1.")
                                                    || version.startsWith("2.0.")
                                                    || version.startsWith("2.1.")
                                            ? "/v1/console/namespaces"
                                            : "/v2/console/namespace/list";
                    assertEquals(2, count(requests, namespace));
                    for (JsonNode request : requests) {
                        String path = request.path("path").asString();
                        if (path.contains("/namespace")) assertEquals(namespace, path);
                        if (path.contains("/cs/"))
                            assertTrue(
                                    version.startsWith("3.")
                                            ? path.startsWith("/v3/admin/cs/")
                                            : path.equals("/v1/cs/configs"));
                    }
                }
                configure(controls, base, "version=3.2.0&stateStatus=500");
                assertTrue(
                        client.callTool(
                                        new CallToolRequest(
                                                "nacos_connect", Map.of("serverAddr", base)))
                                .isError());
                assertEquals(
                        "3.2.0",
                        RuntimeConnectionsTest.call(client, "connector_status", Map.of())
                                .path("nacos")
                                .path("version")
                                .asString());
                assertEquals(0, count(requests(controls, base), "/v1/console/server/state"));
                configure(controls, base, "version=3.2.0&readStatus=404&publishStatus=503");
                assertTrue(
                        client.callTool(
                                        new CallToolRequest(
                                                "nacos_read_config",
                                                Map.of("dataId", "simulation.yaml")))
                                .isError());
                assertTrue(
                        client.callTool(
                                        new CallToolRequest(
                                                "nacos_publish_config",
                                                Map.of(
                                                        "dataId",
                                                        "simulation.yaml",
                                                        "content",
                                                        "key: value")))
                                .isError());
                JsonNode failedRequests = requests(controls, base);
                assertEquals(2, failedRequests.size());
                assertEquals(2, count(failedRequests, "/v3/admin/cs/config"));
            } finally {
                configure(controls, base, "version=2.4.3");
            }
        }
    }

    private static void configure(HttpClient client, String base, String form) throws Exception {
        var request =
                HttpRequest.newBuilder(URI.create(base + "/nacos/__test__/configure"))
                        .header("Content-Type", "application/x-www-form-urlencoded")
                        .POST(HttpRequest.BodyPublishers.ofString(form))
                        .build();
        assertEquals(200, client.send(request, HttpResponse.BodyHandlers.ofString()).statusCode());
    }

    private static JsonNode requests(HttpClient client, String base) throws Exception {
        var request =
                HttpRequest.newBuilder(URI.create(base + "/nacos/__test__/requests")).GET().build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());
        return JSON.readTree(response.body());
    }

    private static long count(JsonNode requests, String path) {
        long count = 0;
        for (var request : requests) if (path.equals(request.path("path").asString())) count++;
        return count;
    }
}
