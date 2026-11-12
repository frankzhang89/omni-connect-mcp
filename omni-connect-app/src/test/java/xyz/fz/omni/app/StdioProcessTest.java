package xyz.fz.omni.app;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.*;
import org.springframework.boot.Banner;
import org.springframework.boot.SpringApplication;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

/** Uses real OS pipes and independent JVMs, including EOF during startup. */
@Timeout(90)
class StdioProcessTest {
    private static final JsonMapper JSON = new JsonMapper();

    public static class Fixture {
        public static void main(String[] args) {
            var app = new SpringApplication(OmniConnectApplication.class, TestDatabase.class);
            app.setBannerMode(Banner.Mode.OFF);
            app.run(args);
        }
    }

    static final class Child implements AutoCloseable {
        final Process process;
        final BufferedWriter input;
        final BlockingQueue<String> output = new LinkedBlockingQueue<>();
        final StringBuilder errors = new StringBuilder();
        final List<String> stdout = new CopyOnWriteArrayList<>();
        int seq;

        Child() throws Exception {
            String java =
                    Path.of(
                                    System.getProperty("java.home"),
                                    "bin",
                                    System.getProperty("os.name").startsWith("Windows")
                                            ? "java.exe"
                                            : "java")
                            .toString();
            String cp =
                    System.getProperty(
                            "surefire.test.class.path", System.getProperty("java.class.path"));
            process =
                    new ProcessBuilder(
                                    java,
                                    "-cp",
                                    cp,
                                    Fixture.class.getName(),
                                    "--spring.config.location=file:"
                                            + Path.of("../application.yml").toAbsolutePath(),
                                    "--spring.ai.mcp.server.stdio=true",
                                    "--spring.main.web-application-type=servlet",
                                    "--spring.main.banner-mode=console",
                                    "--logging.config=file:/missing.xml")
                            .start();
            input =
                    new BufferedWriter(
                            new OutputStreamWriter(
                                    process.getOutputStream(), StandardCharsets.UTF_8));
            Thread.ofVirtual()
                    .start(
                            () -> {
                                try (var reader = process.inputReader(StandardCharsets.UTF_8)) {
                                    String line;
                                    while ((line = reader.readLine()) != null) {
                                        stdout.add(line);
                                        output.add(line);
                                    }
                                } catch (IOException ignored) {
                                }
                            });
            Thread.ofVirtual()
                    .start(
                            () -> {
                                try (var reader = process.errorReader(StandardCharsets.UTF_8)) {
                                    String line;
                                    while ((line = reader.readLine()) != null) {
                                        synchronized (errors) {
                                            errors.append(line).append('\n');
                                        }
                                    }
                                } catch (IOException ignored) {
                                }
                            });
        }

        JsonNode rpc(String method, Map<String, Object> params) throws Exception {
            int id = ++seq;
            input.write(
                    JSON.writeValueAsString(
                            Map.of(
                                    "jsonrpc", "2.0", "id", id, "method", method, "params",
                                    params)));
            input.newLine();
            input.flush();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(35);
            while (System.nanoTime() < deadline) {
                String line = output.poll(1, TimeUnit.SECONDS);
                if (line == null) {
                    assertTrue(process.isAlive(), errors.toString());
                    continue;
                }
                var response = JSON.readTree(line);
                if (response.has("id") && response.path("id").asInt() == id) {
                    assertFalse(response.has("error"), response.toString());
                    return response;
                }
            }
            throw new AssertionError("Timed out waiting for " + method + "; " + errors);
        }

        void initialize() throws Exception {
            rpc(
                    "initialize",
                    Map.of(
                            "protocolVersion",
                            "2025-11-25",
                            "capabilities",
                            Map.of(),
                            "clientInfo",
                            Map.of("name", "stdio-test", "version", "1")));
            input.write("{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}\n");
            input.flush();
        }

        JsonNode call(String name, Map<String, Object> args) throws Exception {
            var result = rpc("tools/call", Map.of("name", name, "arguments", args)).path("result");
            assertFalse(result.path("isError").asBoolean(), result.toString());
            return JSON.readTree(result.path("content").get(0).path("text").asString());
        }

        void eof() throws Exception {
            input.close();
            assertTrue(process.waitFor(30, TimeUnit.SECONDS), errors.toString());
            assertEquals(0, process.exitValue(), errors.toString());
            for (var line : stdout) assertTrue(JSON.readTree(line).has("jsonrpc"));
            assertFalse(errors.toString().contains("Tomcat started"));
        }

        public void close() {
            try {
                input.close();
            } catch (IOException ignored) {
            }
            if (process.isAlive()) {
                process.destroy();
                try {
                    if (!process.waitFor(15, TimeUnit.SECONDS)) process.destroyForcibly();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    @Test
    void threeStdioChildrenCoexistWithHttpAndKeepIndependentConnectionState() throws Exception {
        try (var context = RuntimeConnectionsTest.start();
                var http = RuntimeConnectionsTest.client(context);
                var one = new Child();
                var two = new Child();
                var three = new Child()) {
            one.initialize();
            two.initialize();
            three.initialize();
            assertEquals(6, one.rpc("tools/list", Map.of()).path("result").path("tools").size());
            one.call(
                    "db_switch_datasource",
                    Map.of("jdbcUrl", "jdbc:h2:mem:child-one", "username", "sa", "password", ""));
            assertTrue(
                    one.call("connector_status", Map.of())
                            .path("jdbc")
                            .path("enabled")
                            .asBoolean());
            assertFalse(
                    two.call("connector_status", Map.of())
                            .path("jdbc")
                            .path("enabled")
                            .asBoolean());
            assertFalse(
                    three.call("connector_status", Map.of())
                            .path("jdbc")
                            .path("enabled")
                            .asBoolean());
            assertEquals(6, RuntimeConnectionsTest.names(http).size());
            one.eof();
            assertTrue(two.process.isAlive());
            assertTrue(three.process.isAlive());
            context.close();
            two.call("convert_config", Map.of("content", "test=value"));
            two.eof();
            three.eof();
        }
    }

    @Test
    void earlyEofStillClosesTheContext() throws Exception {
        try (var child = new Child()) {
            child.eof();
        }
    }

    @Test
    void stdioCachesNacosVersionUntilReplacementAndKeepsItAfterFailedActivation() throws Exception {
        try (var peer = new xyz.fz.omni.config.support.UpstreamFixture();
                var child = new Child()) {
            child.initialize();
            assertFalse(child.call("connector_status", Map.of()).path("nacos").has("version"));
            assertEquals(
                    "2.4.3",
                    child.call(
                                    "nacos_connect",
                                    Map.of(
                                            "serverAddr",
                                            peer.base(),
                                            "username",
                                            "user",
                                            "password",
                                            "password"))
                            .path("version")
                            .asString());
            child.call("nacos_publish_config", Map.of("dataId", "a", "content", "key: value"));
            child.call("nacos_read_config", Map.of("dataId", "a"));
            child.call("nacos_list_namespaces", Map.of());
            assertEquals(
                    "2.4.3",
                    child.call("connector_status", Map.of())
                            .path("nacos")
                            .path("version")
                            .asString());
            assertEquals(1, peer.stateV3Reads.get());
            assertEquals(1, peer.stateV1Reads.get());
            peer.stateBody = "{}";
            var failed =
                    child.rpc(
                                    "tools/call",
                                    Map.of(
                                            "name",
                                            "nacos_connect",
                                            "arguments",
                                            Map.of("serverAddr", peer.base())))
                            .path("result");
            assertTrue(failed.path("isError").asBoolean());
            assertEquals(
                    "2.4.3",
                    child.call("connector_status", Map.of())
                            .path("nacos")
                            .path("version")
                            .asString());
            child.call("nacos_read_config", Map.of("dataId", "a"));
            peer.stateBody = null;
            peer.nacosVersion = "3.0.0";
            assertEquals(
                    "3.0.0",
                    child.call(
                                    "nacos_connect",
                                    Map.of(
                                            "serverAddr",
                                            peer.base(),
                                            "username",
                                            "user",
                                            "password",
                                            "password"))
                            .path("version")
                            .asString());
            child.call("nacos_read_config", Map.of("dataId", "a"));
            assertEquals(
                    "3.0.0",
                    child.call("connector_status", Map.of())
                            .path("nacos")
                            .path("version")
                            .asString());
            assertEquals(3, peer.stateV3Reads.get());
            assertEquals(2, peer.stateV1Reads.get());
            child.eof();
            try (var fresh = new Child()) {
                fresh.initialize();
                assertFalse(fresh.call("connector_status", Map.of()).path("nacos").has("version"));
                fresh.eof();
            }
        }
    }
}
