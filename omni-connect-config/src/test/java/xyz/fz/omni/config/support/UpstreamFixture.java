package xyz.fz.omni.config.support;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/** Real HTTP test peers, based on the documented Apollo and Nacos 2.x wire contracts. */
public final class UpstreamFixture implements AutoCloseable {
    public static final String YAML =
            "# agent-converted\r\napp:\r\n  name: '订单'\r\n  enabled: 'true'\r\n";
    private final HttpServer server;
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private final JsonMapper json = new JsonMapper();
    public final AtomicInteger logins = new AtomicInteger();
    public final AtomicInteger writes = new AtomicInteger();
    public final AtomicInteger configReads = new AtomicInteger();
    public final Map<String, String> configurations = new ConcurrentHashMap<>();
    public volatile String lastApolloPath;
    public volatile String lastAuthorization;
    public volatile String lastTimestamp;
    public volatile Map<String, String> lastPublish = Map.of();
    public volatile int apolloStatus = 200;
    public volatile int loginStatus = 200;
    public volatile int publishStatus = 200;
    public volatile int readStatus = 200;
    public volatile int namespaceV2Status = 200;
    public volatile int namespaceV1Status = 200;
    public volatile String namespaceBody;
    public final AtomicInteger namespaceV2Reads = new AtomicInteger();
    public final AtomicInteger namespaceV1Reads = new AtomicInteger();
    public volatile boolean rejectFirstRead;
    public volatile String publishReply = "true";
    public volatile long delayMillis;
    public volatile long tokenTtl = 18000;
    public volatile String discoveryBody;
    public final Map<String, Map<String, String>> apolloDrafts = new ConcurrentHashMap<>();
    public final Map<String, Map<String, String>> apolloReleases = new ConcurrentHashMap<>();
    public final AtomicInteger itemReads = new AtomicInteger();
    public final AtomicInteger itemWrites = new AtomicInteger();
    public final AtomicInteger releaseWrites = new AtomicInteger();
    public volatile String portalAuthorization;
    public volatile String lastPortalPath;
    public volatile JsonNodeHolder lastItemWrite;
    public volatile Map<String, Object> lastReleaseWrite;
    public volatile String namespaceFormat = "properties";
    public volatile int portalStatus = 200;
    public volatile int itemWriteStatus = 200;
    public volatile int releaseWriteStatus = 200;
    public volatile long itemWriteDelay;
    public volatile long releaseWriteDelay;
    public volatile Runnable afterItemWrite = () -> {};
    public volatile Runnable afterReleaseWrite = () -> {};
    public volatile Runnable beforeReleaseWrite = () -> {};

    public record JsonNodeHolder(String key, String value, String comment, String operator) {}

    public static final Map<String, String> APOLLO_VALUES =
            Map.of(
                    "authen.merchantName",
                    "sqmallauth",
                    "service.url",
                    "https://api.example.com:8080/auth?enabled=true",
                    "other.key",
                    "unchanged");

    public String portalNamespace(String env, String app, String cluster, String namespace) {
        return "/apollo/openapi/v1/envs/"
                + env
                + "/apps/"
                + app
                + "/clusters/"
                + cluster
                + "/namespaces/"
                + namespace;
    }

    public Map<String, String> draft(String env, String app, String cluster, String namespace) {
        String path = portalNamespace(env, app, cluster, namespace);
        apolloReleases.computeIfAbsent(path, ignored -> new ConcurrentHashMap<>(APOLLO_VALUES));
        return apolloDrafts.computeIfAbsent(
                path, ignored -> new ConcurrentHashMap<>(APOLLO_VALUES));
    }

    /** Starts synthetic Apollo and Nacos HTTP endpoints on an available loopback port. */
    public UpstreamFixture() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.setExecutor(executor);
        server.createContext("/", this::handle);
        server.start();
    }

    /** Returns the fixture's loopback origin for configuring real HTTP clients. */
    public String base() {
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    /** Returns the synthetic Apollo Meta and ConfigService base URL. */
    public String apollo() {
        return base() + "/apollo";
    }

    /** Returns the synthetic Nacos API base URL including its context path. */
    public String nacos() {
        return base() + "/nacos";
    }

    /**
     * Routes documented upstream calls and applies state, counters, delays, and failure settings.
     */
    private void handle(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        Map<String, String> query = decode(exchange.getRequestURI().getRawQuery());
        String rawBody =
                new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        String contentType = exchange.getRequestHeaders().getFirst("Content-Type");
        Map<String, String> form =
                contentType != null && contentType.startsWith("application/json")
                        ? Map.of()
                        : decode(rawBody);
        try {
            if (path.equals("/apollo/services/config")) {
                respond(
                        exchange,
                        200,
                        discoveryBody != null
                                ? discoveryBody
                                : json.writeValueAsString(List.of(Map.of("homepageUrl", apollo()))),
                        true);
            } else if (path.startsWith("/apollo/configs/")) {
                lastApolloPath = path;
                lastAuthorization = exchange.getRequestHeaders().getFirst("Authorization");
                lastTimestamp = exchange.getRequestHeaders().getFirst("Timestamp");
                if (delayMillis > 0) Thread.sleep(delayMillis);
                if (apolloStatus != 200) {
                    respond(exchange, apolloStatus, "PRIVATE_UPSTREAM_ERROR", false);
                    return;
                }
                String name = path.substring(path.lastIndexOf('/') + 1);
                Map<String, String> values =
                        name.endsWith(".yaml") || name.endsWith(".yml")
                                ? Map.of("content", YAML)
                                : Map.of(
                                        "app.name",
                                        "订单",
                                        "app.enabled",
                                        "true",
                                        "app.code",
                                        "00123");
                String[] parts = path.substring("/apollo/configs/".length()).split("/", 3);
                if (parts.length == 3) {
                    values =
                            apolloReleases.getOrDefault(
                                    portalNamespace("DEV", parts[0], parts[1], parts[2]), values);
                }
                respond(
                        exchange,
                        200,
                        json.writeValueAsString(
                                Map.of("configurations", values, "releaseKey", "release-1")),
                        true);
            } else if (path.startsWith("/apollo/openapi/v1/envs/")) {
                portal(exchange, path, rawBody);
            } else if (path.equals("/nacos/v1/auth/login")) {
                int count = logins.incrementAndGet();
                respond(
                        exchange,
                        loginStatus,
                        json.writeValueAsString(
                                Map.of("accessToken", "test-token-" + count, "tokenTtl", tokenTtl)),
                        true);
            } else if ((path.equals("/nacos/v2/console/namespace/list")
                            || path.equals("/nacos/v1/console/namespaces"))
                    && exchange.getRequestMethod().equals("GET")) {
                boolean legacy = path.equals("/nacos/v1/console/namespaces");
                (legacy ? namespaceV1Reads : namespaceV2Reads).incrementAndGet();
                int status = legacy ? namespaceV1Status : namespaceV2Status;
                if (status != 200) {
                    respond(exchange, status, "namespace endpoint unavailable", false);
                    return;
                }
                long publicCount =
                        configurations.keySet().stream().filter(key -> key.startsWith("|")).count();
                var namespaces =
                        List.of(
                                Map.of(
                                        "namespace",
                                        "",
                                        "namespaceShowName",
                                        "public",
                                        "configCount",
                                        publicCount),
                                Map.of(
                                        "namespace",
                                        "empty-ns",
                                        "namespaceShowName",
                                        "Empty namespace",
                                        "configCount",
                                        0));
                respond(
                        exchange,
                        200,
                        namespaceBody != null ? namespaceBody
                                : json.writeValueAsString(Map.of("code", legacy ? 200 : 0, "data", namespaces)),
                        true);
            } else if (path.equals("/nacos/v1/cs/configs")) {
                if (exchange.getRequestMethod().equals("POST")) {
                    writes.incrementAndGet();
                    lastPublish = form;
                    if (delayMillis > 0) Thread.sleep(delayMillis);
                    if (publishStatus == 200 && publishReply.equals("true"))
                        configurations.put(key(form), form.get("content"));
                    respond(exchange, publishStatus, publishReply, false);
                } else {
                    int count = configReads.incrementAndGet();
                    if (rejectFirstRead && count == 1) {
                        respond(exchange, 403, "expired", false);
                        return;
                    }
                    if (delayMillis > 0) Thread.sleep(delayMillis);
                    if (readStatus != 200) {
                        respond(exchange, readStatus, "PRIVATE_UPSTREAM_ERROR", false);
                        return;
                    }
                    if ("accurate".equals(query.get("search")) && "".equals(query.get("dataId"))) {
                        int pageNo = Integer.parseInt(query.get("pageNo"));
                        int pageSize = Integer.parseInt(query.get("pageSize"));
                        String prefix = key(query);
                        var matches =
                                configurations.keySet().stream()
                                        .filter(key -> key.startsWith(prefix))
                                        .sorted()
                                        .toList();
                        var items =
                                matches.stream()
                                        .skip((long) (pageNo - 1) * pageSize)
                                        .limit(pageSize)
                                        .map(
                                                key ->
                                                        Map.of(
                                                                "dataId",
                                                                key.substring(prefix.length())))
                                        .toList();
                        respond(
                                exchange,
                                200,
                                json.writeValueAsString(
                                        Map.of("totalCount", matches.size(), "pageItems", items)),
                                true);
                        return;
                    }
                    String content = configurations.get(key(query));
                    respond(
                            exchange,
                            content == null ? 404 : 200,
                            content == null ? "not found" : content,
                            false);
                }
            } else respond(exchange, 501, "Endpoint not modeled", false);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            exchange.close();
        }
    }

    /** Models Portal editing items and independent published release snapshots. */
    private void portal(HttpExchange exchange, String path, String rawBody)
            throws IOException, InterruptedException {
        portalAuthorization = exchange.getRequestHeaders().getFirst("Authorization");
        lastPortalPath = path;
        if (portalStatus != 200) {
            respond(exchange, portalStatus, "PRIVATE_UPSTREAM_ERROR", false);
            return;
        }
        if (!"test-openapi-token".equals(portalAuthorization)) {
            respond(exchange, 401, "denied", false);
            return;
        }
        int suffixAt = path.indexOf("/items/");
        if (suffixAt < 0) suffixAt = path.indexOf("/releases");
        String identity = suffixAt < 0 ? path : path.substring(0, suffixAt);
        apolloReleases.computeIfAbsent(identity, ignored -> new ConcurrentHashMap<>(APOLLO_VALUES));
        Map<String, String> draft =
                apolloDrafts.computeIfAbsent(
                        identity, ignored -> new ConcurrentHashMap<>(APOLLO_VALUES));
        String suffix = suffixAt < 0 ? "" : path.substring(suffixAt);
        String method = exchange.getRequestMethod();
        if (suffix.isEmpty() && method.equals("GET")) {
            List<Map<String, String>> items = new ArrayList<>();
            draft.forEach((key, value) -> items.add(Map.of("key", key, "value", value)));
            items.add(Map.of("key", "", "value", "# comment row"));
            respond(
                    exchange,
                    200,
                    json.writeValueAsString(Map.of("format", namespaceFormat, "items", items)),
                    true);
        } else if (suffix.startsWith("/items/")) {
            String key = suffix.substring("/items/".length());
            if (!draft.containsKey(key)) {
                respond(exchange, 404, "not found", false);
                return;
            }
            if (method.equals("GET")) {
                itemReads.incrementAndGet();
                respond(
                        exchange,
                        200,
                        json.writeValueAsString(
                                Map.of(
                                        "key",
                                        key,
                                        "value",
                                        draft.get(key),
                                        "comment",
                                        "original comment",
                                        "dataChangeLastModifiedBy",
                                        "editor",
                                        "dataChangeLastModifiedTime",
                                        "2026-10-26T10:00:00.000+0800")),
                        true);
            } else if (method.equals("PUT")) {
                itemWrites.incrementAndGet();
                var body = json.readTree(rawBody);
                lastItemWrite =
                        new JsonNodeHolder(
                                body.path("key").asString(),
                                body.path("value").asString(),
                                body.path("comment").asString(),
                                body.path("dataChangeLastModifiedBy").asString());
                if (itemWriteStatus == 200) {
                    draft.put(key, lastItemWrite.value());
                    afterItemWrite.run();
                }
                if (itemWriteDelay > 0) Thread.sleep(itemWriteDelay);
                respond(exchange, itemWriteStatus, "", false);
            } else respond(exchange, 501, "Endpoint not modeled", false);
        } else if (suffix.equals("/releases/latest") && method.equals("GET")) {
            Map<String, String> published = apolloReleases.get(identity);
            if (published.isEmpty()) {
                respond(exchange, 404, "no release", false);
                return;
            }
            respond(
                    exchange,
                    200,
                    json.writeValueAsString(Map.of("configurations", published)),
                    true);
        } else if (suffix.equals("/releases") && method.equals("POST")) {
            releaseWrites.incrementAndGet();
            lastReleaseWrite = json.readValue(rawBody, Map.class);
            if (releaseWriteStatus == 200) {
                beforeReleaseWrite.run();
                apolloReleases.put(identity, new ConcurrentHashMap<>(draft));
                afterReleaseWrite.run();
            }
            if (releaseWriteDelay > 0) Thread.sleep(releaseWriteDelay);
            respond(exchange, releaseWriteStatus, "{}", true);
        } else respond(exchange, 501, "Endpoint not modeled", false);
    }

    /** Builds a storage key from the full Nacos namespace, group, and dataId identity. */
    public static String key(Map<String, String> args) {
        return args.getOrDefault("tenant", "") + "|" + args.get("group") + "|" + args.get("dataId");
    }

    /**
     * Writes a UTF-8 response while preserving the difference between empty content and missing
     * content.
     */
    private void respond(HttpExchange exchange, int status, String body, boolean jsonBody)
            throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders()
                .set(
                        "Content-Type",
                        (jsonBody ? "application/json" : "text/plain") + ";charset=UTF-8");
        // A length of zero means chunked, preserving an empty configuration response.
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
    }

    /** Decodes query strings and form bodies so tests observe the actual transmitted values. */
    private static Map<String, String> decode(String raw) {
        Map<String, String> result = new LinkedHashMap<>();
        if (raw != null && !raw.isEmpty())
            for (String part : raw.split("&")) {
                String[] pair = part.split("=", 2);
                result.put(
                        URLDecoder.decode(pair[0], StandardCharsets.UTF_8),
                        URLDecoder.decode(pair.length > 1 ? pair[1] : "", StandardCharsets.UTF_8));
            }
        return result;
    }

    /** Stops the fixture immediately and cancels outstanding synthetic requests. */
    @Override
    public void close() {
        server.stop(0);
        executor.shutdownNow();
    }
}
