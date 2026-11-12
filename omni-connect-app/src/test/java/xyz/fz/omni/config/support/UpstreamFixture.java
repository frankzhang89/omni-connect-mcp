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

/** Real HTTP test peers, based on documented Apollo and Nacos 1.x/2.x/3.x wire contracts. */
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
    public volatile String nacosVersion = "2.4.3";
    public volatile int stateV3Status = -1;
    public volatile int stateV1Status = -1;
    public volatile String stateBody;
    public volatile long stateDelayMillis;
    public volatile int namespaceV3Status = 200;
    public volatile int namespaceV1Status = 200;
    public volatile int namespaceV2Status = 200;
    public volatile String namespaceBody;
    public final AtomicInteger namespaceV1Reads = new AtomicInteger();
    public final AtomicInteger namespaceV2Reads = new AtomicInteger();
    public volatile String configBody;
    public final AtomicInteger stateV3Reads = new AtomicInteger();
    public final AtomicInteger stateV1Reads = new AtomicInteger();
    public final AtomicInteger namespaceV3Reads = new AtomicInteger();
    public final List<NacosRequest> nacosRequests = new CopyOnWriteArrayList<>();

    public record NacosRequest(
            String path,
            String method,
            Map<String, String> query,
            Map<String, String> form,
            String token) {}

    public volatile String lastApolloPath;
    public volatile String lastAuthorization;
    public volatile String lastTimestamp;
    public volatile Map<String, String> lastPublish = Map.of();
    public volatile int apolloStatus = 200;
    public volatile int loginStatus = 200;
    public volatile int publishStatus = 200;
    public volatile int readStatus = 200;
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
            } else if (path.startsWith("/nacos/")) {
                nacos(exchange, path, query, form);
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

    /** Models version-specific wire contracts, rejecting accidental cross-generation calls. */
    private void nacos(
            HttpExchange exchange, String path, Map<String, String> query, Map<String, String> form)
            throws IOException, InterruptedException {
        String method = exchange.getRequestMethod();
        String token = exchange.getRequestHeaders().getFirst("accessToken");
        nacosRequests.add(
                new NacosRequest(path, method, Map.copyOf(query), Map.copyOf(form), token));
        boolean v3 = nacosVersion.startsWith("3.");
        boolean legacy =
                nacosVersion.startsWith("1.")
                        || nacosVersion.startsWith("2.0.")
                        || nacosVersion.startsWith("2.1.");
        if (path.endsWith("/state")) {
            boolean newer = path.equals("/nacos/v3/admin/core/state");
            (newer ? stateV3Reads : stateV1Reads).incrementAndGet();
            if (stateDelayMillis > 0) Thread.sleep(stateDelayMillis);
            int override = newer ? stateV3Status : stateV1Status;
            int status = override >= 0 ? override : (newer == v3 ? 200 : (newer ? 404 : 410));
            Object state = Map.of("version", nacosVersion);
            if (newer) state = Map.of("code", 0, "data", state);
            respond(
                    exchange,
                    status,
                    stateBody != null ? stateBody : json.writeValueAsString(state),
                    true);
            return;
        }
        if (path.endsWith("/login")) {
            if (!path.equals(v3 ? "/nacos/v3/auth/user/login" : "/nacos/v1/auth/login")) {
                respond(exchange, 404, "wrong login API", false);
                return;
            }
            int count = logins.incrementAndGet();
            respond(
                    exchange,
                    loginStatus,
                    json.writeValueAsString(
                            Map.of("accessToken", "test-token-" + count, "tokenTtl", tokenTtl)),
                    true);
            return;
        }
        if (path.contains("/namespace")) {
            boolean v1Path = path.equals("/nacos/v1/console/namespaces");
            boolean v2Path = path.equals("/nacos/v2/console/namespace/list");
            (v1Path ? namespaceV1Reads : v2Path ? namespaceV2Reads : namespaceV3Reads)
                    .incrementAndGet();
            String expected =
                    v3
                            ? "/nacos/v3/admin/core/namespace/list"
                            : legacy
                                    ? "/nacos/v1/console/namespaces"
                                    : "/nacos/v2/console/namespace/list";
            int status =
                    !expected.equals(path)
                            ? 404
                            : v3
                                    ? namespaceV3Status
                                    : legacy ? namespaceV1Status : namespaceV2Status;
            long publicCount =
                    configurations.keySet().stream().filter(key -> key.startsWith("|")).count();
            var namespaces =
                    List.of(
                            Map.of(
                                    "namespace",
                                    v3 ? "public" : "",
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
                    status,
                    namespaceBody != null
                            ? namespaceBody
                            : json.writeValueAsString(
                                    Map.of("code", v1Path ? 200 : 0, "data", namespaces)),
                    true);
            return;
        }
        String configPath = v3 ? "/nacos/v3/admin/cs/config" : "/nacos/v1/cs/configs";
        if (!path.equals(configPath) && !(v3 && path.equals(configPath + "/list"))) {
            respond(exchange, 404, "wrong configuration API", false);
            return;
        }
        Map<String, String> args = method.equals("POST") ? form : query;
        // A V3 token must be a header, and identity fields must use the V3 names.
        if (v3
                && (args.containsKey("accessToken")
                        || args.containsKey("tenant")
                        || args.containsKey("group")
                        || !args.containsKey("namespaceId")
                        || !args.containsKey("groupName")
                        || token == null)) {
            respond(exchange, 400, "wrong V3 parameters or token placement", false);
            return;
        }
        if (method.equals("POST")) {
            writes.incrementAndGet();
            lastPublish = form;
            if (delayMillis > 0) Thread.sleep(delayMillis);
            if (publishStatus == 200 && publishReply.equals("true"))
                configurations.put(key(form), form.get("content"));
            String body =
                    v3
                            ? json.writeValueAsString(
                                    Map.of("code", 0, "data", publishReply.equals("true")))
                            : publishReply;
            respond(exchange, publishStatus, configBody != null ? configBody : body, v3);
            return;
        }
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
                            .map(key -> Map.of("dataId", key.substring(prefix.length())))
                            .toList();
            Object page = Map.of("totalCount", matches.size(), "pageItems", items);
            if (v3) page = Map.of("code", 0, "data", page);
            respond(
                    exchange,
                    200,
                    configBody != null ? configBody : json.writeValueAsString(page),
                    true);
            return;
        }
        String content = configurations.get(key(query));
        String body =
                content == null
                        ? "not found"
                        : v3
                                ? json.writeValueAsString(
                                        Map.of("code", 0, "data", Map.of("content", content)))
                                : content;
        respond(exchange, content == null ? 404 : 200, configBody != null ? configBody : body, v3);
    }

    /** Builds a storage key from the full Nacos namespace, group, and dataId identity. */
    public static String key(Map<String, String> args) {
        String namespace = args.getOrDefault("namespaceId", args.getOrDefault("tenant", ""));
        if (namespace.equals("public")) namespace = "";
        return namespace
                + "|"
                + args.getOrDefault("groupName", args.get("group"))
                + "|"
                + args.get("dataId");
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
