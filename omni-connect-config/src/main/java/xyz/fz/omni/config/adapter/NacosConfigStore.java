package xyz.fz.omni.config.adapter;

import static xyz.fz.omni.config.core.ConfigModels.sha256;
import static xyz.fz.omni.config.core.MigrationException.Code.*;

import org.springframework.http.HttpHeaders;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import tools.jackson.databind.JsonNode;

import xyz.fz.omni.config.config.MigrationProperties;
import xyz.fz.omni.config.core.*;
import xyz.fz.omni.config.core.ConfigModels.*;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class NacosConfigStore
        implements ConfigStore.Readable, ConfigStore.Writable, ConfigStore.Discoverable {
    private static final String CONFIG_PATH = "/v1/cs/configs";
    private static final int CONFIG_PAGE_SIZE = 100;

    private final HttpTransport http;
    private final MigrationProperties.Nacos settings;
    private final String base;
    private volatile Token token;

    private record Token(String value, Instant refreshAt) {}

    /** Binds the Nacos endpoint and context path without connecting during application startup. */
    public NacosConfigStore(HttpTransport http, MigrationProperties properties) {
        this.http = http;
        this.settings = properties.to().nacos();
        String context = settings.contextPath();
        if (!context.isEmpty() && !context.startsWith("/")) context = "/" + context;
        this.base = HttpTransport.baseUrl(settings.serverAddr()) + context.replaceAll("/+$", "");
    }

    /** Identifies this adapter as the Nacos provider. */
    @Override
    public Provider provider() {
        return Provider.NACOS;
    }

    @Override
    public Set<Capability> capabilities() {
        return Set.of(Capability.READ, Capability.WRITE, Capability.DISCOVER);
    }

    /** Reads exact content, refreshing a denied token and retrying the read at most once. */
    @Override
    public ReadResult read(ReadRequest request) {
        String content = get(CONFIG_PATH, parameters(request.location())).body();
        return new ReadResult(
                request.location(), request.fileType().value(), content, sha256(content), null);
    }

    @Override
    public NamespaceListResult listNamespaces() {
        HttpTransport.Response response =
                getResponse("/v2/console/namespace/list", new LinkedHashMap<>());
        boolean legacy = response.status() == 404;
        // Older Nacos servers expose only V1. Do not hide authentication or server failures.
        if (legacy) response = getResponse("/v1/console/namespaces", new LinkedHashMap<>());
        http.requireSuccess(response);
        JsonNode body = http.json(response.body());
        JsonNode code = body.path("code");
        JsonNode data = body.path("data");
        if (!code.isIntegralNumber()
                || !code.canConvertToInt()
                || code.asInt() != (legacy ? 200 : 0)
                || !data.isArray()) {
            throw new MigrationException(
                    UPSTREAM_ERROR, "Nacos returned an invalid namespace list");
        }
        List<NamespaceInfo> namespaces = new ArrayList<>();
        for (JsonNode item : data) {
            JsonNode id = item.path("namespace");
            JsonNode name = item.path("namespaceShowName");
            if (!id.isString() || !name.isString()) {
                throw new MigrationException(
                        UPSTREAM_ERROR, "Nacos returned invalid namespace metadata");
            }
            namespaces.add(
                    new NamespaceInfo(id.asString(), name.asString(), count(item, "configCount")));
        }
        return new NamespaceListResult(List.copyOf(namespaces));
    }

    @Override
    public DataIdCountResult countDataIds(String namespace, String group) {
        Map<String, Object> query = new LinkedHashMap<>();
        // An empty dataId leaves that filter unset while accurate search matches the group exactly.
        query.put("dataId", "");
        query.put("group", group);
        query.put("tenant", namespace);
        query.put("search", "accurate");
        query.put("pageSize", CONFIG_PAGE_SIZE);

        Set<String> dataIds = new LinkedHashSet<>();
        long totalCount = 0;
        for (int pageNo = 1; ; pageNo++) {
            query.put("pageNo", pageNo);
            JsonNode page = http.json(get(CONFIG_PATH, query).body());
            long pageTotal = count(page, "totalCount");
            if (pageNo == 1) totalCount = pageTotal;
            JsonNode items = page.path("pageItems");
            long expectedSize = Math.min(CONFIG_PAGE_SIZE, totalCount - dataIds.size());
            if (pageTotal != totalCount || !items.isArray() || items.size() != expectedSize) {
                throw new MigrationException(
                        UPSTREAM_ERROR, "Nacos returned inconsistent configuration pages");
            }
            for (JsonNode item : items) {
                JsonNode dataId = item.path("dataId");
                if (!dataId.isString()
                        || dataId.asString().isBlank()
                        || !dataIds.add(dataId.asString())) {
                    throw new MigrationException(
                            UPSTREAM_ERROR, "Nacos returned a missing or repeated dataId");
                }
            }
            if (dataIds.size() == totalCount) {
                return new DataIdCountResult(namespace, group, totalCount, List.copyOf(dataIds));
            }
        }
    }

    private static long count(JsonNode body, String field) {
        JsonNode value = body.path(field);
        if (!value.isIntegralNumber() || !value.canConvertToLong() || value.asLong() < 0) {
            throw new MigrationException(UPSTREAM_ERROR, "Nacos returned an invalid " + field);
        }
        return value.asLong();
    }

    private HttpTransport.Response get(String path, Map<String, Object> query) {
        HttpTransport.Response response = getResponse(path, query);
        http.requireSuccess(response);
        return response;
    }

    private HttpTransport.Response getResponse(String path, Map<String, Object> query) {
        addToken(query);
        HttpTransport.Response response =
                http.get(HttpTransport.uri(base, path, query), new HttpHeaders());
        if (response.status() == 401 || response.status() == 403) {
            // A read has no mutation to replay; refresh once and avoid an unbounded retry loop.
            invalidateToken();
            query.remove("accessToken");
            addToken(query);
            response = http.get(HttpTransport.uri(base, path, query), new HttpHeaders());
        }
        return response;
    }

    /**
     * Publishes already-validated target content once and requires a positive Nacos
     * acknowledgement.
     */
    @Override
    public PublishResult write(WriteRequest request) {
        MultiValueMap<String, String> form = new LinkedMultiValueMap<>();
        parameters(request.location()).forEach((key, value) -> form.add(key, value.toString()));
        form.add("content", request.content());
        // Publish the target declaration independently from any configured Apollo source format.
        form.add("type", request.fileType().value());
        String accessToken = accessToken();
        if (accessToken != null) form.add("accessToken", accessToken);
        HttpTransport.Response response =
                http.post(HttpTransport.uri(base, CONFIG_PATH, Map.of()), form, true);
        // Never replay a publish, even when authentication fails: let the agent read and decide.
        if (response.status() == 401 || response.status() == 403) invalidateToken();
        http.requireSuccess(response);
        if (!"true".equals(response.body().trim())) {
            // HTTP 200 alone is insufficient: the v1 publication API acknowledges with true.
            throw new MigrationException(PUBLISH_FAILED, "Nacos did not acknowledge publication");
        }
        return new PublishResult(
                request.location(), request.fileType().value(), true, sha256(request.content()));
    }

    /** Maps the full Nacos identity to v1 API parameters, omitting tenant for public. */
    private Map<String, Object> parameters(Location location) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("dataId", location.dataId());
        result.put("group", location.group());
        if (!location.namespace().isEmpty()) result.put("tenant", location.namespace());
        return result;
    }

    /** Adds a usable access token when server-side credentials enable authentication. */
    private void addToken(Map<String, Object> query) {
        String value = accessToken();
        if (value != null) query.put("accessToken", value);
    }

    /** Clears a rejected token so the next eligible request obtains fresh credentials. */
    private synchronized void invalidateToken() {
        token = null;
    }

    /** Reuses a valid token or serializes login and refresh across concurrent tool requests. */
    private synchronized String accessToken() {
        if (settings.username() == null || settings.username().isBlank()) return null;
        if (token != null && Instant.now().isBefore(token.refreshAt())) return token.value();
        if (settings.password() == null || settings.password().isEmpty()) {
            throw new MigrationException(
                    AUTHENTICATION_FAILED,
                    "Nacos password is required when username is configured");
        }
        MultiValueMap<String, String> form = new LinkedMultiValueMap<>();
        form.add("username", settings.username());
        form.add("password", settings.password());
        HttpTransport.Response response =
                http.post(HttpTransport.uri(base, "/v1/auth/login", Map.of()), form, false);
        if (response.status() == 400 || response.status() == 401 || response.status() == 403) {
            throw new MigrationException(AUTHENTICATION_FAILED, "Nacos login failed");
        }
        http.requireSuccess(response);
        JsonNode body = http.json(response.body());
        JsonNode value = body.get("accessToken");
        JsonNode ttlNode = body.get("tokenTtl");
        long ttl = ttlNode != null && ttlNode.isNumber() ? ttlNode.asLong() : 0;
        if (value == null || !value.isString() || value.asString().isBlank() || ttl <= 0) {
            throw new MigrationException(
                    AUTHENTICATION_FAILED, "Nacos login returned no usable token");
        }
        // Leave a small expiry margin so normal requests do not start with an almost-expired token.
        token =
                new Token(
                        value.asString(),
                        Instant.now().plusSeconds(Math.max(1, ttl - Math.min(30, ttl / 10))));
        return token.value();
    }
}
