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
    private static final int CONFIG_PAGE_SIZE = 100;

    private final HttpTransport http;
    private final MigrationProperties.Nacos settings;
    private final String base;
    private final NacosServerVersion version;
    private volatile Token token;

    private record Token(String value, Instant refreshAt) {}

    /** Detects the server version once, before this connection generation can be activated. */
    public NacosConfigStore(HttpTransport http, MigrationProperties properties) {
        this.http = http;
        this.settings = properties.to().nacos();
        String context = settings.contextPath();
        if (!context.isEmpty() && !context.startsWith("/")) context = "/" + context;
        this.base = HttpTransport.baseUrl(settings.serverAddr()) + context.replaceAll("/+$", "");
        this.version = detectVersion();
    }

    public String version() {
        return version.value();
    }

    private boolean v3() {
        return version.api() == NacosServerVersion.Api.V3;
    }

    private NacosServerVersion detectVersion() {
        var response =
                http.get(
                        HttpTransport.uri(base, "/v3/admin/core/state", Map.of()),
                        new HttpHeaders());
        if (response.status() == 404 || response.status() == 410) {
            response =
                    http.get(
                            HttpTransport.uri(base, "/v1/console/server/state", Map.of()),
                            new HttpHeaders());
        }
        if (response.status() == 404 || response.status() == 410) {
            throw new MigrationException(
                    UPSTREAM_ERROR, "Nacos server state endpoint is unavailable");
        }
        http.requireSuccess(response);
        JsonNode body = http.json(response.body());
        if (body.has("code")) body = data(body, 0);
        JsonNode value = body.path("version");
        if (!value.isString()) {
            throw new MigrationException(UPSTREAM_ERROR, "Nacos state returned no server version");
        }
        return NacosServerVersion.parse(value.asString());
    }

    private JsonNode data(JsonNode body, int successCode) {
        JsonNode code = body.path("code");
        if (!code.isIntegralNumber()
                || !code.canConvertToInt()
                || code.asInt() != successCode
                || !body.has("data")
                || body.path("data").isNull()) {
            throw new MigrationException(UPSTREAM_ERROR, "Nacos returned an invalid API response");
        }
        return body.path("data");
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
        String content = get(version.api().configPath(), parameters(request.location())).body();
        if (v3()) {
            JsonNode value = data(http.json(content), 0).path("content");
            if (!value.isString()) {
                throw new MigrationException(
                        UPSTREAM_ERROR, "Nacos returned invalid configuration content");
            }
            content = value.asString();
        }
        return new ReadResult(
                request.location(), request.fileType().value(), content, sha256(content), null);
    }

    @Override
    public NamespaceListResult listNamespaces() {
        var response = get(version.api().namespaces, new LinkedHashMap<>());
        JsonNode data = data(http.json(response.body()), version.api().successCode);
        if (!data.isArray()) {
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
                    new NamespaceInfo(
                            publicNamespace(id.asString()),
                            name.asString(),
                            count(item, "configCount")));
        }
        return new NamespaceListResult(List.copyOf(namespaces));
    }

    @Override
    public DataIdCountResult countDataIds(String namespace, String group) {
        Map<String, Object> query = new LinkedHashMap<>();
        // An empty dataId leaves that filter unset while accurate search matches the group exactly.
        query.put("dataId", "");
        query.put(v3() ? "groupName" : "group", group);
        query.put(v3() ? "namespaceId" : "tenant", wireNamespace(namespace));
        query.put("search", "accurate");
        query.put("pageSize", CONFIG_PAGE_SIZE);

        Set<String> dataIds = new LinkedHashSet<>();
        long totalCount = 0;
        for (int pageNo = 1; ; pageNo++) {
            query.put("pageNo", pageNo);
            JsonNode page = http.json(get(version.api().listPath(), query).body());
            if (v3()) page = data(page, 0);
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
        HttpHeaders headers = authentication(query);
        HttpTransport.Response response = http.get(HttpTransport.uri(base, path, query), headers);
        if (response.status() == 401 || response.status() == 403) {
            // A read has no mutation to replay; refresh once and avoid an unbounded retry loop.
            invalidateToken();
            query.remove("accessToken");
            headers = authentication(query);
            response = http.get(HttpTransport.uri(base, path, query), headers);
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
        HttpHeaders headers = new HttpHeaders();
        if (accessToken != null) {
            if (v3()) headers.set("accessToken", accessToken);
            else form.add("accessToken", accessToken);
        }
        HttpTransport.Response response =
                http.post(
                        HttpTransport.uri(base, version.api().configPath(), Map.of()),
                        headers,
                        form,
                        true);
        // Never replay a publish, even when authentication fails: let the agent read and decide.
        if (response.status() == 401 || response.status() == 403) invalidateToken();
        http.requireSuccess(response);
        boolean published;
        if (v3()) {
            JsonNode value = data(http.json(response.body()), 0);
            published = value.isBoolean() && value.asBoolean();
        } else published = "true".equals(response.body().trim());
        if (!published) {
            // HTTP 200 alone is insufficient: the v1 publication API acknowledges with true.
            throw new MigrationException(PUBLISH_FAILED, "Nacos did not acknowledge publication");
        }
        return new PublishResult(
                request.location(), request.fileType().value(), true, sha256(request.content()));
    }

    /** Maps the full identity to the API selected during activation. */
    private Map<String, Object> parameters(Location location) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("dataId", location.dataId());
        result.put(v3() ? "groupName" : "group", location.group());
        String namespace = wireNamespace(location.namespace());
        if (v3() || !namespace.isEmpty()) result.put(v3() ? "namespaceId" : "tenant", namespace);
        return result;
    }

    private static String publicNamespace(String namespace) {
        return "public".equals(namespace) ? "" : namespace;
    }

    private String wireNamespace(String namespace) {
        String normalized = publicNamespace(namespace);
        return v3() && normalized.isEmpty() ? "public" : normalized;
    }

    /** Adds a usable access token when server-side credentials enable authentication. */
    private HttpHeaders authentication(Map<String, Object> query) {
        HttpHeaders headers = new HttpHeaders();
        String value = accessToken();
        if (value != null) {
            if (v3()) headers.set("accessToken", value);
            else query.put("accessToken", value);
        }
        return headers;
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
                http.post(
                        HttpTransport.uri(base, version.api().loginPath(), Map.of()), form, false);
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
