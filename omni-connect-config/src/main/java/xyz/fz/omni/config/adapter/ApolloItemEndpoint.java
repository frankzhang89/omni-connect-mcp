package xyz.fz.omni.config.adapter;

import static xyz.fz.omni.config.core.MigrationException.Code.*;

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.web.util.UriUtils;

import tools.jackson.databind.JsonNode;

import xyz.fz.omni.config.core.ConfigModels.*;
import xyz.fz.omni.config.core.MigrationException;

import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

/** Portal wire contracts for a single namespace operation and its selected authentication. */
final class ApolloItemEndpoint {
    private final HttpTransport http;
    private final HttpHeaders tokenHeaders;
    private final ApolloPortalSession session;
    private final String portal;
    private final String namespace;
    private final String key;

    ApolloItemEndpoint(
            HttpTransport http, String portal, ItemReadRequest request, HttpHeaders tokenHeaders,
            ApolloPortalSession session) {
        this.http = http;
        this.portal = portal;
        this.tokenHeaders = tokenHeaders;
        this.session = session;
        this.key = request.key();
        var location = request.location();
        String app = "/apps/" + segment(location.appId());
        String env = "/envs/" + segment(request.env());
        namespace = portal + (session == null ? "/openapi/v1" + env + app : app + env)
                + "/clusters/" + segment(location.cluster())
                + "/namespaces/" + segment(location.namespace());
    }

    void checkActor(ItemChangeRequest request) {
        if (session == null) return;
        var response = session.get(URI.create(portal + "/user"));
        http.requireSuccess(response);
        String user = string(http.json(response.body()), "userId");
        if (user.isBlank()) {
            throw new MigrationException(AUTHENTICATION_FAILED, "Apollo Portal user is unavailable");
        }
        if ((request.operator() != null && !user.equals(request.operator()))
                || (request.releasedBy() != null && !user.equals(request.releasedBy()))) {
            throw new MigrationException(
                    INVALID_ARGUMENT, "operator and releasedBy must match the logged-in Portal user");
        }
    }

    record Draft(Map<String, JsonNode> items, long namespaceId) {
        Map<String, String> values() {
            Map<String, String> values = new LinkedHashMap<>();
            items.forEach((key, item) -> values.put(key, string(item, "value")));
            return values;
        }
    }

    Draft draft() {
        JsonNode metadata = readJson("");
        if (!"properties".equals(string(metadata, "format"))) {
            throw new MigrationException(
                    INVALID_FORMAT, "Apollo item operations support properties namespaces only");
        }
        long namespaceId = 0;
        JsonNode items = metadata.get("items");
        if (session != null) {
            JsonNode hidden = metadata.get("isConfigHidden");
            if (hidden == null || !hidden.isBoolean()) {
                throw new MigrationException(UPSTREAM_ERROR, "Apollo Portal config visibility is unavailable");
            }
            if (hidden.asBoolean()) {
                throw new MigrationException(AUTHENTICATION_FAILED, "Apollo Portal configuration is hidden");
            }
            namespaceId = positiveId(metadata.get("baseInfo"), "id");
            items = readJson("/items");
        }
        if (items == null || !items.isArray()) {
            throw new MigrationException(UPSTREAM_ERROR, "Apollo namespace lacks items");
        }
        Map<String, JsonNode> entries = new LinkedHashMap<>();
        for (JsonNode item : items) {
            String itemKey = string(item, "key");
            if (itemKey.isEmpty()) continue;
            string(item, "value");
            if (entries.putIfAbsent(itemKey, item) != null) {
                throw new MigrationException(UPSTREAM_ERROR, "Apollo namespace contains duplicate keys");
            }
        }
        return new Draft(entries, namespaceId);
    }

    JsonNode requireItem(Draft draft) {
        JsonNode item = session == null ? readJson("/items/" + segment(key)) : draft.items().get(key);
        if (item == null) {
            throw new MigrationException(NOT_FOUND, "Apollo editing key does not exist");
        }
        if (!key.equals(string(item, "key"))) {
            throw new MigrationException(UPSTREAM_ERROR, "Apollo returned a different item key");
        }
        return item;
    }

    JsonNode findItemWithComment(Draft draft) {
        JsonNode item = draft.items().get(key);
        return session == null && item != null && !item.has("comment")
                ? requireItem(draft)
                : item;
    }

    Map<String, String> latest() {
        var response = get(session == null ? "/releases/latest" : "/releases/active?page=0&size=1");
        if (session == null && response.status() == 404) return Map.of();
        http.requireSuccess(response);
        JsonNode release = http.json(response.body());
        JsonNode values;
        if (session == null) {
            values = release.get("configurations");
        } else {
            if (!release.isArray()) {
                throw new MigrationException(UPSTREAM_ERROR, "Apollo active releases are not an array");
            }
            if (release.isEmpty()) return Map.of();
            values = http.json(string(release.get(0), "configurations"));
        }
        if (values == null || !values.isObject()) {
            throw new MigrationException(UPSTREAM_ERROR, "Apollo release lacks configurations");
        }
        Map<String, String> result = new LinkedHashMap<>();
        for (var entry : values.properties()) {
            if (!entry.getValue().isString()) {
                throw new MigrationException(UPSTREAM_ERROR, "Apollo release contains a non-string value");
            }
            result.put(entry.getKey(), entry.getValue().asString());
        }
        return result;
    }

    void change(ItemChangeRequest request, Draft draft, JsonNode before) {
        switch (request.action()) {
            case CREATE -> {
                Map<String, Object> body = new LinkedHashMap<>();
                body.put("key", key);
                body.put("value", request.value());
                body.put("comment", request.comment());
                if (session == null) {
                    body.put("dataChangeCreatedBy", request.operator());
                } else {
                    body.put("namespaceId", draft.namespaceId());
                }
                write(session == null ? "/items" : "/item", HttpMethod.POST, body);
            }
            case UPDATE -> {
                Map<String, Object> body = new LinkedHashMap<>();
                body.put("key", key);
                body.put("value", request.value());
                body.put("comment", comment(before));
                if (session == null) {
                    body.put("dataChangeLastModifiedBy", request.operator());
                    write("/items/" + segment(key) + "?createIfNotExists=false", HttpMethod.PUT, body);
                } else {
                    body.put("id", webItemId(draft, before));
                    body.put("namespaceId", draft.namespaceId());
                    JsonNode lineNum = before.get("lineNum");
                    if (lineNum == null || !lineNum.isIntegralNumber()) {
                        throw new MigrationException(UPSTREAM_ERROR, "Apollo item has no line number");
                    }
                    body.put("lineNum", lineNum);
                    write("/item", HttpMethod.PUT, body);
                }
            }
            case DELETE -> {
                String identifier = session == null
                        ? segment(key) + "?operator=" + URLEncoder.encode(
                                request.operator(), StandardCharsets.UTF_8)
                        : Long.toString(webItemId(draft, before));
                write("/items/" + identifier, HttpMethod.DELETE, null);
            }
        }
    }

    void publish(ItemChangeRequest request) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("releaseTitle", request.releaseTitle());
        body.put("releaseComment", request.releaseComment());
        if (session == null) body.put("releasedBy", request.releasedBy());
        write("/releases", HttpMethod.POST, body);
    }

    private long webItemId(Draft draft, JsonNode item) {
        if (positiveId(item, "namespaceId") != draft.namespaceId()) {
            throw new MigrationException(UPSTREAM_ERROR, "Apollo item belongs to a different namespace");
        }
        return positiveId(item, "id");
    }

    private static long positiveId(JsonNode node, String name) {
        JsonNode value = node == null ? null : node.get(name);
        if (value == null || !value.isIntegralNumber() || !value.canConvertToLong()
                || value.asLong() <= 0) {
            throw new MigrationException(UPSTREAM_ERROR, "Apollo response lacks a valid " + name);
        }
        return value.asLong();
    }

    private JsonNode readJson(String suffix) {
        var response = get(suffix);
        http.requireSuccess(response);
        return http.json(response.body());
    }

    private HttpTransport.Response get(String suffix) {
        URI uri = URI.create(namespace + suffix);
        return session == null ? http.get(uri, tokenHeaders) : session.get(uri);
    }

    private void write(String suffix, HttpMethod method, Map<String, ?> body) {
        URI uri = URI.create(namespace + suffix);
        var response = session == null
                ? http.writeJson(method, uri, tokenHeaders, body)
                : session.write(method, uri, body);
        if (response.status() >= 500 || (response.status() >= 300 && response.status() < 400)) {
            throw new MigrationException(
                    WRITE_RESULT_UNKNOWN, "Apollo write was not acknowledged; read back before retrying");
        }
        if (response.status() == 400) {
            throw new MigrationException(INVALID_ARGUMENT, "Apollo rejected the operation arguments or operator");
        }
        if (response.status() == 409 || response.status() == 423) {
            throw new MigrationException(
                    CONCURRENT_MODIFICATION, "Apollo denied the operation because of a namespace lock or conflict");
        }
        if (response.status() == 201 || response.status() == 204) return;
        http.requireSuccess(response);
    }

    private static String segment(String value) {
        return UriUtils.encodePathSegment(value, StandardCharsets.UTF_8);
    }

    static String string(JsonNode node, String name) {
        String value = optional(node, name);
        if (value == null) {
            throw new MigrationException(UPSTREAM_ERROR, "Apollo response lacks string field " + name);
        }
        return value;
    }

    static String optional(JsonNode node, String name) {
        JsonNode value = node == null ? null : node.get(name);
        return value != null && value.isString() ? value.asString() : null;
    }

    static String comment(JsonNode item) {
        String comment = optional(item, "comment");
        return comment == null ? "" : comment;
    }
}
