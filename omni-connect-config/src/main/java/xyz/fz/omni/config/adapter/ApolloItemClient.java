package xyz.fz.omni.config.adapter;

import static xyz.fz.omni.config.core.ConfigModels.*;
import static xyz.fz.omni.config.core.MigrationException.Code.*;

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.web.util.UriUtils;

import tools.jackson.databind.JsonNode;

import xyz.fz.omni.config.config.MigrationProperties;
import xyz.fz.omni.config.core.*;
import xyz.fz.omni.config.core.ConfigModels.*;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.*;

/** Portal item operations are separate from the ConfigService published-document protocol. */
final class ApolloItemClient {
    private final HttpTransport http;
    private final MigrationProperties.Apollo settings;

    ApolloItemClient(HttpTransport http, MigrationProperties.Apollo settings) {
        this.http = http;
        this.settings = settings;
    }

    /** Shared by the standalone read tool and both update verification reads. */
    ItemReadResult readItem(ItemReadRequest request) {
        namespace(request); // Verify the actual format, not just the caller's declared file type.
        JsonNode item = get(request, "/items/" + segment(request.key()));
        if (!request.key().equals(string(item, "key"))) {
            throw new MigrationException(UPSTREAM_ERROR, "Apollo returned a different item key");
        }
        String value = string(item, "value");
        return new ItemReadResult(
                request.location(),
                request.env(),
                "properties",
                request.key(),
                value,
                optional(item, "comment"),
                optional(item, "dataChangeLastModifiedBy"),
                optional(item, "dataChangeLastModifiedTime"),
                sha256(value),
                "EDITING");
    }

    ItemUpdateResult updateItem(ItemUpdateRequest request) {
        ItemReadRequest item = request.item();
        String stage = "READ_ITEM";
        Boolean updated = false;
        Boolean published = false;
        try {
            ItemReadResult before = readItem(item);
            if (request.publish()) {
                stage = "PRECHECK";
                checkPending(namespace(item), latest(item), item.key());
            }
            if (!before.value().equals(request.value())) {
                stage = "UPDATE";
                Map<String, Object> body = new LinkedHashMap<>();
                body.put("key", item.key());
                body.put("value", request.value());
                body.put("comment", before.comment() == null ? "" : before.comment());
                body.put("dataChangeLastModifiedBy", request.operator());
                // Do not infer success from an interrupted HTTP exchange.
                updated = null;
                write(
                        item,
                        "/items/" + segment(item.key()) + "?createIfNotExists=false",
                        HttpMethod.PUT,
                        body);
                updated = true;
            }
            stage = "VERIFY_ITEM";
            if (!readItem(item).value().equals(request.value())) {
                throw new MigrationException(
                        VERIFICATION_FAILED,
                        "Apollo editing value did not match the requested value");
            }
            if (!request.publish()) return result(item, request.value(), updated, false);

            stage = "PREPUBLISH";
            Map<String, String> draft = namespace(item);
            Map<String, String> active = latest(item);
            checkPending(draft, active, item.key());
            if (!request.value().equals(draft.get(item.key()))) {
                throw new MigrationException(
                        CONCURRENT_MODIFICATION, "Apollo target item changed before publication");
            }
            if (draft.equals(active)) return result(item, request.value(), updated, true);

            stage = "PUBLISH";
            published = null;
            write(
                    item,
                    "/releases",
                    HttpMethod.POST,
                    Map.of(
                            "releaseTitle",
                            request.releaseTitle(),
                            "releaseComment",
                            request.releaseComment(),
                            "releasedBy",
                            request.releasedBy()));
            published = true;
            stage = "VERIFY_RELEASE";
            if (!draft.equals(latest(item))) {
                throw new MigrationException(
                        VERIFICATION_FAILED,
                        "Apollo published configuration did not match the expected snapshot");
            }
            return result(item, request.value(), updated, true);
        } catch (RuntimeException failure) {
            MigrationException e =
                    failure instanceof MigrationException known
                            ? known
                            : new MigrationException(
                                    updated == null || published == null
                                            ? WRITE_RESULT_UNKNOWN
                                            : UPSTREAM_ERROR,
                                    "Unexpected Apollo item operation failure");
            // A rejected 4xx mutation has not been acknowledged; transport/5xx failures remain
            // unknown.
            if (e.code() != WRITE_RESULT_UNKNOWN) {
                if (updated == null) updated = false;
                if (published == null) published = false;
            }
            Map<String, Object> details = new LinkedHashMap<>();
            details.put("stage", stage);
            details.put("location", item.location());
            details.put("env", item.env());
            details.put("key", item.key());
            details.put("updated", updated);
            details.put("published", published);
            details.put("verified", false);
            throw new MigrationException(
                    e.code(),
                    "Apollo item operation failed at " + stage + "; " + e.getMessage(),
                    details);
        }
    }

    private ItemUpdateResult result(
            ItemReadRequest item, String value, boolean updated, boolean published) {
        return new ItemUpdateResult(
                item.location(),
                item.env(),
                "properties",
                item.key(),
                sha256(value),
                updated,
                published,
                true);
    }

    /** Includes additions and deletions; ignores blank comment rows in a properties namespace. */
    private Map<String, String> namespace(ItemReadRequest request) {
        JsonNode body = get(request, "");
        if (!"properties".equals(string(body, "format"))) {
            throw new MigrationException(
                    INVALID_FORMAT, "Apollo item operations support properties namespaces only");
        }
        JsonNode items = body.get("items");
        if (items == null || !items.isArray())
            throw new MigrationException(UPSTREAM_ERROR, "Apollo namespace lacks items");
        Map<String, String> result = new LinkedHashMap<>();
        for (JsonNode entry : items) {
            String key = string(entry, "key");
            if (key.isEmpty()) continue;
            if (result.putIfAbsent(key, string(entry, "value")) != null) {
                throw new MigrationException(
                        UPSTREAM_ERROR, "Apollo namespace contains duplicate keys");
            }
        }
        return result;
    }

    private Map<String, String> latest(ItemReadRequest request) {
        HttpTransport.Response response = http.get(uri(request, "/releases/latest"), headers());
        // namespace() has already established that this namespace exists.
        if (response.status() == 404) return Map.of();
        http.requireSuccess(response);
        JsonNode values = http.json(response.body()).get("configurations");
        if (values == null || !values.isObject())
            throw new MigrationException(UPSTREAM_ERROR, "Apollo release lacks configurations");
        Map<String, String> result = new LinkedHashMap<>();
        for (var entry : values.properties()) {
            if (!entry.getValue().isString())
                throw new MigrationException(
                        UPSTREAM_ERROR, "Apollo release contains a non-string value");
            result.put(entry.getKey(), entry.getValue().asString());
        }
        return result;
    }

    private void checkPending(Map<String, String> draft, Map<String, String> active, String key) {
        Map<String, String> otherDraft = new LinkedHashMap<>(draft);
        Map<String, String> otherActive = new LinkedHashMap<>(active);
        otherDraft.remove(key);
        otherActive.remove(key);
        if (!otherDraft.equals(otherActive)) {
            throw new MigrationException(
                    CONCURRENT_MODIFICATION,
                    "Other Apollo keys have unpublished changes; publication stopped");
        }
    }

    private JsonNode get(ItemReadRequest request, String suffix) {
        HttpTransport.Response response = http.get(uri(request, suffix), headers());
        http.requireSuccess(response);
        return http.json(response.body());
    }

    private void write(
            ItemReadRequest request, String suffix, HttpMethod method, Map<String, ?> body) {
        HttpTransport.Response response =
                http.writeJson(method, uri(request, suffix), headers(), body);
        if (response.status() >= 500 || (response.status() >= 300 && response.status() < 400)) {
            throw new MigrationException(
                    WRITE_RESULT_UNKNOWN,
                    "Apollo write was not acknowledged; read back before retrying");
        }
        if (response.status() == 400)
            throw new MigrationException(
                    INVALID_ARGUMENT, "Apollo rejected the operation arguments or operator");
        if (response.status() == 409 || response.status() == 423) {
            throw new MigrationException(
                    CONCURRENT_MODIFICATION,
                    "Apollo denied the operation because of a namespace lock or conflict");
        }
        if (response.status() == 201 || response.status() == 204) return;
        http.requireSuccess(response);
    }

    private HttpHeaders headers() {
        HttpHeaders headers = new HttpHeaders();
        headers.set(
                "Authorization",
                required(settings.openapiToken(), "omni.config.apollo.openapi-token"));
        return headers;
    }

    private URI uri(ItemReadRequest request, String suffix) {
        Location location = request.location();
        return URI.create(
                HttpTransport.baseUrl(
                                required(settings.portalUrl(), "omni.config.apollo.portal-url"))
                        + "/openapi/v1/envs/"
                        + segment(request.env())
                        + "/apps/"
                        + segment(location.appId())
                        + "/clusters/"
                        + segment(location.cluster())
                        + "/namespaces/"
                        + segment(location.namespace())
                        + suffix);
    }

    private static String segment(String value) {
        return UriUtils.encodePathSegment(value, StandardCharsets.UTF_8);
    }

    private static String string(JsonNode node, String name) {
        JsonNode value = node.get(name);
        if (value == null || !value.isString())
            throw new MigrationException(
                    UPSTREAM_ERROR, "Apollo response lacks string field " + name);
        return value.asString();
    }

    private static String optional(JsonNode node, String name) {
        JsonNode value = node.get(name);
        return value != null && value.isString() ? value.asString() : null;
    }
}
