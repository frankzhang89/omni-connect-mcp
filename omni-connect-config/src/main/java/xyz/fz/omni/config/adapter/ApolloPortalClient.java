package xyz.fz.omni.config.adapter;

import static xyz.fz.omni.config.core.ConfigModels.*;
import static xyz.fz.omni.config.core.ConfigModels.ItemAction.*;
import static xyz.fz.omni.config.core.MigrationException.Code.*;

import org.springframework.http.HttpHeaders;

import tools.jackson.databind.JsonNode;

import xyz.fz.omni.config.config.MigrationProperties;
import xyz.fz.omni.config.core.MigrationException;
import xyz.fz.omni.config.core.ConfigModels.*;

import java.net.URI;
import java.util.*;

/** Uses the active connection's Portal credentials for discovery and editing. */
final class ApolloPortalClient {
    private final HttpTransport http;
    private final MigrationProperties.Apollo settings;

    ApolloPortalClient(HttpTransport http, MigrationProperties.Apollo settings) {
        this.http = http;
        this.settings = settings;
    }

    AppIdListResult listAppIds() {
        String portal = portalBaseUrl();
        var response = settings.hasOpenApiToken()
                ? http.get(URI.create(portal + "/openapi/v1/apps"), tokenHeaders())
                : login(portal).get(URI.create(portal + "/apps"));
        http.requireSuccess(response);
        JsonNode apps = http.json(response.body());
        if (!apps.isArray()) {
            throw new MigrationException(UPSTREAM_ERROR, "Apollo app list is not an array");
        }
        List<String> appIds = new ArrayList<>();
        for (JsonNode app : apps) {
            String appId = ApolloItemEndpoint.string(app, "appId");
            if (appId.isBlank()) {
                throw new MigrationException(UPSTREAM_ERROR, "Apollo response contains a blank appId");
            }
            appIds.add(appId);
        }
        return new AppIdListResult(List.copyOf(appIds));
    }

    ItemReadResult readItem(ItemReadRequest request) {
        var endpoint = itemEndpoint(request);
        JsonNode item = endpoint.requireItem(endpoint.draft());
        String value = ApolloItemEndpoint.string(item, "value");
        return new ItemReadResult(
                request.location(), request.env(), "properties", request.key(), value,
                ApolloItemEndpoint.optional(item, "comment"),
                ApolloItemEndpoint.optional(item, "dataChangeLastModifiedBy"),
                ApolloItemEndpoint.optional(item, "dataChangeLastModifiedTime"),
                sha256(value), "EDITING");
    }

    ItemChangeResult changeItem(ItemChangeRequest request) {
        var item = request.item();
        ItemAction action = request.action();
        String stage = "READ_ITEM";
        Boolean changed = false;
        Boolean published = false;
        try {
            var endpoint = itemEndpoint(item);
            endpoint.checkActor(request);
            var draft = endpoint.draft();
            JsonNode before = switch (action) {
                case UPDATE -> endpoint.requireItem(draft);
                case CREATE -> endpoint.findItemWithComment(draft);
                case DELETE -> draft.items().get(item.key());
            };
            Map<String, String> active = Map.of();
            if (request.publish()) {
                stage = "PRECHECK";
                active = endpoint.latest();
            }
            boolean write = requiresWrite(request, before, active);
            if (request.publish()) {
                checkPending(draft.values(), active, item.key());
            }

            if (write) {
                stage = action.name();
                changed = null;
                endpoint.change(request, draft, before);
                changed = true;
            }
            stage = "VERIFY_ITEM";
            var verified = endpoint.draft();
            JsonNode after = switch (action) {
                case UPDATE -> endpoint.requireItem(verified);
                case CREATE -> endpoint.findItemWithComment(verified);
                case DELETE -> verified.items().get(item.key());
            };
            if (!matches(request, after, before)) {
                throw new MigrationException(
                        VERIFICATION_FAILED, "Apollo editing item did not match the expected state");
            }
            if (!request.publish()) return new ItemChangeResult(changed, false);

            stage = "PREPUBLISH";
            draft = endpoint.draft();
            active = endpoint.latest();
            var expectedConfig = draft.values();
            checkPending(expectedConfig, active, item.key());
            JsonNode target = action == CREATE
                    ? endpoint.findItemWithComment(draft)
                    : draft.items().get(item.key());
            if (!matches(request, target, before)) {
                throw new MigrationException(
                        CONCURRENT_MODIFICATION, "Apollo target item changed before publication");
            }
            if (expectedConfig.equals(active)) return new ItemChangeResult(changed, true);
            if (action == CREATE && !changed && active.containsKey(item.key())) {
                throw new MigrationException(
                        CONCURRENT_MODIFICATION, "Apollo target key was published by another operation");
            }

            stage = "PUBLISH";
            published = null;
            endpoint.publish(request);
            published = true;
            stage = "VERIFY_RELEASE";
            if (!expectedConfig.equals(endpoint.latest())) {
                throw new MigrationException(
                        VERIFICATION_FAILED,
                        "Apollo published configuration did not match the expected snapshot");
            }
            return new ItemChangeResult(changed, true);
        } catch (RuntimeException failure) {
            MigrationException e = failure instanceof MigrationException known
                    ? known
                    : new MigrationException(
                            changed == null || published == null ? WRITE_RESULT_UNKNOWN : UPSTREAM_ERROR,
                            "Unexpected Apollo item operation failure");
            if (e.code() != WRITE_RESULT_UNKNOWN) {
                if (changed == null) changed = false;
                if (published == null) published = false;
            }
            Map<String, Object> details = new LinkedHashMap<>();
            details.put("stage", stage);
            details.put("location", item.location());
            details.put("env", item.env());
            details.put("key", item.key());
            details.put(action.resultField(), changed);
            details.put("published", published);
            details.put("verified", false);
            throw new MigrationException(
                    e.code(),
                    "Apollo item operation failed at " + stage + "; " + e.getMessage(),
                    details);
        }
    }

    private static boolean requiresWrite(
            ItemChangeRequest request, JsonNode before, Map<String, String> active) {
        boolean publishedKey = active.containsKey(request.item().key());
        return switch (request.action()) {
            case CREATE -> {
                if (before == null) yield true;
                if (request.publish() && !publishedKey && matches(request, before, null)) yield false;
                throw new MigrationException(ALREADY_EXISTS, "Apollo editing key already exists");
            }
            case DELETE -> {
                if (before != null) yield true;
                if (request.publish() && publishedKey) yield false;
                throw new MigrationException(NOT_FOUND, "Apollo editing key does not exist");
            }
            case UPDATE -> !ApolloItemEndpoint.string(before, "value").equals(request.value());
        };
    }

    private static boolean matches(ItemChangeRequest request, JsonNode item, JsonNode before) {
        if (request.action() == DELETE) return item == null;
        if (item == null || !request.value().equals(ApolloItemEndpoint.string(item, "value"))) {
            return false;
        }
        return switch (request.action()) {
            case CREATE -> request.comment().equals(ApolloItemEndpoint.comment(item));
            // Some OpenAPI namespace snapshots omit comments. The item read still verifies them.
            case UPDATE -> !item.has("comment")
                    || ApolloItemEndpoint.comment(before).equals(ApolloItemEndpoint.comment(item));
            case DELETE -> throw new IllegalStateException("Deletion handled above");
        };
    }

    private static void checkPending(Map<String, String> draft, Map<String, String> active, String key) {
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

    private ApolloItemEndpoint itemEndpoint(ItemReadRequest item) {
        String portal = portalBaseUrl();
        if (settings.hasOpenApiToken()) {
            return new ApolloItemEndpoint(http, portal, item, tokenHeaders(), null);
        }
        return new ApolloItemEndpoint(http, portal, item, null, login(portal));
    }

    private HttpHeaders tokenHeaders() {
        HttpHeaders headers = new HttpHeaders();
        headers.set(HttpHeaders.AUTHORIZATION, settings.openapiToken());
        return headers;
    }

    private ApolloPortalSession login(String portal) {
        String username = required(settings.portalUsername(), "omni.config.apollo.portal-username");
        String password = required(settings.portalPassword(), "omni.config.apollo.portal-password");
        var session = new ApolloPortalSession(http, portal);
        session.login(username, password);
        return session;
    }

    private String portalBaseUrl() {
        return HttpTransport.baseUrl(
                required(settings.portalUrl(), "omni.config.apollo.portal-url"));
    }
}
