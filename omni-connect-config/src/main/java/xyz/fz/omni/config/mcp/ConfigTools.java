package xyz.fz.omni.config.mcp;

import io.modelcontextprotocol.spec.McpSchema.CallToolResult;

import org.springframework.ai.mcp.annotation.McpTool;
import org.springframework.ai.mcp.annotation.McpToolParam;

import tools.jackson.databind.json.JsonMapper;

import xyz.fz.omni.config.core.ConfigService;
import xyz.fz.omni.config.core.MigrationException;

import java.util.Map;
import java.util.function.Supplier;

public class ConfigTools {
    private final ConfigService service;
    private final JsonMapper json = new JsonMapper();

    /** Connects the MCP-facing tools to the provider-neutral configuration service. */
    public ConfigTools(ConfigService service) {
        this.service = service;
    }

    /** Exposes published Apollo reads with optional overrides for source identifiers and format. */
    @McpTool(
            name = "apollo_read_config",
            description =
                    "Read a published Apollo namespace. Optional parameters override server"
                        + " defaults. YAML namespace names include .yaml/.yml; a missing suffix is"
                        + " appended. Returns content, location, releaseKey and SHA-256. Does not"
                        + " read unpublished edits.",
            annotations = @McpTool.McpAnnotations(readOnlyHint = true, destructiveHint = false))
    public CallToolResult apolloRead(
            @McpToolParam(
                            description = "Apollo app ID; defaults to omni.config.apollo.app-id",
                            required = false)
                    String appId,
            @McpToolParam(
                            description =
                                    "Apollo cluster; defaults to configured cluster or default",
                            required = false)
                    String cluster,
            @McpToolParam(
                            description =
                                    "Apollo namespace; defaults to configured namespace or"
                                        + " application",
                            required = false)
                    String namespace,
            @McpToolParam(
                            description =
                                    "properties, yaml or yml; defaults to Apollo source format",
                            required = false)
                    String fileType) {
        return result(() -> service.apolloRead(appId, cluster, namespace, fileType));
    }

    @McpTool(
            name = "apollo_read_item",
            description =
                    "Read one Apollo properties key through Portal OpenAPI. Returns the CURRENT"
                        + " EDITING value, which may be unpublished, with comment, modification"
                        + " metadata and value SHA-256. Optional identifiers override server"
                        + " defaults. No writes. Use apollo_read_config for published namespace"
                        + " content.",
            annotations = @McpTool.McpAnnotations(readOnlyHint = true, destructiveHint = false))
    public CallToolResult apolloReadItem(
            @McpToolParam(description = "Exact existing properties key", required = true)
                    String key,
            @McpToolParam(
                            description = "Apollo app ID; defaults to configured app ID",
                            required = false)
                    String appId,
            @McpToolParam(
                            description = "Apollo cluster; defaults to configured cluster",
                            required = false)
                    String cluster,
            @McpToolParam(
                            description = "Apollo namespace; defaults to configured namespace",
                            required = false)
                    String namespace,
            @McpToolParam(
                            description =
                                    "Only properties is supported; omitted uses Apollo source"
                                        + " format",
                            required = false)
                    String fileType,
            @McpToolParam(
                            description = "Apollo environment; defaults to configured env",
                            required = false)
                    String env) {
        return result(() -> service.apolloReadItem(key, appId, cluster, namespace, fileType, env));
    }

    @McpTool(
            name = "apollo_update_config",
            description =
                    "Update an EXISTING Apollo properties key to the complete supplied value. Reads"
                        + " before/after using the same API as apollo_read_item and preserves the"
                        + " comment. Publishes the namespace by default; publish=false saves only."
                        + " Stops on detected unpublished changes to other keys. Checks and"
                        + " publication are not atomic: concurrent edits may race; verifies the"
                        + " published snapshot. No automatic write retries. On WRITE_RESULT_UNKNOWN"
                        + " inspect the editing item and latest release before retrying. For"
                        + " partial changes such as replacing a URL port, read with"
                        + " apollo_read_item first and construct the complete replacement while"
                        + " retaining other parts.",
            annotations =
                    @McpTool.McpAnnotations(
                            readOnlyHint = false,
                            destructiveHint = true,
                            idempotentHint = false))
    public CallToolResult apolloUpdate(
            @McpToolParam(
                            description =
                                    "Exact existing properties key; never creates a missing key",
                            required = true)
                    String key,
            @McpToolParam(
                            description = "Complete new string value; empty string allowed",
                            required = true)
                    String value,
            @McpToolParam(
                            description = "Apollo app ID; defaults to configured app ID",
                            required = false)
                    String appId,
            @McpToolParam(
                            description = "Apollo cluster; defaults to configured cluster",
                            required = false)
                    String cluster,
            @McpToolParam(
                            description = "Apollo namespace; defaults to configured namespace",
                            required = false)
                    String namespace,
            @McpToolParam(
                            description =
                                    "Only properties is supported; omitted uses Apollo source"
                                        + " format",
                            required = false)
                    String fileType,
            @McpToolParam(
                            description = "Apollo environment; defaults to configured env",
                            required = false)
                    String env,
            @McpToolParam(
                            description = "Defaults to true; false only saves the editing value",
                            required = false)
                    Boolean publish,
            @McpToolParam(
                            description = "Editor Portal account; defaults to configured operator",
                            required = false)
                    String operator,
            @McpToolParam(
                            description =
                                    "Publisher account; defaults to configured released-by then"
                                        + " operator; lock policy may require a different user",
                            required = false)
                    String releasedBy,
            @McpToolParam(description = "Release title; generated if omitted", required = false)
                    String releaseTitle,
            @McpToolParam(description = "Release comment; empty if omitted", required = false)
                    String releaseComment) {
        return result(
                () -> {
                    try {
                        return service.apolloUpdate(
                                key,
                                value,
                                appId,
                                cluster,
                                namespace,
                                fileType,
                                env,
                                publish,
                                operator,
                                releasedBy,
                                releaseTitle,
                                releaseComment);
                    } catch (MigrationException e) {
                        if (!e.details().isEmpty()) throw e;
                        throw new MigrationException(
                                e.code(),
                                "Apollo update validation failed; " + e.getMessage(),
                                Map.of(
                                        "stage",
                                        "VALIDATE",
                                        "updated",
                                        false,
                                        "published",
                                        false,
                                        "verified",
                                        false));
                    }
                });
    }

    @McpTool(
            name = "nacos_list_namespaces",
            description =
                    "List all namespaces returned by the configured Nacos server, with namespace"
                        + " ID, display name and namespace-wide configCount.",
            annotations = @McpTool.McpAnnotations(readOnlyHint = true, destructiveHint = false))
    public CallToolResult nacosListNamespaces() {
        return result(service::nacosListNamespaces);
    }

    @McpTool(
            name = "nacos_list_and_count_data_ids",
            description =
                    "Count and list Nacos dataIds in exactly one namespace and group. Returns the"
                        + " resolved namespace, group, totalCount and the complete dataIds list"
                        + " across all pages. No matches return zero and an empty list.",
            annotations = @McpTool.McpAnnotations(readOnlyHint = true, destructiveHint = false))
    public CallToolResult nacosCountDataIds(
            @McpToolParam(
                            description =
                                    "Nacos namespace ID; empty string/public selects public;"
                                        + " omitted uses configured ID",
                            required = false)
                    String namespace,
            @McpToolParam(
                            description =
                                    "Exact Nacos group; defaults to configured group or"
                                        + " DEFAULT_GROUP",
                            required = false)
                    String group) {
        return result(() -> service.nacosCountDataIds(namespace, group));
    }

    /** Exposes a Nacos read while preserving the stored document and its exact content hash. */
    @McpTool(
            name = "nacos_read_config",
            description =
                    "Read one Nacos configuration by dataId, group and namespace ID. Returns"
                        + " unmodified UTF-8 content, location, declared format and SHA-256. Empty"
                        + " content differs from a missing configuration.",
            annotations = @McpTool.McpAnnotations(readOnlyHint = true, destructiveHint = false))
    public CallToolResult nacosRead(
            @McpToolParam(
                            description = "Exact target dataId; never inferred from Apollo",
                            required = true)
                    String dataId,
            @McpToolParam(
                            description =
                                    "Nacos group; defaults to configured group or DEFAULT_GROUP",
                            required = false)
                    String group,
            @McpToolParam(
                            description =
                                    "Nacos namespace ID, not display name; empty string/public"
                                        + " selects public; omitted uses configured ID",
                            required = false)
                    String namespace,
            @McpToolParam(
                            description =
                                    "Declared properties/yaml/yml format; defaults to configured"
                                        + " target format; does not convert content",
                            required = false)
                    String fileType) {
        return result(() -> service.nacosRead(dataId, group, namespace, fileType));
    }

    /** Exposes whole-document publication; content must already match the target format. */
    @McpTool(
            name = "nacos_publish_config",
            description =
                    "Create or OVERWRITE the ENTIRE Nacos configuration. Input must already match"
                        + " fileType (default: configured target format). Validates and publishes"
                        + " content UNMODIFIED; never converts, even when source and target formats"
                        + " differ. If the agent already converted the document, publish it"
                        + " directly. Read back after publication; on WRITE_RESULT_UNKNOWN read"
                        + " before any retry.",
            annotations =
                    @McpTool.McpAnnotations(
                            readOnlyHint = false,
                            destructiveHint = true,
                            idempotentHint = true))
    public CallToolResult nacosPublish(
            @McpToolParam(description = "Exact target dataId", required = true) String dataId,
            @McpToolParam(
                            description =
                                    "Complete target-format document, already converted if"
                                        + " necessary; empty string allowed",
                            required = true)
                    String content,
            @McpToolParam(
                            description = "Nacos group; omitted uses configured default",
                            required = false)
                    String group,
            @McpToolParam(
                            description =
                                    "Nacos namespace ID; empty string/public selects public;"
                                        + " omitted uses configured ID",
                            required = false)
                    String namespace,
            @McpToolParam(
                            description =
                                    "properties, yaml or yml; defaults to"
                                        + " omni.config.nacos.file-type",
                            required = false)
                    String fileType) {
        return result(() -> service.nacosPublish(dataId, content, group, namespace, fileType));
    }

    /**
     * Exposes explicit offline conversion and same-format validation without any upstream mutation.
     */
    @McpTool(
            name = "convert_config",
            description =
                    "Explicit properties/YAML conversion without network access. Source/target"
                        + " formats default to server from/to file-type. Dotted keys and [index]"
                        + " paths expand into YAML containers; properties values stay strings."
                        + " Conflicts fail. Equal formats (yaml=yml) validate and return the"
                        + " original content. Skip this tool if input already has the target"
                        + " format.",
            annotations =
                    @McpTool.McpAnnotations(
                            readOnlyHint = true,
                            destructiveHint = false,
                            openWorldHint = false))
    public CallToolResult convert(
            @McpToolParam(description = "Document to convert", required = true) String content,
            @McpToolParam(
                            description =
                                    "Actual input properties/yaml/yml format; defaults to source"
                                        + " format",
                            required = false)
                    String sourceType,
            @McpToolParam(
                            description =
                                    "Desired properties/yaml/yml format; defaults to target format",
                            required = false)
                    String targetType) {
        return result(() -> service.convert(content, sourceType, targetType));
    }

    /** Converts service results and safe failures into structured and text MCP responses. */
    private CallToolResult result(Supplier<?> operation) {
        try {
            Object value = operation.get();
            // Support clients that consume structured results and clients that only consume text
            // content.
            return CallToolResult.builder()
                    .isError(false)
                    .structuredContent(json.convertValue(value, Map.class))
                    .addTextContent(json.writeValueAsString(value))
                    .build();
        } catch (MigrationException e) {
            return error(e.code().name(), e.getMessage(), e.details());
        } catch (RuntimeException e) {
            // Unexpected exceptions must not expose upstream URLs, credentials or configuration
            // snippets.
            return error("INTERNAL_ERROR", "Unexpected configuration operation failure");
        }
    }

    /**
     * Builds a stable MCP tool error without leaking internal exception or upstream response
     * details.
     */
    private CallToolResult error(String code, String message) {
        return error(code, message, Map.of());
    }

    private CallToolResult error(String code, String message, Map<String, Object> details) {
        Map<String, Object> error = new java.util.LinkedHashMap<>(details);
        error.put("code", code);
        error.put("message", message);
        return CallToolResult.builder()
                .isError(true)
                .structuredContent(error)
                .addTextContent(json.writeValueAsString(error))
                .build();
    }
}
