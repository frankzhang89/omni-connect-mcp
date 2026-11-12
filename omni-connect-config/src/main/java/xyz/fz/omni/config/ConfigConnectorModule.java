package xyz.fz.omni.config;

import io.modelcontextprotocol.server.McpServerFeatures.SyncToolSpecification;
import io.modelcontextprotocol.spec.McpSchema.CallToolResult;

import org.springframework.ai.mcp.annotation.*;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import xyz.fz.omni.config.adapter.*;
import xyz.fz.omni.config.config.MigrationProperties;
import xyz.fz.omni.config.config.MigrationProperties.*;
import xyz.fz.omni.config.core.*;
import xyz.fz.omni.config.format.ConfigFormatService;
import xyz.fz.omni.config.mcp.ConfigTools;
import xyz.fz.omni.core.*;

import java.util.*;

/** Creates isolated provider generations rather than mutating shared configuration records. */
@Component
public final class ConfigConnectorModule implements ConnectorModule {
    private final ConnectorRegistry registry;
    private final Environment env;
    private final ConfigFormatService formats = new ConfigFormatService();
    private final MigrationProperties initial;

    public ConfigConnectorModule(ConnectorRegistry registry, Environment env) {
        this.registry = registry;
        this.env = env;
        var binder = Binder.get(env);
        Apollo apollo =
                binder.bind("omni.config.apollo", Apollo.class)
                        .orElse(new Apollo(null, null, null, null, null, null));
        Nacos nacos =
                binder.bind("omni.config.nacos", Nacos.class)
                        .orElse(new Nacos(null, null, null, null, null, null, null));
        initial =
                new MigrationProperties(
                        new From(apollo),
                        new To(nacos),
                        binder.bind("omni.config.http", Http.class).orElse(new Http(null, null)));
    }

    public String id() {
        return "apollo";
    }

    public Object managementTools() {
        return this;
    }

    public void start() {
        if (env.getProperty("omni.config.apollo.enabled", Boolean.class, false))
            installApollo(initial.from().apollo(), false, "YAML");
    }

    public List<SyncToolSpecification> conversionTools() {
        return specs(initial, List.of(), "convert_config");
    }

    private List<SyncToolSpecification> specs(
            MigrationProperties props, List<ConfigStore> stores, String prefix) {
        var tools = new ConfigTools(new ConfigService(props, formats, stores));
        return ConnectorRegistry.specifications(tools).stream()
                .filter(t -> t.tool().name().startsWith(prefix))
                .toList();
    }

    private void required(String value, String name) {
        if (value == null || value.isBlank())
            throw new IllegalArgumentException(name + " is required");
    }

    private Map<String, Object> installApollo(Apollo settings, boolean probe, String source) {
        required(settings.meta(), "meta");
        required(settings.appId(), "appId");
        var props = new MigrationProperties(new From(settings), initial.to(), initial.http());
        var http = new HttpTransport(props);
        try {
            var store = new ApolloConfigStore(http, props, formats);
            var tools = specs(props, List.of(store), "apollo_");
            if (probe)
                new ConfigService(props, formats, List.of(store))
                        .apolloRead(null, null, null, null);
            return registry.activate(
                    "apollo", source, new ConnectorRegistry.Target(tools, http::close));
        } catch (RuntimeException e) {
            http.close();
            throw e;
        }
    }

    @McpTool(
            name = "apollo_connect",
            description =
                    "Activate or replace the process-wide Apollo connection. Validates a published"
                        + " default namespace with a read. Optional Portal settings are checked by"
                        + " item operations. Memory-only; no writes or YAML changes.",
            annotations =
                    @McpTool.McpAnnotations(
                            readOnlyHint = false,
                            destructiveHint = true,
                            idempotentHint = false))
    public synchronized CallToolResult connectApollo(
            @McpToolParam(description = "Apollo meta URL", required = true) String meta,
            @McpToolParam(description = "Default app ID", required = true) String appId,
            @McpToolParam(description = "Cluster, default default", required = false)
                    String cluster,
            @McpToolParam(description = "Namespace, default application", required = false)
                    String namespace,
            @McpToolParam(description = "Format, default properties", required = false)
                    String fileType,
            @McpToolParam(description = "Optional signing key", required = false) String accessKey,
            @McpToolParam(description = "Optional Portal URL", required = false) String portalUrl,
            @McpToolParam(description = "Optional Portal OpenAPI token", required = false)
                    String openapiToken,
            @McpToolParam(description = "Optional Portal environment", required = false) String env,
            @McpToolParam(description = "Optional editor", required = false) String operator,
            @McpToolParam(description = "Optional publisher", required = false) String releasedBy) {
        try {
            return ConnectorRegistry.success(
                    installApollo(
                            new Apollo(
                                    meta,
                                    appId,
                                    cluster,
                                    namespace,
                                    fileType,
                                    accessKey,
                                    portalUrl,
                                    openapiToken,
                                    env,
                                    operator,
                                    releasedBy),
                            true,
                            "RUNTIME"));
        } catch (MigrationException e) {
            return ConnectorRegistry.failure(e.code().name(), e.getMessage());
        } catch (RuntimeException e) {
            return ConnectorRegistry.failure(
                    "CONNECTOR_ACTIVATION_FAILED",
                    "Apollo activation failed; check connection settings");
        }
    }

    @Bean
    NacosModule nacosModule() {
        return new NacosModule();
    }

    public final class NacosModule implements ConnectorModule {
        public String id() {
            return "nacos";
        }

        public Object managementTools() {
            return this;
        }

        public void start() {
            if (env.getProperty("omni.config.nacos.enabled", Boolean.class, false))
                install(initial.to().nacos(), false, "YAML");
        }

        private Map<String, Object> install(Nacos settings, boolean probe, String source) {
            required(settings.serverAddr(), "serverAddr");
            var props = new MigrationProperties(initial.from(), new To(settings), initial.http());
            var http = new HttpTransport(props);
            try {
                var store = new NacosConfigStore(http, props);
                var tools = specs(props, List.of(store), "nacos_");
                if (probe) store.listNamespaces();
                return registry.activate(
                        id(),
                        source,
                        new ConnectorRegistry.Target(
                                tools, http::close, Map.of("version", store.version())));
            } catch (RuntimeException e) {
                http.close();
                throw e;
            }
        }

        @McpTool(
                name = "nacos_connect",
                description =
                        "Activate or replace the process-wide Nacos connection after detecting the"
                            + " server version and validating a read-only namespace query. Returns"
                            + " version; subsequent operations use its cached API selection. Nacos"
                            + " 3.x uses Admin APIs. Memory-only; no publication or YAML changes.",
                annotations =
                        @McpTool.McpAnnotations(
                                readOnlyHint = false,
                                destructiveHint = true,
                                idempotentHint = false))
        public synchronized CallToolResult connect(
                @McpToolParam(description = "Server address", required = true) String serverAddr,
                @McpToolParam(description = "Context path, default /nacos", required = false)
                        String contextPath,
                @McpToolParam(description = "Optional username", required = false) String username,
                @McpToolParam(description = "Optional password", required = false) String password,
                @McpToolParam(description = "Namespace ID, default public", required = false)
                        String namespace,
                @McpToolParam(description = "Group, default DEFAULT_GROUP", required = false)
                        String group,
                @McpToolParam(description = "Format, default yaml", required = false)
                        String fileType) {
            try {
                return ConnectorRegistry.success(
                        install(
                                new Nacos(
                                        serverAddr,
                                        contextPath,
                                        username,
                                        password,
                                        namespace,
                                        group,
                                        fileType),
                                true,
                                "RUNTIME"));
            } catch (MigrationException e) {
                return ConnectorRegistry.failure(e.code().name(), e.getMessage());
            } catch (RuntimeException e) {
                return ConnectorRegistry.failure(
                        "CONNECTOR_ACTIVATION_FAILED",
                        "Nacos activation failed; check connection settings");
            }
        }
    }
}
