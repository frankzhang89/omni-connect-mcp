package xyz.fz.omni.kafka;

import io.modelcontextprotocol.spec.McpSchema.CallToolResult;

import org.springframework.ai.mcp.annotation.*;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import xyz.fz.omni.core.*;

import java.util.*;

@Component
public final class KafkaConnectorModule implements ConnectorModule {
    private final ConnectorRegistry registry;
    private final Environment env;
    private final KafkaClients clients;

    public KafkaConnectorModule(ConnectorRegistry registry, Environment env, KafkaClients clients) {
        this.registry = registry;
        this.env = env;
        this.clients = clients;
    }

    public String id() {
        return "kafka";
    }

    public Object managementTools() {
        return this;
    }

    private String property(String name, String fallback) {
        return env.getProperty("omni.kafka.connection." + name, fallback);
    }

    public void start() {
        if (env.getProperty("omni.kafka.enabled", Boolean.class, false))
            try {
                install(
                        property("bootstrap-servers", ""),
                        property("security-protocol", "PLAINTEXT"),
                        property("username", null),
                        property("password", null),
                        "YAML");
            } catch (Exception e) {
                throw new IllegalStateException("Kafka activation failed; check configuration");
            }
    }

    private static String quote(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private Map<String, Object> install(
            String addresses, String protocol, String username, String password, String source)
            throws Exception {
        MessageSupport.required(addresses);
        String security = protocol == null ? "PLAINTEXT" : protocol;
        if (!Set.of("PLAINTEXT", "SASL_PLAINTEXT").contains(security))
            throw new IllegalArgumentException();
        var p = new Properties();
        p.put("bootstrap.servers", addresses);
        p.put("security.protocol", security);
        p.put("client.id", "omni-connect-mcp");
        int timeout = MessageSupport.setting(env, "omni.kafka.timeouts", "operation-ms", 10000);
        p.put("default.api.timeout.ms", timeout);
        p.put("request.timeout.ms", timeout);
        p.put(
                "socket.connection.setup.timeout.ms",
                MessageSupport.setting(env, "omni.kafka.timeouts", "connect-ms", 5000));
        if (security.equals("SASL_PLAINTEXT")) {
            MessageSupport.required(username);
            if (password == null) throw new IllegalArgumentException();
            p.put("sasl.mechanism", "PLAIN");
            p.put(
                    "sasl.jaas.config",
                    "org.apache.kafka.common.security.plain.PlainLoginModule required username=\""
                            + quote(username)
                            + "\" password=\""
                            + quote(password)
                            + "\";");
        }
        var runtime =
                new KafkaRuntime(
                        p,
                        clients,
                        timeout,
                        MessageSupport.setting(env, "omni.kafka.limits", "preview-bytes", 4096),
                        MessageSupport.setting(
                                env, "omni.kafka.limits", "max-payload-bytes", 65536),
                        MessageSupport.setting(
                                env, "omni.kafka.limits", "max-response-bytes", 1048576));
        try {
            runtime.validate();
            return registry.activate(
                    id(),
                    source,
                    new ConnectorRegistry.Target(
                            ConnectorRegistry.specifications(new KafkaTools(runtime, env)),
                            runtime::close));
        } catch (Exception e) {
            runtime.close();
            throw e;
        }
    }

    @McpTool(
            name = "kafka_connect",
            description =
                    "Validate cluster metadata and activate or replace process-wide Kafka. Failed"
                        + " replacement retains old clients. Resource permissions are checked by"
                        + " operations.",
            annotations =
                    @McpTool.McpAnnotations(
                            readOnlyHint = false,
                            destructiveHint = true,
                            idempotentHint = false))
    public synchronized CallToolResult connect(
            @McpToolParam(
                            description = "Comma separated bootstrap host:port addresses",
                            required = true)
                    String bootstrapServers,
            @McpToolParam(
                            description = "PLAINTEXT (default) or SASL_PLAINTEXT with PLAIN",
                            required = false)
                    String securityProtocol,
            @McpToolParam(description = "SASL username", required = false) String username,
            @McpToolParam(description = "SASL password", required = false) String password) {
        return MessageSupport.call(
                () -> install(bootstrapServers, securityProtocol, username, password, "RUNTIME"));
    }
}
