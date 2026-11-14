package xyz.fz.omni.rabbitmq;

import com.rabbitmq.client.Connection;

import io.modelcontextprotocol.spec.McpSchema.CallToolResult;

import org.springframework.ai.mcp.annotation.*;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import xyz.fz.omni.core.*;

import java.util.*;

@Component
public final class RabbitMqConnectorModule implements ConnectorModule {
    private final ConnectorRegistry registry;
    private final Environment env;
    private final RabbitMqClients clients;

    public RabbitMqConnectorModule(
            ConnectorRegistry registry, Environment env, RabbitMqClients clients) {
        this.registry = registry;
        this.env = env;
        this.clients = clients;
    }

    public String id() {
        return "rabbitmq";
    }

    public Object managementTools() {
        return this;
    }

    private String property(String key, String fallback) {
        return env.getProperty("omni.rabbitmq.connection." + key, fallback);
    }

    public void start() {
        if (env.getProperty("omni.rabbitmq.enabled", Boolean.class, false))
            try {
                install(
                        property("management-url", ""),
                        property("username", ""),
                        property("password", ""),
                        property("virtual-host", "/"),
                        property("amqp.addresses", null),
                        property("amqp.username", null),
                        property("amqp.password", null),
                        "YAML");
            } catch (Exception e) {
                throw new IllegalStateException("RabbitMQ activation failed; check configuration");
            }
    }

    private Map<String, Object> install(
            String url,
            String user,
            String password,
            String vhost,
            String addresses,
            String amqpUser,
            String amqpPassword,
            String source)
            throws Exception {
        MessageSupport.required(url);
        MessageSupport.required(user);
        if (password == null) throw new IllegalArgumentException();
        String selected = vhost == null ? "/" : MessageSupport.required(vhost);
        int timeout = MessageSupport.setting(env, "omni.rabbitmq.timeouts", "operation-ms", 10000);
        Connection connection = null;
        RabbitMqRuntime runtime = null;
        try {
            if (addresses != null && !addresses.isBlank()) {
                connection =
                        clients.connect(
                                addresses,
                                amqpUser == null ? user : amqpUser,
                                amqpPassword == null ? password : amqpPassword,
                                selected,
                                MessageSupport.setting(
                                        env, "omni.rabbitmq.timeouts", "connect-ms", 5000));
                try (var channel = connection.createChannel()) {}
            }
            runtime =
                    new RabbitMqRuntime(
                            url,
                            user,
                            password,
                            selected,
                            connection,
                            timeout,
                            MessageSupport.setting(
                                    env, "omni.rabbitmq.limits", "preview-bytes", 4096),
                            MessageSupport.setting(
                                    env, "omni.rabbitmq.limits", "max-payload-bytes", 65536),
                            MessageSupport.setting(
                                    env, "omni.rabbitmq.limits", "max-response-bytes", 1048576));
            runtime.request("GET", "/api/whoami", null);
            runtime.queues(null, 1, 1);
            return registry.activate(
                    id(),
                    source,
                    new ConnectorRegistry.Target(
                            ConnectorRegistry.specifications(new RabbitMqTools(runtime, env)),
                            runtime::close,
                            Map.of(
                                    "managementCapability",
                                    "AVAILABLE",
                                    "amqpCapability",
                                    connection == null ? "NOT_CONFIGURED" : "AVAILABLE")));
        } catch (Exception e) {
            if (runtime != null) runtime.close();
            else if (connection != null)
                try {
                    connection.close();
                } catch (Exception ignored) {
                }
            throw e;
        }
    }

    @McpTool(
            name = "rabbitmq_connect",
            description =
                    "Validate and activate process-wide RabbitMQ management and optional AMQP"
                        + " connection. Failed replacement retains the old target; omitted fields"
                        + " never inherit old credentials.",
            annotations =
                    @McpTool.McpAnnotations(
                            readOnlyHint = false,
                            destructiveHint = true,
                            idempotentHint = false))
    public synchronized CallToolResult connect(
            @McpToolParam(
                            description =
                                    "HTTP management base URL including optional proxy prefix",
                            required = true)
                    String managementUrl,
            @McpToolParam(description = "Management username", required = true) String username,
            @McpToolParam(description = "Management password", required = true) String password,
            @McpToolParam(description = "Virtual host, default /", required = false)
                    String virtualHost,
            @McpToolParam(
                            description = "Optional comma separated AMQP host:port addresses",
                            required = false)
                    String amqpAddresses,
            @McpToolParam(
                            description =
                                    "Optional AMQP username; defaults to this request username",
                            required = false)
                    String amqpUsername,
            @McpToolParam(
                            description =
                                    "Optional AMQP password; defaults to this request password",
                            required = false)
                    String amqpPassword) {
        return MessageSupport.call(
                () ->
                        install(
                                managementUrl,
                                username,
                                password,
                                virtualHost,
                                amqpAddresses,
                                amqpUsername,
                                amqpPassword,
                                "RUNTIME"));
    }
}
