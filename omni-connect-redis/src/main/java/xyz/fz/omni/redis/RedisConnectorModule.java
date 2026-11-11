package xyz.fz.omni.redis;

import io.modelcontextprotocol.spec.McpSchema.CallToolResult;

import org.springframework.ai.mcp.annotation.*;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import xyz.fz.omni.core.*;

import java.util.*;

/** Owns isolated Redis generations, each with its own selected database and factories. */
@Component
public final class RedisConnectorModule implements ConnectorModule {
    private final ConnectorRegistry registry;
    private final Environment env;

    public RedisConnectorModule(ConnectorRegistry registry, Environment env) {
        this.registry = registry;
        this.env = env;
    }

    public String id() {
        return "redis";
    }

    public Object managementTools() {
        return this;
    }

    private int limit(String name, int fallback) {
        return env.getProperty("omni.redis.limits." + name, Integer.class, fallback);
    }

    public void start() {
        if (env.getProperty("omni.redis.enabled", Boolean.class, false))
            install(
                    env.getProperty("omni.redis.connection.host", "localhost"),
                    env.getProperty("omni.redis.connection.port", Integer.class, 6379),
                    env.getProperty("omni.redis.connection.username", ""),
                    env.getProperty("omni.redis.connection.password", ""),
                    env.getProperty("omni.redis.connection.database", Integer.class, 0),
                    false,
                    "YAML");
    }

    private Map<String, Object> install(
            String host,
            int port,
            String username,
            String password,
            int database,
            boolean probe,
            String source) {
        if (host == null || host.isBlank() || port < 1 || port > 65535)
            throw new IllegalArgumentException("Invalid Redis address");
        var service =
                new RedisDatabaseService(
                        database, host, port, username, password, limit("max-databases", 16));
        try {
            var read =
                    new RedisReadService(
                            service,
                            limit("max-scan-count", 100),
                            limit("max-items", 100),
                            limit("max-value-chars", 4000));
            var write =
                    new RedisWriteService(
                            service, limit("max-items", 100), limit("max-value-chars", 4000));
            var tools = ConnectorRegistry.specifications(new RedisTools(read, write, service));
            if (probe) service.selectDatabase(database);
            return registry.activate(
                    id(), source, new ConnectorRegistry.Target(tools, service::destroy));
        } catch (RuntimeException e) {
            service.destroy();
            throw e;
        }
    }

    @McpTool(
            name = "redis_connect",
            description =
                    "Activate or replace the process-wide Redis connection after PING. Omitted"
                        + " options use defaults, not previous credentials. Memory-only; no YAML"
                        + " changes.",
            annotations =
                    @McpTool.McpAnnotations(
                            readOnlyHint = false,
                            destructiveHint = true,
                            idempotentHint = false))
    public synchronized CallToolResult connect(
            @McpToolParam(description = "Redis hostname", required = true) String host,
            @McpToolParam(description = "Port, default 6379", required = false) Integer port,
            @McpToolParam(description = "Optional username", required = false) String username,
            @McpToolParam(description = "Optional password", required = false) String password,
            @McpToolParam(description = "Database, default 0", required = false) Integer database) {
        try {
            return ConnectorRegistry.success(
                    install(
                            host,
                            port == null ? 6379 : port,
                            username,
                            password,
                            database == null ? 0 : database,
                            true,
                            "RUNTIME"));
        } catch (RuntimeException e) {
            return ConnectorRegistry.failure(
                    "CONNECTOR_ACTIVATION_FAILED",
                    "Redis activation failed; check connection settings");
        }
    }
}
