package xyz.fz.omni.jdbc;

import io.modelcontextprotocol.spec.McpSchema.CallToolResult;

import org.springframework.ai.mcp.annotation.McpTool;
import org.springframework.ai.mcp.annotation.McpToolParam;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import xyz.fz.omni.core.*;

import java.util.*;

/** Keeps the existing switch tool available before the first JDBC target exists. */
@Component
public final class JdbcConnectorModule implements ConnectorModule {
    private final ConnectorRegistry registry;
    private final DatabaseRuntimeManager runtime;
    private final List<io.modelcontextprotocol.server.McpServerFeatures.SyncToolSpecification>
            tools;
    private final boolean enabled;

    public JdbcConnectorModule(
            ConnectorRegistry registry,
            DatabaseRuntimeManager runtime,
            DatabaseTools reads,
            DatabaseWriteTools writes,
            DatabaseTransactionTools transactions,
            @Value("${omni.jdbc.enabled:false}") boolean enabled) {
        this.registry = registry;
        this.runtime = runtime;
        this.enabled = enabled;
        tools = ConnectorRegistry.specifications(reads, writes, transactions);
    }

    public String id() {
        return "jdbc";
    }

    public Object managementTools() {
        return this;
    }

    public void start() {
        if (enabled) activate("YAML");
    }

    private Map<String, Object> activate(String source) {
        return registry.activate(id(), source, new ConnectorRegistry.Target(tools, () -> {}));
    }

    @McpTool(
            name = "db_switch_datasource",
            description =
                    "Validate and switch JDBC for ALL clients in this process. Also activates JDBC"
                        + " when initially disabled. Memory-only; restart restores YAML. Existing"
                        + " transactions retain their original target. Credentials are never"
                        + " returned.",
            annotations =
                    @McpTool.McpAnnotations(
                            readOnlyHint = false,
                            destructiveHint = true,
                            idempotentHint = false,
                            openWorldHint = true))
    public synchronized CallToolResult connect(
            @McpToolParam(description = "JDBC URL", required = true) String jdbcUrl,
            @McpToolParam(description = "Database username", required = true) String username,
            @McpToolParam(description = "Password; empty allowed", required = true) String password,
            @McpToolParam(description = "Optional metadata schema", required = false)
                    String schema) {
        try {
            var result =
                    runtime.switchTo(
                            new DatabaseConnectionSettings(jdbcUrl, username, password, schema));
            activate("RUNTIME");
            return ConnectorRegistry.success(result);
        } catch (DatabaseSwitchException e) {
            return ConnectorRegistry.failure(e.code().name(), e.getMessage());
        } catch (RuntimeException e) {
            return ConnectorRegistry.failure(
                    "CONNECTOR_ACTIVATION_FAILED", "JDBC activation failed");
        }
    }
}
