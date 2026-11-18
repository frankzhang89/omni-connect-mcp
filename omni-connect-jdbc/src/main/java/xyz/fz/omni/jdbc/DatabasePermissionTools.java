package xyz.fz.omni.jdbc;

import io.modelcontextprotocol.spec.McpSchema.CallToolResult;

import org.springframework.ai.mcp.annotation.McpTool;
import org.springframework.ai.mcp.annotation.McpToolParam;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(prefix = "omni.jdbc.write", name = "allow-permissions", havingValue = "true")
public class DatabasePermissionTools {
    private final DatabaseWriteService service;

    public DatabasePermissionTools(DatabaseWriteService service) {
        this.service = service;
    }

    @McpTool(
            name = "db_execute_permission",
            description =
                    "Execute one native GRANT, REVOKE, or supported account or role management"
                        + " statement on the active JDBC data source. Applies independently of"
                        + " db_execute transactions. Syntax and privileges depend on the database."
                        + " SQL password literals are part of the tool request.",
            annotations =
                    @McpTool.McpAnnotations(
                            readOnlyHint = false,
                            destructiveHint = true,
                            idempotentHint = false,
                            openWorldHint = false))
    public CallToolResult executePermission(
            @McpToolParam(description = "Single permission management statement", required = true)
                    String sql) {
        return service.executePermission(sql);
    }
}
