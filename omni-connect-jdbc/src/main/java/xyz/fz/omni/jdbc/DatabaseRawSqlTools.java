package xyz.fz.omni.jdbc;

import io.modelcontextprotocol.spec.McpSchema.CallToolResult;

import org.springframework.ai.mcp.annotation.McpTool;
import org.springframework.ai.mcp.annotation.McpToolParam;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import xyz.fz.omni.core.ConnectorRegistry;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Component
@ConditionalOnProperty(prefix = "omni.jdbc.write", name = "allow-raw-sql", havingValue = "true")
public class DatabaseRawSqlTools {
    private static final int MAX_RETURNED_RESULTS = 20;
    private final DatabaseRuntimeManager runtime;
    private final TransactionResultMapper mapper;
    private final int timeoutSeconds;

    public DatabaseRawSqlTools(
            DatabaseRuntimeManager runtime,
            @Value("${omni.jdbc.query.max-rows:100}") int maxRows,
            @Value("${omni.jdbc.query.max-cell-chars:4000}") int maxCellChars,
            @Value("${omni.jdbc.query.timeout-seconds:10}") int timeoutSeconds) {
        this.runtime = runtime;
        this.mapper = new TransactionResultMapper(maxRows, maxCellChars);
        this.timeoutSeconds = Math.max(1, timeoutSeconds);
    }

    @McpTool(
            name = "db_execute_rawsql",
            description =
                    "Send raw SQL unchanged to the active JDBC data source. The driver and database"
                        + " decide syntax, batch handling, and privileges. Returns ordered query"
                        + " results and update counts. Each call uses an independent auto-commit"
                        + " connection that is discarded afterwards.",
            annotations =
                    @McpTool.McpAnnotations(
                            readOnlyHint = false,
                            destructiveHint = true,
                            idempotentHint = false,
                            openWorldHint = true))
    public CallToolResult execute(
            @McpToolParam(description = "Raw SQL to execute", required = true) String sql) {
        try (var lease = runtime.acquire(); var connection = lease.connection()) {
            try (var statement = connection.createStatement()) {
                statement.setEscapeProcessing(false);
                statement.setQueryTimeout(timeoutSeconds);
                List<Map<String, Object>> results = new ArrayList<>();
                boolean resultsTruncated = readResults(sql, statement, results);
                return ConnectorRegistry.success(
                        Map.of(
                                "success", true,
                                "datasourceId", lease.datasourceId(),
                                "results", results,
                                "resultsTruncated", resultsTruncated));
            } finally {
                // Raw SQL can change session state outside JDBC's pool reset tracking.
                lease.evict(connection);
            }
        } catch (SQLException ex) {
            String state = ex.getSQLState();
            String stateDetail =
                    state != null && state.matches("[0-9A-Z]{5}") ? ", SQLState: " + state : "";
            return ConnectorRegistry.failure(
                    "SQL_EXECUTION_FAILED",
                    "Database execution failed (vendor code: " + ex.getErrorCode() + stateDetail
                            + "). Earlier statements may have taken effect.");
        } catch (RuntimeException ex) {
            return ConnectorRegistry.failure(
                    "SQL_EXECUTION_FAILED", "SQL execution could not be completed.");
        }
    }

    private boolean readResults(
            String sql, Statement statement, List<Map<String, Object>> results) throws SQLException {
        boolean hasRows = statement.execute(sql);
        boolean resultsTruncated = false;
        while (true) {
            long count = hasRows ? -1 : updateCount(statement);
            if (!hasRows && count == -1) {
                return resultsTruncated;
            }
            if (results.size() == MAX_RETURNED_RESULTS) {
                resultsTruncated = true;
            } else if (hasRows) {
                try (var rows = statement.getResultSet()) {
                    results.add(mapper.readPositional(rows));
                }
            } else {
                results.add(Map.of("type", "UPDATE", "affectedRows", count));
            }
            // Advance even after the response cap so later JDBC errors remain observable.
            hasRows = statement.getMoreResults();
        }
    }

    private static long updateCount(Statement statement) throws SQLException {
        try {
            return statement.getLargeUpdateCount();
        } catch (SQLFeatureNotSupportedException | UnsupportedOperationException ignored) {
            return statement.getUpdateCount();
        }
    }
}
