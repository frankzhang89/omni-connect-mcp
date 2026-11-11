package xyz.fz.omni.core;

import io.modelcontextprotocol.server.McpServerFeatures.SyncToolSpecification;
import io.modelcontextprotocol.server.McpSyncServer;
import io.modelcontextprotocol.spec.McpSchema.CallToolResult;

import org.springframework.ai.mcp.annotation.McpTool;
import org.springframework.ai.mcp.annotation.provider.tool.SyncMcpToolProvider;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/** Owns process-local targets. Callbacks retain a generation until their operation completes. */
public final class ConnectorRegistry implements AutoCloseable {
    private final Map<String, Slot> slots = new ConcurrentHashMap<>();
    private volatile McpSyncServer server;
    private volatile boolean closed;

    private static final class Slot {
        final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
        Target target;
        String source;
    }

    public record Target(List<SyncToolSpecification> tools, Runnable cleanup)
            implements AutoCloseable {
        public void close() {
            cleanup.run();
        }
    }

    public static List<SyncToolSpecification> specifications(Object... objects) {
        return new SyncMcpToolProvider(Arrays.asList(objects)).getToolSpecifications();
    }

    public void attach(McpSyncServer server, Collection<String> ids) {
        this.server = server;
        ids.forEach(id -> slots.computeIfAbsent(id, ignored -> new Slot()));
    }

    /**
     * Validation happens in each module before this method; failed registration restores the
     * previous target.
     */
    public Map<String, Object> activate(String id, String source, Target candidate) {
        Slot slot = slots.computeIfAbsent(id, ignored -> new Slot());
        slot.lock.writeLock().lock();
        List<String> added = new ArrayList<>();
        try {
            if (closed) throw new IllegalStateException("Service is closed");
            if (slot.target == null) {
                for (var spec : candidate.tools()) {
                    String name = spec.tool().name();
                    added.add(name);
                    server.addTool(
                            new SyncToolSpecification(
                                    spec.tool(),
                                    (exchange, request) -> {
                                        slot.lock.readLock().lock();
                                        try {
                                            if (slot.target == null)
                                                return failure(
                                                        "CONNECTOR_NOT_ACTIVE",
                                                        "Configure the connector first");
                                            return slot.target.tools().stream()
                                                    .filter(t -> t.tool().name().equals(name))
                                                    .findFirst()
                                                    .orElseThrow()
                                                    .callHandler()
                                                    .apply(exchange, request);
                                        } finally {
                                            slot.lock.readLock().unlock();
                                        }
                                    }));
                }
            }
            Target previous = slot.target;
            slot.target = candidate;
            slot.source = source;
            if (previous != null) {
                try {
                    previous.close();
                } catch (RuntimeException ignored) {
                    /* Publication is committed; cleanup cannot reverse it. */
                }
            }
            try {
                server.notifyToolsListChanged();
            } catch (RuntimeException ignored) {
                /* A subsequent tools/list remains authoritative. */
            }
            // The SDK sends list-change notifications when tools are added. A refresh is always
            // authoritative.
            return Map.of(
                    "success",
                    true,
                    "connector",
                    id,
                    "enabled",
                    true,
                    "tools",
                    candidate.tools().stream().map(t -> t.tool().name()).toList());
        } catch (RuntimeException error) {
            for (String name : added) {
                try {
                    server.removeTool(name);
                } catch (RuntimeException ignored) {
                    /* Continue cleanup. */
                }
            }
            candidate.close();
            throw error;
        } finally {
            slot.lock.writeLock().unlock();
        }
    }

    @McpTool(
            name = "connector_status",
            description =
                    "List process-wide connector activation state, configuration source and tools."
                        + " Credentials and addresses are never returned.")
    public Map<String, Object> status() {
        var result = new TreeMap<String, Object>();
        slots.forEach(
                (id, slot) -> {
                    slot.lock.readLock().lock();
                    try {
                        result.put(
                                id,
                                Map.of(
                                        "enabled",
                                        slot.target != null,
                                        "source",
                                        slot.source == null ? "NONE" : slot.source,
                                        "tools",
                                        slot.target == null
                                                ? List.of()
                                                : slot.target.tools().stream()
                                                        .map(t -> t.tool().name())
                                                        .toList()));
                    } finally {
                        slot.lock.readLock().unlock();
                    }
                });
        return result;
    }

    public static CallToolResult failure(String code, String message) {
        var body = Map.of("success", false, "code", code, "message", message);
        return CallToolResult.builder()
                .isError(true)
                .structuredContent(body)
                .addTextContent(
                        new tools.jackson.databind.json.JsonMapper().writeValueAsString(body))
                .build();
    }

    public static CallToolResult success(Map<String, Object> body) {
        return CallToolResult.builder()
                .isError(false)
                .structuredContent(body)
                .addTextContent(
                        new tools.jackson.databind.json.JsonMapper().writeValueAsString(body))
                .build();
    }

    @Override
    public void close() {
        closed = true;
        slots.values()
                .forEach(
                        slot -> {
                            slot.lock.writeLock().lock();
                            try {
                                if (slot.target != null) slot.target.close();
                                slot.target = null;
                            } finally {
                                slot.lock.writeLock().unlock();
                            }
                        });
    }
}
