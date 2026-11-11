package xyz.fz.omni.core;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import io.modelcontextprotocol.server.McpServerFeatures.SyncToolSpecification;
import io.modelcontextprotocol.server.McpSyncServer;
import io.modelcontextprotocol.spec.McpSchema.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class ConnectorRegistryTest {
    private SyncToolSpecification tool(String name, CountDownLatch entered, CountDownLatch release) {
        var definition = Tool.builder().name(name).description("Test operation")
                .inputSchema(Map.of("type", "object", "properties", Map.of())).build();
        return new SyncToolSpecification(definition, (exchange, request) -> {
            if (entered != null) entered.countDown();
            if (release != null) {
                try { assertTrue(release.await(5, TimeUnit.SECONDS)); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new AssertionError(e); }
            }
            return ConnectorRegistry.success(Map.of("value", name));
        });
    }

    @Test void replacementWaitsForCallsAndClosesEachGenerationOnce() throws Exception {
        var server = mock(McpSyncServer.class);
        var published = new ConcurrentHashMap<String, SyncToolSpecification>();
        doAnswer(call -> { var spec = (SyncToolSpecification) call.getArgument(0); published.put(spec.tool().name(), spec); return null; }).when(server).addTool(any());
        var oldClosed = new AtomicInteger(); var newClosed = new AtomicInteger();
        var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
        try (var registry = new ConnectorRegistry(); var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            registry.attach(server, List.of("test"));
            registry.activate("test", "YAML", new ConnectorRegistry.Target(List.of(tool("test_read", entered, release)), oldClosed::incrementAndGet));
            var operation = executor.submit(() -> published.get("test_read").callHandler().apply(null, new CallToolRequest("test_read", Map.of())));
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            var replacement = executor.submit(() -> registry.activate("test", "RUNTIME", new ConnectorRegistry.Target(List.of(tool("test_read", null, null)), newClosed::incrementAndGet)));
            assertThrows(TimeoutException.class, () -> replacement.get(100, TimeUnit.MILLISECONDS));
            assertEquals(0, oldClosed.get()); release.countDown(); operation.get(5, TimeUnit.SECONDS); replacement.get(5, TimeUnit.SECONDS);
            assertEquals(1, oldClosed.get()); assertEquals(0, newClosed.get());
            verify(server, times(1)).addTool(any());
        }
        assertEquals(1, newClosed.get());
    }

    @Test void partialRegistrationFailureIsUndoneAndNotificationFailureDoesNotUndoActivation() {
        var server = mock(McpSyncServer.class); var removed = new ArrayList<String>();
        doThrow(new IllegalStateException("notification failure")).when(server).addTool(any());
        doAnswer(call -> { removed.add(call.getArgument(0)); return null; }).when(server).removeTool(anyString());
        var closed = new AtomicInteger();
        try (var registry = new ConnectorRegistry()) {
            registry.attach(server, List.of("test"));
            assertThrows(IllegalStateException.class, () -> registry.activate("test", "RUNTIME", new ConnectorRegistry.Target(List.of(tool("test_read", null, null)), closed::incrementAndGet)));
            assertEquals(List.of("test_read"), removed); assertEquals(1, closed.get());
            assertEquals(false, ((Map<?, ?>) registry.status().get("test")).get("enabled"));
            doNothing().when(server).addTool(any());
            doThrow(new IllegalStateException("disconnected client")).when(server).notifyToolsListChanged();
            registry.activate("test", "RUNTIME", new ConnectorRegistry.Target(List.of(tool("test_read", null, null)), () -> {}));
            assertEquals(true, ((Map<?, ?>) registry.status().get("test")).get("enabled"));
        }
    }
}
