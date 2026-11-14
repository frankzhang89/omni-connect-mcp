package xyz.fz.omni.rabbitmq;

import io.modelcontextprotocol.spec.McpSchema.CallToolResult;

import org.springframework.ai.mcp.annotation.*;

import xyz.fz.omni.core.MessageSupport;

import java.util.*;

public final class RabbitMqTools {
    private final RabbitMqRuntime runtime;
    private final org.springframework.core.env.Environment env;

    public RabbitMqTools(RabbitMqRuntime runtime, org.springframework.core.env.Environment env) {
        this.runtime = runtime;
        this.env = env;
    }

    private int limit(String name, int fallback) {
        return MessageSupport.setting(env, "omni.rabbitmq.limits", name, fallback);
    }

    private int count(Integer value) {
        return MessageSupport.bound(value, 10, limit("max-messages", 100));
    }

    private int size(Integer value) {
        return MessageSupport.bound(value, 50, limit("max-items", 100));
    }

    private int page(Integer value) {
        return MessageSupport.bound(value, 1, 1000000);
    }

    private int waitMs(Integer value) {
        return MessageSupport.bound(
                value,
                5000,
                MessageSupport.setting(env, "omni.rabbitmq.timeouts", "max-wait-ms", 15000));
    }

    @McpTool(
            name = "rabbitmq_list_queues",
            description =
                    "List queues in the connected vhost with Ready and Unack counts; missing"
                            + " metrics remain unavailable.",
            annotations =
                    @McpTool.McpAnnotations(
                            readOnlyHint = true,
                            destructiveHint = false,
                            idempotentHint = true))
    public CallToolResult rabbitmq_list_queues(
            @McpToolParam(description = "nameContains", required = false) String nameContains,
            @McpToolParam(description = "page", required = false) Integer page,
            @McpToolParam(description = "pageSize", required = false) Integer pageSize) {
        return MessageSupport.call(() -> runtime.queues(nameContains, page(page), size(pageSize)));
    }

    @McpTool(
            name = "rabbitmq_inspect_queue",
            description = "Inspect queue counts, rates, arguments and effective policies.",
            annotations =
                    @McpTool.McpAnnotations(
                            readOnlyHint = true,
                            destructiveHint = false,
                            idempotentHint = true))
    public CallToolResult rabbitmq_inspect_queue(
            @McpToolParam(description = "queue", required = true) String queue) {
        return MessageSupport.call(() -> runtime.inspectQueue(queue));
    }

    @McpTool(
            name = "rabbitmq_list_bindings",
            description =
                    "Inspect bindings; supply exactly one queue or exchange. Empty exchange means"
                            + " the default exchange.",
            annotations =
                    @McpTool.McpAnnotations(
                            readOnlyHint = true,
                            destructiveHint = false,
                            idempotentHint = true))
    public CallToolResult rabbitmq_list_bindings(
            @McpToolParam(description = "queue", required = false) String queue,
            @McpToolParam(description = "exchange", required = false) String exchange,
            @McpToolParam(description = "page", required = false) Integer page,
            @McpToolParam(description = "pageSize", required = false) Integer pageSize) {
        return MessageSupport.call(
                () -> runtime.bindings(queue, exchange, page(page), size(pageSize)));
    }

    @McpTool(
            name = "rabbitmq_list_consumers",
            description =
                    "Inspect consumers, acknowledgement modes, prefetch and channel identifiers.",
            annotations =
                    @McpTool.McpAnnotations(
                            readOnlyHint = true,
                            destructiveHint = false,
                            idempotentHint = true))
    public CallToolResult rabbitmq_list_consumers(
            @McpToolParam(description = "queue", required = false) String queue,
            @McpToolParam(description = "page", required = false) Integer page,
            @McpToolParam(description = "pageSize", required = false) Integer pageSize) {
        return MessageSupport.call(() -> runtime.consumers(queue, page(page), size(pageSize)));
    }

    @McpTool(
            name = "rabbitmq_inspect_channel",
            description =
                    "Inspect channel-wide Unack and its application connection; counts may cover"
                            + " several queues.",
            annotations =
                    @McpTool.McpAnnotations(
                            readOnlyHint = true,
                            destructiveHint = false,
                            idempotentHint = true))
    public CallToolResult rabbitmq_inspect_channel(
            @McpToolParam(description = "channelName", required = true) String channelName) {
        return MessageSupport.call(() -> runtime.channel(channelName));
    }

    @McpTool(
            name = "rabbitmq_sample_messages",
            description =
                    "Sample Ready messages and requeue them. Alters delivery state and may affect"
                            + " order; cannot inspect Unack bodies.",
            annotations =
                    @McpTool.McpAnnotations(
                            readOnlyHint = false,
                            destructiveHint = true,
                            idempotentHint = false))
    public CallToolResult rabbitmq_sample_messages(
            @McpToolParam(description = "queue", required = true) String queue,
            @McpToolParam(description = "count", required = false) Integer count) {
        return MessageSupport.call(
                () ->
                        runtime.sample(
                                queue, MessageSupport.bound(count, 3, limit("max-samples", 10))));
    }

    @McpTool(
            name = "rabbitmq_publish_message",
            description =
                    "Publish to an existing exchange with mandatory routing and confirms. Unknown"
                            + " outcome must not be blindly retried.",
            annotations =
                    @McpTool.McpAnnotations(
                            readOnlyHint = false,
                            destructiveHint = true,
                            idempotentHint = false))
    public CallToolResult rabbitmq_publish_message(
            @McpToolParam(description = "exchange", required = true) String exchange,
            @McpToolParam(description = "routingKey", required = true) String routingKey,
            @McpToolParam(description = "payload", required = true) String payload,
            @McpToolParam(description = "encoding", required = false) String encoding,
            @McpToolParam(description = "headers", required = false) Map<String, String> headers,
            @McpToolParam(description = "contentType", required = false) String contentType,
            @McpToolParam(description = "messageId", required = false) String messageId,
            @McpToolParam(description = "correlationId", required = false) String correlationId,
            @McpToolParam(description = "persistent", required = false) Boolean persistent) {
        return MessageSupport.call(
                () ->
                        runtime.publish(
                                exchange,
                                routingKey,
                                payload,
                                encoding,
                                headers,
                                contentType,
                                messageId,
                                correlationId,
                                persistent));
    }

    @McpTool(
            name = "rabbitmq_consume_messages",
            description =
                    "Receive and ACK a bounded batch. Removes messages; ACK SENT is not a broker"
                            + " receipt or business processing guarantee.",
            annotations =
                    @McpTool.McpAnnotations(
                            readOnlyHint = false,
                            destructiveHint = true,
                            idempotentHint = false))
    public CallToolResult rabbitmq_consume_messages(
            @McpToolParam(description = "queue", required = true) String queue,
            @McpToolParam(description = "count", required = false) Integer count,
            @McpToolParam(description = "maxWaitMs", required = false) Integer maxWaitMs) {
        return MessageSupport.call(() -> runtime.consume(queue, count(count), waitMs(maxWaitMs)));
    }
}
