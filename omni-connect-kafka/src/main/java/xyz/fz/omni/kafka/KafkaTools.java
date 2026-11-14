package xyz.fz.omni.kafka;

import io.modelcontextprotocol.spec.McpSchema.CallToolResult;

import org.springframework.ai.mcp.annotation.*;

import xyz.fz.omni.core.MessageSupport;

import java.util.*;

public final class KafkaTools {
    private final KafkaRuntime runtime;
    private final org.springframework.core.env.Environment env;

    public KafkaTools(KafkaRuntime runtime, org.springframework.core.env.Environment env) {
        this.runtime = runtime;
        this.env = env;
    }

    private int limit(String name, int fallback) {
        return MessageSupport.setting(env, "omni.kafka.limits", name, fallback);
    }

    private int count(Integer value) {
        return MessageSupport.bound(value, 10, limit("max-messages", 100));
    }

    private int size(Integer value) {
        return MessageSupport.bound(value, 50, limit("max-items", 100));
    }

    private int waitMs(Integer value) {
        return MessageSupport.bound(
                value,
                5000,
                MessageSupport.setting(env, "omni.kafka.timeouts", "max-wait-ms", 15000));
    }

    @McpTool(
            name = "kafka_list_topics",
            description = "List existing topics; internal topics hidden by default.",
            annotations =
                    @McpTool.McpAnnotations(
                            readOnlyHint = true,
                            destructiveHint = false,
                            idempotentHint = true))
    public CallToolResult kafka_list_topics(
            @McpToolParam(description = "nameContains", required = false) String nameContains,
            @McpToolParam(description = "includeInternal", required = false)
                    Boolean includeInternal,
            @McpToolParam(description = "limit", required = false) Integer limit) {
        return MessageSupport.call(
                () -> runtime.topics(nameContains, includeInternal, size(limit)));
    }

    @McpTool(
            name = "kafka_inspect_topic",
            description =
                    "Inspect partitions, leaders, replicas, ISR and retained offset boundaries.",
            annotations =
                    @McpTool.McpAnnotations(
                            readOnlyHint = true,
                            destructiveHint = false,
                            idempotentHint = true))
    public CallToolResult kafka_inspect_topic(
            @McpToolParam(description = "topic", required = true) String topic,
            @McpToolParam(description = "limit", required = false) Integer limit) {
        return MessageSupport.call(() -> runtime.inspectTopic(topic, size(limit)));
    }

    @McpTool(
            name = "kafka_list_consumer_groups",
            description = "List consumer groups with bounded output.",
            annotations =
                    @McpTool.McpAnnotations(
                            readOnlyHint = true,
                            destructiveHint = false,
                            idempotentHint = true))
    public CallToolResult kafka_list_consumer_groups(
            @McpToolParam(description = "nameContains", required = false) String nameContains,
            @McpToolParam(description = "limit", required = false) Integer limit) {
        return MessageSupport.call(() -> runtime.groups(nameContains, size(limit)));
    }

    @McpTool(
            name = "kafka_inspect_consumer_group",
            description =
                    "Inspect assignments and offset lag; lag is not proof of successful business"
                            + " processing.",
            annotations =
                    @McpTool.McpAnnotations(
                            readOnlyHint = true,
                            destructiveHint = false,
                            idempotentHint = true))
    public CallToolResult kafka_inspect_consumer_group(
            @McpToolParam(description = "groupId", required = true) String groupId,
            @McpToolParam(description = "topic", required = false) String topic,
            @McpToolParam(description = "limit", required = false) Integer limit) {
        return MessageSupport.call(() -> runtime.inspectGroup(groupId, topic, size(limit)));
    }

    @McpTool(
            name = "kafka_read_records",
            description =
                    "Observe records without joining a group or committing. start: earliest, latest"
                            + " (recent bounded window), offset or timestamp.",
            annotations =
                    @McpTool.McpAnnotations(
                            readOnlyHint = true,
                            destructiveHint = false,
                            idempotentHint = true))
    public CallToolResult kafka_read_records(
            @McpToolParam(description = "topic", required = true) String topic,
            @McpToolParam(description = "partition", required = true) Integer partition,
            @McpToolParam(description = "start", required = false) String start,
            @McpToolParam(description = "offset", required = false) Long offset,
            @McpToolParam(description = "timestampMs", required = false) Long timestampMs,
            @McpToolParam(description = "count", required = false) Integer count,
            @McpToolParam(description = "maxWaitMs", required = false) Integer maxWaitMs) {
        return MessageSupport.call(
                () ->
                        runtime.read(
                                topic,
                                partition,
                                start,
                                offset,
                                timestampMs,
                                count(count),
                                waitMs(maxWaitMs)));
    }

    @McpTool(
            name = "kafka_publish_record",
            description =
                    "Publish to an existing topic and wait for broker acknowledgement. Unknown"
                            + " outcome must not be blindly retried.",
            annotations =
                    @McpTool.McpAnnotations(
                            readOnlyHint = false,
                            destructiveHint = true,
                            idempotentHint = false))
    public CallToolResult kafka_publish_record(
            @McpToolParam(description = "topic", required = true) String topic,
            @McpToolParam(description = "payload", required = true) String payload,
            @McpToolParam(description = "key", required = false) String key,
            @McpToolParam(description = "partition", required = false) Integer partition,
            @McpToolParam(description = "encoding", required = false) String encoding,
            @McpToolParam(description = "headers", required = false) Map<String, String> headers) {
        return MessageSupport.call(
                () -> runtime.publish(topic, payload, key, partition, encoding, headers));
    }

    @McpTool(
            name = "kafka_consume_records",
            description =
                    "Join the explicitly named group for bounded consumption; may rebalance"
                        + " business consumers. commit defaults false; true commits only returned"
                        + " records. No business processing is performed.",
            annotations =
                    @McpTool.McpAnnotations(
                            readOnlyHint = false,
                            destructiveHint = true,
                            idempotentHint = false))
    public CallToolResult kafka_consume_records(
            @McpToolParam(description = "topic", required = true) String topic,
            @McpToolParam(description = "groupId", required = true) String groupId,
            @McpToolParam(description = "count", required = false) Integer count,
            @McpToolParam(description = "maxWaitMs", required = false) Integer maxWaitMs,
            @McpToolParam(description = "commit", required = false) Boolean commit,
            @McpToolParam(description = "offsetReset", required = false) String offsetReset) {
        return MessageSupport.call(
                () ->
                        runtime.consume(
                                topic,
                                groupId,
                                count(count),
                                waitMs(maxWaitMs),
                                commit,
                                offsetReset));
    }
}
