package xyz.fz.omni.app;

import static org.junit.jupiter.api.Assertions.*;

import io.modelcontextprotocol.client.McpSyncClient;
import io.modelcontextprotocol.spec.McpSchema.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.boot.builder.SpringApplicationBuilder;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/** Opt-in live tests: only UUID-prefixed test topics/groups may be mutated or deleted. */
@EnabledIfEnvironmentVariable(named = "OMNI_KAFKA_ACCEPTANCE", matches = "true")
class KafkaBrokerTest {
    private final String bootstrap = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
    private final String prefix = "omni-mcp-acceptance-" + UUID.randomUUID();
    private final String main = prefix + "-main", stream = prefix + "-stream";
    private final String bounded = prefix + "-bounded", empty = prefix + "-empty", retained = prefix + "-retained";
    private final List<String> topics = List.of(main, stream, bounded, empty, retained);
    private final List<String> groups = new ArrayList<>();
    private static final JsonMapper JSON = new JsonMapper();

    Properties config() {
        var p = new Properties();
        p.put("bootstrap.servers", bootstrap);
        p.put("security.protocol", "PLAINTEXT");
        p.put("default.api.timeout.ms", "10000");
        p.put("request.timeout.ms", "5000");
        p.put("client.id", "omni-kafka-acceptance-fixture");
        return p;
    }

    String group(String suffix) {
        String name = prefix + "-" + suffix;
        groups.add(name);
        return name;
    }

    KafkaConsumer<byte[], byte[]> consumer(String group) {
        var p = config();
        p.put("key.deserializer", ByteArrayDeserializer.class.getName());
        p.put("value.deserializer", ByteArrayDeserializer.class.getName());
        p.put("enable.auto.commit", "false");
        p.put("auto.offset.reset", "earliest");
        p.put("group.protocol", "classic");
        if (group != null) p.put("group.id", group);
        return new KafkaConsumer<>(p);
    }

    JsonNode call(McpSyncClient client, String name, Map<String, Object> args) {
        return RuntimeConnectionsTest.call(client, name, args);
    }

    void error(McpSyncClient client, String name, Map<String, Object> args, String code) {
        var result = client.callTool(new CallToolRequest(name, args));
        assertTrue(Boolean.TRUE.equals(result.isError()), result.toString());
        assertEquals(code, JSON.readTree(((TextContent) result.content().getFirst()).text()).path("code").asString());
    }

    void passed(String text) { System.out.println("KAFKA_ACCEPTANCE PASS " + text); }

    @Test
    void realBrokerThroughMcp() throws Exception {
        try (var admin = Admin.create(config())) {
            var cluster = admin.describeCluster();
            System.out.println("KAFKA_ACCEPTANCE cluster=" + cluster.clusterId().get(10, TimeUnit.SECONDS)
                    + " nodes=" + cluster.nodes().get(10, TimeUnit.SECONDS) + " prefix=" + prefix);
            try {
                admin.createTopics(topics.stream().map(t -> new NewTopic(t, t.equals(main) ? 2 : 1, (short) 1)).toList())
                        .all().get(15, TimeUnit.SECONDS);
                long timestamp = System.currentTimeMillis() - 60000;
                seed(timestamp);
                try (var context = new SpringApplicationBuilder(OmniConnectApplication.class).run(
                        "--server.address=127.0.0.1", "--server.port=0", "--omni.auth.enabled=false",
                        "--spring.config.location=file:../application.yml", "--omni.kafka.timeouts.operation-ms=5000",
                        "--omni.kafka.limits.preview-bytes=64", "--omni.kafka.limits.max-response-bytes=2000");
                        var first = RuntimeConnectionsTest.client(context);
                        var second = RuntimeConnectionsTest.client(context)) {
                    assertEquals(8, RuntimeConnectionsTest.names(first).size());
                    call(first, "kafka_connect", Map.of("bootstrapServers", bootstrap));
                    assertEquals(15, RuntimeConnectionsTest.names(second).size());
                    metadataAndReads(first, admin, timestamp);
                    consumption(first, admin);
                    activeGroupAndRebalance(first, admin);
                    retention(first, admin);
                    error(first, "kafka_connect", Map.of("bootstrapServers", "127.0.0.1:1"), "TIMEOUT");
                    call(second, "kafka_inspect_topic", Map.of("topic", main));
                    passed("failed candidate retains target; cross-session activation");
                }
                try (var context = new SpringApplicationBuilder(OmniConnectApplication.class).run(
                        "--server.address=127.0.0.1", "--server.port=0", "--omni.auth.enabled=false",
                        "--spring.config.location=file:../application.yml", "--omni.kafka.enabled=true",
                        "--omni.kafka.connection.bootstrap-servers=" + bootstrap);
                        var client = RuntimeConnectionsTest.client(context)) {
                    assertEquals(15, RuntimeConnectionsTest.names(client).size());
                    call(client, "kafka_inspect_topic", Map.of("topic", main));
                    passed("YAML startup activation");
                }
                stdio(admin);
            } finally {
                cleanup(admin);
            }
        }
    }

    void seed(long timestamp) throws Exception {
        var p = config();
        p.put("key.serializer", ByteArraySerializer.class.getName());
        p.put("value.serializer", ByteArraySerializer.class.getName());
        p.put("compression.type", "gzip");
        p.put("acks", "all");
        try (var producer = new KafkaProducer<byte[], byte[]>(p)) {
            for (String topic : List.of(main, stream, bounded, retained)) {
                for (int i = 0; i < 3; i++) {
                    var headers = new ArrayList<org.apache.kafka.common.header.Header>();
                    headers.add(new RecordHeader("trace", "first".getBytes(StandardCharsets.UTF_8)));
                    headers.add(new RecordHeader("trace", new byte[] {0, (byte) 255}));
                    if (topic.equals(bounded) && i == 1)
                        for (int h = 0; h < 20; h++) headers.add(new RecordHeader("large" + h, new byte[64]));
                    producer.send(new ProducerRecord<>(topic, 0, timestamp + i * 1000,
                            ("key-" + i).getBytes(StandardCharsets.UTF_8),
                            ("record-" + i).getBytes(StandardCharsets.UTF_8), headers)).get(10, TimeUnit.SECONDS);
                }
            }
        }
    }

    void metadataAndReads(McpSyncClient client, Admin admin, long timestamp) throws Exception {
        var listed = call(client, "kafka_list_topics", Map.of("nameContains", prefix, "limit", 2));
        assertEquals(2, listed.path("topics").size());
        assertTrue(listed.path("truncated").asBoolean());
        var partitions = call(client, "kafka_inspect_topic", Map.of("topic", main)).path("partitions");
        assertEquals(2, partitions.size());
        assertEquals(3, partitions.get(0).path("endOffset").asLong());
        assertEquals(1, partitions.get(0).path("isr").size());
        var published = call(client, "kafka_publish_record", Map.of("topic", main, "partition", 1,
                "payload", "mcp-published", "key", "mcp-key", "headers", Map.of("traceId", "acceptance")));
        assertEquals("CONFIRMED", published.path("outcome").asString());
        var read = call(client, "kafka_read_records", Map.of("topic", main, "partition", 1, "start", "offset", "offset", published.path("offset").asLong(), "count", 1));
        var record = read.path("records").get(0);
        assertEquals("mcp-published", record.path("payload").asString());
        assertEquals("mcp-key", record.path("key").path("payload").asString());
        assertEquals("acceptance", record.path("headers").get(0).path("value").path("payload").asString());
        assertEquals(published.path("timestamp").asLong(), record.path("timestamp").asLong());
        var earliest = call(client, "kafka_read_records", Map.of("topic", main, "partition", 0, "start", "earliest", "count", 1));
        assertEquals(0, earliest.path("records").get(0).path("offset").asLong());
        assertEquals(2, earliest.path("records").get(0).path("headers").size());
        assertEquals("base64", earliest.path("records").get(0).path("headers").get(1).path("value").path("encoding").asString());
        var latest = call(client, "kafka_read_records", Map.of("topic", main, "partition", 0, "start", "latest", "count", 1));
        assertEquals(2, latest.path("records").get(0).path("offset").asLong());
        var timed = call(client, "kafka_read_records", Map.of("topic", main, "partition", 0, "start", "timestamp", "timestampMs", timestamp + 1000, "count", 1));
        assertEquals(1, timed.path("records").get(0).path("offset").asLong());
        assertEquals(0, call(client, "kafka_read_records", Map.of("topic", empty, "partition", 0, "start", "earliest")).path("records").size());
        error(client, "kafka_read_records", Map.of("topic", main, "partition", 0, "start", "offset", "offset", 9999), "OFFSET_OUT_OF_RANGE");
        error(client, "kafka_publish_record", Map.of("topic", prefix + "-missing", "payload", "x"), "RESOURCE_NOT_FOUND");
        assertFalse(admin.listTopics().names().get(10, TimeUnit.SECONDS).contains(prefix + "-missing"));
        passed("topic metadata/filter/limit, gzip and duplicate/binary headers, publish/readback, four read positions, empty topic, no auto-creation");
    }

    void consumption(McpSyncClient client, Admin admin) throws Exception {
        String group = group("commit");
        var tp = new TopicPartition(stream, 0);
        var args = new HashMap<String, Object>(Map.of("topic", stream, "groupId", group, "count", 1, "maxWaitMs", 15000));
        var first = call(client, "kafka_consume_records", args);
        assertEquals(1, first.path("records").size());
        assertEquals("NOT_REQUESTED", first.path("commitStatus").asString());
        assertTrue(admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS).isEmpty());
        args.put("commit", true);
        var committed = call(client, "kafka_consume_records", args);
        assertEquals(0, committed.path("records").get(0).path("offset").asLong());
        assertEquals("COMMITTED", committed.path("commitStatus").asString());
        assertEquals(1, admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS).get(tp).offset());
        call(client, "kafka_read_records", Map.of("topic", stream, "partition", 0, "start", "earliest", "count", 1));
        assertEquals(1, admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS).get(tp).offset());
        var inspected = call(client, "kafka_inspect_consumer_group", Map.of("groupId", group));
        assertEquals("VALID", inspected.path("partitions").get(0).path("offsetStatus").asString());
        assertEquals(2, inspected.path("partitions").get(0).path("lag").asLong());
        assertEquals(0, inspected.path("members").size());
        assertTrue(call(client, "kafka_list_consumer_groups", Map.of("nameContains", group)).path("groups").size() >= 1);
        assertEquals(1, call(client, "kafka_consume_records", args).path("records").get(0).path("offset").asLong());

        String boundedGroup = group("bounded");
        var limited = call(client, "kafka_consume_records", Map.of("topic", bounded, "groupId", boundedGroup, "count", 3, "maxWaitMs", 15000, "commit", true));
        assertEquals(1, limited.path("records").size());
        assertEquals(1, admin.listConsumerGroupOffsets(boundedGroup).partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS).get(new TopicPartition(bounded, 0)).offset());
        var limitedRead = call(client, "kafka_read_records", Map.of("topic", bounded, "partition", 0, "start", "earliest", "count", 3));
        assertEquals(1, limitedRead.path("nextOffset").asLong());
        assertTrue(limitedRead.path("truncated").asBoolean());
        try (var observer = consumer(null)) {
            var partition = new TopicPartition(bounded, 0);
            observer.assign(List.of(partition)); observer.seek(partition, 1);
            var unread = observer.poll(Duration.ofSeconds(5));
            assertFalse(unread.isEmpty());
            assertEquals(1, unread.iterator().next().offset());
        }
        passed("default no commit, exact explicit commits, observation keeps offsets, empty group lag, bounded response does not skip unreturned records");
    }

    void activeGroupAndRebalance(McpSyncClient client, Admin admin) throws Exception {
        String group = group("active");
        var running = new AtomicBoolean(true);
        var assigned = new CountDownLatch(1);
        try (var worker = Executors.newSingleThreadExecutor()) {
            var task = worker.submit(() -> {
                try (var fixture = consumer(group)) {
                    fixture.subscribe(List.of(main), new ConsumerRebalanceListener() {
                        public void onPartitionsRevoked(Collection<TopicPartition> ps) {}
                        public void onPartitionsAssigned(Collection<TopicPartition> ps) { if (!ps.isEmpty()) assigned.countDown(); }
                    });
                    while (running.get()) fixture.poll(Duration.ofMillis(100));
                }
            });
            try {
                assertTrue(assigned.await(15, TimeUnit.SECONDS));
                var description = call(client, "kafka_inspect_consumer_group", Map.of("groupId", group));
                assertEquals(1, description.path("members").size());
                assertEquals(2, description.path("partitions").size());
                for (var row : description.path("partitions")) {
                    assertEquals("NOT_COMMITTED", row.path("offsetStatus").asString());
                    assertTrue(row.path("lag").isNull());
                }
                var joined = call(client, "kafka_consume_records", Map.of("topic", main, "groupId", group, "count", 1, "maxWaitMs", 15000));
                assertEquals(1, joined.path("records").size(), joined.toString());
                assertTrue(admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS).isEmpty());
                passed("active member/assignment, missing commit has null lag, real rebalance with another consumer");
            } finally {
                running.set(false);
                task.get(10, TimeUnit.SECONDS);
            }
        }
    }

    void retention(McpSyncClient client, Admin admin) throws Exception {
        var tp = new TopicPartition(retained, 0);
        admin.deleteRecords(Map.of(tp, RecordsToDelete.beforeOffset(2))).all().get(10, TimeUnit.SECONDS);
        String group = group("out-of-range");
        admin.alterConsumerGroupOffsets(group, Map.of(tp, new OffsetAndMetadata(1))).all().get(10, TimeUnit.SECONDS);
        var row = call(client, "kafka_inspect_consumer_group", Map.of("groupId", group)).path("partitions").get(0);
        assertEquals("OUT_OF_RANGE", row.path("offsetStatus").asString());
        assertTrue(row.path("lag").isNull());
        assertEquals(2, row.path("beginningOffset").asLong());
        error(client, "kafka_read_records", Map.of("topic", retained, "partition", 0, "start", "offset", "offset", 0), "OFFSET_OUT_OF_RANGE");
        passed("retention boundary and invalid committed offset never fabricated as zero lag");
    }

    void stdio(Admin admin) throws Exception {
        String group = group("stdio");
        var log = Path.of("..", "target", "kafka-stdio.log");
        var process = new ProcessBuilder("python", "../tests/test_kafka_stdio.py")
                .redirectErrorStream(true).redirectOutput(log.toFile());
        process.environment().put("KAFKA_ACCEPTANCE_TOPIC", stream);
        process.environment().put("KAFKA_ACCEPTANCE_GROUP", group);
        process.environment().put("KAFKA_BOOTSTRAP_SERVERS", bootstrap);
        var child = process.start();
        try {
            assertTrue(child.waitFor(60, TimeUnit.SECONDS), "stdio acceptance timed out");
            assertEquals(0, child.exitValue());
        } finally {
            if (child.isAlive()) child.destroyForcibly();
            System.out.println(java.nio.file.Files.readString(log));
        }
        assertEquals(1, admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS).get(new TopicPartition(stream, 0)).offset());
        passed("stdio commit independently verified");
    }

    void cleanup(Admin admin) throws Exception {
        // Exact run-owned names only, including a partially completed createTopics call.
        var existingGroups = admin.listConsumerGroups().all().get(10, TimeUnit.SECONDS).stream()
                .map(ConsumerGroupListing::groupId).filter(groups::contains).toList();
        if (!existingGroups.isEmpty()) admin.deleteConsumerGroups(existingGroups).all().get(15, TimeUnit.SECONDS);
        var existingTopics = admin.listTopics().names().get(10, TimeUnit.SECONDS).stream().filter(topics::contains).toList();
        if (!existingTopics.isEmpty()) admin.deleteTopics(existingTopics).all().get(15, TimeUnit.SECONDS);
        assertTrue(admin.listTopics().names().get(10, TimeUnit.SECONDS).stream().noneMatch(topics::contains));
        assertTrue(admin.listConsumerGroups().all().get(10, TimeUnit.SECONDS).stream().noneMatch(g -> groups.contains(g.groupId())));
        passed("all run-owned topics and consumer groups deleted");
    }
}
