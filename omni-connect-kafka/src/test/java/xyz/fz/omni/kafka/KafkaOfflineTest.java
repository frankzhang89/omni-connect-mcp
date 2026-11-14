package xyz.fz.omni.kafka;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import io.modelcontextprotocol.server.McpSyncServer;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.*;
import org.junit.jupiter.api.*;
import org.mockito.ArgumentCaptor;
import org.springframework.mock.env.MockEnvironment;

import xyz.fz.omni.core.*;

import java.util.*;

class KafkaOfflineTest {
    KafkaClients clients;
    Admin admin;
    Consumer<byte[], byte[]> consumer;
    Properties captured;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setup() throws Exception {
        clients = mock(KafkaClients.class);
        admin = mock(Admin.class);
        consumer = mock(Consumer.class);
        when(clients.admin(any())).thenReturn(admin);
        when(clients.consumer(any()))
                .thenAnswer(
                        i -> {
                            captured = i.getArgument(0);
                            return consumer;
                        });
        var describe = mock(DescribeClusterResult.class);
        when(admin.describeCluster()).thenReturn(describe);
        when(describe.clusterId()).thenReturn(KafkaFuture.completedFuture("debug"));
        var topic = mock(DescribeTopicsResult.class);
        when(admin.describeTopics(anyCollection())).thenReturn(topic);
        var description =
                new TopicDescription(
                        "t",
                        false,
                        List.of(
                                new TopicPartitionInfo(
                                        0, new Node(0, "localhost", 1), List.of(), List.of())));
        when(topic.allTopicNames())
                .thenReturn(KafkaFuture.completedFuture(Map.of("t", description)));
    }

    KafkaRuntime runtime(int max) {
        return new KafkaRuntime(new Properties(), clients, 200, 4096, 65536, max);
    }

    @Test
    void saslConfigurationIsTypedAndNeverInherited() {
        var configs = new ArrayList<Properties>();
        when(clients.admin(any()))
                .thenAnswer(
                        i -> {
                            Properties copy = new Properties();
                            copy.putAll((Properties) i.getArgument(0));
                            configs.add(copy);
                            return admin;
                        });
        try (var registry = new ConnectorRegistry()) {
            registry.attach(mock(McpSyncServer.class), List.of("kafka"));
            var module = new KafkaConnectorModule(registry, new MockEnvironment(), clients);
            assertFalse(
                    module.connect("unused:9092", "SASL_PLAINTEXT", "debug", "a\"b\\c").isError());
            assertEquals("PLAIN", configs.get(0).get("sasl.mechanism"));
            assertTrue(configs.get(0).getProperty("sasl.jaas.config").contains("a\\\"b\\\\c"));
            assertFalse(module.connect("unused:9092", null, null, null).isError());
            assertFalse(configs.get(1).containsKey("sasl.jaas.config"));
            assertFalse(registry.status().toString().contains("unused"));
        }
    }

    @Test
    void commitFailureRetainsRecordsAndUncommittedConsumptionDoesNotCommit() throws Exception {
        var p = new TopicPartition("t", 0);
        when(consumer.assignment()).thenReturn(Set.of(p));
        when(consumer.poll(any(java.time.Duration.class)))
                .thenReturn(
                        new ConsumerRecords<>(
                                Map.of(
                                        p,
                                        List.of(
                                                new ConsumerRecord<>(
                                                        "t", 0, 10, null, new byte[] {1})))));
        doThrow(new org.apache.kafka.common.errors.TimeoutException("private-marker"))
                .when(consumer)
                .commitSync(anyMap(), any(java.time.Duration.class));
        try (var r = runtime(1048576)) {
            var error =
                    assertThrows(
                            MessageSupport.Failure.class,
                            () -> r.consume("t", "debug", 1, 100, true, null));
            assertEquals("COMMIT_RESULT_UNKNOWN", error.code);
            assertEquals(1, ((List<?>) error.details.get("records")).size());
            clearInvocations(consumer);
            assertEquals(
                    "NOT_REQUESTED",
                    r.consume("t", "debug", 1, 100, false, null).get("commitStatus"));
            verify(consumer, never()).commitSync(anyMap(), any(java.time.Duration.class));
        }
    }

    @Test
    void activationAndSchemaAreOfflineAndFailedReplacementRetainsOld() throws Exception {
        try (var registry = new ConnectorRegistry()) {
            var server = mock(McpSyncServer.class);
            registry.attach(server, List.of("kafka"));
            var module = new KafkaConnectorModule(registry, new MockEnvironment(), clients);
            assertFalse(module.connect("unused:9092", null, null, null).isError());
            assertEquals(
                    7,
                    ConnectorRegistry.specifications(
                                    new KafkaTools(runtime(1048576), new MockEnvironment()))
                            .size());
            var before = registry.status();
            assertTrue(module.connect("unused:9092", "SSL", null, null).isError());
            assertEquals(before, registry.status());
            verify(server, times(7)).addTool(any());
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void commitOnlyReturnedOffsetsNotPollPosition() throws Exception {
        var p = new TopicPartition("t", 0);
        when(consumer.assignment()).thenReturn(Set.of(p));
        var batch =
                new ConsumerRecords<byte[], byte[]>(
                        Map.of(
                                p,
                                List.of(
                                        new ConsumerRecord<>("t", 0, 10, null, new byte[] {1}),
                                        new ConsumerRecord<>("t", 0, 11, null, new byte[] {2}))));
        when(consumer.poll(any(java.time.Duration.class))).thenReturn(batch);
        try (var r = runtime(1048576)) {
            var result = r.consume("t", "business", 1, 100, true, null);
            assertEquals("COMMITTED", result.get("commitStatus"));
            ArgumentCaptor<Map<TopicPartition, OffsetAndMetadata>> offsets =
                    ArgumentCaptor.forClass(Map.class);
            verify(consumer).commitSync(offsets.capture(), any(java.time.Duration.class));
            assertEquals(11, offsets.getValue().get(p).offset());
            assertEquals("false", captured.get("enable.auto.commit"));
            assertEquals("business", captured.get("group.id"));
        }
    }

    @Test
    void observingNeverJoinsGroupOrCommits() throws Exception {
        var p = new TopicPartition("t", 0);
        when(consumer.beginningOffsets(anyCollection(), any())).thenReturn(Map.of(p, 0L));
        when(consumer.endOffsets(anyCollection(), any())).thenReturn(Map.of(p, 1L));
        when(consumer.poll(any(java.time.Duration.class)))
                .thenReturn(
                        new ConsumerRecords<>(
                                Map.of(
                                        p,
                                        List.of(
                                                new ConsumerRecord<>(
                                                        "t", 0, 0, null, new byte[] {1})))));
        try (var r = runtime(1048576)) {
            assertEquals(1L, r.read("t", 0, "earliest", null, null, 1, 100).get("nextOffset"));
            assertFalse(captured.containsKey("group.id"));
            verify(consumer, never()).subscribe(anyCollection());
            verify(consumer, never()).commitSync(anyMap(), any(java.time.Duration.class));
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void producerWaitsForMetadataAndUsesIdempotence() throws Exception {
        Producer<byte[], byte[]> producer = mock(Producer.class);
        when(clients.producer(any()))
                .thenAnswer(
                        i -> {
                            captured = i.getArgument(0);
                            return producer;
                        });
        when(producer.send(any()))
                .thenReturn(
                        java.util.concurrent.CompletableFuture.completedFuture(
                                new RecordMetadata(new TopicPartition("t", 0), 7, 0, 123, 0, 1)));
        try (var r = runtime(1048576)) {
            assertEquals(
                    7L,
                    r.publish("t", "hello", null, null, null, Map.of("traceId", "x"))
                            .get("offset"));
            assertEquals("true", captured.get("enable.idempotence"));
            assertEquals("all", captured.get("acks"));
        }
    }
}
