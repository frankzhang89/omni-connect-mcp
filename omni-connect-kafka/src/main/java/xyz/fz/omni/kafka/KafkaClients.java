package xyz.fz.omni.kafka;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.springframework.stereotype.Component;

import java.util.Properties;

@Component
public class KafkaClients {
    public Admin admin(Properties properties) {
        return Admin.create(properties);
    }

    public Producer<byte[], byte[]> producer(Properties properties) {
        return new KafkaProducer<>(properties);
    }

    public Consumer<byte[], byte[]> consumer(Properties properties) {
        return new KafkaConsumer<>(properties);
    }
}
