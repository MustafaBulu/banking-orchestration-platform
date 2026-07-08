package com.paymentplatform.orchestration.ledger;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.TestPropertySource;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@TestPropertySource(properties = {
        "app.kafka.retry.max-attempts=2",
        "app.kafka.retry.backoff-ms=100"
})
class PoisonMessageDeadLetterIntegrationTest extends AbstractIntegrationTest {

    private static final String TOPIC = "payment.domain.events";
    private static final String DLT = "payment.domain.events.DLT";

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    void poisonMessageLandsOnDeadLetterTopicAndConsumerContinues() throws Exception {
        createTopics();

        String poison = "{ this is not valid json";
        String sentinelId = UUID.randomUUID().toString();
        String sentinel = envelope(sentinelId, UUID.randomUUID().toString(), "customer-1", "10.00", "EUR");

        try (KafkaProducer<String, String> producer = newProducer()) {
            producer.send(new ProducerRecord<>(TOPIC, "poison", poison)).get();
            producer.send(new ProducerRecord<>(TOPIC, sentinelId, sentinel)).get();
            producer.flush();
        }

        awaitProcessed(sentinelId);

        ConsumerRecord<String, String> deadLettered = pollDeadLetter();
        assertThat(deadLettered).isNotNull();
        assertThat(deadLettered.value()).isEqualTo(poison);

        Integer poisonEntries = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM ledger_entries WHERE source_event_id = ?", Integer.class, "poison");
        assertThat(poisonEntries).isZero();
    }

    private void awaitProcessed(String sentinelId) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 30000;
        while (System.currentTimeMillis() < deadline) {
            Integer count = jdbcTemplate.queryForObject(
                    "SELECT COUNT(*) FROM ledger_entries WHERE source_event_id = ?", Integer.class, sentinelId);
            if (count != null && count == 2) {
                return;
            }
            Thread.sleep(500);
        }
        throw new AssertionError("Sentinel event was not consumed within timeout");
    }

    private ConsumerRecord<String, String> pollDeadLetter() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "dlt-test-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(DLT));
            long deadline = System.currentTimeMillis() + 20000;
            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    return record;
                }
            }
        }
        return null;
    }

    private void createTopics() throws Exception {
        try (Admin admin = Admin.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()))) {
            try {
                admin.createTopics(List.of(
                        new NewTopic(TOPIC, 1, (short) 1),
                        new NewTopic(DLT, 1, (short) 1))).all().get();
            } catch (Exception alreadyExists) {
            }
        }
    }

    private KafkaProducer<String, String> newProducer() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new KafkaProducer<>(props);
    }

    private String envelope(String eventId, String paymentId, String customerId, String amount, String currency) {
        return ("{\"eventId\":\"%s\",\"aggregateId\":\"%s\",\"eventType\":\"PaymentCreated\","
                + "\"occurredAt\":\"2026-07-08T10:15:30Z\","
                + "\"data\":{\"customerId\":\"%s\",\"amount\":%s,\"currency\":\"%s\"}}")
                .formatted(eventId, paymentId, customerId, amount, currency);
    }
}
