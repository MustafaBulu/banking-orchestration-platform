package com.paymentplatform.orchestration.query;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class PaymentProjectionConsumerIdempotencyIntegrationTest extends AbstractIntegrationTest {

    private static final String TOPIC = "payment.domain.events";

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    void deliversTwiceButProjectsPaymentOnce() throws Exception {
        createTopic();

        String eventId = UUID.randomUUID().toString();
        String paymentId = UUID.randomUUID().toString();
        String duplicate = envelope(eventId, paymentId, "customer-1", "120.50", "EUR");

        String sentinelId = UUID.randomUUID().toString();
        String sentinelPaymentId = UUID.randomUUID().toString();
        String sentinel = envelope(sentinelId, sentinelPaymentId, "customer-2", "10.00", "EUR");

        try (KafkaProducer<String, String> producer = newProducer()) {
            producer.send(new ProducerRecord<>(TOPIC, eventId, duplicate)).get();
            producer.send(new ProducerRecord<>(TOPIC, eventId, duplicate)).get();
            producer.send(new ProducerRecord<>(TOPIC, sentinelId, sentinel)).get();
            producer.flush();
        }

        awaitProjected(sentinelPaymentId);

        Integer projections = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM payment_overview WHERE payment_id = ?", Integer.class, paymentId);
        assertThat(projections).isOne();

        Integer processed = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM processed_event WHERE event_id = ?", Integer.class, eventId);
        assertThat(processed).isOne();
    }

    private void awaitProjected(String paymentId) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 30000;
        while (System.currentTimeMillis() < deadline) {
            Integer count = jdbcTemplate.queryForObject(
                    "SELECT COUNT(*) FROM payment_overview WHERE payment_id = ?", Integer.class, paymentId);
            if (count != null && count == 1) {
                return;
            }
            Thread.sleep(500);
        }
        throw new AssertionError("Sentinel payment was not projected within timeout");
    }

    private void createTopic() throws Exception {
        try (Admin admin = Admin.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()))) {
            try {
                admin.createTopics(List.of(new NewTopic(TOPIC, 1, (short) 1))).all().get();
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
                + "\"eventVersion\":1,"
                + "\"occurredAt\":\"2026-07-08T10:15:30Z\","
                + "\"data\":{\"customerId\":\"%s\",\"amount\":%s,\"currency\":\"%s\"}}")
                .formatted(eventId, paymentId, customerId, amount, currency);
    }
}
