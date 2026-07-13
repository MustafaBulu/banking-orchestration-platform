package com.paymentplatform.orchestration.ledger;

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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class LedgerConsumerIdempotencyIntegrationTest extends AbstractIntegrationTest {

    private static final String TOPIC = "payment.domain.events";

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    void deliversLifecycleMoneyEventsTwiceButPostsLedgerEntriesOnce() throws Exception {
        createTopic();

        String paymentId = UUID.randomUUID().toString();
        String createdEventId = UUID.randomUUID().toString();
        Map<String, String> eventIdsByType = new LinkedHashMap<>();
        eventIdsByType.put("PaymentAuthorized", UUID.randomUUID().toString());
        eventIdsByType.put("PaymentCaptured", UUID.randomUUID().toString());
        eventIdsByType.put("PaymentVoided", UUID.randomUUID().toString());
        eventIdsByType.put("PaymentRefunded", UUID.randomUUID().toString());

        String sentinelId = UUID.randomUUID().toString();
        String sentinel = envelope(
                sentinelId, UUID.randomUUID().toString(), "PaymentAuthorized", "customer-2", "10.00", "EUR");

        try (KafkaProducer<String, String> producer = newProducer()) {
            String created = envelope(createdEventId, paymentId, "PaymentCreated", "customer-1", "120.50", "EUR");
            producer.send(new ProducerRecord<>(TOPIC, createdEventId, created)).get();
            producer.send(new ProducerRecord<>(TOPIC, createdEventId, created)).get();
            for (Entry<String, String> entry : eventIdsByType.entrySet()) {
                String event = envelope(entry.getValue(), paymentId, entry.getKey(), "customer-1", "120.50", "EUR");
                producer.send(new ProducerRecord<>(TOPIC, entry.getValue(), event)).get();
                producer.send(new ProducerRecord<>(TOPIC, entry.getValue(), event)).get();
            }
            producer.send(new ProducerRecord<>(TOPIC, sentinelId, sentinel)).get();
            producer.flush();
        }

        awaitProcessed(sentinelId);

        assertThat(entryCount(createdEventId)).isZero();
        assertThat(processedCount(createdEventId)).isEqualTo(1);
        assertPosting(
                eventIdsByType.get("PaymentAuthorized"),
                line("CUSTOMER_AUTH_HOLD", "DEBIT"),
                line("LIMIT_HOLD_LIABILITY", "CREDIT")
        );
        assertPosting(
                eventIdsByType.get("PaymentCaptured"),
                line("SETTLEMENT_ACCOUNT", "DEBIT"),
                line("CUSTOMER_ACCOUNT", "CREDIT"),
                line("LIMIT_HOLD_LIABILITY", "DEBIT"),
                line("CUSTOMER_AUTH_HOLD", "CREDIT")
        );
        assertPosting(
                eventIdsByType.get("PaymentVoided"),
                line("LIMIT_HOLD_LIABILITY", "DEBIT"),
                line("CUSTOMER_AUTH_HOLD", "CREDIT")
        );
        assertPosting(
                eventIdsByType.get("PaymentRefunded"),
                line("CUSTOMER_ACCOUNT", "DEBIT"),
                line("SETTLEMENT_ACCOUNT", "CREDIT")
        );
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

    private void assertPosting(String eventId, ExpectedLine... expectedLines) {
        assertThat(entryCount(eventId)).isEqualTo(expectedLines.length);
        assertThat(processedCount(eventId)).isEqualTo(1);

        List<Map<String, Object>> entries = jdbcTemplate.queryForList(
                "SELECT line_number, account_code, entry_type FROM ledger_entries "
                        + "WHERE source_event_id = ? ORDER BY line_number",
                eventId
        );
        assertThat(entries).hasSize(expectedLines.length);
        for (int index = 0; index < expectedLines.length; index++) {
            assertThat(entries.get(index))
                    .containsEntry("account_code", expectedLines[index].accountCode())
                    .containsEntry("entry_type", expectedLines[index].entryType());
        }
    }

    private int entryCount(String eventId) {
        Integer entries = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM ledger_entries WHERE source_event_id = ?", Integer.class, eventId);
        return entries == null ? 0 : entries;
    }

    private int processedCount(String eventId) {
        Integer processed = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM processed_ledger_event WHERE event_id = ?", Integer.class, eventId);
        return processed == null ? 0 : processed;
    }

    private String envelope(
            String eventId,
            String paymentId,
            String eventType,
            String customerId,
            String amount,
            String currency
    ) {
        return ("{\"eventId\":\"%s\",\"aggregateId\":\"%s\",\"eventType\":\"%s\","
                + "\"eventVersion\":1,"
                + "\"occurredAt\":\"2026-07-08T10:15:30Z\","
                + "\"data\":{\"customerId\":\"%s\",\"amount\":%s,\"currency\":\"%s\"}}")
                .formatted(eventId, paymentId, eventType, customerId, amount, currency);
    }

    private ExpectedLine line(String accountCode, String entryType) {
        return new ExpectedLine(accountCode, entryType);
    }

    private record ExpectedLine(String accountCode, String entryType) {
    }
}
