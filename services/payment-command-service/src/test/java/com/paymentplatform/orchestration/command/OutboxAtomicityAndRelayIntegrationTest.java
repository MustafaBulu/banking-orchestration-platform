package com.paymentplatform.orchestration.command;

import com.paymentplatform.orchestration.command.application.command.CreatePaymentCommand;
import com.paymentplatform.orchestration.command.application.port.in.CreatePaymentUseCase;
import com.paymentplatform.orchestration.command.application.port.out.FraudCheckPort;
import com.paymentplatform.orchestration.command.application.port.out.LimitCheckPort;
import com.paymentplatform.orchestration.command.infrastructure.outbox.OutboxRelayWorker;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@TestPropertySource(properties = "app.outbox.relay.poll-delay-ms=3600000")
class OutboxAtomicityAndRelayIntegrationTest extends AbstractIntegrationTest {

    private static final String TOPIC = "payment.domain.events";

    @MockitoBean
    private FraudCheckPort fraudCheckPort;

    @MockitoBean
    private LimitCheckPort limitCheckPort;

    @Autowired
    private CreatePaymentUseCase createPaymentUseCase;

    @Autowired
    private OutboxRelayWorker outboxRelayWorker;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    void writesOutboxInSameTransactionThenRelaysToKafka() throws Exception {
        when(fraudCheckPort.evaluate(anyString(), anyString(), any()))
                .thenReturn(new FraudCheckPort.FraudCheckResult(true, "OK", 0));
        when(limitCheckPort.reserve(anyString(), anyString(), any()))
                .thenReturn(new LimitCheckPort.LimitCheckResult(true, "OK", "reservation-1"));
        createTopic();

        String paymentId = UUID.randomUUID().toString();
        createPaymentUseCase.handle(new CreatePaymentCommand(
                paymentId, "customer-1", new BigDecimal("120.50"), "EUR"));

        String eventId = jdbcTemplate.queryForObject(
                "SELECT event_id FROM event_store WHERE stream_id = ?", String.class, paymentId);
        assertThat(eventId).isNotNull();
        Integer pendingOutbox = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM outbox WHERE event_id = ? AND status = 'NEW'", Integer.class, eventId);
        assertThat(pendingOutbox).isEqualTo(1);

        outboxRelayWorker.relay();

        String status = jdbcTemplate.queryForObject(
                "SELECT status FROM outbox WHERE event_id = ?", String.class, eventId);
        assertThat(status).isEqualTo("PUBLISHED");

        ConsumerRecord<String, String> record = consumeByKey(eventId);
        assertThat(record).isNotNull();
        assertThat(record.key()).isEqualTo(eventId);
        assertThat(record.value())
                .contains("customer-1")
                .contains("EUR")
                .contains("PaymentCreated");
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

    private ConsumerRecord<String, String> consumeByKey(String eventId) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(TOPIC));
            long deadline = System.currentTimeMillis() + 15000;
            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    if (eventId.equals(record.key())) {
                        return record;
                    }
                }
            }
        }
        return null;
    }
}
