package com.paymentplatform.orchestration.ledger;

import com.paymentplatform.orchestration.ledger.application.service.PaymentCreatedLedgerEvent;
import com.paymentplatform.orchestration.ledger.application.service.PostLedgerEntriesService;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;

@SpringBootTest(properties = {
        "management.tracing.enabled=true",
        "management.tracing.sampling.probability=1.0",
        "app.payment-events.topic=payment.trace.events",
        "spring.kafka.consumer.group-id=ledger-trace-test"
})
@Testcontainers(disabledWithoutDocker = true)
class LedgerTracePropagationIntegrationTest {

    private static final String TOPIC = "payment.trace.events";

    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"));

    static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("apache/kafka:3.8.0"));

    static {
        POSTGRES.start();
        KAFKA.start();
    }

    @DynamicPropertySource
    static void registerContainerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    @Autowired
    private Tracer tracer;

    @MockitoBean
    private PostLedgerEntriesService postLedgerEntriesService;

    @Test
    void ledgerConsumerContinuesTraceFromKafkaTraceparentHeader() throws Exception {
        createTopic();

        CountDownLatch consumed = new CountDownLatch(1);
        AtomicReference<String> observedTraceId = new AtomicReference<>();
        doAnswer(invocation -> {
            Span currentSpan = tracer.currentSpan();
            if (currentSpan != null) {
                observedTraceId.set(currentSpan.context().traceId());
            }
            consumed.countDown();
            return null;
        }).when(postLedgerEntriesService).postPaymentCreated(any(PaymentCreatedLedgerEvent.class));

        String traceId = "4bf92f3577b34da6a3ce929d0e0e4736";
        String traceparent = "00-" + traceId + "-00f067aa0ba902b7-01";
        String eventId = UUID.randomUUID().toString();
        ProducerRecord<String, String> record = new ProducerRecord<>(
                TOPIC,
                eventId,
                envelope(eventId, UUID.randomUUID().toString(), "customer-1", "120.50", "EUR")
        );
        record.headers().add("traceparent", traceparent.getBytes(StandardCharsets.UTF_8));

        try (KafkaProducer<String, String> producer = newProducer()) {
            producer.send(record).get();
            producer.flush();
        }

        assertThat(consumed.await(30, TimeUnit.SECONDS)).isTrue();
        assertThat(observedTraceId).hasValue(traceId);
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
