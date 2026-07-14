package com.paymentplatform.orchestration.command;

import com.paymentplatform.orchestration.command.application.command.CreatePaymentCommand;
import com.paymentplatform.orchestration.command.application.port.in.CreatePaymentUseCase;
import com.paymentplatform.orchestration.command.application.port.out.FraudCheckPort;
import com.paymentplatform.orchestration.command.application.port.out.LimitCheckPort;
import com.paymentplatform.orchestration.command.infrastructure.outbox.OutboxRelayWorker;
import com.paymentplatform.orchestration.command.infrastructure.tracing.Traceparent;
import com.paymentplatform.orchestration.command.support.DefaultAcquirerTestConfig;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@SpringBootTest(properties = {
        "management.tracing.enabled=true",
        "management.tracing.sampling.probability=1.0",
        "app.outbox.relay.topic=payment.trace.probe",
        "app.outbox.relay.poll-delay-ms=3600000"
})
@Testcontainers(disabledWithoutDocker = true)
@Import(DefaultAcquirerTestConfig.class)
class KafkaTracePropagationIntegrationTest {

    private static final String TOPIC = "payment.trace.probe";

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
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private Tracer tracer;

    @Autowired
    private CreatePaymentUseCase createPaymentUseCase;

    @Autowired
    private OutboxRelayWorker outboxRelayWorker;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @MockitoBean
    private FraudCheckPort fraudCheckPort;

    @MockitoBean
    private LimitCheckPort limitCheckPort;

    @Test
    void producerInjectsCurrentTraceIntoKafkaHeaders() throws Exception {
        createTopic();

        String key = UUID.randomUUID().toString();
        Span span = tracer.nextSpan().name("trace-probe").start();
        String expectedTraceId;
        try (Tracer.SpanInScope scope = tracer.withSpan(span)) {
            expectedTraceId = span.context().traceId();
            kafkaTemplate.send(TOPIC, key, "{\"probe\":true}").get();
        } finally {
            span.end();
        }

        ConsumerRecord<String, String> record = consumeByKey(key);
        assertThat(record).isNotNull();

        Header traceparent = record.headers().lastHeader("traceparent");
        assertThat(traceparent).as("traceparent header must be propagated through Kafka").isNotNull();

        String headerValue = new String(traceparent.value(), StandardCharsets.UTF_8);
        assertThat(headerValue).matches("00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}");
        assertThat(headerValue).contains(expectedTraceId);
    }

    @Test
    void outboxRelayContinuesPersistedTraceIntoKafkaHeaders() throws Exception {
        createTopic();
        when(fraudCheckPort.evaluate(anyString(), anyString(), any()))
                .thenReturn(new FraudCheckPort.FraudCheckResult(true, "OK", 0));
        when(limitCheckPort.reserve(anyString(), anyString(), any()))
                .thenReturn(new LimitCheckPort.LimitCheckResult(true, "OK", "reservation-1"));

        String paymentId = UUID.randomUUID().toString();
        Span span = tracer.nextSpan().name("payment-request").start();
        String expectedTraceId;
        try (Tracer.SpanInScope scope = tracer.withSpan(span)) {
            expectedTraceId = span.context().traceId();
            createPaymentUseCase.handle(new CreatePaymentCommand(
                    paymentId, "customer-1", new BigDecimal("120.50"), "EUR"));
        } finally {
            span.end();
        }

        String eventId = jdbcTemplate.queryForObject(
                "SELECT event_id FROM event_store WHERE stream_id = ? AND event_type = 'PaymentCaptured'",
                String.class,
                paymentId);
        String persistedTraceparent = jdbcTemplate.queryForObject(
                "SELECT traceparent FROM outbox WHERE event_id = ?", String.class, eventId);

        assertThat(persistedTraceparent)
                .matches("00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}")
                .contains(expectedTraceId);

        outboxRelayWorker.relay();

        ConsumerRecord<String, String> record = consumeByKey(eventId);
        assertThat(record).isNotNull();

        Header traceparent = record.headers().lastHeader(Traceparent.HEADER_NAME);
        assertThat(traceparent).as("outbox traceparent must be propagated through Kafka").isNotNull();

        String headerValue = new String(traceparent.value(), StandardCharsets.UTF_8);
        assertThat(headerValue).matches("00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}");
        assertThat(headerValue).contains(expectedTraceId);
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

    private ConsumerRecord<String, String> consumeByKey(String key) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "trace-probe-consumer-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(TOPIC));
            long deadline = System.currentTimeMillis() + 15000;
            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    if (key.equals(record.key())) {
                        return record;
                    }
                }
            }
        }
        return null;
    }
}
