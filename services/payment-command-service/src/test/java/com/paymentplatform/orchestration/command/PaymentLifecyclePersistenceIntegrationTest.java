package com.paymentplatform.orchestration.command;

import com.paymentplatform.orchestration.command.application.port.out.PaymentEventStorePort;
import com.paymentplatform.orchestration.command.application.service.PaymentPersister;
import com.paymentplatform.orchestration.command.domain.event.PaymentEvent;
import com.paymentplatform.orchestration.command.domain.model.Money;
import com.paymentplatform.orchestration.command.domain.model.PaymentAggregate;
import com.paymentplatform.orchestration.command.domain.model.PaymentStatus;
import com.paymentplatform.orchestration.events.schema.EventSchemaRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import java.math.BigDecimal;
import java.util.Currency;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class PaymentLifecyclePersistenceIntegrationTest extends AbstractIntegrationTest {

    @Autowired
    private PaymentPersister paymentPersister;

    @Autowired
    private PaymentEventStorePort paymentEventStorePort;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private EventSchemaRegistry eventSchemaRegistry;

    @Test
    void persistsLifecycleEventsInOneStreamAndRehydratesThem() {
        String paymentId = UUID.randomUUID().toString();
        PaymentAggregate payment = PaymentAggregate.create(
                paymentId,
                "customer-1",
                new Money(new BigDecimal("120.50"), Currency.getInstance("EUR"))
        );

        paymentPersister.persist(payment.uncommittedEvents().getLast());
        payment.authorize();
        paymentPersister.persist(payment.uncommittedEvents().getLast());
        payment.capture();
        paymentPersister.persist(payment.uncommittedEvents().getLast());

        List<String> eventTypes = jdbcTemplate.queryForList(
                "SELECT event_type FROM event_store WHERE stream_id = ? ORDER BY stream_version",
                String.class,
                paymentId
        );
        List<Long> streamVersions = jdbcTemplate.queryForList(
                "SELECT stream_version FROM event_store WHERE stream_id = ? ORDER BY stream_version",
                Long.class,
                paymentId
        );

        assertThat(eventTypes)
                .containsExactly("PaymentCreated", "PaymentAuthorized", "PaymentCaptured");
        assertThat(streamVersions).containsExactly(1L, 2L, 3L);

        List<String> outboxPayloads = jdbcTemplate.queryForList(
                "SELECT payload::text FROM outbox WHERE aggregate_id = ? ORDER BY created_at",
                String.class,
                paymentId
        );
        assertThat(outboxPayloads).hasSize(3);
        outboxPayloads.forEach(eventSchemaRegistry::validate);

        List<PaymentEvent> eventStream = paymentEventStorePort.load(paymentId);
        PaymentAggregate rehydrated = PaymentAggregate.rehydrate(eventStream);

        assertThat(rehydrated.status()).isEqualTo(PaymentStatus.CAPTURED);
        assertThat(rehydrated.uncommittedEvents()).isEmpty();
    }
}
