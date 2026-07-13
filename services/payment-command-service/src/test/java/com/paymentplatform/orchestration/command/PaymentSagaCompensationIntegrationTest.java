package com.paymentplatform.orchestration.command;

import com.paymentplatform.orchestration.command.application.command.CreatePaymentCommand;
import com.paymentplatform.orchestration.command.application.port.out.FraudCheckPort;
import com.paymentplatform.orchestration.command.application.port.out.LimitCheckPort;
import com.paymentplatform.orchestration.command.application.port.out.OutboxPort;
import com.paymentplatform.orchestration.command.application.port.out.PaymentEventStorePort;
import com.paymentplatform.orchestration.command.application.port.out.PaymentSagaRepository;
import com.paymentplatform.orchestration.command.application.saga.PaymentSagaCompensationStatus;
import com.paymentplatform.orchestration.command.application.saga.PaymentSagaOrchestrator;
import com.paymentplatform.orchestration.command.application.saga.PaymentSagaStatus;
import com.paymentplatform.orchestration.command.application.saga.PaymentSagaStep;
import com.paymentplatform.orchestration.command.application.saga.PaymentSagaStepName;
import com.paymentplatform.orchestration.command.application.saga.PaymentSagaStepStatus;
import com.paymentplatform.orchestration.command.application.service.PaymentPersister;
import com.paymentplatform.orchestration.command.domain.event.PaymentEvent;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@TestPropertySource(properties = {
        "spring.task.scheduling.enabled=false",
        "app.payment-saga.max-attempts=1"
})
class PaymentSagaCompensationIntegrationTest extends AbstractIntegrationTest {

    @MockitoBean
    private FraudCheckPort fraudCheckPort;

    @MockitoBean
    private LimitCheckPort limitCheckPort;

    @Autowired
    private PaymentSagaOrchestrator paymentSagaOrchestrator;

    @Autowired
    private PaymentSagaRepository paymentSagaRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    void captureFailureVoidsAuthorizationAndReleasesReservedLimit() {
        when(fraudCheckPort.evaluate(anyString(), anyString(), any()))
                .thenReturn(new FraudCheckPort.FraudCheckResult(true, "OK", 0));
        when(limitCheckPort.reserve(anyString(), anyString(), any()))
                .thenReturn(new LimitCheckPort.LimitCheckResult(true, "OK", "reservation-1"));

        String paymentId = UUID.randomUUID().toString();

        assertThatThrownBy(() -> paymentSagaOrchestrator.start(new CreatePaymentCommand(
                paymentId, "customer-1", new BigDecimal("120.50"), "EUR")))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("capture unavailable");

        assertThat(eventStoreEventTypes(paymentId))
                .containsExactly("PaymentCreated", "PaymentAuthorized", "PaymentVoided");
        assertThat(outboxEventTypes(paymentId))
                .containsExactly("PaymentCreated", "PaymentAuthorized", "PaymentVoided");
        verify(limitCheckPort, times(1)).release("reservation-1");

        String sagaId = "saga-" + paymentId;
        assertThat(paymentSagaRepository.findBySagaId(sagaId))
                .hasValueSatisfying(saga -> {
                    assertThat(saga.status()).isEqualTo(PaymentSagaStatus.COMPENSATED);
                    assertThat(saga.reservationId()).isEqualTo("reservation-1");
                });

        Map<PaymentSagaStepName, PaymentSagaStep> steps = paymentSagaRepository.findSteps(sagaId)
                .stream()
                .collect(Collectors.toMap(PaymentSagaStep::step, step -> step));
        assertThat(steps.get(PaymentSagaStepName.CAPTURE).status()).isEqualTo(PaymentSagaStepStatus.FAILED);
        assertThat(steps.get(PaymentSagaStepName.AUTHORIZE).compensationStatus())
                .isEqualTo(PaymentSagaCompensationStatus.DONE);
        assertThat(steps.get(PaymentSagaStepName.RESERVE_LIMIT).compensationStatus())
                .isEqualTo(PaymentSagaCompensationStatus.DONE);
    }

    private List<String> eventStoreEventTypes(String paymentId) {
        return jdbcTemplate.queryForList(
                "SELECT event_type FROM event_store WHERE stream_id = ? ORDER BY stream_version",
                String.class,
                paymentId
        );
    }

    private List<String> outboxEventTypes(String paymentId) {
        return jdbcTemplate.queryForList(
                "SELECT event_type FROM outbox WHERE aggregate_id = ? ORDER BY created_at",
                String.class,
                paymentId
        );
    }

    @TestConfiguration
    static class CaptureFailureConfiguration {

        @Bean
        @Primary
        PaymentPersister captureFailingPaymentPersister(
                PaymentEventStorePort paymentEventStorePort,
                OutboxPort outboxPort
        ) {
            return new PaymentPersister(paymentEventStorePort, outboxPort) {
                @Override
                @Transactional
                public void persist(PaymentEvent event) {
                    if ("PaymentCaptured".equals(event.eventType())) {
                        throw new RuntimeException("capture unavailable");
                    }
                    super.persist(event);
                }
            };
        }
    }
}
