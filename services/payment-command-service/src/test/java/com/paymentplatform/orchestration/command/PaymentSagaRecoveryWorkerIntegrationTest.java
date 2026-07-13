package com.paymentplatform.orchestration.command;

import com.paymentplatform.orchestration.command.application.port.out.FraudCheckPort;
import com.paymentplatform.orchestration.command.application.port.out.LimitCheckPort;
import com.paymentplatform.orchestration.command.application.port.out.PaymentSagaRepository;
import com.paymentplatform.orchestration.command.application.saga.PaymentSagaCompensationStatus;
import com.paymentplatform.orchestration.command.application.saga.PaymentSagaStatus;
import com.paymentplatform.orchestration.command.application.saga.PaymentSagaStep;
import com.paymentplatform.orchestration.command.application.saga.PaymentSagaStepName;
import com.paymentplatform.orchestration.command.application.saga.PaymentSagaStepStatus;
import com.paymentplatform.orchestration.command.application.saga.SagaRecoveryWorker;
import com.paymentplatform.orchestration.command.application.service.PaymentPersister;
import com.paymentplatform.orchestration.command.domain.model.Money;
import com.paymentplatform.orchestration.command.domain.model.PaymentAggregate;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.math.BigDecimal;
import java.util.Currency;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@TestPropertySource(properties = {
        "spring.task.scheduling.enabled=false",
        "app.payment-saga.max-attempts=1",
        "app.payment-saga.recovery-stuck-after-ms=-1",
        "app.payment-saga.recovery-batch-size=10"
})
class PaymentSagaRecoveryWorkerIntegrationTest extends AbstractIntegrationTest {

    @MockitoBean
    private FraudCheckPort fraudCheckPort;

    @MockitoBean
    private LimitCheckPort limitCheckPort;

    @Autowired
    private PaymentSagaRepository paymentSagaRepository;

    @Autowired
    private PaymentPersister paymentPersister;

    @Autowired
    private SagaRecoveryWorker sagaRecoveryWorker;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    void recoveryWorkerContinuesStartedSagaToCompletion() {
        String paymentId = UUID.randomUUID().toString();
        String sagaId = seedAuthorizedSaga(paymentId, PaymentSagaStatus.STARTED);

        sagaRecoveryWorker.recover();

        assertThat(paymentSagaRepository.findBySagaId(sagaId))
                .hasValueSatisfying(saga -> assertThat(saga.status()).isEqualTo(PaymentSagaStatus.COMPLETED));
        assertThat(eventStoreEventTypes(paymentId))
                .containsExactly("PaymentCreated", "PaymentAuthorized", "PaymentCaptured");
        assertThat(paymentSagaRepository.findSteps(sagaId))
                .extracting(PaymentSagaStep::status)
                .containsExactly(
                        PaymentSagaStepStatus.DONE,
                        PaymentSagaStepStatus.DONE,
                        PaymentSagaStepStatus.DONE,
                        PaymentSagaStepStatus.DONE
                );
        verify(limitCheckPort, never()).release(anyString());
    }

    @Test
    void recoveryWorkerReconcilesPersistedCaptureBeforeRetryExhaustion() {
        String paymentId = UUID.randomUUID().toString();
        String sagaId = seedCapturedSagaWithPendingCapture(paymentId);

        sagaRecoveryWorker.recover();

        assertThat(paymentSagaRepository.findBySagaId(sagaId))
                .hasValueSatisfying(saga -> assertThat(saga.status()).isEqualTo(PaymentSagaStatus.COMPLETED));
        assertThat(eventStoreEventTypes(paymentId))
                .containsExactly("PaymentCreated", "PaymentAuthorized", "PaymentCaptured");
        assertThat(paymentSagaRepository.findSteps(sagaId))
                .filteredOn(step -> step.step() == PaymentSagaStepName.CAPTURE)
                .singleElement()
                .satisfies(step -> {
                    assertThat(step.status()).isEqualTo(PaymentSagaStepStatus.DONE);
                    assertThat(step.attempt()).isEqualTo(1);
                });
        verify(limitCheckPort, never()).release(anyString());
    }

    @Test
    void recoveryWorkerCompensatesOnceForCompensatingSaga() {
        String paymentId = UUID.randomUUID().toString();
        String sagaId = seedAuthorizedSaga(paymentId, PaymentSagaStatus.COMPENSATING);

        sagaRecoveryWorker.recover();
        sagaRecoveryWorker.recover();

        assertThat(paymentSagaRepository.findBySagaId(sagaId))
                .hasValueSatisfying(saga -> assertThat(saga.status()).isEqualTo(PaymentSagaStatus.COMPENSATED));
        assertThat(eventStoreEventTypes(paymentId))
                .containsExactly("PaymentCreated", "PaymentAuthorized", "PaymentVoided");
        verify(limitCheckPort, times(1)).release("reservation-" + paymentId);

        List<PaymentSagaStep> steps = paymentSagaRepository.findSteps(sagaId);
        assertThat(steps)
                .filteredOn(step -> step.step() == PaymentSagaStepName.AUTHORIZE)
                .singleElement()
                .satisfies(step -> assertThat(step.compensationStatus()).isEqualTo(PaymentSagaCompensationStatus.DONE));
        assertThat(steps)
                .filteredOn(step -> step.step() == PaymentSagaStepName.RESERVE_LIMIT)
                .singleElement()
                .satisfies(step -> assertThat(step.compensationStatus()).isEqualTo(PaymentSagaCompensationStatus.DONE));
    }

    private String seedAuthorizedSaga(String paymentId, PaymentSagaStatus status) {
        String sagaId = "saga-" + paymentId;
        paymentSagaRepository.start(
                sagaId,
                paymentId,
                "customer-1",
                new BigDecimal("120.50"),
                "EUR",
                PaymentSagaStepName.FRAUD_CHECK
        );
        paymentSagaRepository.recordReservation(sagaId, "reservation-" + paymentId);
        paymentSagaRepository.recordStep(
                sagaId,
                PaymentSagaStepName.FRAUD_CHECK,
                PaymentSagaStepStatus.DONE,
                PaymentSagaCompensationStatus.NOT_REQUIRED,
                1,
                null
        );
        paymentSagaRepository.recordStep(
                sagaId,
                PaymentSagaStepName.RESERVE_LIMIT,
                PaymentSagaStepStatus.DONE,
                PaymentSagaCompensationStatus.PENDING,
                1,
                null
        );
        paymentSagaRepository.recordStep(
                sagaId,
                PaymentSagaStepName.AUTHORIZE,
                PaymentSagaStepStatus.DONE,
                PaymentSagaCompensationStatus.PENDING,
                1,
                null
        );
        paymentSagaRepository.updateStatus(sagaId, PaymentSagaStepName.CAPTURE, status);

        PaymentAggregate payment = PaymentAggregate.create(
                paymentId,
                "customer-1",
                new Money(new BigDecimal("120.50"), Currency.getInstance("EUR"))
        );
        paymentPersister.persist(payment.uncommittedEvents().getLast());
        payment.authorize();
        paymentPersister.persist(payment.uncommittedEvents().getLast());

        return sagaId;
    }

    private String seedCapturedSagaWithPendingCapture(String paymentId) {
        String sagaId = seedAuthorizedSaga(paymentId, PaymentSagaStatus.STARTED);
        paymentSagaRepository.recordStep(
                sagaId,
                PaymentSagaStepName.CAPTURE,
                PaymentSagaStepStatus.PENDING,
                PaymentSagaCompensationStatus.NOT_REQUIRED,
                1,
                null
        );
        PaymentAggregate payment = PaymentAggregate.create(
                paymentId,
                "customer-1",
                new Money(new BigDecimal("120.50"), Currency.getInstance("EUR"))
        );
        payment.authorize();
        payment.capture();
        paymentPersister.persist(payment.uncommittedEvents().getLast());
        return sagaId;
    }

    private List<String> eventStoreEventTypes(String paymentId) {
        return jdbcTemplate.queryForList(
                "SELECT event_type FROM event_store WHERE stream_id = ? ORDER BY stream_version",
                String.class,
                paymentId
        );
    }
}
