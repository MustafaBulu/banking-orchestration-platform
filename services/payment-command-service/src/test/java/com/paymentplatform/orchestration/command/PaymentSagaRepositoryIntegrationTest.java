package com.paymentplatform.orchestration.command;

import com.paymentplatform.orchestration.command.application.port.out.PaymentSagaRepository;
import com.paymentplatform.orchestration.command.application.saga.PaymentSaga;
import com.paymentplatform.orchestration.command.application.saga.PaymentSagaCompensationStatus;
import com.paymentplatform.orchestration.command.application.saga.PaymentSagaStatus;
import com.paymentplatform.orchestration.command.application.saga.PaymentSagaStepName;
import com.paymentplatform.orchestration.command.application.saga.PaymentSagaStepStatus;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class PaymentSagaRepositoryIntegrationTest extends AbstractIntegrationTest {

    @Autowired
    private PaymentSagaRepository paymentSagaRepository;

    @Test
    void persistsSagaStateAndStepLog() {
        String sagaId = UUID.randomUUID().toString();
        String paymentId = UUID.randomUUID().toString();

        paymentSagaRepository.start(
                sagaId,
                paymentId,
                "customer-1",
                new BigDecimal("120.50"),
                "EUR",
                PaymentSagaStepName.FRAUD_CHECK
        );
        paymentSagaRepository.recordReservation(sagaId, "reservation-1");
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
        paymentSagaRepository.recordStep(
                sagaId,
                PaymentSagaStepName.CAPTURE,
                PaymentSagaStepStatus.DONE,
                PaymentSagaCompensationStatus.NOT_REQUIRED,
                1,
                null
        );
        paymentSagaRepository.updateStatus(sagaId, PaymentSagaStepName.CAPTURE, PaymentSagaStatus.COMPLETED);

        assertThat(paymentSagaRepository.findBySagaId(sagaId))
                .hasValueSatisfying(saga -> {
                    assertThat(saga.paymentId()).isEqualTo(paymentId);
                    assertThat(saga.customerId()).isEqualTo("customer-1");
                    assertThat(saga.amount()).isEqualByComparingTo("120.50");
                    assertThat(saga.currency()).isEqualTo("EUR");
                    assertThat(saga.reservationId()).isEqualTo("reservation-1");
                    assertThat(saga.currentStep()).isEqualTo(PaymentSagaStepName.CAPTURE);
                    assertThat(saga.status()).isEqualTo(PaymentSagaStatus.COMPLETED);
                });
        assertThat(paymentSagaRepository.findSteps(sagaId))
                .hasSize(4)
                .allSatisfy(step -> assertThat(step.status()).isEqualTo(PaymentSagaStepStatus.DONE));
    }

    @Test
    void claimRecoverableLocksSagaForAnotherWorker() {
        String sagaId = UUID.randomUUID().toString();
        String paymentId = UUID.randomUUID().toString();

        paymentSagaRepository.start(
                sagaId,
                paymentId,
                "customer-1",
                new BigDecimal("120.50"),
                "EUR",
                PaymentSagaStepName.FRAUD_CHECK
        );

        assertThat(paymentSagaRepository.claimRecoverable(
                Duration.ofMillis(-1),
                10,
                Duration.ofSeconds(30),
                "worker-1"
        )).extracting(PaymentSaga::sagaId).containsExactly(sagaId);
        assertThat(paymentSagaRepository.claimRecoverable(
                Duration.ofMillis(-1),
                10,
                Duration.ofSeconds(30),
                "worker-2"
        )).isEmpty();
    }
}
