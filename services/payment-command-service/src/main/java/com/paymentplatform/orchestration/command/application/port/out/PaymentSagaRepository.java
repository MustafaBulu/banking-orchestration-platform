package com.paymentplatform.orchestration.command.application.port.out;

import com.paymentplatform.orchestration.command.application.saga.PaymentSaga;
import com.paymentplatform.orchestration.command.application.saga.PaymentSagaCompensationStatus;
import com.paymentplatform.orchestration.command.application.saga.PaymentSagaStatus;
import com.paymentplatform.orchestration.command.application.saga.PaymentSagaStep;
import com.paymentplatform.orchestration.command.application.saga.PaymentSagaStepName;
import com.paymentplatform.orchestration.command.application.saga.PaymentSagaStepStatus;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

public interface PaymentSagaRepository {

    void start(
            String sagaId,
            String paymentId,
            String customerId,
            BigDecimal amount,
            String currency,
            PaymentSagaStepName currentStep
    );

    void updateStatus(String sagaId, PaymentSagaStepName currentStep, PaymentSagaStatus status);

    void recordReservation(String sagaId, String reservationId);

    void recordStep(
            String sagaId,
            PaymentSagaStepName step,
            PaymentSagaStepStatus status,
            PaymentSagaCompensationStatus compensationStatus,
            int attempt,
            String lastError
    );

    Optional<PaymentSaga> findBySagaId(String sagaId);

    Optional<PaymentSaga> findByPaymentId(String paymentId);

    List<PaymentSagaStep> findSteps(String sagaId);

    List<PaymentSaga> claimRecoverable(Duration olderThan, int batchSize, Duration lockFor, String ownerId);
}
