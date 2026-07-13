package com.paymentplatform.orchestration.command.application.saga;

import com.paymentplatform.orchestration.command.application.port.out.PaymentSagaRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.UUID;

@Component
public class SagaRecoveryWorker {

    private static final Logger log = LoggerFactory.getLogger(SagaRecoveryWorker.class);

    private final PaymentSagaRepository paymentSagaRepository;
    private final PaymentSagaOrchestrator paymentSagaOrchestrator;
    private final PaymentSagaProperties properties;
    private final String ownerId = UUID.randomUUID().toString();

    public SagaRecoveryWorker(
            PaymentSagaRepository paymentSagaRepository,
            PaymentSagaOrchestrator paymentSagaOrchestrator,
            PaymentSagaProperties properties
    ) {
        this.paymentSagaRepository = paymentSagaRepository;
        this.paymentSagaOrchestrator = paymentSagaOrchestrator;
        this.properties = properties;
    }

    @Scheduled(fixedDelayString = "${app.payment-saga.recovery-poll-delay-ms:5000}")
    public void recover() {
        Duration stuckAfter = Duration.ofMillis(properties.recoveryStuckAfterMs());
        Duration lockFor = Duration.ofMillis(Math.max(properties.recoveryStuckAfterMs(), 1000L));
        for (PaymentSaga saga : paymentSagaRepository.claimRecoverable(
                stuckAfter,
                properties.recoveryBatchSize(),
                lockFor,
                ownerId
        )) {
            try {
                paymentSagaOrchestrator.recover(saga);
            } catch (RuntimeException ex) {
                log.warn("Payment saga recovery failed. sagaId={}, paymentId={}", saga.sagaId(), saga.paymentId(), ex);
            }
        }
    }
}
