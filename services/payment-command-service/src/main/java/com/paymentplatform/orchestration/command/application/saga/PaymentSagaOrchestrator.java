package com.paymentplatform.orchestration.command.application.saga;

import com.paymentplatform.orchestration.command.application.command.CreatePaymentCommand;
import com.paymentplatform.orchestration.command.application.port.out.AcquirerPort;
import com.paymentplatform.orchestration.command.application.port.out.FraudCheckPort;
import com.paymentplatform.orchestration.command.application.port.out.LimitCheckPort;
import com.paymentplatform.orchestration.command.application.port.out.PaymentEventStorePort;
import com.paymentplatform.orchestration.command.application.port.out.PaymentSagaRepository;
import com.paymentplatform.orchestration.command.application.service.PaymentPersister;
import com.paymentplatform.orchestration.command.domain.event.PaymentEvent;
import com.paymentplatform.orchestration.command.domain.exception.PaymentRejectedException;
import com.paymentplatform.orchestration.command.domain.model.Money;
import com.paymentplatform.orchestration.command.domain.model.PaymentAggregate;
import com.paymentplatform.orchestration.command.domain.model.PaymentStatus;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Service;

import java.util.Currency;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@Service
public class PaymentSagaOrchestrator {

    private final PaymentSagaRepository paymentSagaRepository;
    private final PaymentEventStorePort paymentEventStorePort;
    private final PaymentPersister paymentPersister;
    private final FraudCheckPort fraudCheckPort;
    private final LimitCheckPort limitCheckPort;
    private final AcquirerPort acquirerPort;
    private final PaymentSagaProperties properties;
    private final Counter reservationCompensatedCounter;

    public PaymentSagaOrchestrator(
            PaymentSagaRepository paymentSagaRepository,
            PaymentEventStorePort paymentEventStorePort,
            PaymentPersister paymentPersister,
            FraudCheckPort fraudCheckPort,
            LimitCheckPort limitCheckPort,
            AcquirerPort acquirerPort,
            PaymentSagaProperties properties,
            MeterRegistry meterRegistry
    ) {
        this.paymentSagaRepository = paymentSagaRepository;
        this.paymentEventStorePort = paymentEventStorePort;
        this.paymentPersister = paymentPersister;
        this.fraudCheckPort = fraudCheckPort;
        this.limitCheckPort = limitCheckPort;
        this.acquirerPort = acquirerPort;
        this.properties = properties;
        this.reservationCompensatedCounter = Counter.builder("limit.reservation.compensated")
                .description("Limit reservations released by saga compensation")
                .register(meterRegistry);
    }

    public String start(CreatePaymentCommand command) {
        paymentSagaRepository.start(
                sagaId(command.paymentId()),
                command.paymentId(),
                command.customerId(),
                command.amount(),
                command.currency(),
                PaymentSagaStepName.FRAUD_CHECK
        );
        PaymentSaga saga = paymentSagaRepository.findByPaymentId(command.paymentId())
                .orElseThrow(() -> new IllegalStateException("Payment saga was not created"));
        return advanceOrCompensate(saga);
    }

    public String recover(PaymentSaga saga) {
        if (saga.status() == PaymentSagaStatus.COMPENSATING) {
            compensate(saga);
            return saga.paymentId();
        }
        if (saga.status() == PaymentSagaStatus.STARTED) {
            return advanceOrCompensate(saga);
        }
        return saga.paymentId();
    }

    private String advanceOrCompensate(PaymentSaga saga) {
        try {
            advance(saga);
            return saga.paymentId();
        } catch (PaymentRejectedException ex) {
            PaymentSaga fresh = fresh(saga);
            paymentSagaRepository.updateStatus(fresh.sagaId(), fresh.currentStep(), PaymentSagaStatus.FAILED);
            throw ex;
        } catch (RuntimeException ex) {
            compensate(fresh(saga));
            throw ex;
        }
    }

    private void advance(PaymentSaga initialSaga) {
        PaymentSaga fraudSaga = fresh(initialSaga);
        runStep(fraudSaga, PaymentSagaStepName.FRAUD_CHECK, PaymentSagaCompensationStatus.NOT_REQUIRED, () -> {
            Money money = money(fraudSaga);
            FraudCheckPort.FraudCheckResult fraudResult =
                    fraudCheckPort.evaluate(fraudSaga.paymentId(), fraudSaga.customerId(), money);
            if (!fraudResult.approved()) {
                throw new PaymentRejectedException("Fraud check rejected: " + fraudResult.reasonCode());
            }
            return null;
        });

        PaymentSaga reserveSaga = fresh(initialSaga);
        runStep(reserveSaga, PaymentSagaStepName.RESERVE_LIMIT, PaymentSagaCompensationStatus.PENDING, () -> {
            Money money = money(reserveSaga);
            LimitCheckPort.LimitCheckResult limitResult =
                    limitCheckPort.reserve(reserveSaga.paymentId(), reserveSaga.customerId(), money);
            if (!limitResult.approved()) {
                throw new PaymentRejectedException("Limit check rejected: " + limitResult.reasonCode());
            }
            paymentSagaRepository.recordReservation(reserveSaga.sagaId(), limitResult.reservationId());
            return null;
        });

        PaymentSaga authorizeSaga = fresh(initialSaga);
        runStep(authorizeSaga, PaymentSagaStepName.AUTHORIZE, PaymentSagaCompensationStatus.PENDING, () -> {
            authorize(authorizeSaga);
            return null;
        });

        PaymentSaga captureSaga = fresh(initialSaga);
        runStep(captureSaga, PaymentSagaStepName.CAPTURE, PaymentSagaCompensationStatus.NOT_REQUIRED, () -> {
            capture(captureSaga);
            return null;
        });

        PaymentSaga completed = fresh(initialSaga);
        paymentSagaRepository.updateStatus(completed.sagaId(), PaymentSagaStepName.CAPTURE, PaymentSagaStatus.COMPLETED);
    }

    private void runStep(
            PaymentSaga saga,
            PaymentSagaStepName step,
            PaymentSagaCompensationStatus compensationStatus,
            Supplier<Void> action
    ) {
        PaymentSagaStep existing = steps(saga).get(step);
        if (existing != null && existing.status() == PaymentSagaStepStatus.DONE) {
            return;
        }
        if (reconcileCompletedStep(saga, step, compensationStatus, existing)) {
            return;
        }
        if (existing != null
                && existing.status() == PaymentSagaStepStatus.FAILED
                && existing.attempt() >= properties.maxAttempts()) {
            throw new IllegalStateException("Saga step exhausted retry attempts: " + step);
        }

        paymentSagaRepository.updateStatus(saga.sagaId(), step, PaymentSagaStatus.STARTED);
        int firstAttempt = nextAttempt(existing);
        for (int attempt = firstAttempt; attempt <= properties.maxAttempts(); attempt++) {
            paymentSagaRepository.recordStep(
                    saga.sagaId(),
                    step,
                    PaymentSagaStepStatus.PENDING,
                    compensationStatus,
                    attempt,
                    null
            );
            try {
                action.get();
                paymentSagaRepository.recordStep(
                        saga.sagaId(),
                        step,
                        PaymentSagaStepStatus.DONE,
                        compensationStatus,
                        attempt,
                        null
                );
                return;
            } catch (PaymentRejectedException ex) {
                paymentSagaRepository.recordStep(
                        saga.sagaId(),
                        step,
                        PaymentSagaStepStatus.FAILED,
                        PaymentSagaCompensationStatus.NOT_REQUIRED,
                        attempt,
                        truncate(ex.getMessage())
                );
                throw ex;
            } catch (AcquirerDeclinedException ex) {
                paymentSagaRepository.recordStep(
                        saga.sagaId(),
                        step,
                        PaymentSagaStepStatus.FAILED,
                        PaymentSagaCompensationStatus.NOT_REQUIRED,
                        attempt,
                        truncate(ex.getMessage())
                );
                throw ex;
            } catch (RuntimeException ex) {
                paymentSagaRepository.recordStep(
                        saga.sagaId(),
                        step,
                        PaymentSagaStepStatus.FAILED,
                        compensationStatus,
                        attempt,
                        truncate(ex.getMessage())
                );
                if (attempt == properties.maxAttempts()) {
                    throw ex;
                }
            }
        }
    }

    private boolean reconcileCompletedStep(
            PaymentSaga saga,
            PaymentSagaStepName step,
            PaymentSagaCompensationStatus compensationStatus,
            PaymentSagaStep existing
    ) {
        if (step == PaymentSagaStepName.RESERVE_LIMIT && fresh(saga).reservationId() != null) {
            paymentSagaRepository.recordStep(
                    saga.sagaId(),
                    step,
                    PaymentSagaStepStatus.DONE,
                    compensationStatus(existing, compensationStatus),
                    attempt(existing),
                    null
            );
            return true;
        }

        PaymentStatus status = paymentStatus(saga);
        if (step == PaymentSagaStepName.AUTHORIZE && authorizedOrLater(status)) {
            paymentSagaRepository.recordStep(
                    saga.sagaId(),
                    step,
                    PaymentSagaStepStatus.DONE,
                    compensationStatus(existing, compensationStatus),
                    attempt(existing),
                    null
            );
            return true;
        }
        if (step == PaymentSagaStepName.CAPTURE && capturedOrLater(status)) {
            paymentSagaRepository.recordStep(
                    saga.sagaId(),
                    step,
                    PaymentSagaStepStatus.DONE,
                    compensationStatus(existing, compensationStatus),
                    attempt(existing),
                    null
            );
            return true;
        }
        return false;
    }

    private int nextAttempt(PaymentSagaStep existing) {
        if (existing == null || existing.attempt() < 1) {
            return 1;
        }
        if (existing.status() == PaymentSagaStepStatus.PENDING) {
            return Math.min(existing.attempt(), properties.maxAttempts());
        }
        return existing.attempt() + 1;
    }

    private void authorize(PaymentSaga saga) {
        List<PaymentEvent> events = paymentEventStorePort.load(saga.paymentId());
        if (events.isEmpty()) {
            PaymentAggregate created = PaymentAggregate.create(saga.paymentId(), saga.customerId(), money(saga));
            paymentPersister.persist(created.uncommittedEvents().getLast());
            events = paymentEventStorePort.load(saga.paymentId());
        }

        PaymentAggregate payment = PaymentAggregate.rehydrate(events);
        if (payment.status() == PaymentStatus.CREATED) {
            requireApproved(acquirerPort.authorize(saga.paymentId(), money(saga)));
            payment.authorize();
            paymentPersister.persist(payment.uncommittedEvents().getLast());
        }
    }

    private void capture(PaymentSaga saga) {
        PaymentAggregate payment = PaymentAggregate.rehydrate(paymentEventStorePort.load(saga.paymentId()));
        if (payment.status() == PaymentStatus.AUTHORIZED) {
            requireApproved(acquirerPort.capture(saga.paymentId(), money(saga)));
            payment.capture();
            paymentPersister.persist(payment.uncommittedEvents().getLast());
        }
    }

    private void requireApproved(AcquirerPort.AcquirerResult result) {
        switch (result.outcome()) {
            case APPROVED -> {
            }
            case DECLINED -> throw new AcquirerDeclinedException("Acquirer declined: " + result.reasonCode());
            case UNKNOWN -> throw new IllegalStateException("Acquirer outcome unresolved: " + result.reasonCode());
        }
    }

    private void requireReversed(AcquirerPort.AcquirerResult result) {
        if (result.outcome() != AcquirerPort.Outcome.APPROVED) {
            throw new IllegalStateException("Acquirer reversal not confirmed: " + result.reasonCode());
        }
    }

    private void compensate(PaymentSaga saga) {
        paymentSagaRepository.updateStatus(saga.sagaId(), saga.currentStep(), PaymentSagaStatus.COMPENSATING);
        try {
            compensateLocalEvents(saga);
            compensateLimitReservation(fresh(saga));
            PaymentSaga fresh = fresh(saga);
            paymentSagaRepository.updateStatus(fresh.sagaId(), fresh.currentStep(), PaymentSagaStatus.COMPENSATED);
        } catch (RuntimeException compensationFailure) {
            PaymentSaga fresh = fresh(saga);
            paymentSagaRepository.updateStatus(fresh.sagaId(), fresh.currentStep(), PaymentSagaStatus.FAILED);
            throw compensationFailure;
        }
    }

    private void compensateLocalEvents(PaymentSaga saga) {
        List<PaymentEvent> events = paymentEventStorePort.load(saga.paymentId());
        if (events.isEmpty()) {
            return;
        }

        PaymentAggregate payment = PaymentAggregate.rehydrate(events);
        Map<PaymentSagaStepName, PaymentSagaStep> steps = steps(saga);
        if (payment.status() == PaymentStatus.CAPTURED
                && !compensationDone(steps, PaymentSagaStepName.CAPTURE)) {
            requireReversed(acquirerPort.refund(saga.paymentId(), money(saga)));
            payment.refund();
            paymentPersister.persist(payment.uncommittedEvents().getLast());
            paymentSagaRepository.recordStep(
                    saga.sagaId(),
                    PaymentSagaStepName.CAPTURE,
                    PaymentSagaStepStatus.DONE,
                    PaymentSagaCompensationStatus.DONE,
                    attempt(steps, PaymentSagaStepName.CAPTURE),
                    null
            );
            return;
        }

        if (payment.status() == PaymentStatus.AUTHORIZED
                && !compensationDone(steps, PaymentSagaStepName.AUTHORIZE)) {
            requireReversed(acquirerPort.voidAuthorization(saga.paymentId()));
            payment.voidAuthorization();
            paymentPersister.persist(payment.uncommittedEvents().getLast());
            paymentSagaRepository.recordStep(
                    saga.sagaId(),
                    PaymentSagaStepName.AUTHORIZE,
                    PaymentSagaStepStatus.DONE,
                    PaymentSagaCompensationStatus.DONE,
                    attempt(steps, PaymentSagaStepName.AUTHORIZE),
                    null
            );
        }
    }

    private void compensateLimitReservation(PaymentSaga saga) {
        Map<PaymentSagaStepName, PaymentSagaStep> steps = steps(saga);
        if (saga.reservationId() == null || compensationDone(steps, PaymentSagaStepName.RESERVE_LIMIT)) {
            return;
        }
        limitCheckPort.release(saga.reservationId());
        reservationCompensatedCounter.increment();
        paymentSagaRepository.recordStep(
                saga.sagaId(),
                PaymentSagaStepName.RESERVE_LIMIT,
                PaymentSagaStepStatus.DONE,
                PaymentSagaCompensationStatus.DONE,
                attempt(steps, PaymentSagaStepName.RESERVE_LIMIT),
                null
        );
    }

    private boolean compensationDone(Map<PaymentSagaStepName, PaymentSagaStep> steps, PaymentSagaStepName step) {
        PaymentSagaStep sagaStep = steps.get(step);
        return sagaStep != null && sagaStep.compensationStatus() == PaymentSagaCompensationStatus.DONE;
    }

    private PaymentSagaCompensationStatus compensationStatus(
            PaymentSagaStep existing,
            PaymentSagaCompensationStatus fallback
    ) {
        return existing == null ? fallback : existing.compensationStatus();
    }

    private int attempt(PaymentSagaStep step) {
        return step == null ? 1 : Math.max(step.attempt(), 1);
    }

    private int attempt(Map<PaymentSagaStepName, PaymentSagaStep> steps, PaymentSagaStepName step) {
        PaymentSagaStep sagaStep = steps.get(step);
        return sagaStep == null ? 1 : Math.max(sagaStep.attempt(), 1);
    }

    private PaymentStatus paymentStatus(PaymentSaga saga) {
        List<PaymentEvent> events = paymentEventStorePort.load(saga.paymentId());
        if (events.isEmpty()) {
            return null;
        }
        return PaymentAggregate.rehydrate(events).status();
    }

    private boolean authorizedOrLater(PaymentStatus status) {
        return status == PaymentStatus.AUTHORIZED
                || status == PaymentStatus.CAPTURED
                || status == PaymentStatus.VOIDED
                || status == PaymentStatus.REFUNDED;
    }

    private boolean capturedOrLater(PaymentStatus status) {
        return status == PaymentStatus.CAPTURED || status == PaymentStatus.REFUNDED;
    }

    private Map<PaymentSagaStepName, PaymentSagaStep> steps(PaymentSaga saga) {
        return paymentSagaRepository.findSteps(saga.sagaId()).stream()
                .collect(Collectors.toMap(PaymentSagaStep::step, step -> step));
    }

    private PaymentSaga fresh(PaymentSaga saga) {
        return paymentSagaRepository.findBySagaId(saga.sagaId())
                .orElseThrow(() -> new IllegalStateException("Payment saga not found: " + saga.sagaId()));
    }

    private Money money(PaymentSaga saga) {
        return new Money(saga.amount(), Currency.getInstance(saga.currency()));
    }

    private String sagaId(String paymentId) {
        return "saga-" + paymentId;
    }

    private String truncate(String message) {
        if (message == null || message.length() <= 512) {
            return message;
        }
        return message.substring(0, 512);
    }
}
