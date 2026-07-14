package com.paymentplatform.orchestration.command.application.saga;

import com.paymentplatform.orchestration.command.application.command.CreatePaymentCommand;
import com.paymentplatform.orchestration.command.application.port.out.AcquirerPort;
import com.paymentplatform.orchestration.command.application.port.out.FraudCheckPort;
import com.paymentplatform.orchestration.command.application.port.out.LimitCheckPort;
import com.paymentplatform.orchestration.command.application.port.out.PaymentEventStorePort;
import com.paymentplatform.orchestration.command.application.port.out.PaymentSagaRepository;
import com.paymentplatform.orchestration.command.application.service.PaymentPersister;
import com.paymentplatform.orchestration.command.domain.event.PaymentAuthorizedEvent;
import com.paymentplatform.orchestration.command.domain.event.PaymentCreatedEvent;
import com.paymentplatform.orchestration.command.domain.event.PaymentEvent;
import com.paymentplatform.orchestration.command.domain.model.Money;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Currency;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

class PaymentSagaOrchestratorTest {

    @Test
    void completesHappyPathWithoutDoubleApplyingSteps() {
        Fixtures fixtures = new Fixtures(false);

        fixtures.orchestrator.start(command("payment-1"));

        assertThat(fixtures.eventStore.eventTypes("payment-1"))
                .containsExactly("PaymentCreated", "PaymentAuthorized", "PaymentCaptured");
        assertThat(fixtures.repository.findByPaymentId("payment-1"))
                .hasValueSatisfying(saga -> assertThat(saga.status()).isEqualTo(PaymentSagaStatus.COMPLETED));
        assertThat(fixtures.repository.findSteps("saga-payment-1"))
                .extracting(PaymentSagaStep::status)
                .containsExactly(
                        PaymentSagaStepStatus.DONE,
                        PaymentSagaStepStatus.DONE,
                        PaymentSagaStepStatus.DONE,
                        PaymentSagaStepStatus.DONE
                );
        assertThat(fixtures.limitCheckPort.releaseCount).isZero();
    }

    @Test
    void compensatesAuthorizedPaymentWhenCaptureFails() {
        Fixtures fixtures = new Fixtures(true);

        assertThatThrownBy(() -> fixtures.orchestrator.start(command("payment-1")))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("capture unavailable");

        assertThat(fixtures.eventStore.eventTypes("payment-1"))
                .containsExactly("PaymentCreated", "PaymentAuthorized", "PaymentVoided");
        assertThat(fixtures.limitCheckPort.releaseCount).isEqualTo(1);
        assertThat(fixtures.repository.findByPaymentId("payment-1"))
                .hasValueSatisfying(saga -> assertThat(saga.status()).isEqualTo(PaymentSagaStatus.COMPENSATED));

        Map<PaymentSagaStepName, PaymentSagaStep> steps = fixtures.repository.stepsByName("saga-payment-1");
        assertThat(steps.get(PaymentSagaStepName.AUTHORIZE).compensationStatus())
                .isEqualTo(PaymentSagaCompensationStatus.DONE);
        assertThat(steps.get(PaymentSagaStepName.RESERVE_LIMIT).compensationStatus())
                .isEqualTo(PaymentSagaCompensationStatus.DONE);
    }

    @Test
    void recoveryContinuesStartedSagaFromPersistedStepLog() {
        Fixtures fixtures = new Fixtures(false);
        String sagaId = "saga-payment-1";
        fixtures.repository.start(sagaId, "payment-1", "customer-1", new BigDecimal("120.50"), "EUR",
                PaymentSagaStepName.FRAUD_CHECK);
        fixtures.repository.recordReservation(sagaId, "reservation-1");
        fixtures.repository.recordStep(sagaId, PaymentSagaStepName.FRAUD_CHECK, PaymentSagaStepStatus.DONE,
                PaymentSagaCompensationStatus.NOT_REQUIRED, 1, null);
        fixtures.repository.recordStep(sagaId, PaymentSagaStepName.RESERVE_LIMIT, PaymentSagaStepStatus.DONE,
                PaymentSagaCompensationStatus.PENDING, 1, null);
        fixtures.repository.recordStep(sagaId, PaymentSagaStepName.AUTHORIZE, PaymentSagaStepStatus.DONE,
                PaymentSagaCompensationStatus.PENDING, 1, null);
        fixtures.repository.updateStatus(sagaId, PaymentSagaStepName.CAPTURE, PaymentSagaStatus.STARTED);
        fixtures.eventStore.append(PaymentCreatedEvent.of("payment-1", "customer-1", new BigDecimal("120.50"), "EUR"));
        fixtures.eventStore.append(PaymentAuthorizedEvent.of("payment-1", "customer-1", new BigDecimal("120.50"), "EUR"));

        fixtures.orchestrator.recover(fixtures.repository.findBySagaId(sagaId).orElseThrow());

        assertThat(fixtures.eventStore.eventTypes("payment-1"))
                .containsExactly("PaymentCreated", "PaymentAuthorized", "PaymentCaptured");
        assertThat(fixtures.repository.findBySagaId(sagaId))
                .hasValueSatisfying(saga -> assertThat(saga.status()).isEqualTo(PaymentSagaStatus.COMPLETED));
    }

    @Test
    void recoveryCompensatesCompensatingSagaOnce() {
        Fixtures fixtures = new Fixtures(false);
        String sagaId = "saga-payment-1";
        fixtures.repository.start(sagaId, "payment-1", "customer-1", new BigDecimal("120.50"), "EUR",
                PaymentSagaStepName.FRAUD_CHECK);
        fixtures.repository.recordReservation(sagaId, "reservation-1");
        fixtures.repository.recordStep(sagaId, PaymentSagaStepName.FRAUD_CHECK, PaymentSagaStepStatus.DONE,
                PaymentSagaCompensationStatus.NOT_REQUIRED, 1, null);
        fixtures.repository.recordStep(sagaId, PaymentSagaStepName.RESERVE_LIMIT, PaymentSagaStepStatus.DONE,
                PaymentSagaCompensationStatus.PENDING, 1, null);
        fixtures.repository.recordStep(sagaId, PaymentSagaStepName.AUTHORIZE, PaymentSagaStepStatus.DONE,
                PaymentSagaCompensationStatus.PENDING, 1, null);
        fixtures.repository.updateStatus(sagaId, PaymentSagaStepName.CAPTURE, PaymentSagaStatus.COMPENSATING);
        fixtures.eventStore.append(PaymentCreatedEvent.of("payment-1", "customer-1", new BigDecimal("120.50"), "EUR"));
        fixtures.eventStore.append(PaymentAuthorizedEvent.of("payment-1", "customer-1", new BigDecimal("120.50"), "EUR"));

        fixtures.orchestrator.recover(fixtures.repository.findBySagaId(sagaId).orElseThrow());
        fixtures.orchestrator.recover(fixtures.repository.findBySagaId(sagaId).orElseThrow());

        assertThat(fixtures.eventStore.eventTypes("payment-1"))
                .containsExactly("PaymentCreated", "PaymentAuthorized", "PaymentVoided");
        assertThat(fixtures.limitCheckPort.releaseCount).isEqualTo(1);
        assertThat(fixtures.repository.findBySagaId(sagaId))
                .hasValueSatisfying(saga -> assertThat(saga.status()).isEqualTo(PaymentSagaStatus.COMPENSATED));
    }

    @Test
    void acquirerDeclineAtAuthorizeCompensatesWithoutRetrying() {
        Fixtures fixtures = new Fixtures(false, 3);
        fixtures.acquirerPort.authorizeOutcome = AcquirerPort.Outcome.DECLINED;

        assertThatThrownBy(() -> fixtures.orchestrator.start(command("payment-1")))
                .isInstanceOf(AcquirerDeclinedException.class);

        assertThat(fixtures.eventStore.eventTypes("payment-1")).containsExactly("PaymentCreated");
        assertThat(fixtures.acquirerPort.authorizeCalls).isEqualTo(1);
        assertThat(fixtures.limitCheckPort.releaseCount).isEqualTo(1);
        assertThat(fixtures.repository.findByPaymentId("payment-1"))
                .hasValueSatisfying(saga -> assertThat(saga.status()).isEqualTo(PaymentSagaStatus.COMPENSATED));
    }

    @Test
    void ambiguousCaptureRetriesThenVoidsAndReleases() {
        Fixtures fixtures = new Fixtures(false, 2);
        fixtures.acquirerPort.captureOutcome = AcquirerPort.Outcome.UNKNOWN;

        assertThatThrownBy(() -> fixtures.orchestrator.start(command("payment-1")))
                .isInstanceOf(RuntimeException.class);

        assertThat(fixtures.eventStore.eventTypes("payment-1"))
                .containsExactly("PaymentCreated", "PaymentAuthorized", "PaymentVoided");
        assertThat(fixtures.acquirerPort.captureCalls).isEqualTo(2);
        assertThat(fixtures.acquirerPort.voidCalls).isEqualTo(1);
        assertThat(fixtures.limitCheckPort.releaseCount).isEqualTo(1);
        assertThat(fixtures.repository.findByPaymentId("payment-1"))
                .hasValueSatisfying(saga -> assertThat(saga.status()).isEqualTo(PaymentSagaStatus.COMPENSATED));
    }

    @Test
    void unconfirmedAcquirerVoidDoesNotFalselyCompleteCompensation() {
        Fixtures fixtures = new Fixtures(true);
        fixtures.acquirerPort.voidOutcome = AcquirerPort.Outcome.UNKNOWN;

        assertThatThrownBy(() -> fixtures.orchestrator.start(command("payment-1")))
                .isInstanceOf(RuntimeException.class);

        assertThat(fixtures.eventStore.eventTypes("payment-1"))
                .containsExactly("PaymentCreated", "PaymentAuthorized");
        assertThat(fixtures.acquirerPort.voidCalls).isGreaterThanOrEqualTo(1);
        assertThat(fixtures.repository.findByPaymentId("payment-1"))
                .hasValueSatisfying(saga -> assertThat(saga.status()).isEqualTo(PaymentSagaStatus.FAILED));
    }

    private CreatePaymentCommand command(String paymentId) {
        return new CreatePaymentCommand(paymentId, "customer-1", new BigDecimal("120.50"), "EUR");
    }

    private static class Fixtures {

        private final InMemoryPaymentSagaRepository repository = new InMemoryPaymentSagaRepository();
        private final InMemoryPaymentEventStore eventStore = new InMemoryPaymentEventStore();
        private final FakeLimitCheckPort limitCheckPort = new FakeLimitCheckPort();
        private final FakeAcquirerPort acquirerPort = new FakeAcquirerPort();
        private final PaymentSagaOrchestrator orchestrator;

        private Fixtures(boolean failCapture) {
            this(failCapture, 1);
        }

        private Fixtures(boolean failCapture, int maxAttempts) {
            PaymentPersister persister = mock(PaymentPersister.class);
            doAnswer(invocation -> {
                PaymentEvent event = invocation.getArgument(0);
                if (failCapture && "PaymentCaptured".equals(event.eventType())) {
                    throw new RuntimeException("capture unavailable");
                }
                eventStore.append(event);
                return null;
            }).when(persister).persist(any(PaymentEvent.class));

            FraudCheckPort fraudCheckPort = (paymentId, customerId, money) ->
                    new FraudCheckPort.FraudCheckResult(true, "OK", 0);

            orchestrator = new PaymentSagaOrchestrator(
                    repository,
                    eventStore,
                    persister,
                    fraudCheckPort,
                    limitCheckPort,
                    acquirerPort,
                    new PaymentSagaProperties(maxAttempts, 10, 0),
                    new SimpleMeterRegistry()
            );
        }
    }

    private static class FakeAcquirerPort implements AcquirerPort {

        private Outcome authorizeOutcome = Outcome.APPROVED;
        private Outcome captureOutcome = Outcome.APPROVED;
        private Outcome voidOutcome = Outcome.APPROVED;
        private Outcome refundOutcome = Outcome.APPROVED;
        private int authorizeCalls;
        private int captureCalls;
        private int voidCalls;
        private int refundCalls;

        @Override
        public AcquirerResult authorize(String paymentId, Money money) {
            authorizeCalls++;
            return new AcquirerResult(authorizeOutcome, "R", "ref");
        }

        @Override
        public AcquirerResult capture(String paymentId, Money money) {
            captureCalls++;
            return new AcquirerResult(captureOutcome, "R", "ref");
        }

        @Override
        public AcquirerResult voidAuthorization(String paymentId) {
            voidCalls++;
            return new AcquirerResult(voidOutcome, "R", "ref");
        }

        @Override
        public AcquirerResult refund(String paymentId, Money money) {
            refundCalls++;
            return new AcquirerResult(refundOutcome, "R", "ref");
        }
    }

    private static class FakeLimitCheckPort implements LimitCheckPort {

        private int releaseCount;

        @Override
        public LimitCheckResult reserve(String paymentId, String customerId, Money money) {
            return new LimitCheckResult(true, "OK", "reservation-1");
        }

        @Override
        public void release(String reservationId) {
            releaseCount++;
        }
    }

    private static class InMemoryPaymentEventStore implements PaymentEventStorePort {

        private final Map<String, List<PaymentEvent>> events = new HashMap<>();

        @Override
        public void append(PaymentEvent event) {
            events.computeIfAbsent(event.aggregateId(), ignored -> new ArrayList<>()).add(event);
        }

        @Override
        public List<PaymentEvent> load(String paymentId) {
            return List.copyOf(events.getOrDefault(paymentId, List.of()));
        }

        private List<String> eventTypes(String paymentId) {
            return load(paymentId).stream().map(PaymentEvent::eventType).toList();
        }
    }

    private static class InMemoryPaymentSagaRepository implements PaymentSagaRepository {

        private final Map<String, PaymentSaga> sagas = new HashMap<>();
        private final Map<String, Map<PaymentSagaStepName, PaymentSagaStep>> steps = new HashMap<>();

        @Override
        public void start(
                String sagaId,
                String paymentId,
                String customerId,
                BigDecimal amount,
                String currency,
                PaymentSagaStepName currentStep
        ) {
            sagas.putIfAbsent(sagaId, new PaymentSaga(
                    sagaId,
                    paymentId,
                    customerId,
                    amount,
                    currency,
                    null,
                    currentStep,
                    PaymentSagaStatus.STARTED,
                    Instant.now(),
                    Instant.now()
            ));
        }

        @Override
        public void updateStatus(String sagaId, PaymentSagaStepName currentStep, PaymentSagaStatus status) {
            PaymentSaga saga = sagas.get(sagaId);
            sagas.put(sagaId, new PaymentSaga(
                    saga.sagaId(),
                    saga.paymentId(),
                    saga.customerId(),
                    saga.amount(),
                    saga.currency(),
                    saga.reservationId(),
                    currentStep,
                    status,
                    saga.createdAt(),
                    Instant.now()
            ));
        }

        @Override
        public void recordReservation(String sagaId, String reservationId) {
            PaymentSaga saga = sagas.get(sagaId);
            sagas.put(sagaId, new PaymentSaga(
                    saga.sagaId(),
                    saga.paymentId(),
                    saga.customerId(),
                    saga.amount(),
                    saga.currency(),
                    reservationId,
                    saga.currentStep(),
                    saga.status(),
                    saga.createdAt(),
                    Instant.now()
            ));
        }

        @Override
        public void recordStep(
                String sagaId,
                PaymentSagaStepName step,
                PaymentSagaStepStatus status,
                PaymentSagaCompensationStatus compensationStatus,
                int attempt,
                String lastError
        ) {
            steps.computeIfAbsent(sagaId, ignored -> new HashMap<>())
                    .put(step, new PaymentSagaStep(
                            sagaId,
                            step,
                            status,
                            compensationStatus,
                            attempt,
                            lastError,
                            Instant.now(),
                            Instant.now()
                    ));
        }

        @Override
        public Optional<PaymentSaga> findBySagaId(String sagaId) {
            return Optional.ofNullable(sagas.get(sagaId));
        }

        @Override
        public Optional<PaymentSaga> findByPaymentId(String paymentId) {
            return sagas.values().stream()
                    .filter(saga -> saga.paymentId().equals(paymentId))
                    .findFirst();
        }

        @Override
        public List<PaymentSagaStep> findSteps(String sagaId) {
            return steps.getOrDefault(sagaId, Map.of()).values().stream()
                    .sorted((left, right) -> Integer.compare(left.step().ordinal(), right.step().ordinal()))
                    .toList();
        }

        @Override
        public List<PaymentSaga> claimRecoverable(Duration olderThan, int batchSize, Duration lockFor, String ownerId) {
            return sagas.values().stream()
                    .filter(saga -> saga.status() == PaymentSagaStatus.STARTED
                            || saga.status() == PaymentSagaStatus.COMPENSATING)
                    .limit(batchSize)
                    .toList();
        }

        private Map<PaymentSagaStepName, PaymentSagaStep> stepsByName(String sagaId) {
            return steps.getOrDefault(sagaId, Map.of());
        }
    }
}
