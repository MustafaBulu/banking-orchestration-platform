package com.paymentplatform.orchestration.ledger.application.service;

import com.paymentplatform.orchestration.ledger.application.port.out.LedgerEntryRepository;
import com.paymentplatform.orchestration.ledger.domain.model.LedgerEntry;
import com.paymentplatform.orchestration.ledger.domain.model.LedgerEntryType;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PostLedgerEntriesServiceTest {

    private static final PaymentLedgerEvent PAYMENT_AUTHORIZED_EVENT = new PaymentLedgerEvent(
            "event-1",
            "payment-1",
            "PaymentAuthorized",
            "customer-1",
            new BigDecimal("120.50"),
            "EUR",
            Instant.parse("2026-06-03T10:15:30Z")
    );

    @Test
    void shouldCreateBalancedDoubleEntryLedgerPosting() {
        FakeLedgerEntryRepository repository = new FakeLedgerEntryRepository();
        PostLedgerEntriesService service = new PostLedgerEntriesService(repository, new SimpleMeterRegistry());

        service.postPaymentEvent(PAYMENT_AUTHORIZED_EVENT);

        assertEquals(2, repository.entries.size());
        assertEquals(0, total(repository.entries, LedgerEntryType.DEBIT)
                .compareTo(total(repository.entries, LedgerEntryType.CREDIT)));
        assertTrue(repository.processedEventIds.contains(PAYMENT_AUTHORIZED_EVENT.eventId()));
    }

    @Test
    void shouldCreateBalancedReversalForVoidedAuthorization() {
        FakeLedgerEntryRepository repository = new FakeLedgerEntryRepository();
        PostLedgerEntriesService service = new PostLedgerEntriesService(repository, new SimpleMeterRegistry());

        service.postPaymentEvent(new PaymentLedgerEvent(
                "event-void-1",
                "payment-1",
                "PaymentVoided",
                "customer-1",
                new BigDecimal("120.50"),
                "EUR",
                Instant.parse("2026-06-03T10:16:30Z")
        ));

        assertEquals(2, repository.entries.size());
        assertEquals("LIMIT_HOLD_LIABILITY", repository.entries.get(0).accountCode());
        assertEquals(LedgerEntryType.DEBIT, repository.entries.get(0).entryType());
        assertEquals("CUSTOMER_AUTH_HOLD", repository.entries.get(1).accountCode());
        assertEquals(LedgerEntryType.CREDIT, repository.entries.get(1).entryType());
    }

    @Test
    void captureShouldReleaseAuthorizationHoldAndPostSettlement() {
        FakeLedgerEntryRepository repository = new FakeLedgerEntryRepository();
        PostLedgerEntriesService service = new PostLedgerEntriesService(repository, new SimpleMeterRegistry());

        service.postPaymentEvent(PAYMENT_AUTHORIZED_EVENT);
        service.postPaymentEvent(event("event-capture-1", "PaymentCaptured"));

        assertEquals(6, repository.entries.size());
        assertEquals(0, net(repository.entries, "CUSTOMER_AUTH_HOLD").compareTo(BigDecimal.ZERO));
        assertEquals(0, net(repository.entries, "LIMIT_HOLD_LIABILITY").compareTo(BigDecimal.ZERO));
        assertEquals(0, net(repository.entries, "SETTLEMENT_ACCOUNT").compareTo(new BigDecimal("120.50")));
        assertEquals(0, net(repository.entries, "CUSTOMER_ACCOUNT").compareTo(new BigDecimal("-120.50")));
    }

    @Test
    void refundedCapturedPaymentShouldNetToZero() {
        FakeLedgerEntryRepository repository = new FakeLedgerEntryRepository();
        PostLedgerEntriesService service = new PostLedgerEntriesService(repository, new SimpleMeterRegistry());

        service.postPaymentEvent(PAYMENT_AUTHORIZED_EVENT);
        service.postPaymentEvent(event("event-capture-1", "PaymentCaptured"));
        service.postPaymentEvent(event("event-refund-1", "PaymentRefunded"));

        assertEquals(0, net(repository.entries, "CUSTOMER_AUTH_HOLD").compareTo(BigDecimal.ZERO));
        assertEquals(0, net(repository.entries, "LIMIT_HOLD_LIABILITY").compareTo(BigDecimal.ZERO));
        assertEquals(0, net(repository.entries, "SETTLEMENT_ACCOUNT").compareTo(BigDecimal.ZERO));
        assertEquals(0, net(repository.entries, "CUSTOMER_ACCOUNT").compareTo(BigDecimal.ZERO));
    }

    @Test
    void voidedAuthorizationShouldNetToZero() {
        FakeLedgerEntryRepository repository = new FakeLedgerEntryRepository();
        PostLedgerEntriesService service = new PostLedgerEntriesService(repository, new SimpleMeterRegistry());

        service.postPaymentEvent(PAYMENT_AUTHORIZED_EVENT);
        service.postPaymentEvent(event("event-void-1", "PaymentVoided"));

        assertEquals(0, net(repository.entries, "CUSTOMER_AUTH_HOLD").compareTo(BigDecimal.ZERO));
        assertEquals(0, net(repository.entries, "LIMIT_HOLD_LIABILITY").compareTo(BigDecimal.ZERO));
    }

    @Test
    void shouldNotCreateDuplicateEntriesForAlreadyProcessedEvent() {
        FakeLedgerEntryRepository repository = new FakeLedgerEntryRepository();
        PostLedgerEntriesService service = new PostLedgerEntriesService(repository, new SimpleMeterRegistry());

        service.postPaymentEvent(PAYMENT_AUTHORIZED_EVENT);
        service.postPaymentEvent(PAYMENT_AUTHORIZED_EVENT);

        assertEquals(2, repository.entries.size());
    }

    private static BigDecimal total(List<LedgerEntry> entries, LedgerEntryType entryType) {
        return entries.stream()
                .filter(entry -> entry.entryType() == entryType)
                .map(LedgerEntry::amount)
                .reduce(BigDecimal.ZERO, BigDecimal::add);
    }

    private static BigDecimal net(List<LedgerEntry> entries, String accountCode) {
        return entries.stream()
                .filter(entry -> entry.accountCode().equals(accountCode))
                .map(entry -> entry.entryType() == LedgerEntryType.DEBIT
                        ? entry.amount()
                        : entry.amount().negate())
                .reduce(BigDecimal.ZERO, BigDecimal::add);
    }

    private static PaymentLedgerEvent event(String eventId, String eventType) {
        return new PaymentLedgerEvent(
                eventId,
                "payment-1",
                eventType,
                "customer-1",
                new BigDecimal("120.50"),
                "EUR",
                Instant.parse("2026-06-03T10:16:30Z")
        );
    }

    private static class FakeLedgerEntryRepository implements LedgerEntryRepository {

        private final List<LedgerEntry> entries = new ArrayList<>();
        private final Set<String> processedEventIds = new HashSet<>();

        @Override
        public boolean isProcessed(String eventId) {
            return processedEventIds.contains(eventId);
        }

        @Override
        public void saveEntries(List<LedgerEntry> entries) {
            this.entries.addAll(entries);
        }

        @Override
        public void markProcessed(String eventId, String consumerName) {
            processedEventIds.add(eventId);
        }
    }
}
