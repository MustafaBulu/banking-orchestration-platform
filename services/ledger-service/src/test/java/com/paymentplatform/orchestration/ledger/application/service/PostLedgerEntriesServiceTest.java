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

    private static final PaymentCreatedLedgerEvent PAYMENT_CREATED_EVENT = new PaymentCreatedLedgerEvent(
            "event-1",
            "payment-1",
            "customer-1",
            new BigDecimal("120.50"),
            "EUR",
            Instant.parse("2026-06-03T10:15:30Z")
    );

    @Test
    void shouldCreateBalancedDoubleEntryLedgerPosting() {
        FakeLedgerEntryRepository repository = new FakeLedgerEntryRepository();
        PostLedgerEntriesService service = new PostLedgerEntriesService(repository, new SimpleMeterRegistry());

        service.postPaymentCreated(PAYMENT_CREATED_EVENT);

        assertEquals(2, repository.entries.size());
        assertEquals(0, total(repository.entries, LedgerEntryType.DEBIT)
                .compareTo(total(repository.entries, LedgerEntryType.CREDIT)));
        assertTrue(repository.processedEventIds.contains(PAYMENT_CREATED_EVENT.eventId()));
    }

    @Test
    void shouldNotCreateDuplicateEntriesForAlreadyProcessedEvent() {
        FakeLedgerEntryRepository repository = new FakeLedgerEntryRepository();
        PostLedgerEntriesService service = new PostLedgerEntriesService(repository, new SimpleMeterRegistry());

        service.postPaymentCreated(PAYMENT_CREATED_EVENT);
        service.postPaymentCreated(PAYMENT_CREATED_EVENT);

        assertEquals(2, repository.entries.size());
    }

    private static BigDecimal total(List<LedgerEntry> entries, LedgerEntryType entryType) {
        return entries.stream()
                .filter(entry -> entry.entryType() == entryType)
                .map(LedgerEntry::amount)
                .reduce(BigDecimal.ZERO, BigDecimal::add);
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
