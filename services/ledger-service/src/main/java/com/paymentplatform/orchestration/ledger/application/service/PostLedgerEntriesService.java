package com.paymentplatform.orchestration.ledger.application.service;

import com.paymentplatform.orchestration.ledger.application.port.out.LedgerEntryRepository;
import com.paymentplatform.orchestration.ledger.domain.model.LedgerPosting;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class PostLedgerEntriesService {

    private static final String CONSUMER_NAME = "ledger-service";

    private final LedgerEntryRepository ledgerEntryRepository;
    private final Counter ledgerEntriesPostedCounter;

    public PostLedgerEntriesService(LedgerEntryRepository ledgerEntryRepository, MeterRegistry meterRegistry) {
        this.ledgerEntryRepository = ledgerEntryRepository;
        this.ledgerEntriesPostedCounter = Counter.builder("ledger.entries.posted")
                .description("Ledger entries posted from payment events")
                .register(meterRegistry);
    }

    @Transactional
    public void postPaymentCreated(PaymentCreatedLedgerEvent event) {
        postPaymentEvent(new PaymentLedgerEvent(
                event.eventId(),
                event.paymentId(),
                "PaymentCreated",
                event.customerId(),
                event.amount(),
                event.currency(),
                event.occurredAt()
        ));
    }

    @Transactional
    public void postPaymentEvent(PaymentLedgerEvent event) {
        if (ledgerEntryRepository.isProcessed(event.eventId())) {
            return;
        }

        LedgerPosting posting = LedgerPosting.fromPaymentEvent(
                event.eventType(),
                event.eventId(),
                event.paymentId(),
                event.customerId(),
                event.amount(),
                event.currency(),
                event.occurredAt()
        );

        ledgerEntryRepository.saveEntries(posting.entries());
        ledgerEntryRepository.markProcessed(event.eventId(), CONSUMER_NAME);
        ledgerEntriesPostedCounter.increment(posting.entries().size());
    }
}
