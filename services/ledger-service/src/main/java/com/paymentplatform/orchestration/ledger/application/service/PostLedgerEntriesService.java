package com.paymentplatform.orchestration.ledger.application.service;

import com.paymentplatform.orchestration.ledger.application.port.out.LedgerEntryRepository;
import com.paymentplatform.orchestration.ledger.domain.model.LedgerPosting;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class PostLedgerEntriesService {

    private static final String CONSUMER_NAME = "ledger-service";

    private final LedgerEntryRepository ledgerEntryRepository;

    public PostLedgerEntriesService(LedgerEntryRepository ledgerEntryRepository) {
        this.ledgerEntryRepository = ledgerEntryRepository;
    }

    @Transactional
    public void postPaymentCreated(PaymentCreatedLedgerEvent event) {
        if (ledgerEntryRepository.isProcessed(event.eventId())) {
            return;
        }

        LedgerPosting posting = LedgerPosting.fromPaymentCreated(
                event.eventId(),
                event.paymentId(),
                event.customerId(),
                event.amount(),
                event.currency(),
                event.occurredAt()
        );

        ledgerEntryRepository.saveEntries(posting.entries());
        ledgerEntryRepository.markProcessed(event.eventId(), CONSUMER_NAME);
    }
}
