package com.paymentplatform.orchestration.ledger.application.port.out;

import com.paymentplatform.orchestration.ledger.domain.model.LedgerEntry;

import java.util.List;

public interface LedgerEntryRepository {

    boolean isProcessed(String eventId);

    void saveEntries(List<LedgerEntry> entries);

    void markProcessed(String eventId, String consumerName);
}
