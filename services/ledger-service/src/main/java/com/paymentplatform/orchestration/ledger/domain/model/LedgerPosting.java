package com.paymentplatform.orchestration.ledger.domain.model;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

public class LedgerPosting {

    private static final String CUSTOMER_ACCOUNT = "CUSTOMER_ACCOUNT";
    private static final String SETTLEMENT_ACCOUNT = "SETTLEMENT_ACCOUNT";

    private final List<LedgerEntry> entries;

    private LedgerPosting(List<LedgerEntry> entries) {
        this.entries = List.copyOf(entries);
        if (!isBalanced()) {
            throw new IllegalArgumentException("ledger posting must be balanced");
        }
    }

    public static LedgerPosting fromPaymentCreated(
            String eventId,
            String paymentId,
            String customerId,
            BigDecimal amount,
            String currency,
            Instant occurredAt
    ) {
        return new LedgerPosting(List.of(
                new LedgerEntry(
                        UUID.randomUUID(),
                        paymentId,
                        customerId,
                        eventId,
                        1,
                        CUSTOMER_ACCOUNT,
                        LedgerEntryType.DEBIT,
                        amount,
                        currency,
                        occurredAt
                ),
                new LedgerEntry(
                        UUID.randomUUID(),
                        paymentId,
                        customerId,
                        eventId,
                        2,
                        SETTLEMENT_ACCOUNT,
                        LedgerEntryType.CREDIT,
                        amount,
                        currency,
                        occurredAt
                )
        ));
    }

    public List<LedgerEntry> entries() {
        return entries;
    }

    public boolean isBalanced() {
        BigDecimal debitTotal = total(LedgerEntryType.DEBIT);
        BigDecimal creditTotal = total(LedgerEntryType.CREDIT);
        return debitTotal.compareTo(creditTotal) == 0;
    }

    private BigDecimal total(LedgerEntryType entryType) {
        return entries.stream()
                .filter(entry -> entry.entryType() == entryType)
                .map(LedgerEntry::amount)
                .reduce(BigDecimal.ZERO, BigDecimal::add);
    }
}
