package com.paymentplatform.orchestration.ledger.domain.model;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

public class LedgerPosting {

    private static final String CUSTOMER_AUTH_HOLD = "CUSTOMER_AUTH_HOLD";
    private static final String LIMIT_HOLD_LIABILITY = "LIMIT_HOLD_LIABILITY";
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
        return fromPaymentEvent("PaymentCreated", eventId, paymentId, customerId, amount, currency, occurredAt);
    }

    public static LedgerPosting fromPaymentEvent(
            String eventType,
            String eventId,
            String paymentId,
            String customerId,
            BigDecimal amount,
            String currency,
            Instant occurredAt
    ) {
        return switch (eventType) {
            case "PaymentCreated" -> new LedgerPosting(List.of());
            case "PaymentAuthorized" -> twoLinePosting(
                    eventId,
                    paymentId,
                    customerId,
                    CUSTOMER_AUTH_HOLD,
                    LedgerEntryType.DEBIT,
                    LIMIT_HOLD_LIABILITY,
                    LedgerEntryType.CREDIT,
                    amount,
                    currency,
                    occurredAt
            );
            case "PaymentCaptured" -> twoLinePosting(
                    eventId,
                    paymentId,
                    customerId,
                    SETTLEMENT_ACCOUNT,
                    LedgerEntryType.DEBIT,
                    CUSTOMER_ACCOUNT,
                    LedgerEntryType.CREDIT,
                    amount,
                    currency,
                    occurredAt
            ).and(
                    entry(eventId, paymentId, customerId, 3, LIMIT_HOLD_LIABILITY,
                            LedgerEntryType.DEBIT, amount, currency, occurredAt),
                    entry(eventId, paymentId, customerId, 4, CUSTOMER_AUTH_HOLD,
                            LedgerEntryType.CREDIT, amount, currency, occurredAt)
            );
            case "PaymentVoided" -> twoLinePosting(
                    eventId,
                    paymentId,
                    customerId,
                    LIMIT_HOLD_LIABILITY,
                    LedgerEntryType.DEBIT,
                    CUSTOMER_AUTH_HOLD,
                    LedgerEntryType.CREDIT,
                    amount,
                    currency,
                    occurredAt
            );
            case "PaymentRefunded" -> twoLinePosting(
                    eventId,
                    paymentId,
                    customerId,
                    CUSTOMER_ACCOUNT,
                    LedgerEntryType.DEBIT,
                    SETTLEMENT_ACCOUNT,
                    LedgerEntryType.CREDIT,
                    amount,
                    currency,
                    occurredAt
            );
            default -> new LedgerPosting(List.of());
        };
    }

    private static LedgerPosting twoLinePosting(
            String eventId,
            String paymentId,
            String customerId,
            String firstAccount,
            LedgerEntryType firstType,
            String secondAccount,
            LedgerEntryType secondType,
            BigDecimal amount,
            String currency,
            Instant occurredAt
    ) {
        return new LedgerPosting(List.of(
                entry(eventId, paymentId, customerId, 1, firstAccount, firstType, amount, currency, occurredAt),
                entry(eventId, paymentId, customerId, 2, secondAccount, secondType, amount, currency, occurredAt)
        ));
    }

    private LedgerPosting and(LedgerEntry... additionalEntries) {
        java.util.ArrayList<LedgerEntry> combined = new java.util.ArrayList<>(entries);
        combined.addAll(List.of(additionalEntries));
        return new LedgerPosting(combined);
    }

    private static LedgerEntry entry(
            String eventId,
            String paymentId,
            String customerId,
            int lineNumber,
            String account,
            LedgerEntryType type,
            BigDecimal amount,
            String currency,
            Instant occurredAt
    ) {
        return new LedgerEntry(
                UUID.randomUUID(),
                paymentId,
                customerId,
                eventId,
                lineNumber,
                account,
                type,
                amount,
                currency,
                occurredAt
        );
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
