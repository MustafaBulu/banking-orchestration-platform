package com.paymentplatform.orchestration.command.domain.model;

import com.paymentplatform.orchestration.command.domain.event.PaymentCreatedEvent;

import java.util.ArrayList;
import java.util.Currency;
import java.util.List;

public class PaymentAggregate {

    private final String paymentId;
    private final String customerId;
    private final Money amount;
    private PaymentStatus status;

    private PaymentAggregate(String paymentId, String customerId, Money amount, PaymentStatus status) {
        this.paymentId = paymentId;
        this.customerId = customerId;
        this.amount = amount;
        this.status = status;
    }

    public static PaymentAggregate create(PaymentCreatedEvent event) {
        Money money = new Money(event.amount(), Currency.getInstance(event.currency()));
        return new PaymentAggregate(event.aggregateId(), event.customerId(), money, PaymentStatus.CREATED);
    }

    public List<Object> uncommittedEvents() {
        return new ArrayList<>();
    }

    public String paymentId() {
        return paymentId;
    }

    public String customerId() {
        return customerId;
    }

    public Money amount() {
        return amount;
    }

    public PaymentStatus status() {
        return status;
    }
}
