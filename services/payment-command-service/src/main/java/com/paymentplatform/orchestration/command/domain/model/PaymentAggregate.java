package com.paymentplatform.orchestration.command.domain.model;

import com.paymentplatform.orchestration.command.domain.event.PaymentCreatedEvent;

import java.util.ArrayList;
import java.util.Currency;
import java.util.List;

public class PaymentAggregate {

    private final String paymentId;
    private final String customerId;
    private final Money amount;
    private final List<PaymentCreatedEvent> uncommittedEvents = new ArrayList<>();

    private PaymentAggregate(String paymentId, String customerId, Money amount) {
        this.paymentId = paymentId;
        this.customerId = customerId;
        this.amount = amount;
    }

    public static PaymentAggregate create(String paymentId, String customerId, Money amount) {
        PaymentCreatedEvent event = PaymentCreatedEvent.of(
                paymentId,
                customerId,
                amount.amount(),
                amount.currency().getCurrencyCode()
        );
        PaymentAggregate aggregate = create(event);
        aggregate.uncommittedEvents.add(event);
        return aggregate;
    }

    public static PaymentAggregate create(PaymentCreatedEvent event) {
        Money money = new Money(event.amount(), Currency.getInstance(event.currency()));
        return new PaymentAggregate(event.aggregateId(), event.customerId(), money);
    }

    public List<PaymentCreatedEvent> uncommittedEvents() {
        return List.copyOf(uncommittedEvents);
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

}
