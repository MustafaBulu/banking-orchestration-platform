package com.paymentplatform.orchestration.command.domain.model;

import com.paymentplatform.orchestration.command.domain.event.PaymentAuthorizedEvent;
import com.paymentplatform.orchestration.command.domain.event.PaymentCapturedEvent;
import com.paymentplatform.orchestration.command.domain.event.PaymentCreatedEvent;
import com.paymentplatform.orchestration.command.domain.event.PaymentEvent;
import com.paymentplatform.orchestration.command.domain.event.PaymentRefundedEvent;
import com.paymentplatform.orchestration.command.domain.event.PaymentVoidedEvent;
import com.paymentplatform.orchestration.command.domain.exception.InvalidPaymentStateTransitionException;

import java.util.ArrayList;
import java.util.Currency;
import java.util.List;

public class PaymentAggregate {

    private final String paymentId;
    private final String customerId;
    private final Money amount;
    private final List<PaymentEvent> uncommittedEvents = new ArrayList<>();
    private PaymentStatus status;

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
        PaymentAggregate aggregate = empty(event);
        aggregate.recordThat(event);
        return aggregate;
    }

    public static PaymentAggregate create(PaymentCreatedEvent event) {
        return rehydrate(List.of(event));
    }

    public static PaymentAggregate rehydrate(List<PaymentEvent> events) {
        if (events == null || events.isEmpty()) {
            throw new IllegalArgumentException("Payment event stream is empty");
        }
        if (!(events.getFirst() instanceof PaymentCreatedEvent)) {
            throw new InvalidPaymentStateTransitionException("Payment event stream must start with PaymentCreated");
        }
        PaymentAggregate aggregate = empty(events.getFirst());
        events.forEach(aggregate::apply);
        return aggregate;
    }

    public void authorize() {
        requireStatus(PaymentStatus.CREATED, "authorize");
        recordThat(PaymentAuthorizedEvent.of(
                paymentId,
                customerId,
                amount.amount(),
                amount.currency().getCurrencyCode()
        ));
    }

    public void capture() {
        requireStatus(PaymentStatus.AUTHORIZED, "capture");
        recordThat(PaymentCapturedEvent.of(
                paymentId,
                customerId,
                amount.amount(),
                amount.currency().getCurrencyCode()
        ));
    }

    public void voidAuthorization() {
        requireStatus(PaymentStatus.AUTHORIZED, "void");
        recordThat(PaymentVoidedEvent.of(
                paymentId,
                customerId,
                amount.amount(),
                amount.currency().getCurrencyCode()
        ));
    }

    public void refund() {
        requireStatus(PaymentStatus.CAPTURED, "refund");
        recordThat(PaymentRefundedEvent.of(
                paymentId,
                customerId,
                amount.amount(),
                amount.currency().getCurrencyCode()
        ));
    }

    public List<PaymentEvent> uncommittedEvents() {
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

    public PaymentStatus status() {
        return status;
    }

    private static PaymentAggregate empty(PaymentEvent firstEvent) {
        Money money = new Money(firstEvent.amount(), Currency.getInstance(firstEvent.currency()));
        return new PaymentAggregate(firstEvent.aggregateId(), firstEvent.customerId(), money);
    }

    private void recordThat(PaymentEvent event) {
        apply(event);
        uncommittedEvents.add(event);
    }

    private void apply(PaymentEvent event) {
        if (!paymentId.equals(event.aggregateId())) {
            throw new InvalidPaymentStateTransitionException("Payment event belongs to a different aggregate");
        }
        status = switch (event.eventType()) {
            case "PaymentCreated" -> {
                if (status != null) {
                    throw new InvalidPaymentStateTransitionException("PaymentCreated must be the first event");
                }
                yield PaymentStatus.CREATED;
            }
            case "PaymentAuthorized" -> {
                requireStatus(PaymentStatus.CREATED, "authorize");
                yield PaymentStatus.AUTHORIZED;
            }
            case "PaymentCaptured" -> {
                requireStatus(PaymentStatus.AUTHORIZED, "capture");
                yield PaymentStatus.CAPTURED;
            }
            case "PaymentVoided" -> {
                requireStatus(PaymentStatus.AUTHORIZED, "void");
                yield PaymentStatus.VOIDED;
            }
            case "PaymentRefunded" -> {
                requireStatus(PaymentStatus.CAPTURED, "refund");
                yield PaymentStatus.REFUNDED;
            }
            default -> throw new InvalidPaymentStateTransitionException("Unsupported payment event: " + event.eventType());
        };
    }

    private void requireStatus(PaymentStatus expected, String action) {
        if (status != expected) {
            throw new InvalidPaymentStateTransitionException(
                    "Cannot " + action + " payment " + paymentId + " from status " + status
            );
        }
    }
}
