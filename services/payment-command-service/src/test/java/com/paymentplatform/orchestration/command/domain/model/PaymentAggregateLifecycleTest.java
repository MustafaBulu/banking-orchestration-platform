package com.paymentplatform.orchestration.command.domain.model;

import com.paymentplatform.orchestration.command.domain.event.PaymentEvent;
import com.paymentplatform.orchestration.command.domain.exception.InvalidPaymentStateTransitionException;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Currency;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PaymentAggregateLifecycleTest {

    @Test
    void drivesCreatedAuthorizedCapturedLifecycle() {
        PaymentAggregate payment = newPayment();

        assertThat(payment.status()).isEqualTo(PaymentStatus.CREATED);

        payment.authorize();
        payment.capture();

        assertThat(payment.status()).isEqualTo(PaymentStatus.CAPTURED);
        assertThat(payment.uncommittedEvents())
                .extracting(PaymentEvent::eventType)
                .containsExactly("PaymentCreated", "PaymentAuthorized", "PaymentCaptured");
    }

    @Test
    void rehydratesStateFromEventStream() {
        PaymentAggregate payment = newPayment();
        payment.authorize();
        payment.capture();

        PaymentAggregate rehydrated = PaymentAggregate.rehydrate(payment.uncommittedEvents());

        assertThat(rehydrated.paymentId()).isEqualTo(payment.paymentId());
        assertThat(rehydrated.customerId()).isEqualTo(payment.customerId());
        assertThat(rehydrated.status()).isEqualTo(PaymentStatus.CAPTURED);
        assertThat(rehydrated.uncommittedEvents()).isEmpty();
    }

    @Test
    void voidsOnlyAuthorizedPayment() {
        PaymentAggregate payment = newPayment();
        payment.authorize();

        payment.voidAuthorization();

        assertThat(payment.status()).isEqualTo(PaymentStatus.VOIDED);
        assertThat(payment.uncommittedEvents())
                .extracting(PaymentEvent::eventType)
                .containsExactly("PaymentCreated", "PaymentAuthorized", "PaymentVoided");
    }

    @Test
    void refundsOnlyCapturedPayment() {
        PaymentAggregate payment = newPayment();
        payment.authorize();
        payment.capture();

        payment.refund();

        assertThat(payment.status()).isEqualTo(PaymentStatus.REFUNDED);
        assertThat(payment.uncommittedEvents())
                .extracting(PaymentEvent::eventType)
                .containsExactly("PaymentCreated", "PaymentAuthorized", "PaymentCaptured", "PaymentRefunded");
    }

    @Test
    void rejectsIllegalTransitions() {
        PaymentAggregate created = newPayment();
        assertThatThrownBy(created::capture)
                .isInstanceOf(InvalidPaymentStateTransitionException.class);
        assertThatThrownBy(created::refund)
                .isInstanceOf(InvalidPaymentStateTransitionException.class);

        PaymentAggregate authorized = newPayment();
        authorized.authorize();
        assertThatThrownBy(authorized::authorize)
                .isInstanceOf(InvalidPaymentStateTransitionException.class);
        assertThatThrownBy(authorized::refund)
                .isInstanceOf(InvalidPaymentStateTransitionException.class);

        PaymentAggregate captured = newPayment();
        captured.authorize();
        captured.capture();
        assertThatThrownBy(captured::capture)
                .isInstanceOf(InvalidPaymentStateTransitionException.class);
        assertThatThrownBy(captured::voidAuthorization)
                .isInstanceOf(InvalidPaymentStateTransitionException.class);

        PaymentAggregate voided = newPayment();
        voided.authorize();
        voided.voidAuthorization();
        assertThatThrownBy(voided::capture)
                .isInstanceOf(InvalidPaymentStateTransitionException.class);

        PaymentAggregate refunded = PaymentAggregate.rehydrate(List.of(
                newPayment().uncommittedEvents().getFirst()
        ));
        refunded.authorize();
        refunded.capture();
        refunded.refund();
        assertThatThrownBy(refunded::refund)
                .isInstanceOf(InvalidPaymentStateTransitionException.class);
    }

    private PaymentAggregate newPayment() {
        return PaymentAggregate.create(
                "payment-1",
                "customer-1",
                new Money(new BigDecimal("120.50"), Currency.getInstance("EUR"))
        );
    }
}
