package com.paymentplatform.orchestration.command.application.service;

import com.paymentplatform.orchestration.command.application.command.CreatePaymentCommand;
import com.paymentplatform.orchestration.command.application.port.in.CreatePaymentUseCase;
import com.paymentplatform.orchestration.command.application.port.out.FraudCheckPort;
import com.paymentplatform.orchestration.command.application.port.out.LimitCheckPort;
import com.paymentplatform.orchestration.command.domain.exception.PaymentRejectedException;
import com.paymentplatform.orchestration.command.domain.event.PaymentCreatedEvent;
import com.paymentplatform.orchestration.command.domain.model.Money;
import com.paymentplatform.orchestration.command.domain.model.PaymentAggregate;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Service;

import java.util.Currency;

@Service
public class CreatePaymentService implements CreatePaymentUseCase {

    private final PaymentPersister paymentPersister;
    private final FraudCheckPort fraudCheckPort;
    private final LimitCheckPort limitCheckPort;
    private final Counter paymentsCreatedCounter;
    private final Counter fraudRejectedCounter;
    private final Counter limitRejectedCounter;
    private final Counter reservationCompensatedCounter;

    public CreatePaymentService(
            PaymentPersister paymentPersister,
            FraudCheckPort fraudCheckPort,
            LimitCheckPort limitCheckPort,
            MeterRegistry meterRegistry
    ) {
        this.paymentPersister = paymentPersister;
        this.fraudCheckPort = fraudCheckPort;
        this.limitCheckPort = limitCheckPort;
        this.paymentsCreatedCounter = Counter.builder("payments.created")
                .description("Accepted payment commands")
                .register(meterRegistry);
        this.fraudRejectedCounter = Counter.builder("payments.rejected")
                .description("Rejected payment commands")
                .tag("reason", "fraud")
                .register(meterRegistry);
        this.limitRejectedCounter = Counter.builder("payments.rejected")
                .description("Rejected payment commands")
                .tag("reason", "limit")
                .register(meterRegistry);
        this.reservationCompensatedCounter = Counter.builder("limit.reservation.compensated")
                .description("Limit reservations released after a downstream failure")
                .register(meterRegistry);
    }

    @Override
    public String handle(CreatePaymentCommand command) {
        Money money = new Money(command.amount(), Currency.getInstance(command.currency()));

        FraudCheckPort.FraudCheckResult fraudResult =
                fraudCheckPort.evaluate(command.paymentId(), command.customerId(), money);
        if (!fraudResult.approved()) {
            fraudRejectedCounter.increment();
            throw new PaymentRejectedException("Fraud check rejected: " + fraudResult.reasonCode());
        }

        LimitCheckPort.LimitCheckResult limitResult =
                limitCheckPort.reserve(command.paymentId(), command.customerId(), money);
        if (!limitResult.approved()) {
            limitRejectedCounter.increment();
            throw new PaymentRejectedException("Limit check rejected: " + limitResult.reasonCode());
        }

        PaymentAggregate aggregate = PaymentAggregate.create(
                command.paymentId(),
                command.customerId(),
                money
        );
        PaymentCreatedEvent event = aggregate.uncommittedEvents().getFirst();
        try {
            paymentPersister.persist(event);
        } catch (RuntimeException ex) {
            limitCheckPort.release(limitResult.reservationId());
            reservationCompensatedCounter.increment();
            throw ex;
        }
        paymentsCreatedCounter.increment();
        return aggregate.paymentId();
    }
}
