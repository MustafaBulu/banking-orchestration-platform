package com.paymentplatform.orchestration.command.application.service;

import com.paymentplatform.orchestration.command.application.command.CreatePaymentCommand;
import com.paymentplatform.orchestration.command.application.port.in.CreatePaymentUseCase;
import com.paymentplatform.orchestration.command.application.port.out.FraudCheckPort;
import com.paymentplatform.orchestration.command.application.port.out.LimitCheckPort;
import com.paymentplatform.orchestration.command.application.port.out.OutboxPort;
import com.paymentplatform.orchestration.command.application.port.out.PaymentEventStorePort;
import com.paymentplatform.orchestration.command.domain.exception.PaymentRejectedException;
import com.paymentplatform.orchestration.command.domain.event.PaymentCreatedEvent;
import com.paymentplatform.orchestration.command.domain.model.Money;
import com.paymentplatform.orchestration.command.domain.model.PaymentAggregate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Currency;

@Service
public class CreatePaymentService implements CreatePaymentUseCase {

    private final PaymentEventStorePort paymentEventStorePort;
    private final OutboxPort outboxPort;
    private final FraudCheckPort fraudCheckPort;
    private final LimitCheckPort limitCheckPort;

    public CreatePaymentService(
            PaymentEventStorePort paymentEventStorePort,
            OutboxPort outboxPort,
            FraudCheckPort fraudCheckPort,
            LimitCheckPort limitCheckPort
    ) {
        this.paymentEventStorePort = paymentEventStorePort;
        this.outboxPort = outboxPort;
        this.fraudCheckPort = fraudCheckPort;
        this.limitCheckPort = limitCheckPort;
    }

    @Override
    @Transactional
    public String handle(CreatePaymentCommand command) {
        Money money = new Money(command.amount(), Currency.getInstance(command.currency()));
        FraudCheckPort.FraudCheckResult fraudResult =
                fraudCheckPort.evaluate(command.paymentId(), command.customerId(), money);
        if (!fraudResult.approved()) {
            throw new PaymentRejectedException("Fraud check rejected: " + fraudResult.reasonCode());
        }

        LimitCheckPort.LimitCheckResult limitResult =
                limitCheckPort.reserve(command.paymentId(), command.customerId(), money);
        if (!limitResult.approved()) {
            throw new PaymentRejectedException("Limit check rejected: " + limitResult.reasonCode());
        }

        PaymentAggregate aggregate = PaymentAggregate.create(
                command.paymentId(),
                command.customerId(),
                money
        );
        PaymentCreatedEvent event = aggregate.uncommittedEvents().get(0);
        paymentEventStorePort.append(event);
        outboxPort.enqueue(event);
        return aggregate.paymentId();
    }
}
