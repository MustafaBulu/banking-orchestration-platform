package com.paymentplatform.orchestration.command.application.service;

import com.paymentplatform.orchestration.command.application.command.CreatePaymentCommand;
import com.paymentplatform.orchestration.command.application.port.in.CreatePaymentUseCase;
import com.paymentplatform.orchestration.command.application.port.out.OutboxPort;
import com.paymentplatform.orchestration.command.application.port.out.PaymentEventStorePort;
import com.paymentplatform.orchestration.command.domain.event.PaymentCreatedEvent;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class CreatePaymentService implements CreatePaymentUseCase {

    private final PaymentEventStorePort paymentEventStorePort;
    private final OutboxPort outboxPort;

    public CreatePaymentService(
            PaymentEventStorePort paymentEventStorePort,
            OutboxPort outboxPort
    ) {
        this.paymentEventStorePort = paymentEventStorePort;
        this.outboxPort = outboxPort;
    }

    @Override
    @Transactional
    public String handle(CreatePaymentCommand command) {
        PaymentCreatedEvent event = PaymentCreatedEvent.of(
                command.paymentId(),
                command.customerId(),
                command.amount(),
                command.currency()
        );
        paymentEventStorePort.append(event);
        outboxPort.enqueue(event);
        return event.aggregateId();
    }
}
