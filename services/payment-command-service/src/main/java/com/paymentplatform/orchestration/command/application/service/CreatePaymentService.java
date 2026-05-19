package com.paymentplatform.orchestration.command.application.service;

import com.paymentplatform.orchestration.command.application.command.CreatePaymentCommand;
import com.paymentplatform.orchestration.command.application.port.in.CreatePaymentUseCase;
import com.paymentplatform.orchestration.command.application.port.out.PaymentEventStorePort;
import com.paymentplatform.orchestration.command.domain.event.PaymentCreatedEvent;
import org.springframework.stereotype.Service;

@Service
public class CreatePaymentService implements CreatePaymentUseCase {

    private final PaymentEventStorePort paymentEventStorePort;

    public CreatePaymentService(PaymentEventStorePort paymentEventStorePort) {
        this.paymentEventStorePort = paymentEventStorePort;
    }

    @Override
    public String handle(CreatePaymentCommand command) {
        PaymentCreatedEvent event = PaymentCreatedEvent.of(
                command.paymentId(),
                command.customerId(),
                command.amount(),
                command.currency()
        );
        paymentEventStorePort.append(event);
        return event.aggregateId();
    }
}
