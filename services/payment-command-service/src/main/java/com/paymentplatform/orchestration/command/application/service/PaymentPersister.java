package com.paymentplatform.orchestration.command.application.service;

import com.paymentplatform.orchestration.command.application.port.out.OutboxPort;
import com.paymentplatform.orchestration.command.application.port.out.PaymentEventStorePort;
import com.paymentplatform.orchestration.command.domain.event.PaymentEvent;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class PaymentPersister {

    private final PaymentEventStorePort paymentEventStorePort;
    private final OutboxPort outboxPort;

    public PaymentPersister(PaymentEventStorePort paymentEventStorePort, OutboxPort outboxPort) {
        this.paymentEventStorePort = paymentEventStorePort;
        this.outboxPort = outboxPort;
    }

    @Transactional
    public void persist(PaymentEvent event) {
        paymentEventStorePort.append(event);
        outboxPort.enqueue(event);
    }
}
