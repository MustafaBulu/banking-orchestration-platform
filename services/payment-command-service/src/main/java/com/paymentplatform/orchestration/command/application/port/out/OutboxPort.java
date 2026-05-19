package com.paymentplatform.orchestration.command.application.port.out;

import com.paymentplatform.orchestration.command.domain.event.PaymentCreatedEvent;

public interface OutboxPort {

    void enqueue(PaymentCreatedEvent event);
}
