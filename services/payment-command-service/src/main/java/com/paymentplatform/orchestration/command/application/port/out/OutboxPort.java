package com.paymentplatform.orchestration.command.application.port.out;

import com.paymentplatform.orchestration.command.domain.event.PaymentEvent;

public interface OutboxPort {

    void enqueue(PaymentEvent event);
}
