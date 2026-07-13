package com.paymentplatform.orchestration.command.application.port.out;

import com.paymentplatform.orchestration.command.domain.event.PaymentEvent;

import java.util.List;

public interface PaymentEventStorePort {

    void append(PaymentEvent event);

    List<PaymentEvent> load(String paymentId);
}
