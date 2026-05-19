package com.paymentplatform.orchestration.command.adapters.out.memory;

import com.paymentplatform.orchestration.command.application.port.out.PaymentEventStorePort;
import com.paymentplatform.orchestration.command.domain.event.PaymentCreatedEvent;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Component
public class InMemoryPaymentEventStoreAdapter implements PaymentEventStorePort {

    private final List<PaymentCreatedEvent> events = new ArrayList<>();

    @Override
    public void append(PaymentCreatedEvent event) {
        events.add(event);
    }

    public List<PaymentCreatedEvent> events() {
        return Collections.unmodifiableList(events);
    }
}
