package com.paymentplatform.orchestration.command.domain.event;

import com.paymentplatform.orchestration.common.domain.DomainEvent;

import java.math.BigDecimal;

public interface PaymentEvent extends DomainEvent {

    int EVENT_VERSION = 1;

    String customerId();

    BigDecimal amount();

    String currency();

    default int eventVersion() {
        return EVENT_VERSION;
    }
}
