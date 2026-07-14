package com.paymentplatform.orchestration.command.application.port.out;

import com.paymentplatform.orchestration.command.domain.model.Money;

public interface AcquirerPort {

    AcquirerResult authorize(String paymentId, Money money);

    AcquirerResult capture(String paymentId, Money money);

    AcquirerResult voidAuthorization(String paymentId);

    AcquirerResult refund(String paymentId, Money money);

    enum Outcome {
        APPROVED,
        DECLINED,
        UNKNOWN
    }

    record AcquirerResult(Outcome outcome, String reasonCode, String acquirerRef) {
    }
}
