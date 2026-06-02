package com.paymentplatform.orchestration.command.application.port.out;

import com.paymentplatform.orchestration.command.domain.model.Money;

public interface FraudCheckPort {

    FraudCheckResult evaluate(String paymentId, String customerId, Money money);

    record FraudCheckResult(boolean approved, String reasonCode, int riskScore) {
    }
}
