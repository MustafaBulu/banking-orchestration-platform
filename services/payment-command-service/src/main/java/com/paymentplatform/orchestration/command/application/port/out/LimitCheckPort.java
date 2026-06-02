package com.paymentplatform.orchestration.command.application.port.out;

import com.paymentplatform.orchestration.command.domain.model.Money;

public interface LimitCheckPort {

    LimitCheckResult reserve(String paymentId, String customerId, Money money);

    record LimitCheckResult(boolean approved, String reasonCode, String reservationId) {
    }
}
