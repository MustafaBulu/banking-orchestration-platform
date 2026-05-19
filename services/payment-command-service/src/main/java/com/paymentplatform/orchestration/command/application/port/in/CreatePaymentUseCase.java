package com.paymentplatform.orchestration.command.application.port.in;

import com.paymentplatform.orchestration.command.application.command.CreatePaymentCommand;

public interface CreatePaymentUseCase {

    String handle(CreatePaymentCommand command);
}
