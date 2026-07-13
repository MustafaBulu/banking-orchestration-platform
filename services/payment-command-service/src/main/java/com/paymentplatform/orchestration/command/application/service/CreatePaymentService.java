package com.paymentplatform.orchestration.command.application.service;

import com.paymentplatform.orchestration.command.application.command.CreatePaymentCommand;
import com.paymentplatform.orchestration.command.application.port.in.CreatePaymentUseCase;
import com.paymentplatform.orchestration.command.application.saga.PaymentSagaOrchestrator;
import com.paymentplatform.orchestration.command.domain.exception.PaymentRejectedException;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Service;

@Service
public class CreatePaymentService implements CreatePaymentUseCase {

    private final PaymentSagaOrchestrator paymentSagaOrchestrator;
    private final Counter paymentsCreatedCounter;
    private final Counter fraudRejectedCounter;
    private final Counter limitRejectedCounter;

    public CreatePaymentService(
            PaymentSagaOrchestrator paymentSagaOrchestrator,
            MeterRegistry meterRegistry
    ) {
        this.paymentSagaOrchestrator = paymentSagaOrchestrator;
        this.paymentsCreatedCounter = Counter.builder("payments.created")
                .description("Accepted payment commands")
                .register(meterRegistry);
        this.fraudRejectedCounter = Counter.builder("payments.rejected")
                .description("Rejected payment commands")
                .tag("reason", "fraud")
                .register(meterRegistry);
        this.limitRejectedCounter = Counter.builder("payments.rejected")
                .description("Rejected payment commands")
                .tag("reason", "limit")
                .register(meterRegistry);
    }

    @Override
    public String handle(CreatePaymentCommand command) {
        try {
            String paymentId = paymentSagaOrchestrator.start(command);
            paymentsCreatedCounter.increment();
            return paymentId;
        } catch (PaymentRejectedException ex) {
            if (ex.getMessage() != null && ex.getMessage().startsWith("Fraud check rejected")) {
                fraudRejectedCounter.increment();
            } else if (ex.getMessage() != null && ex.getMessage().startsWith("Limit check rejected")) {
                limitRejectedCounter.increment();
            }
            throw ex;
        }
    }
}
