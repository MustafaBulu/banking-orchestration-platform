package com.paymentplatform.orchestration.command.adapters.in.rest;

import com.paymentplatform.orchestration.command.application.command.CreatePaymentCommand;
import com.paymentplatform.orchestration.command.application.port.in.CreatePaymentUseCase;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@RequestMapping("/v1/payments")
public class PaymentCommandController {

    private final CreatePaymentUseCase createPaymentUseCase;

    public PaymentCommandController(CreatePaymentUseCase createPaymentUseCase) {
        this.createPaymentUseCase = createPaymentUseCase;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public CreatePaymentResponse create(@Valid @RequestBody CreatePaymentRequest request) {
        String paymentId = UUID.randomUUID().toString();
        CreatePaymentCommand command = new CreatePaymentCommand(
                paymentId,
                request.customerId(),
                request.amount(),
                request.currency()
        );
        String aggregateId = createPaymentUseCase.handle(command);
        return new CreatePaymentResponse(aggregateId, "CREATED");
    }
}
