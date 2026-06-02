package com.paymentplatform.orchestration.command.adapters.in.rest;

import com.paymentplatform.orchestration.command.application.command.CreatePaymentCommand;
import com.paymentplatform.orchestration.command.application.port.in.CreatePaymentUseCase;
import com.paymentplatform.orchestration.command.infrastructure.idempotency.IdempotencyService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@RequestMapping("/v1/payments")
public class PaymentCommandController {

    private static final String IDEMPOTENCY_KEY_HEADER = "Idempotency-Key";

    private final CreatePaymentUseCase createPaymentUseCase;
    private final IdempotencyService idempotencyService;

    public PaymentCommandController(
            CreatePaymentUseCase createPaymentUseCase,
            IdempotencyService idempotencyService
    ) {
        this.createPaymentUseCase = createPaymentUseCase;
        this.idempotencyService = idempotencyService;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public CreatePaymentResponse create(
            @Valid @RequestBody CreatePaymentRequest request,
            @RequestHeader(value = IDEMPOTENCY_KEY_HEADER, required = false) String idempotencyKey
    ) {
        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            return createPayment(request);
        }
        return idempotencyService.execute(
                idempotencyKey,
                request,
                CreatePaymentResponse.class,
                () -> createPayment(request)
        );
    }

    private CreatePaymentResponse createPayment(CreatePaymentRequest request) {
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
