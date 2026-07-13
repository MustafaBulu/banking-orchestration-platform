package com.paymentplatform.orchestration.command.adapters.in.rest;

import com.paymentplatform.orchestration.common.api.ApiHeaders;
import com.paymentplatform.orchestration.command.application.port.in.CreatePaymentUseCase;
import com.paymentplatform.orchestration.command.infrastructure.idempotency.IdempotencyService;
import com.paymentplatform.orchestration.command.infrastructure.idempotency.InvalidIdempotencyKeyException;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.function.Supplier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class PaymentCommandControllerTest {

    private static final String REQUEST_BODY = """
            {
              "customerId": "customer-1",
              "amount": 120.50,
              "currency": "EUR"
            }
            """;

    @Test
    void shouldRejectCreatePaymentWithoutIdempotencyKey() throws Exception {
        CreatePaymentUseCase createPaymentUseCase = mock(CreatePaymentUseCase.class);
        IdempotencyService idempotencyService = mock(IdempotencyService.class);
        MockMvc mockMvc = mockMvc(createPaymentUseCase, idempotencyService);

        when(idempotencyService.execute(
                isNull(),
                any(CreatePaymentRequest.class),
                eq(CreatePaymentResponse.class),
                anySupplier()
        )).thenThrow(new InvalidIdempotencyKeyException("Idempotency-Key must not be blank"));

        mockMvc.perform(post("/v1/payments")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(REQUEST_BODY))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("INVALID_IDEMPOTENCY_KEY"));

        verifyNoInteractions(createPaymentUseCase);
    }

    @Test
    void shouldExecuteCreatePaymentWithIdempotencyKey() throws Exception {
        CreatePaymentUseCase createPaymentUseCase = mock(CreatePaymentUseCase.class);
        IdempotencyService idempotencyService = mock(IdempotencyService.class);
        MockMvc mockMvc = mockMvc(createPaymentUseCase, idempotencyService);

        when(idempotencyService.execute(
                eq("payment-key-1"),
                any(CreatePaymentRequest.class),
                eq(CreatePaymentResponse.class),
                anySupplier()
        )).thenReturn(new CreatePaymentResponse("payment-1", "CAPTURED"));

        mockMvc.perform(post("/v1/payments")
                        .header(ApiHeaders.idempotencyKey(), "payment-key-1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(REQUEST_BODY))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.paymentId").value("payment-1"))
                .andExpect(jsonPath("$.status").value("CAPTURED"));

        verify(idempotencyService).execute(
                eq("payment-key-1"),
                any(CreatePaymentRequest.class),
                eq(CreatePaymentResponse.class),
                anySupplier()
        );
        verifyNoInteractions(createPaymentUseCase);
    }

    private MockMvc mockMvc(CreatePaymentUseCase createPaymentUseCase, IdempotencyService idempotencyService) {
        PaymentCommandController controller = new PaymentCommandController(createPaymentUseCase, idempotencyService);
        return MockMvcBuilders.standaloneSetup(controller)
                .setControllerAdvice(new PaymentCommandExceptionHandler())
                .build();
    }

    private Supplier<CreatePaymentResponse> anySupplier() {
        return any();
    }
}
