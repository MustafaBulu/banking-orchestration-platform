package com.paymentplatform.orchestration.command.infrastructure.idempotency;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.paymentplatform.orchestration.command.adapters.in.rest.CreatePaymentRequest;
import com.paymentplatform.orchestration.command.adapters.in.rest.CreatePaymentResponse;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.math.BigDecimal;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class IdempotencyServiceTest {

    private final ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules();
    private final IdempotencyStore store = mock(IdempotencyStore.class);
    private final IdempotencyService service =
            new IdempotencyService(store, objectMapper, new SimpleMeterRegistry());

    @Test
    void shouldExecuteActionAndStoreResponseWhenKeyIsOwned() {
        when(store.claim(anyString(), anyString(), any()))
                .thenReturn(new IdempotencyStore.Claim(IdempotencyStore.State.OWNED, null));

        CreatePaymentResponse response = service.execute(
                "key-1",
                request(),
                CreatePaymentResponse.class,
                () -> new CreatePaymentResponse("payment-1", "CAPTURED")
        );

        assertEquals("payment-1", response.paymentId());
        verify(store).recordResponse(eq("key-1"), anyString(), anyInt());
    }

    @Test
    void shouldReturnStoredResponseWithoutRunningActionOnReplay() {
        CreatePaymentResponse stored = new CreatePaymentResponse("payment-1", "CAPTURED");
        when(store.claim(anyString(), anyString(), any()))
                .thenReturn(new IdempotencyStore.Claim(IdempotencyStore.State.REPLAY, serialize(stored)));

        AtomicInteger actionCalls = new AtomicInteger();
        CreatePaymentResponse response = service.execute(
                "key-1",
                request(),
                CreatePaymentResponse.class,
                () -> {
                    actionCalls.incrementAndGet();
                    return new CreatePaymentResponse("payment-2", "CAPTURED");
                }
        );

        assertEquals("payment-1", response.paymentId());
        assertEquals(0, actionCalls.get());
        verify(store, never()).recordResponse(anyString(), anyString(), anyInt());
    }

    @Test
    void shouldRejectSameKeyWithDifferentRequestBody() {
        when(store.claim(anyString(), anyString(), any()))
                .thenReturn(new IdempotencyStore.Claim(IdempotencyStore.State.CONFLICT, null));

        Executable action = () -> service.execute(
                "key-1",
                request(),
                CreatePaymentResponse.class,
                () -> new CreatePaymentResponse("payment-1", "CAPTURED")
        );
        assertThrows(IdempotencyConflictException.class, action);
    }

    @Test
    void shouldRejectConcurrentInFlightRequest() {
        when(store.claim(anyString(), anyString(), any()))
                .thenReturn(new IdempotencyStore.Claim(IdempotencyStore.State.IN_PROGRESS, null));

        Executable action = () -> service.execute(
                "key-1",
                request(),
                CreatePaymentResponse.class,
                () -> new CreatePaymentResponse("payment-1", "CAPTURED")
        );
        assertThrows(IdempotencyInProgressException.class, action);
    }

    @Test
    void shouldDiscardClaimWhenActionFails() {
        when(store.claim(anyString(), anyString(), any()))
                .thenReturn(new IdempotencyStore.Claim(IdempotencyStore.State.OWNED, null));

        Executable action = () -> service.execute(
                "key-1",
                request(),
                CreatePaymentResponse.class,
                () -> {
                    throw new IllegalStateException("boom");
                }
        );

        assertThrows(IllegalStateException.class, action);
        verify(store).discard("key-1");
        verify(store, never()).recordResponse(anyString(), anyString(), anyInt());
    }

    private CreatePaymentRequest request() {
        return new CreatePaymentRequest("customer-1", new BigDecimal("120.50"), "EUR");
    }

    private String serialize(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("Failed to serialize test value", ex);
        }
    }
}
