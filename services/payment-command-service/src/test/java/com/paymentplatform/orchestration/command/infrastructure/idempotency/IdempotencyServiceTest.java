package com.paymentplatform.orchestration.command.infrastructure.idempotency;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.paymentplatform.orchestration.command.adapters.in.rest.CreatePaymentRequest;
import com.paymentplatform.orchestration.command.adapters.in.rest.CreatePaymentResponse;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.ResultSet;
import java.util.HexFormat;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class IdempotencyServiceTest {

    private final ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules();

    @Test
    void shouldExecuteActionAndStoreResponseForNewKey() {
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        CreatePaymentRequest request = request();
        mockRecord(jdbcTemplate, hash(request), null);

        IdempotencyService service = new IdempotencyService(jdbcTemplate, objectMapper, new SimpleMeterRegistry());
        CreatePaymentResponse response = service.execute(
                "key-1",
                request,
                CreatePaymentResponse.class,
                () -> new CreatePaymentResponse("payment-1", "CREATED")
        );

        assertEquals("payment-1", response.paymentId());
    }

    @Test
    void shouldReturnStoredResponseForSameKeyAndRequestBody() {
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        CreatePaymentRequest request = request();
        CreatePaymentResponse storedResponse = new CreatePaymentResponse("payment-1", "CREATED");
        mockRecord(jdbcTemplate, hash(request), serialize(storedResponse));

        AtomicInteger actionCalls = new AtomicInteger();
        IdempotencyService service = new IdempotencyService(jdbcTemplate, objectMapper, new SimpleMeterRegistry());

        CreatePaymentResponse response = service.execute(
                "key-1",
                request,
                CreatePaymentResponse.class,
                () -> {
                    actionCalls.incrementAndGet();
                    return new CreatePaymentResponse("payment-2", "CREATED");
                }
        );

        assertEquals("payment-1", response.paymentId());
        assertEquals(0, actionCalls.get());
    }

    @Test
    void shouldRejectSameKeyWithDifferentRequestBody() {
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        mockRecord(jdbcTemplate, "different-hash", null);

        IdempotencyService service = new IdempotencyService(jdbcTemplate, objectMapper, new SimpleMeterRegistry());
        CreatePaymentRequest request = request();

        Executable action = () -> service.execute(
                "key-1",
                request,
                CreatePaymentResponse.class,
                () -> new CreatePaymentResponse("payment-1", "CREATED")
        );
        assertThrows(IdempotencyConflictException.class, action);
    }

    private CreatePaymentRequest request() {
        return new CreatePaymentRequest("customer-1", new BigDecimal("120.50"), "EUR");
    }

    private String hash(Object value) {
        try {
            byte[] json = serialize(value).getBytes(StandardCharsets.UTF_8);
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(json);
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException("SHA-256 is not available", ex);
        }
    }

    private String serialize(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("Failed to serialize test value", ex);
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private void mockRecord(JdbcTemplate jdbcTemplate, String requestHash, String responseBody) {
        when(jdbcTemplate.query(anyString(), any(ResultSetExtractor.class), any())).thenAnswer(invocation -> {
            ResultSetExtractor extractor = invocation.getArgument(1);
            ResultSet resultSet = mock(ResultSet.class);
            when(resultSet.next()).thenReturn(true);
            when(resultSet.getString("request_hash")).thenReturn(requestHash);
            when(resultSet.getString("response_body")).thenReturn(responseBody);
            return extractor.extractData(resultSet);
        });
    }
}
