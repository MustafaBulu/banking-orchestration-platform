package com.paymentplatform.orchestration.command;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.paymentplatform.orchestration.command.adapters.in.rest.CreatePaymentResponse;
import com.paymentplatform.orchestration.command.application.port.out.FraudCheckPort;
import com.paymentplatform.orchestration.command.application.port.out.LimitCheckPort;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@AutoConfigureMockMvc
class IdempotencyKeyReplayIntegrationTest extends AbstractIntegrationTest {

    @MockitoBean
    private FraudCheckPort fraudCheckPort;

    @MockitoBean
    private LimitCheckPort limitCheckPort;

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @BeforeEach
    void approveChecks() {
        when(fraudCheckPort.evaluate(anyString(), anyString(), any()))
                .thenReturn(new FraudCheckPort.FraudCheckResult(true, "OK", 0));
        when(limitCheckPort.reserve(anyString(), anyString(), any()))
                .thenReturn(new LimitCheckPort.LimitCheckResult(true, "OK", "reservation-1"));
    }

    @Test
    void sameKeyAndBodyCreatesOnePaymentAndReplaysStoredResponse() throws Exception {
        String key = UUID.randomUUID().toString();
        String customerId = "customer-" + UUID.randomUUID();
        String body = body(customerId, "120.50");

        String firstPaymentId = paymentId(submit(key, body).andExpect(status().isAccepted()).andReturn());
        String secondPaymentId = paymentId(submit(key, body).andExpect(status().isAccepted()).andReturn());

        assertThat(secondPaymentId).isEqualTo(firstPaymentId);

        Integer eventCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM event_store WHERE payload->>'customerId' = ?", Integer.class, customerId);
        assertThat(eventCount).isEqualTo(3);
    }

    @Test
    void sameKeyDifferentBodyReturnsConflict() throws Exception {
        String key = UUID.randomUUID().toString();
        String customerId = "customer-" + UUID.randomUUID();

        submit(key, body(customerId, "120.50")).andExpect(status().isAccepted());

        submit(key, body(customerId, "999.99"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.code").value("IDEMPOTENCY_CONFLICT"));

        Integer eventCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM event_store WHERE payload->>'customerId' = ?", Integer.class, customerId);
        assertThat(eventCount).isEqualTo(3);
    }

    private org.springframework.test.web.servlet.ResultActions submit(String idempotencyKey, String body) throws Exception {
        return mockMvc.perform(post("/v1/payments")
                .header("Idempotency-Key", idempotencyKey)
                .contentType(MediaType.APPLICATION_JSON)
                .content(body));
    }

    private String paymentId(MvcResult result) throws Exception {
        CreatePaymentResponse response = objectMapper.readValue(
                result.getResponse().getContentAsString(), CreatePaymentResponse.class);
        return response.paymentId();
    }

    private String body(String customerId, String amount) {
        return "{\"customerId\":\"%s\",\"amount\":%s,\"currency\":\"EUR\"}".formatted(customerId, amount);
    }
}
