package com.paymentplatform.orchestration.command;

import com.paymentplatform.orchestration.command.application.port.out.FraudCheckPort;
import com.paymentplatform.orchestration.command.application.port.out.LimitCheckPort;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@AutoConfigureMockMvc
class GrpcOutsideTransactionIntegrationTest extends AbstractIntegrationTest {

    @MockitoBean
    private FraudCheckPort fraudCheckPort;

    @MockitoBean
    private LimitCheckPort limitCheckPort;

    @Autowired
    private MockMvc mockMvc;

    @Test
    void remoteChecksRunWithoutAnOpenDatabaseTransaction() throws Exception {
        AtomicBoolean fraudTransactionActive = new AtomicBoolean(true);
        AtomicBoolean limitTransactionActive = new AtomicBoolean(true);

        when(fraudCheckPort.evaluate(anyString(), anyString(), any())).thenAnswer(invocation -> {
            fraudTransactionActive.set(TransactionSynchronizationManager.isActualTransactionActive());
            return new FraudCheckPort.FraudCheckResult(true, "OK", 0);
        });
        when(limitCheckPort.reserve(anyString(), anyString(), any())).thenAnswer(invocation -> {
            limitTransactionActive.set(TransactionSynchronizationManager.isActualTransactionActive());
            return new LimitCheckPort.LimitCheckResult(true, "OK", "resv-1");
        });

        mockMvc.perform(post("/v1/payments")
                        .header("Idempotency-Key", UUID.randomUUID().toString())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"customerId\":\"customer-1\",\"amount\":120.50,\"currency\":\"EUR\"}"))
                .andExpect(status().isAccepted());

        assertThat(fraudTransactionActive).isFalse();
        assertThat(limitTransactionActive).isFalse();
    }
}
