package com.paymentplatform.orchestration.command;

import com.paymentplatform.orchestration.command.application.command.CreatePaymentCommand;
import com.paymentplatform.orchestration.command.application.port.in.CreatePaymentUseCase;
import com.paymentplatform.orchestration.command.application.port.out.FraudCheckPort;
import com.paymentplatform.orchestration.command.application.port.out.LimitCheckPort;
import com.paymentplatform.orchestration.command.application.port.out.PaymentEventStorePort;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.math.BigDecimal;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ReservationCompensationIntegrationTest extends AbstractIntegrationTest {

    @MockitoBean
    private FraudCheckPort fraudCheckPort;

    @MockitoBean
    private LimitCheckPort limitCheckPort;

    @MockitoBean
    private PaymentEventStorePort paymentEventStorePort;

    @Autowired
    private CreatePaymentUseCase createPaymentUseCase;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    void releasesReservationWhenPersistenceFailsAfterReserve() {
        String reservationId = "resv-" + UUID.randomUUID();
        when(fraudCheckPort.evaluate(anyString(), anyString(), any()))
                .thenReturn(new FraudCheckPort.FraudCheckResult(true, "OK", 0));
        when(limitCheckPort.reserve(anyString(), anyString(), any()))
                .thenReturn(new LimitCheckPort.LimitCheckResult(true, "OK", reservationId));
        doThrow(new RuntimeException("event store unavailable"))
                .when(paymentEventStorePort).append(any());

        String paymentId = UUID.randomUUID().toString();
        assertThatThrownBy(() -> createPaymentUseCase.handle(new CreatePaymentCommand(
                paymentId, "customer-1", new BigDecimal("120.50"), "EUR")))
                .isInstanceOf(RuntimeException.class);

        verify(limitCheckPort).release(reservationId);

        Integer events = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM event_store WHERE stream_id = ?", Integer.class, paymentId);
        assertThat(events).isZero();
        Integer outbox = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM outbox WHERE aggregate_id = ?", Integer.class, paymentId);
        assertThat(outbox).isZero();
    }
}
