package com.paymentplatform.orchestration.command.adapters.out.postgres;

import com.paymentplatform.orchestration.command.application.port.out.PaymentSagaRepository;
import com.paymentplatform.orchestration.command.application.saga.PaymentSaga;
import com.paymentplatform.orchestration.command.application.saga.PaymentSagaCompensationStatus;
import com.paymentplatform.orchestration.command.application.saga.PaymentSagaStatus;
import com.paymentplatform.orchestration.command.application.saga.PaymentSagaStep;
import com.paymentplatform.orchestration.command.application.saga.PaymentSagaStepName;
import com.paymentplatform.orchestration.command.application.saga.PaymentSagaStepStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

@Repository
public class JdbcPaymentSagaRepository implements PaymentSagaRepository {

    private final JdbcTemplate jdbcTemplate;

    public JdbcPaymentSagaRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public void start(
            String sagaId,
            String paymentId,
            String customerId,
            BigDecimal amount,
            String currency,
            PaymentSagaStepName currentStep
    ) {
        String sql = """
                INSERT INTO payment_saga (
                    saga_id, payment_id, customer_id, amount, currency, current_step, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (payment_id) DO NOTHING
                """;
        jdbcTemplate.update(
                sql,
                sagaId,
                paymentId,
                customerId,
                amount,
                currency,
                currentStep.name(),
                PaymentSagaStatus.STARTED.name()
        );
    }

    @Override
    public void updateStatus(String sagaId, PaymentSagaStepName currentStep, PaymentSagaStatus status) {
        String sql = """
                UPDATE payment_saga
                SET current_step = ?,
                    status = ?,
                    updated_at = NOW()
                WHERE saga_id = ?
                """;
        jdbcTemplate.update(sql, currentStep.name(), status.name(), sagaId);
    }

    @Override
    public void recordReservation(String sagaId, String reservationId) {
        String sql = """
                UPDATE payment_saga
                SET reservation_id = ?,
                    updated_at = NOW()
                WHERE saga_id = ?
                """;
        jdbcTemplate.update(sql, reservationId, sagaId);
    }

    @Override
    public void recordStep(
            String sagaId,
            PaymentSagaStepName step,
            PaymentSagaStepStatus status,
            PaymentSagaCompensationStatus compensationStatus,
            int attempt,
            String lastError
    ) {
        String sql = """
                INSERT INTO payment_saga_step (
                    saga_id, step, status, compensation_status, attempt, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT (saga_id, step)
                DO UPDATE SET
                    status = EXCLUDED.status,
                    compensation_status = EXCLUDED.compensation_status,
                    attempt = EXCLUDED.attempt,
                    last_error = EXCLUDED.last_error,
                    updated_at = NOW()
                """;
        jdbcTemplate.update(
                sql,
                sagaId,
                step.name(),
                status.name(),
                compensationStatus.name(),
                attempt,
                lastError
        );
    }

    @Override
    public Optional<PaymentSaga> findBySagaId(String sagaId) {
        String sql = """
                SELECT saga_id, payment_id, customer_id, amount, currency, reservation_id,
                       current_step, status, created_at, updated_at
                FROM payment_saga
                WHERE saga_id = ?
                """;
        return jdbcTemplate.query(
                sql,
                (rs, rowNum) -> mapSaga(rs),
                sagaId
        ).stream().findFirst();
    }

    @Override
    public Optional<PaymentSaga> findByPaymentId(String paymentId) {
        String sql = """
                SELECT saga_id, payment_id, customer_id, amount, currency, reservation_id,
                       current_step, status, created_at, updated_at
                FROM payment_saga
                WHERE payment_id = ?
                """;
        return jdbcTemplate.query(
                sql,
                (rs, rowNum) -> mapSaga(rs),
                paymentId
        ).stream().findFirst();
    }

    @Override
    public List<PaymentSagaStep> findSteps(String sagaId) {
        String sql = """
                SELECT saga_id, step, status, compensation_status, attempt, last_error, created_at, updated_at
                FROM payment_saga_step
                WHERE saga_id = ?
                ORDER BY id
                """;
        return jdbcTemplate.query(
                sql,
                (rs, rowNum) -> new PaymentSagaStep(
                        rs.getString("saga_id"),
                        PaymentSagaStepName.valueOf(rs.getString("step")),
                        PaymentSagaStepStatus.valueOf(rs.getString("status")),
                        PaymentSagaCompensationStatus.valueOf(rs.getString("compensation_status")),
                        rs.getInt("attempt"),
                        rs.getString("last_error"),
                        rs.getTimestamp("created_at").toInstant(),
                        rs.getTimestamp("updated_at").toInstant()
                ),
                sagaId
        );
    }

    @Override
    public List<PaymentSaga> claimRecoverable(Duration olderThan, int batchSize, Duration lockFor, String ownerId) {
        String sql = """
                WITH candidate AS (
                    SELECT saga_id
                    FROM payment_saga
                    WHERE status IN ('STARTED', 'COMPENSATING')
                      AND updated_at <= ?
                      AND (recovery_locked_until IS NULL OR recovery_locked_until <= NOW())
                    ORDER BY updated_at
                    LIMIT ?
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE payment_saga saga
                SET recovery_owner = ?,
                    recovery_locked_until = ?
                FROM candidate
                WHERE saga.saga_id = candidate.saga_id
                RETURNING saga.saga_id, saga.payment_id, saga.customer_id, saga.amount, saga.currency,
                          saga.reservation_id, saga.current_step, saga.status, saga.created_at, saga.updated_at
                """;
        Instant cutoff = Instant.now().minus(olderThan);
        Instant lockedUntil = Instant.now().plus(lockFor);
        return jdbcTemplate.query(
                sql,
                (rs, rowNum) -> mapSaga(rs),
                Timestamp.from(cutoff),
                batchSize,
                ownerId,
                Timestamp.from(lockedUntil)
        );
    }

    private PaymentSaga mapSaga(java.sql.ResultSet rs) throws java.sql.SQLException {
        return new PaymentSaga(
                rs.getString("saga_id"),
                rs.getString("payment_id"),
                rs.getString("customer_id"),
                rs.getBigDecimal("amount"),
                rs.getString("currency"),
                rs.getString("reservation_id"),
                PaymentSagaStepName.valueOf(rs.getString("current_step")),
                PaymentSagaStatus.valueOf(rs.getString("status")),
                rs.getTimestamp("created_at").toInstant(),
                rs.getTimestamp("updated_at").toInstant()
        );
    }
}
