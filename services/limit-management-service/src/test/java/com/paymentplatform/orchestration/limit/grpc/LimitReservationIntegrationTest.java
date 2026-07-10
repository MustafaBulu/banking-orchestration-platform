package com.paymentplatform.orchestration.limit.grpc;

import com.paymentplatform.orchestration.contracts.limit.v1.LimitReleaseRequest;
import com.paymentplatform.orchestration.contracts.limit.v1.LimitReleaseResponse;
import com.paymentplatform.orchestration.contracts.limit.v1.LimitReserveRequest;
import com.paymentplatform.orchestration.contracts.limit.v1.LimitReserveResponse;
import com.paymentplatform.orchestration.limit.reservation.LimitReservationReaper;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class LimitReservationIntegrationTest extends AbstractIntegrationTest {

    @Autowired
    private LimitGrpcService service;

    @Autowired
    private LimitReservationReaper reaper;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @BeforeEach
    void cleanDatabase() {
        jdbcTemplate.update("TRUNCATE TABLE limit_reservation");
    }

    @Test
    void reservePersistsAndReleaseMarksReservationInactive() {
        String reservationId = reserve("payment-release", "1500");
        assertThat(status(reservationId)).isEqualTo("ACTIVE");
        assertThat(service.hasReservation(reservationId)).isTrue();

        LimitReleaseResponse response = release(reservationId);

        assertThat(response.getReleased()).isTrue();
        assertThat(status(reservationId)).isEqualTo("RELEASED");
        assertThat(service.hasReservation(reservationId)).isFalse();
    }

    @Test
    void reserveIsIdempotentForSamePaymentWhileActive() {
        String firstReservationId = reserve("payment-retry", "1500");
        String secondReservationId = reserve("payment-retry", "1500");

        assertThat(secondReservationId).isEqualTo(firstReservationId);
        assertThat(activeReservationCount("payment-retry")).isOne();
    }

    @Test
    void expiresReservationWhenReleaseIsNeverDelivered() throws Exception {
        String reservationId = reserve("payment-expire", "1500");
        assertThat(status(reservationId)).isEqualTo("ACTIVE");

        Thread.sleep(300);
        reaper.expireExpiredReservations();

        assertThat(status(reservationId)).isEqualTo("EXPIRED");
        assertThat(service.hasReservation(reservationId)).isFalse();
    }

    private String reserve(String paymentId, String amount) {
        Capturing<LimitReserveResponse> observer = new Capturing<>();
        service.reserve(LimitReserveRequest.newBuilder()
                .setPaymentId(paymentId)
                .setCustomerId("customer-1")
                .setAmount(amount)
                .setCurrency("EUR")
                .build(), observer);
        return observer.single().getReservationId();
    }

    private LimitReleaseResponse release(String reservationId) {
        Capturing<LimitReleaseResponse> observer = new Capturing<>();
        service.release(LimitReleaseRequest.newBuilder()
                .setReservationId(reservationId)
                .build(), observer);
        return observer.single();
    }

    private String status(String reservationId) {
        return jdbcTemplate.queryForObject(
                "SELECT status FROM limit_reservation WHERE reservation_id = ?",
                String.class,
                reservationId
        );
    }

    private Integer activeReservationCount(String paymentId) {
        return jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM limit_reservation WHERE payment_id = ? AND status = 'ACTIVE'",
                Integer.class,
                paymentId
        );
    }

    private static final class Capturing<T> implements StreamObserver<T> {
        private final List<T> values = new ArrayList<>();

        @Override
        public void onNext(T value) {
            values.add(value);
        }

        @Override
        public void onError(Throwable throwable) {
            throw new IllegalStateException(throwable);
        }

        @Override
        public void onCompleted() {
        }

        T single() {
            return values.getFirst();
        }
    }
}
