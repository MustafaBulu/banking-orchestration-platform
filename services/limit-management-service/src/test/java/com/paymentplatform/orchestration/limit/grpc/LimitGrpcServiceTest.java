package com.paymentplatform.orchestration.limit.grpc;

import com.paymentplatform.orchestration.contracts.limit.v1.LimitReleaseRequest;
import com.paymentplatform.orchestration.contracts.limit.v1.LimitReleaseResponse;
import com.paymentplatform.orchestration.contracts.limit.v1.LimitReserveRequest;
import com.paymentplatform.orchestration.contracts.limit.v1.LimitReserveResponse;
import com.paymentplatform.orchestration.limit.reservation.LimitReservationProperties;
import com.paymentplatform.orchestration.limit.reservation.LimitReservationRepository;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class LimitGrpcServiceTest {

    private final LimitReservationRepository reservationRepository = mock(LimitReservationRepository.class);
    private final LimitGrpcService service = new LimitGrpcService(
            reservationRepository,
            new LimitReservationProperties(new BigDecimal("5000"), 900000)
    );

    @Test
    void reserveStoresApprovedRequests() {
        when(reservationRepository.reserve(
                eq("payment-1"),
                eq("customer-1"),
                eq(new BigDecimal("1500")),
                eq("EUR"),
                any(Instant.class)
        )).thenReturn("resv-1");

        String reservationId = reserve("payment-1", "1500");

        assertThat(reservationId).isEqualTo("resv-1");
    }

    @Test
    void releaseReturnsRepositoryResult() {
        when(reservationRepository.release("resv-1")).thenReturn(true);

        LimitReleaseResponse response = release("resv-1");

        assertThat(response.getReleased()).isTrue();
        verify(reservationRepository).release("resv-1");
    }

    @Test
    void reserveDoesNotTrackRejectedRequests() {
        String reservationId = reserve("payment-1", "9000");

        assertThat(reservationId).isEmpty();
        verify(reservationRepository, never()).reserve(any(), any(), any(), any(), any());
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
