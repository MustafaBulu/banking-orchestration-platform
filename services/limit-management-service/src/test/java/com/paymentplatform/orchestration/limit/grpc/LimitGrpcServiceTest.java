package com.paymentplatform.orchestration.limit.grpc;

import com.paymentplatform.orchestration.contracts.limit.v1.LimitReleaseRequest;
import com.paymentplatform.orchestration.contracts.limit.v1.LimitReleaseResponse;
import com.paymentplatform.orchestration.contracts.limit.v1.LimitReserveRequest;
import com.paymentplatform.orchestration.contracts.limit.v1.LimitReserveResponse;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class LimitGrpcServiceTest {

    private final LimitGrpcService service = new LimitGrpcService();

    @Test
    void releaseRemovesAnActiveReservation() {
        String reservationId = reserve("1500");
        assertThat(service.hasReservation(reservationId)).isTrue();

        LimitReleaseResponse response = release(reservationId);

        assertThat(response.getReleased()).isTrue();
        assertThat(service.hasReservation(reservationId)).isFalse();
    }

    @Test
    void releaseIsNoOpForUnknownReservation() {
        LimitReleaseResponse response = release("resv-does-not-exist");

        assertThat(response.getReleased()).isFalse();
    }

    @Test
    void reserveDoesNotTrackRejectedRequests() {
        String reservationId = reserve("9000");

        assertThat(reservationId).isEmpty();
    }

    private String reserve(String amount) {
        Capturing<LimitReserveResponse> observer = new Capturing<>();
        service.reserve(LimitReserveRequest.newBuilder()
                .setPaymentId("payment-1")
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
