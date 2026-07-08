package com.paymentplatform.orchestration.limit.grpc;

import com.paymentplatform.orchestration.contracts.limit.v1.LimitControlServiceGrpc;
import com.paymentplatform.orchestration.contracts.limit.v1.LimitReleaseRequest;
import com.paymentplatform.orchestration.contracts.limit.v1.LimitReleaseResponse;
import com.paymentplatform.orchestration.contracts.limit.v1.LimitReserveRequest;
import com.paymentplatform.orchestration.contracts.limit.v1.LimitReserveResponse;
import io.grpc.stub.StreamObserver;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class LimitGrpcService extends LimitControlServiceGrpc.LimitControlServiceImplBase {

    private static final BigDecimal LIMIT = new BigDecimal("5000");

    private final Map<String, BigDecimal> reservations = new ConcurrentHashMap<>();

    @Override
    public void reserve(LimitReserveRequest request, StreamObserver<LimitReserveResponse> responseObserver) {
        BigDecimal amount = new BigDecimal(request.getAmount());
        boolean approved = amount.compareTo(LIMIT) < 0;
        String reservationId = "";
        if (approved) {
            reservationId = "resv-" + UUID.randomUUID();
            reservations.put(reservationId, amount);
        }
        LimitReserveResponse response = LimitReserveResponse.newBuilder()
                .setApproved(approved)
                .setReasonCode(approved ? "APPROVED" : "LIMIT_EXCEEDED")
                .setReservationId(reservationId)
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void release(LimitReleaseRequest request, StreamObserver<LimitReleaseResponse> responseObserver) {
        boolean released = reservations.remove(request.getReservationId()) != null;
        LimitReleaseResponse response = LimitReleaseResponse.newBuilder()
                .setReleased(released)
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    boolean hasReservation(String reservationId) {
        return reservations.containsKey(reservationId);
    }
}
