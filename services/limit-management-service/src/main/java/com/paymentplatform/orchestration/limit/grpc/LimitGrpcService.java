package com.paymentplatform.orchestration.limit.grpc;

import com.paymentplatform.orchestration.contracts.limit.v1.LimitControlServiceGrpc;
import com.paymentplatform.orchestration.contracts.limit.v1.LimitReleaseRequest;
import com.paymentplatform.orchestration.contracts.limit.v1.LimitReleaseResponse;
import com.paymentplatform.orchestration.contracts.limit.v1.LimitReserveRequest;
import com.paymentplatform.orchestration.contracts.limit.v1.LimitReserveResponse;
import com.paymentplatform.orchestration.limit.reservation.LimitReservationProperties;
import com.paymentplatform.orchestration.limit.reservation.LimitReservationRepository;
import io.grpc.stub.StreamObserver;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.Instant;

@Service
public class LimitGrpcService extends LimitControlServiceGrpc.LimitControlServiceImplBase {

    private final LimitReservationRepository reservationRepository;
    private final LimitReservationProperties properties;

    public LimitGrpcService(
            LimitReservationRepository reservationRepository,
            LimitReservationProperties properties
    ) {
        this.reservationRepository = reservationRepository;
        this.properties = properties;
    }

    @Override
    public void reserve(LimitReserveRequest request, StreamObserver<LimitReserveResponse> responseObserver) {
        BigDecimal amount = new BigDecimal(request.getAmount());
        boolean approved = amount.compareTo(properties.perPaymentLimit()) < 0;
        String reservationId = "";
        if (approved) {
            reservationId = reservationRepository.reserve(
                    request.getPaymentId(),
                    request.getCustomerId(),
                    amount,
                    request.getCurrency(),
                    Instant.now().plusMillis(properties.leaseTtlMs())
            );
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
        boolean released = reservationRepository.release(request.getReservationId());
        LimitReleaseResponse response = LimitReleaseResponse.newBuilder()
                .setReleased(released)
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    boolean hasReservation(String reservationId) {
        return reservationRepository.hasActiveReservation(reservationId);
    }
}
