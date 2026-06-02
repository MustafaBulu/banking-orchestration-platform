package com.paymentplatform.orchestration.limit.grpc;

import com.paymentplatform.orchestration.contracts.limit.v1.LimitControlServiceGrpc;
import com.paymentplatform.orchestration.contracts.limit.v1.LimitReserveRequest;
import com.paymentplatform.orchestration.contracts.limit.v1.LimitReserveResponse;
import io.grpc.stub.StreamObserver;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.UUID;

@Service
public class LimitGrpcService extends LimitControlServiceGrpc.LimitControlServiceImplBase {

    @Override
    public void reserve(LimitReserveRequest request, StreamObserver<LimitReserveResponse> responseObserver) {
        BigDecimal amount = new BigDecimal(request.getAmount());
        boolean approved = amount.compareTo(new BigDecimal("5000")) < 0;
        LimitReserveResponse response = LimitReserveResponse.newBuilder()
                .setApproved(approved)
                .setReasonCode(approved ? "APPROVED" : "LIMIT_EXCEEDED")
                .setReservationId(approved ? "resv-" + UUID.randomUUID() : "")
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
