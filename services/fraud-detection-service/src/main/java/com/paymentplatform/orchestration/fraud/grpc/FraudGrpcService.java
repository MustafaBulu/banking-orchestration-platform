package com.paymentplatform.orchestration.fraud.grpc;

import com.paymentplatform.orchestration.contracts.fraud.v1.FraudCheckRequest;
import com.paymentplatform.orchestration.contracts.fraud.v1.FraudCheckResponse;
import com.paymentplatform.orchestration.contracts.fraud.v1.FraudControlServiceGrpc;
import io.grpc.stub.StreamObserver;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;

@Service
public class FraudGrpcService extends FraudControlServiceGrpc.FraudControlServiceImplBase {

    @Override
    public void evaluate(FraudCheckRequest request, StreamObserver<FraudCheckResponse> responseObserver) {
        BigDecimal amount = new BigDecimal(request.getAmount());
        boolean approved = amount.compareTo(new BigDecimal("10000")) < 0;
        FraudCheckResponse response = FraudCheckResponse.newBuilder()
                .setApproved(approved)
                .setReasonCode(approved ? "APPROVED" : "AMOUNT_RISK_THRESHOLD")
                .setRiskScore(approved ? 15 : 92)
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
