package com.paymentplatform.orchestration.command.infrastructure.grpc;

import com.paymentplatform.orchestration.contracts.fraud.v1.FraudControlServiceGrpc;
import com.paymentplatform.orchestration.contracts.limit.v1.LimitControlServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class GrpcClientConfig {

    @Bean(destroyMethod = "shutdownNow")
    public ManagedChannel fraudManagedChannel(
            @Value("${app.grpc.fraud.target:localhost:9091}") String target,
            GrpcClientTraceInterceptor traceInterceptor
    ) {
        return ManagedChannelBuilder.forTarget(target).usePlaintext().intercept(traceInterceptor).build();
    }

    @Bean(destroyMethod = "shutdownNow")
    public ManagedChannel limitManagedChannel(
            @Value("${app.grpc.limit.target:localhost:9094}") String target,
            GrpcClientTraceInterceptor traceInterceptor
    ) {
        return ManagedChannelBuilder.forTarget(target).usePlaintext().intercept(traceInterceptor).build();
    }

    @Bean
    public FraudControlServiceGrpc.FraudControlServiceBlockingStub fraudBlockingStub(ManagedChannel fraudManagedChannel) {
        return FraudControlServiceGrpc.newBlockingStub(fraudManagedChannel);
    }

    @Bean
    public LimitControlServiceGrpc.LimitControlServiceBlockingStub limitBlockingStub(ManagedChannel limitManagedChannel) {
        return LimitControlServiceGrpc.newBlockingStub(limitManagedChannel);
    }
}
