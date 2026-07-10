package com.paymentplatform.orchestration.fraud.grpc;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

@Component
public class FraudGrpcServer implements SmartLifecycle {

    private final int port;
    private final FraudGrpcService fraudGrpcService;
    private final GrpcServerTraceInterceptor traceInterceptor;
    private Server server;
    private volatile boolean running;

    public FraudGrpcServer(
            @Value("${app.grpc.port:9091}") int port,
            FraudGrpcService fraudGrpcService,
            GrpcServerTraceInterceptor traceInterceptor
    ) {
        this.port = port;
        this.fraudGrpcService = fraudGrpcService;
        this.traceInterceptor = traceInterceptor;
    }

    @Override
    public void start() {
        try {
            server = ServerBuilder.forPort(port)
                    .intercept(traceInterceptor)
                    .addService(fraudGrpcService)
                    .build()
                    .start();
            running = true;
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to start fraud gRPC server", ex);
        }
    }

    @Override
    public void stop() {
        if (server != null) {
            server.shutdownNow();
        }
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }
}
