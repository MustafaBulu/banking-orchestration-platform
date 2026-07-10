package com.paymentplatform.orchestration.limit.grpc;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

@Component
public class LimitGrpcServer implements SmartLifecycle {

    private final int port;
    private final LimitGrpcService limitGrpcService;
    private final GrpcServerTraceInterceptor traceInterceptor;
    private Server server;
    private volatile boolean running;

    public LimitGrpcServer(
            @Value("${app.grpc.port:9094}") int port,
            LimitGrpcService limitGrpcService,
            GrpcServerTraceInterceptor traceInterceptor
    ) {
        this.port = port;
        this.limitGrpcService = limitGrpcService;
        this.traceInterceptor = traceInterceptor;
    }

    @Override
    public void start() {
        try {
            server = ServerBuilder.forPort(port)
                    .intercept(traceInterceptor)
                    .addService(limitGrpcService)
                    .build()
                    .start();
            running = true;
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to start limit gRPC server", ex);
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
