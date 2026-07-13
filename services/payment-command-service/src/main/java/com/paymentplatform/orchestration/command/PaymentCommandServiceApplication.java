package com.paymentplatform.orchestration.command;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;
import com.paymentplatform.orchestration.command.application.saga.PaymentSagaProperties;
import com.paymentplatform.orchestration.command.infrastructure.outbox.OutboxRelayProperties;
import com.paymentplatform.orchestration.command.infrastructure.resilience.GrpcResilienceProperties;

@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties({OutboxRelayProperties.class, GrpcResilienceProperties.class, PaymentSagaProperties.class})
public class PaymentCommandServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(PaymentCommandServiceApplication.class, args);
    }
}
