package com.paymentplatform.orchestration.command;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;
import com.paymentplatform.orchestration.command.infrastructure.outbox.OutboxRelayProperties;

@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties(OutboxRelayProperties.class)
public class PaymentCommandServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(PaymentCommandServiceApplication.class, args);
    }
}
