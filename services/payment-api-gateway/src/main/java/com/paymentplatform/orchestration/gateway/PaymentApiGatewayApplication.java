package com.paymentplatform.orchestration.gateway;

import com.paymentplatform.orchestration.gateway.config.GatewayProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(GatewayProperties.class)
public class PaymentApiGatewayApplication {

    public static void main(String[] args) {
        SpringApplication.run(PaymentApiGatewayApplication.class, args);
    }
}
