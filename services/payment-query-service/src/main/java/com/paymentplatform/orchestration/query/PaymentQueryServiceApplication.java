package com.paymentplatform.orchestration.query;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@EnableKafka
@SpringBootApplication
public class PaymentQueryServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(PaymentQueryServiceApplication.class, args);
    }
}
