package com.paymentplatform.orchestration.limit;

import com.paymentplatform.orchestration.limit.reservation.LimitReservationProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties(LimitReservationProperties.class)
public class LimitManagementServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(LimitManagementServiceApplication.class, args);
    }
}
