package com.paymentplatform.orchestration.limit.reservation;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.math.BigDecimal;

@ConfigurationProperties(prefix = "app.limit.reservations")
public record LimitReservationProperties(
        BigDecimal perPaymentLimit,
        long leaseTtlMs
) {
    public LimitReservationProperties {
        if (perPaymentLimit == null) {
            perPaymentLimit = new BigDecimal("5000");
        }
        if (leaseTtlMs <= 0) {
            leaseTtlMs = 900000;
        }
    }
}
