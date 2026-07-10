package com.paymentplatform.orchestration.limit.reservation;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class LimitReservationReaper {

    private final LimitReservationRepository repository;
    private final Counter expiredCounter;

    public LimitReservationReaper(LimitReservationRepository repository, MeterRegistry meterRegistry) {
        this.repository = repository;
        this.expiredCounter = Counter.builder("limit.reservation.expired")
                .description("Limit reservations expired by the lease reaper")
                .register(meterRegistry);
    }

    @Scheduled(fixedDelayString = "${app.limit.reservations.reaper-delay-ms:5000}")
    public void expireExpiredReservations() {
        int expired = repository.expireExpiredReservations();
        if (expired > 0) {
            expiredCounter.increment(expired);
        }
    }
}
