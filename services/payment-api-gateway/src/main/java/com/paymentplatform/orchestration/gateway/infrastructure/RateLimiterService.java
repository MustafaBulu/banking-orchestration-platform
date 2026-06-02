package com.paymentplatform.orchestration.gateway.infrastructure;

import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class RateLimiterService {

    private final Map<String, CounterWindow> counters = new ConcurrentHashMap<>();

    public boolean allow(String key, int limitPerMinute) {
        long nowMinute = Instant.now().getEpochSecond() / 60;
        CounterWindow current = counters.compute(key, (k, v) -> {
            if (v == null || v.minute() != nowMinute) {
                return new CounterWindow(nowMinute, new AtomicInteger(0));
            }
            return v;
        });
        return current.counter().incrementAndGet() <= limitPerMinute;
    }

    private record CounterWindow(long minute, AtomicInteger counter) {
    }
}
