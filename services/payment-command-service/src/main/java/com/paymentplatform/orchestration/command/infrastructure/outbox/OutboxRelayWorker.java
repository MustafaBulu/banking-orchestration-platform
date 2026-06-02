package com.paymentplatform.orchestration.command.infrastructure.outbox;

import com.paymentplatform.orchestration.command.adapters.out.postgres.OutboxJdbcRepository;
import com.paymentplatform.orchestration.command.adapters.out.postgres.OutboxMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;

@Component
public class OutboxRelayWorker {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelayWorker.class);

    private final OutboxJdbcRepository outboxJdbcRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final OutboxRelayProperties properties;

    public OutboxRelayWorker(
            OutboxJdbcRepository outboxJdbcRepository,
            KafkaTemplate<String, String> kafkaTemplate,
            OutboxRelayProperties properties
    ) {
        this.outboxJdbcRepository = outboxJdbcRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.properties = properties;
    }

    @Scheduled(fixedDelayString = "${app.outbox.relay.poll-delay-ms:2000}")
    @Transactional
    public void relay() {
        if (!properties.enabled()) {
            return;
        }

        List<OutboxMessage> messages = outboxJdbcRepository.fetchBatchForUpdate(properties.batchSize());
        for (OutboxMessage message : messages) {
            try {
                kafkaTemplate.send(properties.topic(), message.eventId(), message.payload())
                        .get(properties.publishTimeoutMs(), TimeUnit.MILLISECONDS);
                outboxJdbcRepository.markPublished(message.id());
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                markRetryOrFailed(message, ex);
            } catch (ExecutionException | TimeoutException ex) {
                markRetryOrFailed(message, ex);
            }
        }
    }

    private void markRetryOrFailed(OutboxMessage message, Exception ex) {
                int nextRetryCount = message.retryCount() + 1;
                if (nextRetryCount >= properties.maxRetries()) {
                    outboxJdbcRepository.markFailed(message.id(), nextRetryCount);
                    log.error("Outbox message marked FAILED after max retries. eventId={}", message.eventId(), ex);
                    return;
                }
                Duration backoff = calculateBackoff(nextRetryCount);
                outboxJdbcRepository.markRetry(message.id(), nextRetryCount, backoff);
                log.warn("Outbox message marked RETRY. eventId={}, retryCount={}", message.eventId(), nextRetryCount, ex);
    }

    private Duration calculateBackoff(int retryCount) {
        long base = Math.max(properties.baseBackoffMs(), 500L);
        long multiplier = 1L << Math.min(retryCount - 1, 6);
        return Duration.ofMillis(base * multiplier);
    }
}
