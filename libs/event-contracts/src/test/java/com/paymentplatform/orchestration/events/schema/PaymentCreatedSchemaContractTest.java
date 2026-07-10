package com.paymentplatform.orchestration.events.schema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.paymentplatform.orchestration.events.payment.v1.PaymentCreatedEventEnvelope;
import com.paymentplatform.orchestration.events.payment.v1.PaymentCreatedPayload;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PaymentCreatedSchemaContractTest {

    private final ObjectMapper objectMapper = new ObjectMapper()
            .findAndRegisterModules()
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    private final EventSchemaRegistry registry = new EventSchemaRegistry(objectMapper);

    @Test
    void acceptsCurrentPaymentCreatedEnvelope() throws Exception {
        PaymentCreatedEventEnvelope envelope = new PaymentCreatedEventEnvelope(
                "event-1",
                "payment-1",
                Instant.parse("2026-07-08T10:15:30Z"),
                new PaymentCreatedPayload("customer-1", new BigDecimal("120.50"), "EUR")
        );

        String json = objectMapper.writeValueAsString(envelope);

        registry.validate(json);
    }

    @Test
    void rejectsProducerPayloadThatDoesNotMatchSchema() {
        String invalid = """
                {
                  "eventId": "event-1",
                  "aggregateId": "payment-1",
                  "eventType": "PaymentCreated",
                  "eventVersion": 1,
                  "occurredAt": "2026-07-08T10:15:30Z",
                  "data": {
                    "customerId": "customer-1",
                    "currency": "EUR"
                  }
                }
                """;

        assertThatThrownBy(() -> registry.validate(invalid))
                .isInstanceOf(EventSchemaValidationException.class)
                .hasMessageContaining("$.data")
                .hasMessageContaining("amount");
    }

    @Test
    void currentSchemaRemainsBackwardCompatibleWithV1Baseline() throws Exception {
        JsonNode baseline = read("/schema-baselines/payment-created-v1.schema.json");
        JsonNode current = registry.schemaNode(
                PaymentCreatedEventEnvelope.EVENT_TYPE,
                PaymentCreatedEventEnvelope.EVENT_VERSION
        );

        List<String> errors = JsonSchemaCompatibility.backwardCompatibilityErrors(baseline, current);

        assertThat(errors).isEmpty();
    }

    @Test
    void compatibilityCheckerRejectsRemovedRequiredFields() throws Exception {
        JsonNode baseline = read("/schema-baselines/payment-created-v1.schema.json");
        JsonNode incompatible = objectMapper.readTree("""
                {
                  "type": "object",
                  "additionalProperties": false,
                  "required": ["eventId", "eventType", "eventVersion", "occurredAt", "data"],
                  "properties": {
                    "eventId": {"type": "string"},
                    "eventType": {"const": "PaymentCreated"},
                    "eventVersion": {"const": 1},
                    "occurredAt": {"type": "string", "format": "date-time"},
                    "data": {
                      "type": "object",
                      "additionalProperties": false,
                      "required": ["customerId", "currency"],
                      "properties": {
                        "customerId": {"type": "string"},
                        "currency": {"type": "string", "pattern": "^[A-Z]{3}$"}
                      }
                    }
                  }
                }
                """);

        List<String> errors = JsonSchemaCompatibility.backwardCompatibilityErrors(baseline, incompatible);

        assertThat(errors)
                .anyMatch(error -> error.contains("required no longer requires aggregateId"))
                .anyMatch(error -> error.contains("data.required no longer requires amount"))
                .anyMatch(error -> error.contains("data.properties.amount was removed"));
    }

    private JsonNode read(String resourcePath) throws Exception {
        try (var stream = PaymentCreatedSchemaContractTest.class.getResourceAsStream(resourcePath)) {
            if (stream == null) {
                throw new IllegalStateException("Missing test resource: " + resourcePath);
            }
            return objectMapper.readTree(stream);
        }
    }
}
