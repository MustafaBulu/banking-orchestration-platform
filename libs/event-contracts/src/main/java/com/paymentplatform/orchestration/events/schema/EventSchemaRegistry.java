package com.paymentplatform.orchestration.events.schema;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import com.paymentplatform.orchestration.events.payment.v1.PaymentCreatedEventEnvelope;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class EventSchemaRegistry {

    public static final String PAYMENT_CREATED_SUBJECT = "payment.domain.events.PaymentCreated";

    private static final String PAYMENT_CREATED_SCHEMA =
            "/schemas/payment/domain-events/payment-created-v1.schema.json";

    private final ObjectMapper objectMapper;
    private final Map<EventSchemaKey, JsonSchema> schemas = new HashMap<>();

    public EventSchemaRegistry(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        register(PaymentCreatedEventEnvelope.EVENT_TYPE, PaymentCreatedEventEnvelope.EVENT_VERSION, PAYMENT_CREATED_SCHEMA);
    }

    public void validate(String rawJson) {
        JsonNode payload = readJson(rawJson);
        String eventType = payload.path("eventType").asText(null);
        int eventVersion = payload.path("eventVersion").isInt() ? payload.path("eventVersion").asInt() : -1;
        validate(eventType, eventVersion, payload);
    }

    public void validate(String eventType, int eventVersion, String rawJson) {
        validate(eventType, eventVersion, readJson(rawJson));
    }

    public JsonNode schemaNode(String eventType, int eventVersion) {
        return schema(eventType, eventVersion).getSchemaNode();
    }

    private void validate(String eventType, int eventVersion, JsonNode payload) {
        Set<ValidationMessage> errors = schema(eventType, eventVersion).validate(payload);
        if (!errors.isEmpty()) {
            List<String> violations = errors.stream()
                    .map(ValidationMessage::getMessage)
                    .sorted()
                    .toList();
            throw new EventSchemaValidationException(
                    "Event payload failed schema validation for " + eventType + " v" + eventVersion,
                    violations
            );
        }
    }

    private JsonSchema schema(String eventType, int eventVersion) {
        JsonSchema schema = schemas.get(new EventSchemaKey(eventType, eventVersion));
        if (schema == null) {
            throw new EventSchemaValidationException(
                    "No schema registered for event",
                    List.of("eventType=" + eventType, "eventVersion=" + eventVersion)
            );
        }
        return schema;
    }

    private void register(String eventType, int eventVersion, String resourcePath) {
        schemas.put(new EventSchemaKey(eventType, eventVersion), loadSchema(resourcePath));
    }

    private JsonSchema loadSchema(String resourcePath) {
        try (InputStream stream = EventSchemaRegistry.class.getResourceAsStream(resourcePath)) {
            if (stream == null) {
                throw new IllegalStateException("Missing event schema resource: " + resourcePath);
            }
            JsonSchemaFactory factory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012);
            return factory.getSchema(stream);
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to load event schema: " + resourcePath, ex);
        }
    }

    private JsonNode readJson(String rawJson) {
        try {
            return objectMapper.readTree(rawJson);
        } catch (JsonProcessingException ex) {
            throw new EventSchemaValidationException("Event payload is not valid JSON", List.of(ex.getOriginalMessage()));
        }
    }

    private record EventSchemaKey(String eventType, int eventVersion) {
    }
}
