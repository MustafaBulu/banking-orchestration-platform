package com.paymentplatform.orchestration.events.schema;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public final class JsonSchemaCompatibility {

    public static List<String> backwardCompatibilityErrors(JsonNode baseline, JsonNode candidate) {
        List<String> errors = new ArrayList<>();
        compare("", baseline, candidate, errors);
        return errors;
    }

    private static void compare(String path, JsonNode baseline, JsonNode candidate, List<String> errors) {
        requireSameScalar(path, "type", baseline, candidate, errors);
        requireSameScalar(path, "const", baseline, candidate, errors);
        requireSameScalar(path, "format", baseline, candidate, errors);
        requireSameScalar(path, "pattern", baseline, candidate, errors);
        requireSameScalar(path, "additionalProperties", baseline, candidate, errors);
        requireRequiredFields(path, baseline, candidate, errors);
        requireProperties(path, baseline, candidate, errors);
    }

    private static void requireSameScalar(
            String path,
            String field,
            JsonNode baseline,
            JsonNode candidate,
            List<String> errors
    ) {
        if (!baseline.has(field)) {
            return;
        }
        if (!candidate.has(field)) {
            errors.add(path(path, field) + " was removed");
            return;
        }
        if (!baseline.get(field).equals(candidate.get(field))) {
            errors.add(path(path, field) + " changed from " + baseline.get(field) + " to " + candidate.get(field));
        }
    }

    private static void requireRequiredFields(
            String path,
            JsonNode baseline,
            JsonNode candidate,
            List<String> errors
    ) {
        JsonNode baselineRequired = baseline.get("required");
        if (baselineRequired == null || !baselineRequired.isArray()) {
            return;
        }
        JsonNode candidateRequired = candidate.get("required");
        if (candidateRequired == null || !candidateRequired.isArray()) {
            errors.add(path(path, "required") + " was removed");
            return;
        }
        for (JsonNode requiredField : baselineRequired) {
            if (!contains(candidateRequired, requiredField.asText())) {
                errors.add(path(path, "required") + " no longer requires " + requiredField.asText());
            }
        }
    }

    private static void requireProperties(
            String path,
            JsonNode baseline,
            JsonNode candidate,
            List<String> errors
    ) {
        JsonNode baselineProperties = baseline.get("properties");
        if (baselineProperties == null || !baselineProperties.isObject()) {
            return;
        }
        JsonNode candidateProperties = candidate.get("properties");
        if (candidateProperties == null || !candidateProperties.isObject()) {
            errors.add(path(path, "properties") + " was removed");
            return;
        }
        Iterator<String> propertyNames = baselineProperties.fieldNames();
        while (propertyNames.hasNext()) {
            String property = propertyNames.next();
            if (!candidateProperties.has(property)) {
                errors.add(path(path, "properties." + property) + " was removed");
                continue;
            }
            compare(path(path, property), baselineProperties.get(property), candidateProperties.get(property), errors);
        }
    }

    private static boolean contains(JsonNode array, String value) {
        for (JsonNode node : array) {
            if (value.equals(node.asText())) {
                return true;
            }
        }
        return false;
    }

    private static String path(String base, String child) {
        return base.isBlank() ? child : base + "." + child;
    }

    private JsonSchemaCompatibility() {
    }
}
