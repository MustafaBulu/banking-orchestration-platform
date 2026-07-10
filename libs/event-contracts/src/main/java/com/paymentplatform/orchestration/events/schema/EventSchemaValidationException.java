package com.paymentplatform.orchestration.events.schema;

import java.util.List;

public class EventSchemaValidationException extends RuntimeException {

    private final List<String> violations;

    public EventSchemaValidationException(String message, List<String> violations) {
        super(message + ": " + String.join("; ", violations));
        this.violations = List.copyOf(violations);
    }

    public List<String> violations() {
        return violations;
    }
}
