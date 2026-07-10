package com.paymentplatform.orchestration.ledger.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.paymentplatform.orchestration.events.schema.EventSchemaRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class JacksonConfig {

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper()
                .findAndRegisterModules()
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    @Bean
    public EventSchemaRegistry eventSchemaRegistry(ObjectMapper objectMapper) {
        return new EventSchemaRegistry(objectMapper);
    }
}
