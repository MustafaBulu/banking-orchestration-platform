package com.paymentplatform.orchestration.gateway.adapters.in.rest;

import com.paymentplatform.orchestration.common.api.ApiHeaders;
import com.paymentplatform.orchestration.gateway.config.GatewayProperties;
import com.paymentplatform.orchestration.gateway.infrastructure.RequestContextFilter;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;

import java.time.Duration;
import java.util.Map;

@RestController
@RequestMapping("/v1/payments")
public class PaymentGatewayController {

    private final WebClient webClient;
    private final GatewayProperties properties;

    public PaymentGatewayController(WebClient webClient, GatewayProperties properties) {
        this.webClient = webClient;
        this.properties = properties;
    }

    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> createPayment(@RequestBody String body, HttpServletRequest request) {
        String correlationId = (String) request.getAttribute(RequestContextFilter.CORRELATION_ID_HEADER);
        try {
            String responseBody = webClient.post()
                    .uri(properties.commandBaseUrl() + "/v1/payments")
                    .header(RequestContextFilter.CORRELATION_ID_HEADER, correlationId)
                    .headers(headers -> addIdempotencyKeyIfPresent(headers, request.getHeader(ApiHeaders.idempotencyKey())))
                    .contentType(MediaType.APPLICATION_JSON)
                    .bodyValue(body)
                    .retrieve()
                    .bodyToMono(String.class)
                    .timeout(Duration.ofMillis(properties.timeoutMs()))
                    .block();
            return ResponseEntity.accepted().body(responseBody);
        } catch (WebClientResponseException ex) {
            return ResponseEntity.status(ex.getStatusCode()).body(ex.getResponseBodyAsString());
        } catch (Exception ex) {
            return ResponseEntity.status(504).body("{\"code\":\"GATEWAY_TIMEOUT\",\"message\":\"Downstream timeout\"}");
        }
    }

    @GetMapping(value = "/{paymentId}", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> getPayment(@PathVariable String paymentId, HttpServletRequest request) {
        String correlationId = (String) request.getAttribute(RequestContextFilter.CORRELATION_ID_HEADER);
        try {
            String responseBody = webClient.get()
                    .uri(properties.queryBaseUrl() + "/v1/payments/" + paymentId)
                    .header(RequestContextFilter.CORRELATION_ID_HEADER, correlationId)
                    .retrieve()
                    .bodyToMono(String.class)
                    .timeout(Duration.ofMillis(properties.timeoutMs()))
                    .block();
            return ResponseEntity.ok(responseBody);
        } catch (WebClientResponseException ex) {
            return ResponseEntity.status(ex.getStatusCode()).body(ex.getResponseBodyAsString());
        } catch (Exception ex) {
            return ResponseEntity.status(504).body("{\"code\":\"GATEWAY_TIMEOUT\",\"message\":\"Downstream timeout\"}");
        }
    }

    @GetMapping("/_health")
    public Map<String, String> health() {
        return Map.of("status", "UP");
    }

    private void addIdempotencyKeyIfPresent(org.springframework.http.HttpHeaders headers, String value) {
        if (value != null && !value.isBlank()) {
            headers.set(ApiHeaders.idempotencyKey(), value);
        }
    }
}
