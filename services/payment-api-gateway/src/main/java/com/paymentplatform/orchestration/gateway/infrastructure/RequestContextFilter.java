package com.paymentplatform.orchestration.gateway.infrastructure;

import com.paymentplatform.orchestration.gateway.config.GatewayProperties;
import org.jspecify.annotations.NonNull;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.UUID;

@Component
public class RequestContextFilter extends OncePerRequestFilter {

    public static final String CORRELATION_ID_HEADER = "X-Correlation-Id";
    private static final Logger log = LoggerFactory.getLogger(RequestContextFilter.class);

    private final RateLimiterService rateLimiterService;
    private final GatewayProperties properties;

    public RequestContextFilter(RateLimiterService rateLimiterService, GatewayProperties properties) {
        this.rateLimiterService = rateLimiterService;
        this.properties = properties;
    }

    @Override
    protected void doFilterInternal(
            @NonNull HttpServletRequest request,
            @NonNull HttpServletResponse response,
            @NonNull FilterChain filterChain
    ) throws ServletException, IOException {
        long start = System.currentTimeMillis();
        String correlationId = request.getHeader(CORRELATION_ID_HEADER);
        if (correlationId == null || correlationId.isBlank()) {
            correlationId = UUID.randomUUID().toString();
        }
        response.setHeader(CORRELATION_ID_HEADER, correlationId);
        request.setAttribute(CORRELATION_ID_HEADER, correlationId);

        String key = request.getRemoteAddr();
        if (!rateLimiterService.allow(key, properties.rateLimitPerMinute())) {
            response.setStatus(429);
            response.setContentType("application/json");
            response.getWriter().write("{\"code\":\"RATE_LIMIT_EXCEEDED\",\"message\":\"Too many requests\"}");
            return;
        }

        try {
            filterChain.doFilter(request, response);
        } finally {
            long duration = System.currentTimeMillis() - start;
            log.info("gateway request method={} path={} status={} durationMs={} correlationId={}",
                    request.getMethod(), request.getRequestURI(), response.getStatus(), duration, correlationId);
        }
    }
}
