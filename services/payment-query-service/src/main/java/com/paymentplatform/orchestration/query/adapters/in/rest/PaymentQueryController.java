package com.paymentplatform.orchestration.query.adapters.in.rest;

import com.paymentplatform.orchestration.query.adapters.out.postgres.PaymentOverview;
import com.paymentplatform.orchestration.query.adapters.out.postgres.PaymentProjectionRepository;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/v1/payments")
public class PaymentQueryController {

    private final PaymentProjectionRepository paymentProjectionRepository;

    public PaymentQueryController(PaymentProjectionRepository paymentProjectionRepository) {
        this.paymentProjectionRepository = paymentProjectionRepository;
    }

    @GetMapping("/{paymentId}")
    public ResponseEntity<PaymentOverviewResponse> getById(@PathVariable String paymentId) {
        return paymentProjectionRepository.findByPaymentId(paymentId)
                .map(this::toResponse)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    private PaymentOverviewResponse toResponse(PaymentOverview overview) {
        return new PaymentOverviewResponse(
                overview.paymentId(),
                overview.customerId(),
                overview.amount(),
                overview.currency(),
                overview.status(),
                overview.createdAt(),
                overview.updatedAt()
        );
    }
}
