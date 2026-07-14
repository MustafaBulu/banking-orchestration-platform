package com.paymentplatform.orchestration.command.support;

import com.paymentplatform.orchestration.command.application.port.out.AcquirerPort;
import com.paymentplatform.orchestration.command.domain.model.Money;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

@TestConfiguration
public class DefaultAcquirerTestConfig {

    @Bean
    @Primary
    public AcquirerPort approveAllAcquirerPort() {
        return new ApproveAllAcquirerPort();
    }

    public static final class ApproveAllAcquirerPort implements AcquirerPort {

        @Override
        public AcquirerResult authorize(String paymentId, Money money) {
            return approved();
        }

        @Override
        public AcquirerResult capture(String paymentId, Money money) {
            return approved();
        }

        @Override
        public AcquirerResult voidAuthorization(String paymentId) {
            return approved();
        }

        @Override
        public AcquirerResult refund(String paymentId, Money money) {
            return approved();
        }

        private static AcquirerResult approved() {
            return new AcquirerResult(Outcome.APPROVED, "OK", "acq-test");
        }
    }
}
