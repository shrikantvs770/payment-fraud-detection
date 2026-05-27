package com.srikant.frauddetector.consumer;

import com.srikant.frauddetector.model.FraudAlert;
import com.srikant.frauddetector.model.PaymentEvent;
import com.srikant.frauddetector.producer.FraudAlertProducer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class FraudDetectorConsumer {

    private final FraudAlertProducer fraudAlertProducer;

    @KafkaListener(topics = "payments", groupId = "fraud-detector-group", concurrency = "3")
    public void consume(PaymentEvent paymentEvent) throws Exception {

        // log.info("Checking payment: {}", paymentEvent);
        
        if (paymentEvent.getAmount() > 90000) {
            FraudAlert fraudAlert = FraudAlert.builder()
                    .transactionId(paymentEvent.getTransactionId())
                    .cardId(paymentEvent.getCardId())
                    .amount(paymentEvent.getAmount())
                    .reason("HIGH AMOUNT")
                    .detectedAt(java.time.Instant.now())
                    .retryCount(0)
                    .build();

            fraudAlertProducer.sendFraudAlert(fraudAlert);

        }

    }
}