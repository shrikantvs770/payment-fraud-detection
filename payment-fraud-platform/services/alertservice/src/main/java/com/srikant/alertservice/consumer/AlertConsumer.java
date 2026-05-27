package com.srikant.alertservice.consumer;

import com.srikant.alertservice.model.FraudAlert;
import com.srikant.alertservice.producer.RetryProducer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Random;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class AlertConsumer {

    private final Random random = new Random();
    private final RetryProducer retryProducer;

    @KafkaListener(topics = "fraud-alerts", groupId = "alert-service-group")
    public void consume(FraudAlert fraudAlert) {

        try {
            if (random.nextInt(10) < 8) {
                throw new RuntimeException("Simulated alert-service failure");
            }
            log.error("FRAUD ALERT RECEIVED {}", fraudAlert);
        } catch (Exception e) {
            log.error("Processing failed: {}", fraudAlert);
            retryProducer.sendToRetry(fraudAlert);
        }
    
    }
}