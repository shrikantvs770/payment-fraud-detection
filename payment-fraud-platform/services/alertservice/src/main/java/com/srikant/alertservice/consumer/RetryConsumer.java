package com.srikant.alertservice.consumer;

import java.util.Random;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.srikant.alertservice.model.FraudAlert;
import com.srikant.alertservice.producer.RetryProducer;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AllArgsConstructor
@Service
public class RetryConsumer {

    private final RetryProducer retryProducer;
    private final Random random = new Random();

    @KafkaListener(topics = "fraud-alerts-retry", groupId = "fraud-alerts-retry-grp")
    public void consume(FraudAlert fraudAlert) throws Exception {

        try {

            Thread.sleep(3000);

            if (random.nextInt(10) < 8) {
                throw new RuntimeException("Retry processing failed");
            }

            log.info("Retry success :: {} ", fraudAlert);
        } catch (Exception ex) {
            if (fraudAlert.getRetryCount() < 3) {
                // we can retry again brother
                retryProducer.sendToRetry(fraudAlert);// this what causing the loop brother
            } else {
                log.error("Retry Exceeded, Moving message to DLQ", fraudAlert);
                retryProducer.sendToDlq(fraudAlert);
            }
        }

    }

}
