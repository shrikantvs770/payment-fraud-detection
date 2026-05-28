package com.srikant.alertservice.consumer;

import com.srikant.alertservice.model.FraudAlert;
import com.srikant.alertservice.producer.RetryProducer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class AlertConsumer {

    private final Random random = new Random();
    private final RetryProducer retryProducer;
    private final Set<String> processedTransactions = ConcurrentHashMap.newKeySet();

    @KafkaListener(topics = "fraud-alerts", groupId = "alert-service-group", concurrency = "3")
    public void consume(FraudAlert fraudAlert) throws Exception {

        String txId = fraudAlert.getTransactionId();
        if (!processedTransactions.add(txId)) {

            log.warn("DUPLICATE ALERT SKIPPED {}", fraudAlert.getTransactionId());
            return;
        } else {
            Thread.sleep(5000);

            try {
                if (random.nextInt(10) < 8) {
                    throw new RuntimeException("Simulated alert-service failure");
                }
                log.error("FRAUD ALERT RECEIVED {}", fraudAlert);
            } catch (Exception e) {
                processedTransactions.remove(txId); // in the if condition data is added. We remove it because Hey for this transaction exception was thrown.
                log.error("Processing failed: {}", fraudAlert);
                retryProducer.sendToRetry(fraudAlert);
            }
        }

    }
}