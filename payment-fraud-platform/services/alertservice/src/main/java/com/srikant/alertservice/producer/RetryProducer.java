package com.srikant.alertservice.producer;

import com.srikant.alertservice.config.KafkaConfig;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.srikant.alertservice.model.FraudAlert;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
public class RetryProducer {


    private final KafkaTemplate<String, FraudAlert> kafkaTemplate;

    public void sendToRetry(FraudAlert fraudAlert) {
        fraudAlert.setRetryCount(fraudAlert.getRetryCount()+1);
        kafkaTemplate.send("fraud-alerts-retry", fraudAlert.getCardId(), fraudAlert);
        log.warn("Sent to RETRY topic {}", fraudAlert);
    }

    public void sendToDlq(FraudAlert fraudAlert){
        kafkaTemplate.send("fraud-alerts-dlq", fraudAlert.getCardId(), fraudAlert);
        log.error("Sent to DLQ {}", fraudAlert);
    }

}
