package com.srikant.frauddetector.producer;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.srikant.frauddetector.model.FraudAlert;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
public class FraudAlertProducer {

    private final KafkaTemplate<String, FraudAlert> kafkaTemplate;

    public void sendFraudAlert(FraudAlert fraudAlert) {
        kafkaTemplate.send("fraud-alerts", fraudAlert.getCardId(), fraudAlert);
        log.warn("FRAUD ALERT SENT :{}", fraudAlert);
    }

}
