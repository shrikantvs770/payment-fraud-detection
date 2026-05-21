package com.srikant.alertservice.consumer;


import com.srikant.alertservice.model.FraudAlert;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class AlertConsumer {

    @KafkaListener(
            topics = "fraud-alerts",
            groupId = "alert-service-group"
    )
    public void consume(FraudAlert fraudAlert) {

        log.error("🚨🚨 FRAUD ALERT RECEIVED: {}", fraudAlert);
    }
}