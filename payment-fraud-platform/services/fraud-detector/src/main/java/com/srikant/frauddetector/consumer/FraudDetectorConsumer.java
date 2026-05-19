package com.srikant.frauddetector.consumer;

import com.srikant.frauddetector.model.PaymentEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class FraudDetectorConsumer {

    @KafkaListener(topics = "payments", groupId = "fraud-detector-group")
    public void consume(PaymentEvent paymentEvent) throws Exception{

        log.info("FRAUD CHECKING PAYMENT: {}", paymentEvent);
        Thread.sleep(1000);
    }
}