package com.srikant.kafka.paymentgenerator.producer;

import com.srikant.kafka.paymentgenerator.model.PaymentEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class PaymentProducer {

    private final KafkaTemplate<String, PaymentEvent> kafkaTemplate;

    public void sendPayment(PaymentEvent paymentEvent) {

        kafkaTemplate.send(
                "payments",
                paymentEvent.getCardId(),
                paymentEvent);

        log.info("Payment sent: {}", paymentEvent);
    }
}