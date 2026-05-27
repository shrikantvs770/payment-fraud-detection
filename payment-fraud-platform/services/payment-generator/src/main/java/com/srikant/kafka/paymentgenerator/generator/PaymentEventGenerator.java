package com.srikant.kafka.paymentgenerator.generator;

import com.srikant.kafka.paymentgenerator.model.PaymentEvent;
import com.srikant.kafka.paymentgenerator.producer.PaymentProducer;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;

import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Random;
import java.util.UUID;
import org.springframework.boot.context.event.ApplicationReadyEvent;

@Component
@RequiredArgsConstructor
public class PaymentEventGenerator {

    private final PaymentProducer paymentProducer;

    private final Random random = new Random();

    private final String[] merchants = {
            "Amazon",
            "Flipkart",
            "Swiggy",
            "Zomato",
            "Uber",
            "Netflix"
    };

    private final String[] countries = {
            "IN",
            "US",
            "UK",
            "SG",
            "DE"
    };

    // @PostConstruct
    @EventListener(ApplicationReadyEvent.class)
    public void startGenerating() {

        new Thread(() -> {

            while (true) {

                PaymentEvent event = PaymentEvent.builder()
                        .transactionId(UUID.randomUUID().toString())
                        .userId("user-" + random.nextInt(1000))
                        .cardId("card-" + random.nextInt(100))
                        .amount(random.nextDouble() * 100000)
                        .merchant(merchants[random.nextInt(merchants.length)])
                        .country(countries[random.nextInt(countries.length)])
                        .timestamp(Instant.now())
                        .build();

                paymentProducer.sendPayment(event);

                try {
                    Thread.sleep(5);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

        }).start();
    }
}