package com.example.kafka;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.example.kafka.avro.UserEvent;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
@Service
public class MyProducer {

    private final KafkaTemplate<String, UserEvent> kafkaTemplate;


    public void sendUserEvent(String topic, UserEvent event) {
        kafkaTemplate.send(topic, event.getUserId(), event);
        log.info("Successfully produced event for User: {}", event.getUserId());
    }

}
