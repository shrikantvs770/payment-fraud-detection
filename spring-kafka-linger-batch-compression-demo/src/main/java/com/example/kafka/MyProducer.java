package com.example.kafka;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
@Service
public class MyProducer {

    private final KafkaTemplate<Integer, Student> kafkaTemplate;

    public void sendMessage(String topicName, Integer key, Student student) {
        kafkaTemplate.send(topicName, key, student);
    }
}
