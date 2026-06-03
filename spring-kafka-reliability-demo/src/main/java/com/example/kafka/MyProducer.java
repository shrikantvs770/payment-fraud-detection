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
    public void sendMessage(String topicName, Student student){

        kafkaTemplate.send(topicName, student.getId(), student);

        // log.info("Student {} detail sent suceessfully ", student.getId());
    }
}
