package com.example.kafka;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
@Service
public class MyConsumer {

    @KafkaListener(topics = "mytopic", groupId = "my-group-id")
    public void consume(Student student, Acknowledgment acknowledgment) {
        log.info("Consumed student {}", student);
        acknowledgment.acknowledge();
    }

}
