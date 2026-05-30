package com.example.kafka;

import java.util.Random;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
@Service
public class MyConsumer {



    @KafkaListener(topics = "mytopic", groupId = "my-group-id-x")
    public void consume(Student student) {
        log.info("Consumed student {}", student);
    }

}
