package com.example.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.example.kafka.avro.UserEvent;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
@Service
public class MyConsumer {



    @KafkaListener(topics = "user-updates-topic", groupId = "avro-consumer-group")
    public void consume(ConsumerRecord<String, UserEvent> record) {
       UserEvent userEvent = record.value();
       log.info("Successfully consumed Avro record {} {} {} {}", userEvent.getUserId(), userEvent.getFullName(), userEvent.getEmail(), userEvent.getAge());
    }

}
