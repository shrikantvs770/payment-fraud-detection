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



    @KafkaListener(topics = "mytopic", groupId = "my-group-id")
    public void consume(Student student, 
        
        
        @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
        
        
        Acknowledgment acknowledgment) {
        log.info("Consumed student {}", student);


        Random random = new Random();

        if(random.nextInt(10)< 8)
            {
                log.error("Something bad happend, will retry..."); // by default spring boot kafka will retry 9 times and then give up
                throw new RuntimeException("Oops simulated failure");
            }
        
        log.info("topic is {}", topic);
        // Okay I'm happy with the process, lets move offset forward.
        acknowledgment.acknowledge();
    }

}
