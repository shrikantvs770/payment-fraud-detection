package com.example.myconsumer;

import java.util.Random;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;

import lombok.extern.slf4j.Slf4j;

@SpringBootApplication
@Slf4j
public class MyconsumerApplication {

	public static void main(String[] args) {
		SpringApplication.run(MyconsumerApplication.class, args);
	}

	@KafkaListener(topics="apnatopic")
	public void handleEvent(String eventData, Acknowledgment acknowledgment){
		

		Random random = new Random();
		if(random.nextInt(10) < 8){
			log.error("Simulating failure");
			throw new RuntimeException("Simulated failure");
		}
		// Process
		log.info("consumed {}", eventData);
		// Okay I'm happy now, lets move offset.
		acknowledgment.acknowledge();
	}

}
