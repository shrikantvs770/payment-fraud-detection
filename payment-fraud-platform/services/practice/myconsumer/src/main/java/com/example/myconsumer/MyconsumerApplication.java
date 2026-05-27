package com.example.myconsumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;

import lombok.extern.slf4j.Slf4j;

@SpringBootApplication
@Slf4j
public class MyconsumerApplication {

	public static void main(String[] args) {
		SpringApplication.run(MyconsumerApplication.class, args);
	}

	@KafkaListener(topics="apnatopic", concurrency="3")
	public void handleEvent(String eventData){
		log.info(eventData);
	}

}
