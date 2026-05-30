package com.example.kafka;

import java.util.Random;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;

import com.example.kafka.avro.UserEvent;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@SpringBootApplication
@RequiredArgsConstructor
@Slf4j
public class SpringApacheKafkaBatchingDemo implements CommandLineRunner {

	private final MyProducer myProducer;

	private static final String TOPIC_NAME = "user-updates-topic";
	private Random random = new Random();

	@Bean
	NewTopic topic() {
		return TopicBuilder.name(TOPIC_NAME)
				.partitions(6)
				.replicas(1)
				.build();
	}

	public static void main(String[] args) {
		SpringApplication.run(SpringApacheKafkaBatchingDemo.class, args);
	}

	public void run(String... args) throws Exception {

		
			UserEvent event = UserEvent.newBuilder()
                .setUserId(random.nextInt(10)+"")
                .setFullName("elias")
                .setEmail("elias@wwe.com")
                .setAge(45)
                .build();

			myProducer.sendUserEvent(TOPIC_NAME, event);
			// Thread.sleep(1000);
		
		log.info("Event pushed to kafka pipeline!");
	}
}
