package com.example.kafka;

import java.util.Random;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@SpringBootApplication
@RequiredArgsConstructor
@Slf4j
public class SpringApacheKafkaBatchingDemo implements CommandLineRunner {

	private final MyProducer myProducer;

	private static final String TOPIC_NAME = "mytopic";
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

		String[] names = new String[] { "Elias", "Rhea", "Edge", "Cena", "Undertaker" };
		String[] addresses = new String[]{"Bangalore", "New Delhi", "New York", "Paris"};

		// send a demo burst to show linger/ms-based batching clearly
		for (int i = 0; i < 50000; i++) {
			Student student = Student.builder()
					.name(names[random.nextInt(names.length)])
					.id(i)
					.address(addresses[random.nextInt(addresses.length)])
					.build();

			myProducer.sendMessage(TOPIC_NAME, student);
			// Thread.sleep(1000);
		}
		log.info("Sent demo burst of messages to show producer batching.");
	}
}
