package com.example.kafka;

import java.util.Random;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
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
public class SpringKafkaManualAckDemoApplication implements CommandLineRunner {

	private final MyProducer myProducer;

	private static final String TOPIC_NAME = "mytopic";
	private Random random = new Random();

	@Bean
	NewTopic topic() {
		return TopicBuilder.name(TOPIC_NAME)
				.partitions(3)
				.config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "3") // We have 3 brokers see the docker compose file
				.replicas(3)
				.compact().build();
	}

	public static void main(String[] args) {
		SpringApplication.run(SpringKafkaManualAckDemoApplication.class, args);
	}

	public void run(String... args) throws Exception {

		log.info("started....");
		String[] names = new String[] { "Elias", "Rhea", "Edge", "Cena", "Undertaker" };

		// send 5 student details
		
			Student student = Student.builder()
					.name(names[random.nextInt(5)])
					.id(random.nextInt(10) + random.nextInt(20))
					.build();

					
			log.info("Will send Student {}  ", student);
			myProducer.sendMessage(TOPIC_NAME, student);
	
		

	}
}
