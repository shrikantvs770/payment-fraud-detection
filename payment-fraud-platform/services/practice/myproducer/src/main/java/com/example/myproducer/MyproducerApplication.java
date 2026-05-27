package com.example.myproducer;

import java.util.Random;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;

import lombok.extern.slf4j.Slf4j;

@SpringBootApplication
@Slf4j
public class MyproducerApplication implements CommandLineRunner {

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	@Bean
	NewTopic createNewtopic() {
		return TopicBuilder.name("apnatopic")
				.partitions(6)
				.build();
	}

	public static void main(String[] args) {
		SpringApplication.run(MyproducerApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {

		String payload = """
				{
				  "eventType":"USER_LOGIN",
				  "service":"auth-service",
				  "region":"ap-south-1",
				  "environment":"production",
				  "message":"User successfully authenticated",
				  "status":"SUCCESS",
				  "metadata":{
				    "browser":"chrome",
				    "device":"macbook",
				    "os":"macos"
				  }
				}
				""";

		// while (true) {

		// kafkaTemplate.send("apnatopic", String.valueOf(Math.random()*100), payload);
		// log.info("sent...");
		// Thread.sleep(500);

String[] users = {
    "user-1", "user-2", "user-3",
    "user-4", "user-5", "user-6",
    "user-7", "user-8", "user-9",
    "user-10"
};

Random random = new Random();

for (int i = 0; i < 500000; i++) {

    String userId = users[random.nextInt(users.length)];

    kafkaTemplate.send(
        "apnatopic",
        userId,
        payload
    );
}

		System.out.println("done");

	}

}
