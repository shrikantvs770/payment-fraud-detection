package com.example.kafka;

import java.util.Random;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@SpringBootApplication
@RequiredArgsConstructor
@Slf4j
public class SpringKafkaManualAckDemoApplication implements CommandLineRunner {


	private static final String TOPIC_NAME = "mytopic";
	private Random random = new Random();
	private final KafkaTemplate<String, String> kafkaTemplate;

	@Bean
	NewTopic topic() {
		return TopicBuilder.name(TOPIC_NAME)
				.partitions(3)
				.config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2") // So that we can kill one broker man.
				.replicas(3)
				.compact().build();
	}

	public static void main(String[] args) {
		SpringApplication.run(SpringKafkaManualAckDemoApplication.class, args);
	}



	@Override
    public void run(String... args) throws Exception {
        int targetPartition = 0; // Keeping it on one partition to watch order break

        System.out.println("🚀 Starting FAFO Flood: Sending 20 ordered messages...");

        for (int i = 1; i <= 2000; i++) {
            String key = "test-key";
            String value = "Message-" + i;

            // Constructing record targeting partition 0 explicitly
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, targetPartition, key, value);

            // Asynchronously send without waiting (.get()) to saturate the 5 in-flight slots
            kafkaTemplate.send(record).whenComplete((result, ex) -> {
                if (ex != null) {
                    System.err.println("❌ Failed to send: " + value + " due to " + ex.getMessage());
                } else {
                    System.out.println("✅ Sent: " + value + " to offset: " + result.getRecordMetadata().offset());
                }
            });
			Thread.sleep(5000);
        }

        System.out.println("🏁 All messages pushed to the local buffer loop!");
	}
}



