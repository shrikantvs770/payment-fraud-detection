package com.srikant.frauddetector;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin.NewTopics;

@Configuration
public class KafkaConfig {

        @Bean
        public NewTopics topics() {
                return new NewTopics(
                                TopicBuilder.name("fraud-alerts")
                                                .partitions(3)
                                                .replicas(1)
                                                .build());
        }

}