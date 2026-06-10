package com.srikant.kafka.paymentgenerator;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;

@SpringBootApplication
public class PaymentGeneratorApplication {

	public static void main(String[] args) {
		SpringApplication.run(PaymentGeneratorApplication.class, args);
	}

    @Bean
    public NewTopic paymentsTopic() {
        return TopicBuilder.name("payments")
                    .partitions(3)
                    .replicas(3)
                    .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2") // so that we can kill atleast one.
                    .build();
    }

}
