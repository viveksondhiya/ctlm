package com.ctlm.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaConfig {

    @Bean
    public NewTopic lcmCommandsTopic() {
        return TopicBuilder.name("lcm-commands")
            .partitions(1)
            .replicas(1)
            .build();
    }

    @Bean
    public NewTopic lcmStateChangesTopic() {
        return TopicBuilder.name("lcm-state-changes")
            .partitions(1)
            .replicas(1)
            .build();
    }
}
