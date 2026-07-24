package com.ctlm.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaConfig {

    /** Individual CNF LCM commands (instantiate, scale, terminate…) */
    @Bean
    public NewTopic lcmCommandsTopic() {
        return TopicBuilder.name("lcm-commands").partitions(3).replicas(1).build();
    }

    /** State change events (internal) */
    @Bean
    public NewTopic lcmStateChangesTopic() {
        return TopicBuilder.name("lcm-state-changes").partitions(3).replicas(1).build();
    }

    /**
     * Site Manager → CTLM: NS create trigger.
     * NsCreateConsumer listens here.
     */
    @Bean
    public NewTopic lcmNsCreateTriggerTopic() {
        return TopicBuilder.name("lcm-ns-create-trigger").partitions(3).replicas(1).build();
    }

    /**
     * CTLM → Site Manager: NS instantiation result callback.
     * Published when all CNFs reach RUNNING or any CNF FAILS.
     */
    @Bean
    public NewTopic lcmNsCallbackTopic() {
        return TopicBuilder.name("lcm-ns-callback").partitions(3).replicas(1).build();
    }
}
