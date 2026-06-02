package com.message_broker.kafka_producer.config;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaTopicConfig {
    @Value("${kafka.brokerAddress}")
    private String brokerAddress;

    @Value("${kafka.topics.user.name}")
    private String userTopicName;

    @Value("${kafka.topics.company.name}")
    private String companyTopicName;

    @Value("${kafka.topics.message.name}")
    private String messageTopicName;

    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerAddress);
        return new KafkaAdmin(configs);
    }

    @Bean
    public NewTopic userTopic() {
        return new NewTopic(userTopicName, 4, (short) 3)
                .configs(Map.of(
                        "retention.ms", "604800000",
                        "retention.bytes", "1073741824",
                        "cleanup.policy", "compact,delete",
                        "min.insync.replicas", "2",
                        "unclean.leader.election.enable", "false"
                ));
    }

    @Bean
    public NewTopic companyTopic() {
        return new NewTopic(companyTopicName, 4, (short) 3);
    }

    @Bean
    public NewTopic messageTopic() {
        return new NewTopic(messageTopicName, 1, (short) 3);
    }

    // DLQ topics
    @Bean
    public NewTopic userDlqTopic() {
        return new NewTopic(userTopicName + ".DLT", 4, (short) 3)
                .configs(Map.of("retention.ms", "259200000"));
    }

    @Bean
    public NewTopic companyDlqTopic() {
        return new NewTopic(companyTopicName + ".DLT", 4, (short) 3);
    }
}
